// Package phy provides a transport sending and receiving MeshCore packets over
// a LoRa radio attached to the host, e.g. a SX1262 on a Raspberry Pi HAT or
// behind a USB-to-SPI bridge.
//
// The radio is set up by the caller and configured by the transport, for a
// SX126x:
//
//	dev, err := sx126x.NewSPI(conn, irq, busy, rst, &sx126x.Opts{...})
//	dev.Reset()
//	tr := phy.New(phy.Config{
//		Radio:     dev,
//		Configure: phy.ConfigureSX126x(dev),
//		// MeshCore EU/UK narrow
//		Params:    phy.RadioParams{Frequency: 869618000, Bandwidth: 62500, SpreadingFactor: 8, CodingRate: 8, TxPower: 22},
//		DutyCycle: 0.1,
//	})
//
// Unlike the other transports, a LoRa radio needs time to transmit and is
// subject to duty cycle limits. SendPacket therefore queues the packet and
// returns immediately; the packets are transmitted one after the other in the
// background. Like the firmware's Dispatcher, the transport keeps an airtime
// budget to comply with the duty cycle, and forwarded flood packets are delayed
// randomly so repeaters hearing the same packet don't transmit at the same
// time.
package phy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/kabili207/meshcore-go/core/codec"
	"github.com/kabili207/meshcore-go/transport"
	"periph.io/x/conn/v3/physic"
	"periph.io/x/devices/v3/sx126x"
)

// Compile-time interface checks.
var (
	_ transport.Transport = (*Transport)(nil)
	_ Radio               = (*sx126x.Dev)(nil)
)

const (
	// DefaultDutyCycle matches the firmware's default airtime factor of 1.0.
	DefaultDutyCycle = 0.5
	// DefaultDutyCycleWindow is the firmware's window for the airtime budget.
	DefaultDutyCycleWindow = time.Hour
	// DefaultTxDelayFactor matches the firmware's default tx delay factor
	// ("set txdelay").
	DefaultTxDelayFactor = 0.5
	// DefaultQueueSize is the default number of packets waiting to be sent.
	DefaultQueueSize = 16

	// maxPacketSize is the maximum size of a LoRa packet.
	maxPacketSize = 255
)

// Radio is the LoRa radio used by the transport. *sx126x.Dev implements it.
type Radio interface {
	// Receive receives packets continuously and calls handler with each of
	// them until ctx is done. Transmit can be called meanwhile.
	Receive(ctx context.Context, handler func(sx126x.Packet)) error
	// Transmit sends data and blocks until it has been sent or ctx is done.
	Transmit(ctx context.Context, data []byte) error
	// TimeOnAir returns the time it takes to transmit a packet with the given
	// payload length.
	TimeOnAir(payloadLength int) time.Duration
}

// RadioParams are the LoRa parameters which can be changed, e.g. by the
// companion app. The others are fixed by MeshCore.
type RadioParams struct {
	// Frequency and Bandwidth are in Hz.
	Frequency uint32
	Bandwidth uint32
	// SpreadingFactor is 5 to 12.
	SpreadingFactor uint8
	// CodingRate is 5 to 8 for 4/5 to 4/8.
	CodingRate uint8
	// TxPower is in dBm.
	TxPower int8
}

// Config holds the configuration for a phy transport.
type Config struct {
	// Radio is the LoRa radio.
	Radio Radio
	// Configure, if set, applies the radio parameters. It's called when the
	// transport starts and by SetRadioParams, never while receiving or
	// transmitting. Without it, the radio has to be configured by the caller
	// and the parameters can't be changed. ConfigureSX126x returns it for a
	// SX126x.
	Configure func(RadioParams) error
	// Params are the radio parameters applied by Configure.
	Params RadioParams
	// DutyCycle is the maximum fraction of time spent transmitting, e.g. 0.1
	// for the 10% allowed in the EU 869.4-869.65 MHz band. Like in the
	// firmware, the airtime is budgeted over DutyCycleWindow: the budget starts
	// full and is refilled continuously, so short bursts are possible. The
	// firmware calls this the airtime factor ("set af"), which is
	// 1/DutyCycle - 1. Defaults to 0.5, 1 disables the limit.
	DutyCycle float64
	// DutyCycleWindow is the time the duty cycle is measured over. Defaults to
	// one hour.
	DutyCycleWindow time.Duration
	// TxDelayFactor scales the random delay before transmitting a forwarded
	// flood packet, which is up to 5 * TxDelayFactor times the packet's time
	// on air like in the firmware. Defaults to 0.5, a negative value disables
	// the delay.
	TxDelayFactor float64
	// QueueSize is the maximum number of packets waiting to be sent. Defaults
	// to 16.
	QueueSize int
	// Logger is the logger to use. If nil, slog.Default() is used.
	Logger *slog.Logger
}

// ConfigureSX126x returns a Config.Configure function for a SX126x, setting
// MeshCore's fixed parameters along with the given ones.
func ConfigureSX126x(dev *sx126x.Dev) func(RadioParams) error {
	return func(p RadioParams) error {
		return dev.ConfigureLoRa(&sx126x.LoRaConfig{
			Frequency:           physic.Frequency(p.Frequency) * physic.Hertz,
			SpreadingFactor:     p.SpreadingFactor,
			Bandwidth:           physic.Frequency(p.Bandwidth) * physic.Hertz,
			CodingRate:          p.CodingRate,
			LowDataRateOptimize: LowDataRateOptimize(p),
			TxPower:             p.TxPower,
			PreambleLength:      PreambleLength(p.SpreadingFactor),
			MaxPayloadLength:    maxPacketSize,
			CrcEnabled:          true,
			SyncWord:            sx126x.SyncWordPrivate,
		})
	}
}

// PreambleLength returns the preamble length in symbols the firmware uses for
// the spreading factor; a longer preamble improves reliability for low ones.
// Firmware: preambleLengthForSF().
func PreambleLength(spreadingFactor uint8) uint16 {
	if spreadingFactor <= 8 {
		return 32
	}
	return 16
}

// LowDataRateOptimize returns whether low data rate optimization is needed,
// which is the case for symbols of 16 ms or longer like in RadioLib.
func LowDataRateOptimize(p RadioParams) bool {
	return uint64(1)<<p.SpreadingFactor*1000 >= 16*uint64(p.Bandwidth)
}

// Stats holds radio statistics, similar to what the firmware reports.
type Stats struct {
	// TxAirtime and RxAirtime are the total time spent transmitting and
	// receiving packets.
	TxAirtime time.Duration
	RxAirtime time.Duration
	// PacketsSent and PacketsReceived count the packets transmitted and
	// received, the latter including packets which couldn't be parsed.
	PacketsSent     uint64
	PacketsReceived uint64
	// LastRSSI and LastSNR are the RSSI in dBm and SNR in dB of the last
	// packet received.
	LastRSSI float32
	LastSNR  float32
}

// Transport implements transport.Transport over a LoRa radio.
type Transport struct {
	cfg           Config
	log           *slog.Logger
	mu            sync.RWMutex
	connected     bool
	cancel        context.CancelFunc
	done          chan struct{}
	packetHandler transport.PacketHandler
	stateHandler  transport.StateHandler

	// queue holds the packets waiting to be sent, guarded by mu. queued
	// wakes up the send loop when a packet is added.
	queue  []txEntry
	queued chan struct{}

	// params and dutyCycle are the current settings, guarded by mu.
	params    RadioParams
	dutyCycle float64

	// radioMu is held while transmitting or configuring the radio. pause asks
	// the receive loop to pause receiving for configuring.
	radioMu sync.Mutex
	pause   chan pauseRequest

	stats Stats // guarded by mu
}

// txEntry is a packet waiting to be sent.
type txEntry struct {
	data      []byte
	notBefore time.Time
}

// pauseRequest asks the receive loop to pause receiving. It closes paused when
// it has and waits for resume to be closed.
type pauseRequest struct {
	paused chan struct{}
	resume chan struct{}
}

// New creates a new phy transport with the given configuration.
func New(cfg Config) *Transport {
	if cfg.DutyCycle <= 0 {
		cfg.DutyCycle = DefaultDutyCycle
	}
	cfg.DutyCycle = min(cfg.DutyCycle, 1)
	if cfg.DutyCycleWindow <= 0 {
		cfg.DutyCycleWindow = DefaultDutyCycleWindow
	}
	if cfg.TxDelayFactor == 0 {
		cfg.TxDelayFactor = DefaultTxDelayFactor
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Transport{
		cfg:       cfg,
		log:       cfg.Logger.WithGroup("phy"),
		queued:    make(chan struct{}, 1),
		params:    cfg.Params,
		dutyCycle: cfg.DutyCycle,
		pause:     make(chan pauseRequest),
	}
}

// logger returns the configured logger, falling back to the default. A
// zero-value Transport has no logger, so the receive path must not assume New
// was used to build it.
func (t *Transport) logger() *slog.Logger {
	if t.log == nil {
		return slog.Default()
	}
	return t.log
}

// Start configures the radio, if Config.Configure is set, and begins receiving
// and sending packets.
func (t *Transport) Start(ctx context.Context) error {
	if t.cfg.Radio == nil {
		return errors.New("radio is required")
	}
	if t.cfg.Configure != nil {
		t.radioMu.Lock()
		err := t.cfg.Configure(t.RadioParams())
		t.radioMu.Unlock()
		if err != nil {
			return fmt.Errorf("configuring radio: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	t.mu.Lock()
	t.connected = true
	t.cancel = cancel
	t.done = done
	t.queue = nil
	handler := t.stateHandler
	t.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		t.receiveLoop(ctx, cancel)
	}()
	go func() {
		defer wg.Done()
		t.sendLoop(ctx)
	}()
	go func() {
		wg.Wait()
		close(done)
	}()

	t.logger().Info("radio started", "params", t.RadioParams(), "duty_cycle", t.DutyCycle())

	if handler != nil {
		handler(t, transport.EventConnected)
	}

	return nil
}

// Stop stops receiving and sending packets. Packets waiting to be sent are
// dropped.
func (t *Transport) Stop() error {
	t.mu.Lock()
	handler := t.stateHandler
	cancel := t.cancel
	done := t.done
	t.connected = false
	t.cancel = nil
	t.done = nil
	t.queue = nil
	t.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	// Wait for the loops to finish
	if done != nil {
		<-done
	}

	if handler != nil {
		handler(t, transport.EventDisconnected)
	}

	return nil
}

// IsConnected returns true if the radio is running.
func (t *Transport) IsConnected() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.connected
}

// SetPacketHandler sets the callback for incoming MeshCore packets.
func (t *Transport) SetPacketHandler(fn transport.PacketHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.packetHandler = fn
}

// SetStateHandler sets the callback for transport state changes.
func (t *Transport) SetStateHandler(fn transport.StateHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stateHandler = fn
}

// Stats returns the radio statistics.
func (t *Transport) Stats() Stats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.stats
}

// RadioParams returns the current radio parameters.
func (t *Transport) RadioParams() RadioParams {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.params
}

// SetRadioParams applies new radio parameters using Config.Configure. Receiving
// is paused meanwhile and transmissions wait. If the parameters can't be
// applied, the previous ones are restored.
func (t *Transport) SetRadioParams(p RadioParams) error {
	if t.cfg.Configure == nil {
		return errors.New("radio parameters can't be changed without Configure")
	}
	old := t.RadioParams()
	err := t.withRadioPaused(func() error {
		err := t.cfg.Configure(p)
		if err != nil {
			if rerr := t.cfg.Configure(old); rerr != nil {
				t.logger().Error("failed to restore radio parameters", "error", rerr)
			}
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("configuring radio: %w", err)
	}

	t.mu.Lock()
	t.params = p
	t.mu.Unlock()
	t.logger().Info("radio parameters changed", "params", p)
	return nil
}

// DutyCycle returns the current duty cycle.
func (t *Transport) DutyCycle() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.dutyCycle
}

// SetDutyCycle changes the duty cycle, see Config.DutyCycle. Values outside of
// (0, 1] are clamped.
func (t *Transport) SetDutyCycle(dutyCycle float64) {
	dutyCycle = min(max(dutyCycle, 0.001), 1)
	t.mu.Lock()
	t.dutyCycle = dutyCycle
	t.mu.Unlock()
	// Let the send loop recalculate the budget.
	select {
	case t.queued <- struct{}{}:
	default:
	}
	t.logger().Info("duty cycle changed", "duty_cycle", dutyCycle)
}

// withRadioPaused calls fn while the radio is neither receiving nor
// transmitting.
func (t *Transport) withRadioPaused(fn func() error) error {
	t.radioMu.Lock()
	defer t.radioMu.Unlock()

	t.mu.RLock()
	done := t.done
	t.mu.RUnlock()
	if done == nil {
		return fn() // not running
	}

	req := pauseRequest{paused: make(chan struct{}), resume: make(chan struct{})}
	select {
	case t.pause <- req:
	case <-done:
		return fn()
	}
	defer close(req.resume)
	<-req.paused
	return fn()
}

// SendPacket queues a MeshCore packet for transmission and returns without
// waiting for it to be sent. Transmission errors are logged.
func (t *Transport) SendPacket(packet *codec.Packet) error {
	data := packet.WriteTo()
	if len(data) > maxPacketSize {
		return fmt.Errorf("packet of %d bytes exceeds the maximum of %d", len(data), maxPacketSize)
	}
	entry := txEntry{
		data:      data,
		notBefore: time.Now().Add(t.txDelay(packet, len(data))),
	}

	t.mu.Lock()
	if !t.connected {
		t.mu.Unlock()
		return errors.New("not connected")
	}
	if len(t.queue) >= t.cfg.QueueSize {
		t.mu.Unlock()
		return errors.New("transmit queue full")
	}
	t.queue = append(t.queue, entry)
	t.mu.Unlock()

	select {
	case t.queued <- struct{}{}:
	default:
	}
	return nil
}

// txDelay returns the random delay before transmitting a forwarded flood
// packet, so repeaters hearing the same packet don't transmit it at the same
// time. Firmware: MyMesh::getRetransmitDelay().
func (t *Transport) txDelay(packet *codec.Packet, size int) time.Duration {
	if t.cfg.TxDelayFactor < 0 {
		return 0
	}
	switch packet.RouteType() {
	case codec.RouteTypeFlood, codec.RouteTypeTransportFlood:
	default:
		return 0
	}
	// Packets originated by this node have an empty path.
	if packet.PathInfo().HopCount == 0 {
		return 0
	}
	limit := time.Duration(5 * t.cfg.TxDelayFactor * float64(t.cfg.Radio.TimeOnAir(size)))
	return rand.N(limit + 1)
}

// receiveLoop receives packets until ctx is done or the radio fails. It pauses
// receiving on request.
func (t *Transport) receiveLoop(ctx context.Context, cancel context.CancelFunc) {
	for {
		rctx, rcancel := context.WithCancel(ctx)
		received := make(chan error, 1)
		go func() {
			received <- t.cfg.Radio.Receive(rctx, t.handleRx)
		}()

		select {
		case err := <-received:
			rcancel()
			if ctx.Err() != nil {
				return // context cancelled, clean shutdown
			}
			// The radio failed, stop sending too.
			cancel()
			t.handleDisconnect(err)
			return
		case req := <-t.pause:
			rcancel()
			<-received
			close(req.paused)
			select {
			case <-req.resume:
			case <-ctx.Done():
				return
			}
		}
	}
}

// handleRx parses a received packet and dispatches it.
func (t *Transport) handleRx(p sx126x.Packet) {
	t.mu.Lock()
	t.stats.RxAirtime += t.cfg.Radio.TimeOnAir(len(p.Data))
	t.stats.PacketsReceived++
	t.stats.LastRSSI = p.RSSI
	t.stats.LastSNR = p.SNR
	handler := t.packetHandler
	t.mu.Unlock()

	var packet codec.Packet
	if err := packet.ReadFrom(p.Data); err != nil {
		t.logger().Debug("failed to parse MeshCore packet", "error", err, "rssi", p.RSSI, "snr", p.SNR)
		return
	}
	packet.SNR = int8(max(min(math.Round(float64(p.SNR)*4), math.MaxInt8), math.MinInt8))

	t.logger().Debug("packet received", "size", len(p.Data), "rssi", p.RSSI, "snr", p.SNR)

	if handler != nil {
		handler(&packet, transport.PacketSourcePHY)
	}
}

// sendLoop transmits the queued packets until ctx is done.
func (t *Transport) sendLoop(ctx context.Context) {
	// The airtime budget is a token bucket like the firmware's Dispatcher. It
	// starts full and is refilled at the rate of the duty cycle.
	budget := time.Duration(float64(t.cfg.DutyCycleWindow) * t.DutyCycle())
	refilled := time.Now()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		dutyCycle := t.DutyCycle()
		now := time.Now()
		budget = min(budget+time.Duration(float64(now.Sub(refilled))*dutyCycle), time.Duration(float64(t.cfg.DutyCycleWindow)*dutyCycle))
		refilled = now

		// Like the firmware, wait until the budget covers half the time on
		// air of the largest packet.
		var nextTx time.Time
		if need := t.cfg.Radio.TimeOnAir(maxPacketSize) / 2; budget < need {
			nextTx = now.Add(time.Duration(float64(need-budget) / dutyCycle))
		}

		data, wait := t.nextPacket(nextTx)
		if data == nil {
			if wait > 0 {
				timer.Reset(wait)
			}
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case <-t.queued:
				// A packet ready earlier might have been added or the duty
				// cycle changed.
			}
			timer.Stop()
			continue
		}

		t.radioMu.Lock()
		toa := t.cfg.Radio.TimeOnAir(len(data))
		// The radio might have to finish receiving a packet first.
		tctx, cancel := context.WithTimeout(ctx, toa*3/2+t.cfg.Radio.TimeOnAir(maxPacketSize)+time.Second)
		err := t.cfg.Radio.Transmit(tctx, data)
		cancel()
		t.radioMu.Unlock()
		if ctx.Err() != nil {
			return
		}
		// Even a failed transmission might have used the time on air.
		budget = max(budget-toa, 0)
		if err != nil {
			t.logger().Warn("failed to transmit packet", "size", len(data), "error", err)
			continue
		}

		t.mu.Lock()
		t.stats.TxAirtime += toa
		t.stats.PacketsSent++
		t.mu.Unlock()

		t.logger().Debug("packet sent", "size", len(data), "airtime", toa, "budget", budget)
	}
}

// nextPacket removes the packet to send next from the queue and returns it, if
// it's due and not before nextTx. Otherwise it returns how long to wait for it,
// 0 meaning the queue is empty.
func (t *Transport) nextPacket(nextTx time.Time) ([]byte, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.queue) == 0 {
		return nil, 0
	}
	// The packet due first, the oldest one among equals.
	i := 0
	for j, e := range t.queue {
		if e.notBefore.Before(t.queue[i].notBefore) {
			i = j
		}
	}
	due := t.queue[i].notBefore
	if nextTx.After(due) {
		due = nextTx
	}
	if wait := time.Until(due); wait > 0 {
		return nil, wait
	}
	data := t.queue[i].data
	t.queue = append(t.queue[:i], t.queue[i+1:]...)
	return data, 0
}

func (t *Transport) handleDisconnect(err error) {
	t.mu.Lock()
	t.connected = false
	t.queue = nil
	handler := t.stateHandler
	t.mu.Unlock()

	if err != nil {
		t.logger().Error("radio failed", "error", err)
	}

	if handler != nil {
		handler(t, transport.EventDisconnected)
	}
}
