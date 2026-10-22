// Package companion serves the MeshCore companion protocol to a host app (a
// phone app, or MeshMonitor via @liamcottle/meshcore.js) over a byte stream. It
// makes a meshcore-go node present itself as a companion device that the app
// connects to and drives: it answers the handshake, streams contacts, relays
// messages, and gateways remote repeater/room admin.
//
// The wire framing and payload codecs live in core/codec/serial. This package
// is the stateful server: it accepts a connection, reads command frames,
// dispatches them, and writes response/push frames. The framing is identical on
// serial and TCP, so ListenAndServe (TCP) and Serve (any stream, e.g. a pty)
// share one dispatch path.
//
// Implemented: the connect handshake, contacts (list/add/remove/import/export),
// device state (time, battery, channels, radio config, stats, auto-add),
// messaging (direct and channel, incoming and outgoing), live contact-update
// pushes, and the remote-admin gateway (login, CLI, status, telemetry). See the
// README for the full command list. Unimplemented commands return RESP_CODE_ERR
// so the app degrades gracefully.
package companion

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kabili207/meshcore-go/core"
	"github.com/kabili207/meshcore-go/core/clock"
	"github.com/kabili207/meshcore-go/core/codec"
	"github.com/kabili207/meshcore-go/core/codec/serial"
	"github.com/kabili207/meshcore-go/core/crypto"
	"github.com/kabili207/meshcore-go/device/contact"
	"github.com/kabili207/meshcore-go/device/event"
)

// Node is the subset of a meshcore-go node the companion server reads. A
// *node.BaseNode satisfies it, so callers pass companionNode.Base().
type Node interface {
	PublicKey() [32]byte
	Contacts() contact.ContactStore
	Clock() *clock.Clock
	// AddChannel registers a group-channel key so the node decrypts incoming
	// messages on it. Returns the channel hash. Called when SET_CHANNEL
	// configures a channel.
	AddChannel(key []byte) uint8
}

// Identity is the static device description the server reports in SELF_INFO and
// DEVICE_INFO. The public key comes from the Node; everything the node does not
// model (radio params, firmware strings) is supplied here.
type Identity struct {
	Name       string
	AdvType    uint8 // advertised role: 1 chat (default), 2 repeater, 3 room
	TxPower    uint8
	MaxTxPower uint8
	Lat, Lon   float64 // degrees; sent as fixed-point x1e6

	RadioFreqMHz float64 // e.g. 915.0
	RadioBWkHz   float64 // e.g. 250
	RadioSF      uint8
	RadioCR      uint8

	// AirtimeFactor is reported in GET_TUNING_PARAMS, e.g. 1.0 (the firmware
	// default) for a 50% duty cycle. Leave it 0 on a node without radio.
	AirtimeFactor float64

	FirmwareVerCode  uint8  // protocol/firmware version code; default 13 (v1.16.0)
	FirmwareVersion  string // default "v1.16.0"
	BuildDate        string // firmware build date; default "6 Jun 2026"
	Manufacturer     string // model/manufacturer string; default "meshcore-go"
	MaxContacts      int    // default 256
	MaxGroupChannels uint8  // default 8

	// BatteryMilliVolts is reported in GET_BATT_AND_STORAGE; default 4200.
	// StorageUsedKB / StorageTotalKB report flash usage (default 0, no storage).
	BatteryMilliVolts uint16
	StorageUsedKB     uint32
	StorageTotalKB    uint32
}

// Config configures a Server.
type Config struct {
	// Node supplies identity, contacts, and clock. Required.
	Node Node
	// Identity describes the device to report to the app.
	Identity Identity

	// Events, if set, is invoked once during NewServer with the server's event
	// handler; wire it to the node's OnEvent so incoming messages are delivered
	// to connected apps. Without it, no incoming messages are queued.
	Events func(handler func(evt any))

	// SendDM, if set, sends a direct text message to a contact. It returns
	// whether the message went via flood (no known direct path) and invokes
	// onAck when the recipient acknowledges. Without it, CMD_SEND_TXT_MSG
	// returns an error.
	SendDM func(ctx context.Context, to core.MeshCoreID, text string, txtType, attempt uint8, onAck func()) (flood bool, err error)

	// SendChannel, if set, sends a text message to a group channel using its
	// resolved 16-byte key (the server maps the channel index to the key).
	// Without it, CMD_SEND_CHANNEL_TXT_MSG returns an error.
	SendChannel func(ctx context.Context, channelKey []byte, text string) error

	// SendLogin, if set, sends a login request to a remote repeater or room
	// server (for remote admin). The login result arrives asynchronously as an
	// event.LoginResponse, which the server pushes as LOGIN_SUCCESS. Without it,
	// CMD_SEND_LOGIN returns an error.
	SendLogin func(ctx context.Context, to core.MeshCoreID, password string) error

	// SendStatus, if set, requests device status from a remote repeater or room
	// server. The reply arrives as an event.StatusResponse, which the server
	// pushes as STATUS_RESPONSE. Without it, CMD_SEND_STATUS_REQ returns an error.
	SendStatus func(ctx context.Context, to core.MeshCoreID) error

	// SendTelemetry, if set, requests telemetry from a remote node and returns
	// the request tag (used as the SENT correlation). The reply arrives as an
	// event.TelemetryResponse, pushed as TELEMETRY_RESPONSE. Without it, a remote
	// CMD_SEND_TELEMETRY_REQ returns an error; a self request still gets a reply.
	SendTelemetry func(ctx context.Context, to core.MeshCoreID) (tag uint32, err error)

	// SendTrace, if set, sends a TRACE along a relay-hash path with the app's tag
	// and auth code. The result arrives as an event.TraceReceived, pushed as
	// TRACE_DATA. Without it, CMD_SEND_TRACE_PATH returns an error.
	SendTrace func(ctx context.Context, tag, authCode uint32, flags uint8, path []byte) error

	// OnRadioParams, if set, applies radio parameters changed by the app with
	// CMD_SET_RADIO_PARAMS: frequency in kHz, bandwidth in Hz, spreading factor
	// and coding rate (5-8). An error rejects the change. Without it, the
	// parameters are only stored and reported back, fine for a node without
	// radio. For a phy transport:
	//
	//	func(freqKHz, bwHz uint32, sf, cr uint8) error {
	//		p := tr.RadioParams()
	//		p.Frequency, p.Bandwidth, p.SpreadingFactor, p.CodingRate = freqKHz*1000, bwHz, sf, cr
	//		return tr.SetRadioParams(p)
	//	}
	OnRadioParams func(freqKHz, bwHz uint32, sf, cr uint8) error

	// OnTxPower, if set, applies the TX power in dBm changed by the app with
	// CMD_SET_RADIO_TX_POWER. An error rejects the change. Without it, the
	// power is only stored and reported back.
	OnTxPower func(dBm int8) error

	// OnTuningParams, if set, applies the tuning parameters changed by the app
	// with CMD_SET_TUNING_PARAMS, both x1000 like on the wire: the RX delay
	// base and the airtime factor. For a phy transport, the duty cycle is
	// 1/(1 + airtimeFactor/1000). An error rejects the change. Without it, the
	// parameters are only stored and reported back.
	OnTuningParams func(rxDelay, airtimeFactor uint32) error

	// SendSelfAdvert, if set, sends this node's advert for CMD_SEND_SELF_ADVERT:
	// flooded if the app asks for it, otherwise zero-hop. Without it,
	// CMD_SEND_SELF_ADVERT returns an error. For a CompanionNode:
	//
	//	func(flood bool) error {
	//		comp.AdvertScheduler().SendNow(flood)
	//		return nil
	//	}
	SendSelfAdvert func(flood bool) error

	// Stats, if set, provides device statistics for GET_STATS (the app polls
	// this). Without it, GET_STATS still answers with battery and uptime, and
	// zeroed packet/radio counters.
	Stats func() Stats

	// ExportSelf, if set, returns this node's own advert as a serialized packet
	// for EXPORT_CONTACT (self), which the app turns into a share URI/QR. Return
	// nil if it cannot be built. Without it, EXPORT_CONTACT of self is
	// unsupported. Exporting a saved contact is never supported (meshcore-go does
	// not store others' advert signatures).
	ExportSelf func() []byte

	// Logger for connection events. Falls back to slog.Default() if nil.
	Logger *slog.Logger
}

// Stats provides device statistics for GET_STATS. Any field the node does not
// track may be left zero; the server fills in battery and uptime itself.
type Stats struct {
	// Packet counters (STATS_TYPE_PACKETS).
	PacketsRecv, PacketsSent          uint32
	SentFlood, SentDirect             uint32
	RecvFlood, RecvDirect, RecvErrors uint32

	// Radio (STATS_TYPE_RADIO); zero on a software node with no RF.
	NoiseFloor           int16
	LastRSSI, LastSNR    int8 // LastSNR is scaled x4 (0.25 dB units)
	TxAirSecs, RxAirSecs uint32

	// Core extras (STATS_TYPE_CORE); battery and uptime are added by the server.
	ErrFlags uint16
	QueueLen uint8
}

// Server serves the companion protocol for one Node over accepted connections.
type Server struct {
	node          Node
	id            Identity
	sendDM        func(ctx context.Context, to core.MeshCoreID, text string, txtType, attempt uint8, onAck func()) (bool, error)
	sendChannel   func(ctx context.Context, channelKey []byte, text string) error
	sendLogin     func(ctx context.Context, to core.MeshCoreID, password string) error
	sendStatus    func(ctx context.Context, to core.MeshCoreID) error
	sendTelemetry func(ctx context.Context, to core.MeshCoreID) (uint32, error)
	sendTrace     func(ctx context.Context, tag, authCode uint32, flags uint8, path []byte) error
	sendAdvert    func(flood bool) error
	stats         func() Stats
	exportSelf    func() []byte
	startTime     time.Time
	log           *slog.Logger

	onRadioParams  func(freqKHz, bwHz uint32, sf, cr uint8) error
	onTxPower      func(dBm int8) error
	onTuningParams func(rxDelay, airtimeFactor uint32) error

	mu       sync.RWMutex   // guards mutable identity fields and the channel table
	name     string         // current advertised name; SET_ADVERT_NAME updates it
	channels []channelEntry // group channels by index; [0] is Public

	// Mutable radio/tuning params reported in SELF_INFO and GET_TUNING_PARAMS.
	// Freq is kHz, Bw is Hz (matching SELF_INFO); rx/airtime are x1000.
	radioFreqKHz  uint32
	radioBwHz     uint32
	radioSF       uint8
	radioCR       uint8
	txPower       uint8
	rxDelay       uint32
	airtimeFactor uint32

	autoaddConfig  uint8 // CMD_SET/GET_AUTOADD_CONFIG
	autoaddMaxHops uint8

	ackCounter atomic.Uint32 // per-send correlation token for SENT/SEND_CONFIRMED

	msgMu    sync.Mutex            // guards queue and sessions
	queue    []queuedMessage       // offline incoming-message queue
	sessions map[*session]struct{} // currently connected apps
}

// channelEntry is one configured group channel. secret is 16 bytes, or empty
// for an unconfigured slot.
type channelEntry struct {
	name   string
	secret []byte
}

// queuedMessage is an incoming message awaiting CMD_SYNC_NEXT_MESSAGE drain. It
// is either a direct message (senderPrefix set) or a channel message (isChannel
// with channelIdx set).
type queuedMessage struct {
	isChannel    bool
	senderPrefix [6]byte
	channelIdx   uint8
	pathLen      uint8
	txtType      uint8
	senderTS     uint32
	snr          int8
	text         string
}

// NewServer builds a Server, filling in Identity defaults. It panics if
// cfg.Node is nil.
func NewServer(cfg Config) *Server {
	if cfg.Node == nil {
		panic("companion: Config.Node is required")
	}
	id := cfg.Identity
	if id.AdvType == 0 {
		id.AdvType = serial.AdvTypeChat
	}
	if id.FirmwareVerCode == 0 {
		id.FirmwareVerCode = serial.CompanionFirmwareVerCode
	}
	if id.FirmwareVersion == "" {
		id.FirmwareVersion = "v1.16.0"
	}
	if id.BuildDate == "" {
		id.BuildDate = "6 Jun 2026"
	}
	if id.Manufacturer == "" {
		id.Manufacturer = "meshcore-go"
	}
	if id.MaxContacts == 0 {
		id.MaxContacts = 256
	}
	if id.MaxGroupChannels == 0 {
		id.MaxGroupChannels = 8
	}
	if id.BatteryMilliVolts == 0 {
		id.BatteryMilliVolts = 4200
	}
	if id.MaxTxPower == 0 {
		id.MaxTxPower = 22
	}
	if id.TxPower == 0 {
		id.TxPower = 20
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	// Channel table indexed by channel index; index 0 is the built-in Public
	// channel, the rest start unconfigured until SET_CHANNEL populates them.
	channels := make([]channelEntry, id.MaxGroupChannels)
	channels[0] = channelEntry{name: "Public", secret: crypto.DefaultChannelKey}

	s := &Server{
		node:          cfg.Node,
		id:            id,
		sendDM:        cfg.SendDM,
		sendChannel:   cfg.SendChannel,
		sendLogin:     cfg.SendLogin,
		sendStatus:    cfg.SendStatus,
		sendTelemetry: cfg.SendTelemetry,
		sendTrace:     cfg.SendTrace,
		sendAdvert:    cfg.SendSelfAdvert,
		stats:         cfg.Stats,
		exportSelf:    cfg.ExportSelf,

		onRadioParams:  cfg.OnRadioParams,
		onTxPower:      cfg.OnTxPower,
		onTuningParams: cfg.OnTuningParams,

		startTime:     time.Now(),
		log:           log.WithGroup("companion"),
		name:          id.Name,
		channels:      channels,
		radioFreqKHz:  uint32(math.Round(id.RadioFreqMHz * 1000)),
		radioBwHz:     uint32(math.Round(id.RadioBWkHz * 1000)),
		radioSF:       id.RadioSF,
		radioCR:       id.RadioCR,
		txPower:       id.TxPower,
		airtimeFactor: uint32(math.Round(id.AirtimeFactor * 1000)),
		sessions:      make(map[*session]struct{}),
	}
	if cfg.Events != nil {
		cfg.Events(s.handleEvent)
	}
	return s
}

// ListenAndServe accepts TCP connections on addr and serves each. MeshMonitor's
// MeshCore backend connects to companion devices over TCP unmodified, so this
// is the primary entry point. It blocks until ctx is cancelled (which closes
// the listener and open connections) or Accept fails.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	s.log.Info("companion server listening", "addr", ln.Addr().String())
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		go func() {
			defer conn.Close()
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				select {
				case <-ctx.Done():
					conn.Close()
				case <-stop:
				}
			}()
			if err := s.Serve(ctx, conn); err != nil && ctx.Err() == nil {
				s.log.Debug("companion connection ended", "error", err)
			}
		}()
	}
}

// Serve runs the companion protocol over rw until the stream closes or ctx is
// cancelled. Use it directly to serve a pty (the serial path) instead of TCP.
func (s *Server) Serve(ctx context.Context, rw io.ReadWriter) error {
	sess := &session{srv: s, w: rw, ctx: ctx}
	s.addSession(sess)
	defer s.removeSession(sess)

	fr := serial.NewFrameReader(rw)
	for {
		marker, payload, err := fr.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		// Ignore device-marked frames from the app side; only commands are valid.
		if marker != serial.FrameAppToNode || len(payload) == 0 {
			continue
		}
		if err := s.dispatch(sess, payload); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

// session is the per-connection state. appTargetVer is negotiated by
// DEVICE_QUERY and selects the incoming-message layout (V3 vs pre-V3).
type session struct {
	srv          *Server
	w            io.Writer
	ctx          context.Context
	mu           sync.Mutex // serializes frame writes (loop replies and async pushes)
	appTargetVer uint8
}

// send frames a payload as a device->app frame and writes it.
func (ss *session) send(payload []byte) error {
	frame, err := serial.EncodeFrame(serial.FrameNodeToApp, payload)
	if err != nil {
		return err
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	_, err = ss.w.Write(frame)
	return err
}

// dispatch routes one command frame to its handler.
func (s *Server) dispatch(ss *session, payload []byte) error {
	switch payload[0] {
	case serial.CmdAppStart:
		if name, err := serial.ParseAppStart(payload); err == nil {
			s.log.Info("app connected", "name", name)
		}
		return ss.send(s.selfInfo())

	case serial.CmdDeviceQuery:
		if ver, err := serial.ParseDeviceQuery(payload); err == nil {
			ss.appTargetVer = ver
		}
		return ss.send(s.deviceInfo())

	case serial.CmdGetContacts:
		return s.sendContacts(ss, payload)

	case serial.CmdAddUpdateContact:
		return s.addUpdateContact(ss, payload)

	case serial.CmdRemoveContact:
		return s.removeContact(ss, payload)

	case serial.CmdResetPath:
		return s.resetPath(ss, payload)

	case serial.CmdGetContactByKey:
		return s.getContactByKey(ss, payload)

	case serial.CmdExportContact:
		return s.exportContact(ss, payload)

	case serial.CmdImportContact:
		return s.importContact(ss, payload)

	case serial.CmdShareContact:
		// Sharing rebroadcasts a saved contact's signed advert, which meshcore-go
		// does not store.
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))

	case serial.CmdSendTxtMsg:
		return s.sendTextMsg(ss, payload)

	case serial.CmdSendChannelTxtMsg:
		return s.sendChannelMsg(ss, payload)

	case serial.CmdSendLogin:
		return s.sendLoginCmd(ss, payload)

	case serial.CmdSendStatusReq:
		return s.sendStatusReq(ss, payload)

	case serial.CmdSendTelemetryReq:
		return s.sendTelemetryReq(ss, payload)

	case serial.CmdSendTracePath:
		return s.sendTracePath(ss, payload)

	case serial.CmdGetChannel:
		return s.getChannel(ss, payload)

	case serial.CmdSetChannel:
		return s.setChannel(ss, payload)

	case serial.CmdGetDeviceTime:
		return ss.send(serial.EncodeCurrTime(s.node.Clock().GetCurrentTime()))

	case serial.CmdSetDeviceTime:
		// A transport-attached node keeps its own (host) clock; acknowledge the
		// app's set without overriding it.
		return ss.send(serial.EncodeOK())

	case serial.CmdGetBattAndStorage:
		return ss.send(serial.EncodeBattAndStorage(
			s.id.BatteryMilliVolts, s.id.StorageUsedKB, s.id.StorageTotalKB))

	case serial.CmdGetDefaultFloodScope:
		// No default flood scope configured; report unset.
		return ss.send(serial.EncodeDefaultFloodScope("", nil))

	case serial.CmdGetStats:
		return s.getStats(ss, payload)

	case serial.CmdSetRadioParams:
		return s.setRadioParams(ss, payload)

	case serial.CmdSetRadioTxPower:
		return s.setTxPower(ss, payload)

	case serial.CmdSetTuningParams:
		return s.setTuningParams(ss, payload)

	case serial.CmdGetTuningParams:
		return s.getTuningParams(ss)

	case serial.CmdGetCustomVars:
		// A software node exposes no sensor settings.
		return ss.send(serial.EncodeCustomVars(""))

	case serial.CmdGetAutoaddConfig:
		return s.getAutoaddConfig(ss)

	case serial.CmdSetAutoaddConfig:
		return s.setAutoaddConfig(ss, payload)

	case serial.CmdGetAdvertPath:
		// No inbound advert-path cache is kept, so no path is known.
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))

	case serial.CmdSyncNextMessage:
		return s.sendNextMessage(ss)

	case serial.CmdSetAdvertName:
		if name, err := serial.ParseSetAdvertName(payload); err == nil {
			s.setName(name)
			s.log.Info("advert name set", "name", name)
		}
		return ss.send(serial.EncodeOK())

	case serial.CmdSendSelfAdvert:
		return s.sendSelfAdvert(ss, payload)

	case serial.CmdSetAdvertLatLon, serial.CmdSetOtherParams,
		serial.CmdSetFloodScopeKey, serial.CmdSetDefaultFloodScope:
		// Device-parameter writes we accept but do not persist on a virtual node.
		return ss.send(serial.EncodeOK())

	default:
		// Log the opcode so a real client's command sequence reveals what to
		// implement next. ErrCodeUnsupportedCmd (1) matches the firmware's
		// UNSUPPORTED_CMD wire value.
		s.log.Debug("unsupported command", "cmd", payload[0])
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
}

// selfInfo builds the SELF_INFO reply from the node's key and the current
// (mutable) identity: name and radio params may have been changed by SET_*
// commands since construction.
func (s *Server) selfInfo() []byte {
	s.mu.RLock()
	si := &serial.SelfInfo{
		AdvType:    s.id.AdvType,
		TxPower:    s.txPower,
		MaxTxPower: s.id.MaxTxPower,
		PublicKey:  s.node.PublicKey(),
		AdvLat:     degToFixed(s.id.Lat),
		AdvLon:     degToFixed(s.id.Lon),
		RadioFreq:  s.radioFreqKHz,
		RadioBw:    s.radioBwHz,
		RadioSf:    s.radioSF,
		RadioCr:    s.radioCR,
		Name:       s.name,
	}
	s.mu.RUnlock()
	return si.Encode()
}

// setName updates the advertised name reported in subsequent SELF_INFO replies.
func (s *Server) setName(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.name = name
}

// getChannel answers CMD_GET_CHANNEL. Configured indices return their name and
// secret; in-range unconfigured indices report empty; out-of-range indices
// return NotFound, matching the firmware.
func (s *Server) getChannel(ss *session, payload []byte) error {
	idx, err := serial.ParseGetChannel(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	s.mu.RLock()
	if int(idx) >= len(s.channels) {
		s.mu.RUnlock()
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	ch := s.channels[idx]
	s.mu.RUnlock()
	return ss.send((&serial.ChannelInfo{Index: idx, Name: ch.name, Secret: ch.secret}).Encode())
}

// setChannel handles CMD_SET_CHANNEL, storing a channel's name and 128-bit key
// and registering the key with the node so incoming messages on it decrypt.
func (s *Server) setChannel(ss *session, payload []byte) error {
	// The firmware supports only 128-bit secrets; a 256-bit frame is rejected.
	if len(payload) >= serial.SetChannel256Len {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	idx, name, secret, err := serial.ParseSetChannel(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	s.mu.Lock()
	if int(idx) >= len(s.channels) {
		s.mu.Unlock()
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	s.channels[idx] = channelEntry{name: name, secret: secret}
	s.mu.Unlock()

	s.node.AddChannel(secret)
	return ss.send(serial.EncodeOK())
}

// channelKey returns the 16-byte key for a channel index, or nil if the index is
// out of range or unconfigured.
func (s *Server) channelKey(idx uint8) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if int(idx) >= len(s.channels) || len(s.channels[idx].secret) == 0 {
		return nil
	}
	return s.channels[idx].secret
}

// channelIndexForHash finds the index of the configured channel whose key hashes
// to hash.
func (s *Server) channelIndexForHash(hash uint8) (uint8, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, ch := range s.channels {
		if len(ch.secret) > 0 && crypto.ComputeChannelHash(ch.secret) == hash {
			return uint8(i), true
		}
	}
	return 0, false
}

// sendTextMsg handles CMD_SEND_TXT_MSG: resolve the recipient, send the message
// through the node, reply RESP_CODE_SENT, and push PUSH_CODE_SEND_CONFIRMED when
// the recipient acknowledges.
func (s *Server) sendTextMsg(ss *session, payload []byte) error {
	if s.sendDM == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	req, err := serial.ParseSendTxtMsg(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	ct := s.resolveContact(req.DestPrefix)
	if ct == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}

	// Remote-admin CLI commands are not ACKed: reply SENT with a zero
	// expected-ack and register no confirmation. The reply returns later as a
	// CONTACT_MSG_RECV with the CLI type, which the app routes as a cli_reply.
	if req.TxtType == serial.TxtTypeCLI {
		flood, err := s.sendDM(ss.ctx, ct.ID, req.Text, req.TxtType, req.Attempt, nil)
		if err != nil {
			s.log.Warn("cli send failed", "to", ct.ID.String(), "error", err)
			return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
		}
		sentType := uint8(serial.SentTypeDirect)
		if flood {
			sentType = serial.SentTypeFlood
		}
		return ss.send(serial.EncodeSent(sentType, 0, 8000))
	}

	// The app correlates its RESP_CODE_SENT.expected_ack with the later
	// PUSH_CODE_SEND_CONFIRMED.ack_code; a server-side token keeps that pairing
	// unique regardless of the on-air ACK CRC.
	token := s.ackCounter.Add(1)
	start := time.Now()
	flood, err := s.sendDM(ss.ctx, ct.ID, req.Text, req.TxtType, req.Attempt, func() {
		rtt := uint32(time.Since(start).Milliseconds())
		_ = ss.send(serial.EncodeSendConfirmed(token, rtt))
	})
	if err != nil {
		s.log.Warn("send failed", "to", ct.ID.String(), "error", err)
		return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
	}

	sentType := uint8(serial.SentTypeDirect)
	estTimeout := uint32(4000)
	if flood {
		sentType = serial.SentTypeFlood
		estTimeout = 8000
	}
	return ss.send(serial.EncodeSent(sentType, token, estTimeout))
}

// sendChannelMsg handles CMD_SEND_CHANNEL_TXT_MSG. Unlike a direct message, the
// firmware replies with RESP_CODE_OK (not RESP_CODE_SENT) and there is no
// delivery confirmation: group messages are unacknowledged broadcasts.
func (s *Server) sendChannelMsg(ss *session, payload []byte) error {
	if s.sendChannel == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	req, err := serial.ParseSendChannelTxtMsg(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	if req.TxtType != 0 { // firmware only accepts TXT_TYPE_PLAIN on a channel
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	key := s.channelKey(req.ChannelIdx)
	if key == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	if err := s.sendChannel(ss.ctx, key, req.Text); err != nil {
		s.log.Warn("channel send failed", "channel", req.ChannelIdx, "error", err)
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	return ss.send(serial.EncodeOK())
}

// resolveContact finds the stored contact whose public key starts with prefix
// (the 6-byte prefix the app sends), or nil if none match.
func (s *Server) resolveContact(prefix []byte) *contact.ContactInfo {
	var found *contact.ContactInfo
	s.node.Contacts().ForEach(func(c *contact.ContactInfo) bool {
		if bytes.HasPrefix(c.ID[:], prefix) {
			found = c
			return false
		}
		return true
	})
	return found
}

// sendNextMessage drains one queued incoming message, encoded for the session's
// negotiated protocol version, or reports the queue empty.
func (s *Server) sendNextMessage(ss *session) error {
	s.msgMu.Lock()
	if len(s.queue) == 0 {
		s.msgMu.Unlock()
		return ss.send(serial.EncodeNoMoreMessages())
	}
	qm := s.queue[0]
	s.queue = s.queue[1:]
	s.msgMu.Unlock()

	v3 := ss.appTargetVer >= 3
	var frame []byte
	if qm.isChannel {
		frame = serial.EncodeChannelMsgRecv(v3, qm.snr, qm.channelIdx, qm.pathLen, qm.txtType, qm.senderTS, qm.text)
	} else {
		frame = serial.EncodeContactMsgRecv(v3, qm.snr, qm.senderPrefix[:], qm.pathLen, qm.txtType, qm.senderTS, qm.text)
	}
	return ss.send(frame)
}

// handleEvent receives node events, queuing incoming messages and pushing live
// contact updates to connected apps.
func (s *Server) handleEvent(evt any) {
	switch e := evt.(type) {
	case *event.TextMessageReceived:
		s.enqueueDM(e)
	case *event.GroupTextReceived:
		s.enqueueChannel(e)
	case *event.AdvertReceived:
		s.pushAdvert(e)
	case *event.LoginResponse:
		s.pushLoginSuccess(e)
	case *event.StatusResponse:
		s.pushStatusResponse(e)
	case *event.TelemetryResponse:
		s.pushTelemetryResponse(e)
	case *event.TraceReceived:
		s.pushTraceData(e)
	}
}

// sendLoginCmd handles CMD_SEND_LOGIN: send a login to a remote server and reply
// SENT. The result arrives later as a LoginResponse event, pushed as
// LOGIN_SUCCESS.
func (s *Server) sendLoginCmd(ss *session, payload []byte) error {
	if s.sendLogin == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	if len(payload) < 1+32 {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	var to core.MeshCoreID
	copy(to[:], payload[1:33])
	if s.node.Contacts().GetByPubKey(to) == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	password := string(payload[33:])
	if err := s.sendLogin(ss.ctx, to, password); err != nil {
		s.log.Warn("login send failed", "to", to.String(), "error", err)
		return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
	}
	// The app correlates LOGIN_SUCCESS by the target's pubkey prefix; the SENT
	// frame carries the first 4 pubkey bytes as its expected-ack (firmware parity).
	return ss.send(serial.EncodeSent(serial.SentTypeFlood, binary.LittleEndian.Uint32(to[:4]), 12000))
}

// pushLoginSuccess emits PUSH_CODE_LOGIN_SUCCESS from a login response event.
func (s *Server) pushLoginSuccess(e *event.LoginResponse) {
	s.pushToSessions(serial.EncodeLoginSuccess(
		e.From[:6], e.IsAdmin, e.ServerTimestamp, e.Permissions, e.FirmwareVerLevel))
}

// sendStatusReq handles CMD_SEND_STATUS_REQ: request status from a remote server
// and reply SENT. The status blob arrives later as a StatusResponse event.
func (s *Server) sendStatusReq(ss *session, payload []byte) error {
	if s.sendStatus == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	if len(payload) < 1+32 {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	var to core.MeshCoreID
	copy(to[:], payload[1:33])
	if s.node.Contacts().GetByPubKey(to) == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	if err := s.sendStatus(ss.ctx, to); err != nil {
		s.log.Warn("status request failed", "to", to.String(), "error", err)
		return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
	}
	return ss.send(serial.EncodeSent(serial.SentTypeFlood, binary.LittleEndian.Uint32(to[:4]), 12000))
}

// pushStatusResponse emits PUSH_CODE_STATUS_RESPONSE from a status response event.
func (s *Server) pushStatusResponse(e *event.StatusResponse) {
	s.pushToSessions(serial.EncodeStatusResponse(e.From[:6], e.Data))
}

// sendTelemetryReq handles CMD_SEND_TELEMETRY_REQ. A self request (len 4) is
// answered immediately (empty, since a software node has no sensors); a remote
// request ([code][3 reserved][pubkey]) sends a request and replies SENT, with
// the telemetry arriving later as a TelemetryResponse event.
func (s *Server) sendTelemetryReq(ss *session, payload []byte) error {
	if len(payload) <= 4 {
		pk := s.node.PublicKey()
		return ss.send(serial.EncodeTelemetryResponse(pk[:6], nil))
	}
	if s.sendTelemetry == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	if len(payload) < 4+32 {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	var to core.MeshCoreID
	copy(to[:], payload[4:36])
	if s.node.Contacts().GetByPubKey(to) == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	tag, err := s.sendTelemetry(ss.ctx, to)
	if err != nil {
		s.log.Warn("telemetry request failed", "to", to.String(), "error", err)
		return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
	}
	return ss.send(serial.EncodeSent(serial.SentTypeFlood, tag, 12000))
}

// pushTelemetryResponse emits PUSH_CODE_TELEMETRY_RESPONSE from a telemetry event.
func (s *Server) pushTelemetryResponse(e *event.TelemetryResponse) {
	s.pushToSessions(serial.EncodeTelemetryResponse(e.From[:6], e.Data))
}

// sendTracePath handles CMD_SEND_TRACE_PATH: send a trace with the app's tag and
// reply SENT. The result arrives later as a TraceReceived event.
func (s *Server) sendTracePath(ss *session, payload []byte) error {
	if s.sendTrace == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	tag, authCode, flags, path, err := serial.ParseSendTracePath(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	if err := s.sendTrace(ss.ctx, tag, authCode, flags, path); err != nil {
		s.log.Warn("trace failed", "error", err)
		return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
	}
	// A trace is sent direct; the app correlates the TRACE_DATA reply by tag.
	return ss.send(serial.EncodeSent(serial.SentTypeDirect, tag, 12000))
}

// pushTraceData emits PUSH_CODE_TRACE_DATA from a completed-trace event.
func (s *Server) pushTraceData(e *event.TraceReceived) {
	var lastSNR int8
	if e.RawPacket != nil {
		lastSNR = e.RawPacket.SNR
	}
	s.pushToSessions(serial.EncodeTraceData(e.Flags, e.Tag, e.AuthCode, e.PathHashes, e.SNRs, lastSNR))
}

// pushAdvert emits NEW_ADVERT for a first-seen contact (the full contact frame)
// or ADVERT for a re-heard one (pubkey only), so the app's contact list updates
// live without a manual refresh.
func (s *Server) pushAdvert(e *event.AdvertReceived) {
	if e.Contact == nil {
		return
	}
	if e.IsNew {
		s.pushToSessions(contactToWire(e.Contact).EncodeWithCode(serial.PushCodeNewAdvert))
	} else {
		s.pushToSessions(serial.EncodeAdvert([32]byte(e.Contact.ID)))
	}
}

// NotifyContactDeleted emits PUSH_CODE_CONTACT_DELETED. Wire it to the contact
// store's overwrite/removal callback (e.g. ContactManager.SetOnContactOverwrite).
func (s *Server) NotifyContactDeleted(id core.MeshCoreID) {
	s.pushToSessions(serial.EncodeContactDeleted([32]byte(id)))
}

// NotifyPathUpdated emits PUSH_CODE_PATH_UPDATED for a contact whose route
// changed. meshcore-go has no dedicated path-changed signal, so wire this from
// your own if you have one.
func (s *Server) NotifyPathUpdated(id core.MeshCoreID) {
	s.pushToSessions(serial.EncodePathUpdated([32]byte(id)))
}

// NotifyContactsFull emits PUSH_CODE_CONTACTS_FULL when the contact table is full.
func (s *Server) NotifyContactsFull() {
	s.pushToSessions(serial.EncodeContactsFull())
}

// enqueueDM queues an incoming direct message and tickles connected apps.
func (s *Server) enqueueDM(e *event.TextMessageReceived) {
	qm := queuedMessage{
		pathLen:  serial.PathLenUnknown, // direct/unknown; refined when packet path is exposed
		txtType:  e.TxtType,
		senderTS: e.Timestamp,
		text:     e.Message,
	}
	copy(qm.senderPrefix[:], e.From[:])
	s.enqueue(qm)
}

// enqueueChannel queues an incoming group message, mapping its channel hash to a
// configured channel index. Messages on unknown channels are dropped.
func (s *Server) enqueueChannel(e *event.GroupTextReceived) {
	idx, ok := s.channelIndexForHash(e.ChannelHash)
	if !ok {
		s.log.Debug("dropping message for unknown channel", "hash", e.ChannelHash)
		return
	}
	s.enqueue(queuedMessage{
		isChannel:  true,
		channelIdx: idx,
		pathLen:    serial.PathLenUnknown,
		txtType:    0, // plain
		senderTS:   s.node.Clock().GetCurrentTime(),
		text:       e.Message,
	})
}

// enqueue appends a message to the offline queue and tickles connected apps.
func (s *Server) enqueue(qm queuedMessage) {
	s.msgMu.Lock()
	s.queue = append(s.queue, qm)
	s.msgMu.Unlock()
	s.pushToSessions(serial.EncodeMsgWaiting())
}

// pushToSessions writes an unsolicited push frame to every connected app. Apps
// that are not connected simply miss it, matching the firmware.
func (s *Server) pushToSessions(payload []byte) {
	s.msgMu.Lock()
	sessions := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.msgMu.Unlock()

	for _, sess := range sessions {
		if err := sess.send(payload); err != nil {
			s.log.Debug("push failed", "error", err)
		}
	}
}

func (s *Server) addSession(sess *session) {
	s.msgMu.Lock()
	s.sessions[sess] = struct{}{}
	s.msgMu.Unlock()
}

func (s *Server) removeSession(sess *session) {
	s.msgMu.Lock()
	delete(s.sessions, sess)
	s.msgMu.Unlock()
}

// deviceInfo builds the DEVICE_INFO reply.
func (s *Server) deviceInfo() []byte {
	di := &serial.DeviceInfo{
		FirmwareVerCode:  s.id.FirmwareVerCode,
		MaxContactsDiv2:  uint8(s.id.MaxContacts / 2),
		MaxGroupChannels: s.id.MaxGroupChannels,
		BuildDate:        s.id.BuildDate,
		Manufacturer:     s.id.Manufacturer,
		FirmwareVersion:  s.id.FirmwareVersion,
	}
	return di.Encode()
}

// sendContacts streams ContactsStart -> Contact* -> EndOfContacts, honoring the
// optional "since" filter (only contacts modified after it).
func (s *Server) sendContacts(ss *session, payload []byte) error {
	since, hasSince := serial.ParseGetContactsSince(payload)

	var matched []*contact.ContactInfo
	var mostRecent uint32
	s.node.Contacts().ForEach(func(c *contact.ContactInfo) bool {
		if c.LastMod > mostRecent {
			mostRecent = c.LastMod
		}
		if hasSince && c.LastMod <= since {
			return true
		}
		matched = append(matched, c)
		return true
	})

	// Count is the number of Contact frames that follow; the client terminates
	// on EndOfContacts regardless (matching real-firmware incremental sync).
	if err := ss.send(serial.EncodeContactsStart(uint32(len(matched)))); err != nil {
		return err
	}
	for _, c := range matched {
		if err := ss.send(contactToWire(c).Encode()); err != nil {
			return err
		}
	}
	return ss.send(serial.EncodeEndOfContacts(mostRecent))
}

// addUpdateContact handles CMD_ADD_UPDATE_CONTACT: update the app-visible fields
// of an existing contact, or add a new one. Replies OK, or TABLE_FULL if a new
// contact cannot be stored.
func (s *Server) addUpdateContact(ss *session, payload []byte) error {
	c, err := serial.ParseContact(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	var id core.MeshCoreID
	copy(id[:], c.PublicKey[:])

	lastMod := c.LastMod
	if lastMod == 0 {
		lastMod = s.node.Clock().GetCurrentTime()
	}

	store := s.node.Contacts()
	if existing := store.GetByPubKey(id); existing != nil {
		existing.Name = c.Name
		existing.Type = c.Type
		existing.Flags = c.Flags
		existing.OutPathLen = c.OutPathLen
		existing.OutPath = c.OutPath
		existing.LastAdvertTimestamp = c.LastAdvert
		existing.GPSLat = c.GPSLat
		existing.GPSLon = c.GPSLon
		existing.LastMod = lastMod
		if err := store.UpdateContact(existing); err != nil {
			return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
		}
		return ss.send(serial.EncodeOK())
	}

	ci := &contact.ContactInfo{
		ID:                  id,
		Name:                c.Name,
		Type:                c.Type,
		Flags:               c.Flags,
		OutPathLen:          c.OutPathLen,
		OutPath:             c.OutPath,
		LastAdvertTimestamp: c.LastAdvert,
		GPSLat:              c.GPSLat,
		GPSLon:              c.GPSLon,
		LastMod:             lastMod,
	}
	if _, err := store.AddContact(ci); err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
	}
	return ss.send(serial.EncodeOK())
}

// removeContact handles CMD_REMOVE_CONTACT.
func (s *Server) removeContact(ss *session, payload []byte) error {
	id, ok := parseContactKey(payload)
	if !ok {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	store := s.node.Contacts()
	if store.GetByPubKey(id) == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	if err := store.RemoveContact(id); err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	return ss.send(serial.EncodeOK())
}

// resetPath handles CMD_RESET_PATH: forget a contact's cached direct path so the
// next message floods.
func (s *Server) resetPath(ss *session, payload []byte) error {
	id, ok := parseContactKey(payload)
	if !ok {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	store := s.node.Contacts()
	ct := store.GetByPubKey(id)
	if ct == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	ct.OutPathLen = serial.PathLenUnknown
	ct.OutPath = nil
	if err := store.UpdateContact(ct); err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	return ss.send(serial.EncodeOK())
}

// getContactByKey handles CMD_GET_CONTACT_BY_KEY.
func (s *Server) getContactByKey(ss *session, payload []byte) error {
	id, ok := parseContactKey(payload)
	if !ok {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	ct := s.node.Contacts().GetByPubKey(id)
	if ct == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	return ss.send(contactToWire(ct).Encode())
}

// sendSelfAdvert handles CMD_SEND_SELF_ADVERT. Like the firmware, an optional
// parameter byte of 1 after the command floods the advert, otherwise it's sent
// zero-hop.
func (s *Server) sendSelfAdvert(ss *session, payload []byte) error {
	if s.sendAdvert == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	flood := len(payload) >= 2 && payload[1] == 1
	if err := s.sendAdvert(flood); err != nil {
		s.log.Warn("failed to send self advert", "flood", flood, "error", err)
		return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
	}
	return ss.send(serial.EncodeOK())
}

// setRadioParams handles CMD_SET_RADIO_PARAMS, validating and storing the radio
// parameters reported in SELF_INFO. It uses the firmware's validation ranges.
func (s *Server) setRadioParams(ss *session, payload []byte) error {
	rp, err := serial.ParseSetRadioParams(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	if rp.Freq < 150000 || rp.Freq > 2500000 || rp.Bw < 7000 || rp.Bw > 500000 ||
		rp.SF < 5 || rp.SF > 12 || rp.CR < 5 || rp.CR > 8 {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	if s.onRadioParams != nil {
		if err := s.onRadioParams(rp.Freq, rp.Bw, rp.SF, rp.CR); err != nil {
			s.log.Warn("failed to apply radio params", "error", err)
			return ss.send(serial.EncodeErr(serial.ErrCodeBadState))
		}
	}
	s.mu.Lock()
	s.radioFreqKHz, s.radioBwHz, s.radioSF, s.radioCR = rp.Freq, rp.Bw, rp.SF, rp.CR
	s.mu.Unlock()
	return ss.send(serial.EncodeOK())
}

// setTxPower handles CMD_SET_RADIO_TX_POWER.
func (s *Server) setTxPower(ss *session, payload []byte) error {
	power, err := serial.ParseSetTxPower(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	if power < -9 || power > int8(s.id.MaxTxPower) {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	if s.onTxPower != nil {
		if err := s.onTxPower(power); err != nil {
			s.log.Warn("failed to apply tx power", "error", err)
			return ss.send(serial.EncodeErr(serial.ErrCodeBadState))
		}
	}
	s.mu.Lock()
	s.txPower = uint8(power)
	s.mu.Unlock()
	return ss.send(serial.EncodeOK())
}

// setTuningParams handles CMD_SET_TUNING_PARAMS.
func (s *Server) setTuningParams(ss *session, payload []byte) error {
	rx, af, err := serial.ParseSetTuningParams(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	if s.onTuningParams != nil {
		if err := s.onTuningParams(rx, af); err != nil {
			s.log.Warn("failed to apply tuning params", "error", err)
			return ss.send(serial.EncodeErr(serial.ErrCodeBadState))
		}
	}
	s.mu.Lock()
	s.rxDelay, s.airtimeFactor = rx, af
	s.mu.Unlock()
	return ss.send(serial.EncodeOK())
}

// getTuningParams handles CMD_GET_TUNING_PARAMS.
func (s *Server) getTuningParams(ss *session) error {
	s.mu.RLock()
	rx, af := s.rxDelay, s.airtimeFactor
	s.mu.RUnlock()
	return ss.send(serial.EncodeTuningParams(rx, af))
}

// getAutoaddConfig handles CMD_GET_AUTOADD_CONFIG.
func (s *Server) getAutoaddConfig(ss *session) error {
	s.mu.RLock()
	cfg, hops := s.autoaddConfig, s.autoaddMaxHops
	s.mu.RUnlock()
	return ss.send(serial.EncodeAutoaddConfig(cfg, hops))
}

// setAutoaddConfig handles CMD_SET_AUTOADD_CONFIG. The max-hops byte is optional.
func (s *Server) setAutoaddConfig(ss *session, payload []byte) error {
	cfg, hops, hasHops, err := serial.ParseSetAutoaddConfig(payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	s.mu.Lock()
	s.autoaddConfig = cfg
	if hasHops {
		s.autoaddMaxHops = hops
	}
	s.mu.Unlock()
	return ss.send(serial.EncodeOK())
}

// getStats handles CMD_GET_STATS. The sub-type byte selects core, radio, or
// packet statistics. Battery and uptime are server-owned; the rest come from the
// optional Stats callback (zeroed when it is not set).
func (s *Server) getStats(ss *session, payload []byte) error {
	if len(payload) < 2 {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	var st Stats
	if s.stats != nil {
		st = s.stats()
	}
	switch payload[1] {
	case serial.StatsTypeCore:
		uptime := uint32(time.Since(s.startTime).Seconds())
		return ss.send(serial.EncodeStatsCore(s.id.BatteryMilliVolts, uptime, st.ErrFlags, st.QueueLen))
	case serial.StatsTypeRadio:
		return ss.send(serial.EncodeStatsRadio(st.NoiseFloor, st.LastRSSI, st.LastSNR, st.TxAirSecs, st.RxAirSecs))
	case serial.StatsTypePackets:
		return ss.send(serial.EncodeStatsPackets(
			st.PacketsRecv, st.PacketsSent, st.SentFlood, st.SentDirect,
			st.RecvFlood, st.RecvDirect, st.RecvErrors))
	default:
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
}

// exportContact handles CMD_EXPORT_CONTACT. A frame without a pubkey exports
// this node's own advert (for a share URI/QR); a frame naming a saved contact is
// unsupported because meshcore-go does not store others' advert signatures.
func (s *Server) exportContact(ss *session, payload []byte) error {
	if len(payload) >= 1+32 {
		return ss.send(serial.EncodeErr(serial.ErrCodeNotFound))
	}
	if s.exportSelf == nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeUnsupportedCmd))
	}
	data := s.exportSelf()
	if len(data) == 0 {
		return ss.send(serial.EncodeErr(serial.ErrCodeTableFull))
	}
	return ss.send(serial.EncodeExportContact(data))
}

// importContact handles CMD_IMPORT_CONTACT: parse the shared advert packet,
// verify it, and add the contact.
func (s *Server) importContact(ss *session, payload []byte) error {
	if len(payload) < 2 {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	var pkt codec.Packet
	if err := pkt.ReadFrom(payload[1:]); err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	advert, err := codec.ParseAdvertPayload(pkt.Payload)
	if err != nil {
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	result := contact.ProcessAdvert(s.node.Contacts(), advert, s.node.Clock().GetCurrentTime(), true)
	if result.Rejected {
		s.log.Debug("import contact rejected", "reason", result.RejectReason)
		return ss.send(serial.EncodeErr(serial.ErrCodeIllegalArg))
	}
	return ss.send(serial.EncodeOK())
}

// parseContactKey reads the leading 32-byte public key from a contact command.
func parseContactKey(payload []byte) (core.MeshCoreID, bool) {
	if len(payload) < 1+32 {
		return core.MeshCoreID{}, false
	}
	var id core.MeshCoreID
	copy(id[:], payload[1:33])
	return id, true
}

// contactToWire maps a stored contact to the companion Contact frame.
func contactToWire(c *contact.ContactInfo) *serial.Contact {
	wc := &serial.Contact{
		Type:       c.Type,
		Flags:      c.Flags,
		OutPathLen: c.OutPathLen,
		OutPath:    c.OutPath,
		Name:       c.Name,
		LastAdvert: c.LastAdvertTimestamp,
		GPSLat:     c.GPSLat,
		GPSLon:     c.GPSLon,
		LastMod:    c.LastMod,
	}
	copy(wc.PublicKey[:], c.ID[:])
	return wc
}

// degToFixed converts degrees to the firmware's fixed-point degrees x1e6.
func degToFixed(deg float64) int32 {
	return int32(math.Round(deg * 1e6))
}
