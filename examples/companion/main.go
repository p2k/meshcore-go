// Command companion runs a meshcore-go CompanionNode and exposes it over the
// companion protocol so a host app (MeshMonitor via @liamcottle/meshcore.js, or
// the MeshCore phone app) can connect over TCP and drive it.
//
// With no transport flags it still serves the companion handshake and an empty
// contact list, which is enough to confirm a client can connect. Point it at a
// real LoRa radio (-serial for a MeshCore device, -phy for a SX1262 attached to
// this host) or an MQTT bridge (-mqtt) to put it on the mesh so contacts
// populate from adverts.
//
//	go run ./examples/companion -listen 127.0.0.1:5000 -name demo
//	go run ./examples/companion -serial /dev/ttyUSB0 -name demo
//	go run ./examples/companion -phy meshstick -freq 915 -bw 250 -sf 11 -cr 5 -name demo
//
// With -phy, the radio flags configure the radio and the app can change them,
// otherwise they are only reported to the app. Pay attention to the
// regulations of your region, e.g. frequencies, TX power and duty cycle.
//
// Then add a MeshCore source in MeshMonitor with connection type TCP, host
// 127.0.0.1, port 5000.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"

	sloghelper "github.com/kabili207/slog-helper"

	"github.com/kabili207/meshcore-go/core"
	"github.com/kabili207/meshcore-go/core/codec"
	"github.com/kabili207/meshcore-go/device/advert"
	"github.com/kabili207/meshcore-go/device/companion"
	"github.com/kabili207/meshcore-go/device/contact"
	"github.com/kabili207/meshcore-go/device/event"
	"github.com/kabili207/meshcore-go/device/node"
	"github.com/kabili207/meshcore-go/transport"
	mqtttransport "github.com/kabili207/meshcore-go/transport/mqtt"
	phytransport "github.com/kabili207/meshcore-go/transport/phy"
	serialtransport "github.com/kabili207/meshcore-go/transport/serial"
	udptransport "github.com/kabili207/meshcore-go/transport/udp"
)

func main() {
	sloghelper.InitFromEnv()

	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen  = flag.String("listen", "127.0.0.1:5000", "companion server TCP listen address")
		name    = flag.String("name", "meshcore-go", "node advertised name")
		keyPath = flag.String("key", "companion.key", "path to the node key file (created if missing)")

		serialPort = flag.String("serial", "", "serial port for the LoRa radio (optional, e.g. /dev/ttyUSB0)")
		baud       = flag.Int("baud", 115200, "serial baud rate")
		mqttBroker = flag.String("mqtt", "", "MQTT bridge broker URL (optional, e.g. tcp://host:1883)")
		mqttTopic  = flag.String("mqtt-topic", "", "MQTT bridge topic (default: the firmware's topic for the framing in use)")
		mqttSecret = flag.String("mqtt-secret", "", "bridge.secret shared with the EastMesh repeaters (optional)")
		mqttRaw    = flag.Bool("mqtt-raw", false, "publish bare packets, for the vrybdpkt firmware fork")
		mqttUser   = flag.String("mqtt-user", "", "MQTT username (optional)")
		mqttPass   = flag.String("mqtt-pass", "", "MQTT password (optional)")
		mqttTLS    = flag.Bool("mqtt-tls", false, "use TLS for the MQTT connection")

		udpEnable = flag.Bool("udp", false, "enable the UDP multicast bridge transport")
		udpGroup  = flag.String("udp-group", udptransport.DefaultGroupAddress, "UDP multicast group address")
		udpPort   = flag.Int("udp-port", udptransport.DefaultPort, "UDP multicast port")
		udpIface  = flag.String("udp-iface", "", "network interface for UDP multicast (optional, e.g. eth0)")

		phyBoard  = flag.String("phy", "", "SX1262 board attached to this host (optional): meshstick or waveshare-hat")
		dutyCycle = flag.Float64("duty-cycle", phytransport.DefaultDutyCycle, "maximum fraction of time transmitting with -phy, e.g. 0.1 in the EU 869.4-869.65 MHz band")

		freq    = flag.Float64("freq", 915.0, "radio frequency in MHz")
		bw      = flag.Float64("bw", 250, "radio bandwidth in kHz")
		sf      = flag.Int("sf", 11, "radio spreading factor")
		cr      = flag.Int("cr", 5, "radio coding rate, 5-8 for 4/5-4/8")
		txPower = flag.Int("tx-power", 22, "radio TX power in dBm")
	)
	flag.Parse()

	priv, err := loadOrCreateKey(*keyPath)
	if err != nil {
		slog.Error("Failed to load node key", "error", err)
		return err
	}
	pub := priv.Public().(ed25519.PublicKey)
	nodeID := hex.EncodeToString(pub)

	var udpCfg *udptransport.Config
	if *udpEnable {
		udpCfg = &udptransport.Config{
			GroupAddress: *udpGroup,
			Port:         *udpPort,
			Interface:    *udpIface,
		}
	}

	mqttFraming := mqtttransport.FramingBridge
	if *mqttRaw {
		mqttFraming = mqtttransport.FramingRaw
	}

	var phyTr *phytransport.Transport
	if *phyBoard != "" {
		dev, closeRadio, err := openRadio(*phyBoard)
		if err != nil {
			slog.Error("Failed to open radio", "error", err)
			return err
		}
		defer closeRadio()
		phyTr = newPHY(dev, phytransport.RadioParams{
			Frequency:       uint32(math.Round(*freq * 1e6)),
			Bandwidth:       uint32(math.Round(*bw * 1e3)),
			SpreadingFactor: uint8(*sf),
			CodingRate:      uint8(*cr),
			TxPower:         int8(*txPower),
		}, *dutyCycle)
	}

	transports, err := buildTransports(*serialPort, *baud, phyTr, mqtttransport.Config{
		Broker:   *mqttBroker,
		Topic:    *mqttTopic,
		Framing:  mqttFraming,
		Secret:   *mqttSecret,
		Username: *mqttUser,
		Password: *mqttPass,
		UseTLS:   *mqttTLS,
	}, udpCfg, nodeID)
	if err != nil {
		slog.Error("Failed to configure transport", "error", err)
		return err
	}

	comp, err := node.NewCompanion(node.CompanionConfig{
		PrivateKey: priv,
		Transports: transports,
		Name:       *name,
		Logger:     slog.Default(),
	})
	if err != nil {
		slog.Error("Failed to create companion node", "error", err)
		return err
	}

	comp.OnEvent(func(evt any) {
		switch e := evt.(type) {
		case *event.TextMessageReceived:
			slog.Info("message received", "from", e.From.String(), "text", e.Message)
		case *event.AdvertReceived:
			if e.IsNew {
				slog.Info("new contact", "name", e.Contact.Name, "id", e.Contact.ID.String())
			}
		}
	})

	srvCfg := companion.Config{
		Node: comp.Base(),
		Identity: companion.Identity{
			Name:         *name,
			TxPower:      uint8(max(*txPower, 0)),
			RadioFreqMHz: *freq,
			RadioBWkHz:   *bw,
			RadioSF:      uint8(*sf),
			RadioCR:      uint8(*cr),
		},
		Events: func(h func(evt any)) { comp.OnEvent(h) },
		SendDM: func(ctx context.Context, to core.MeshCoreID, text string, txtType, attempt uint8, onAck func()) (bool, error) {
			ct := comp.Base().Contacts().GetByPubKey(to)
			flood := ct == nil || !ct.HasDirectPath()
			err := comp.SendText(ctx, to, text,
				node.WithTxtType(txtType), node.WithAttempt(attempt), node.WithOnACK(onAck))
			return flood, err
		},
		SendChannel: func(_ context.Context, channelKey []byte, text string) error {
			return comp.SendChannelText(channelKey, text)
		},
		SendLogin: func(_ context.Context, to core.MeshCoreID, password string) error {
			_, err := comp.SendLogin(to, password)
			return err
		},
		SendStatus: func(_ context.Context, to core.MeshCoreID) error {
			_, err := comp.SendStatusReq(to)
			return err
		},
		SendTelemetry: func(_ context.Context, to core.MeshCoreID) (uint32, error) {
			return comp.SendTelemetryReq(to)
		},
		SendTrace: func(_ context.Context, tag, authCode uint32, flags uint8, path []byte) error {
			return comp.SendTrace(tag, authCode, flags, path)
		},
		ExportSelf: func() []byte {
			builder := advert.NewSelfAdvertBuilder(&advert.SelfAdvertConfig{
				PrivateKey: priv,
				PublicKey:  comp.Base().PublicKey(),
				Clock:      comp.Base().Clock(),
				AppData:    &codec.AdvertAppData{Name: *name, NodeType: codec.NodeTypeChat},
			})
			pkt := builder()
			if pkt == nil {
				return nil
			}
			return pkt.WriteTo()
		},
		Stats: func() companion.Stats {
			c := comp.Base().Router.Counters().Snapshot()
			return companion.Stats{
				PacketsRecv: c.PacketsRecv,
				PacketsSent: c.PacketsSent,
				SentFlood:   c.SentFlood,
				SentDirect:  c.SentDirect,
				RecvFlood:   c.RecvFlood,
				RecvDirect:  c.RecvDirect,
			}
		},
		Logger: slog.Default(),
	}
	if phyTr != nil {
		wirePHY(&srvCfg, phyTr)
	}
	srv := companion.NewServer(srvCfg)

	// Push PUSH_CODE_CONTACT_DELETED when the contact table evicts an entry, so
	// the app's contact list stays in sync.
	if mgr, ok := comp.Base().Contacts().(*contact.ContactManager); ok {
		mgr.SetOnContactOverwrite(srv.NotifyContactDeleted)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// One error slot per goroutine we start, so a failing goroutine can report
	// without blocking on an unread channel.
	errc := make(chan error, 2)
	if len(transports) > 0 {
		go func() { errc <- comp.Run(ctx) }()
	} else {
		slog.Warn("no transport configured; serving companion protocol only (no live mesh)")
	}
	go func() { errc <- srv.ListenAndServe(ctx, *listen) }()

	slog.Info("companion example running", "listen", *listen, "id", comp.ID().String(), "name", *name)

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
		return nil
	case err := <-errc:
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}
}

// buildTransports assembles the mesh transports from the serial/phy/MQTT flags. An
// empty result is valid: the node then serves the companion protocol without a
// live mesh connection. phyTr is added if not nil. mqttCfg is used only when its
// Broker is set; NodeID and Logger are filled in here.
func buildTransports(serialPort string, baud int, phyTr *phytransport.Transport, mqttCfg mqtttransport.Config, udpCfg *udptransport.Config, nodeID string) ([]node.TransportOption, error) {
	var transports []node.TransportOption

	if serialPort != "" {
		st := serialtransport.New(serialtransport.Config{
			Port:     serialPort,
			BaudRate: baud,
			Logger:   slog.Default(),
		})
		transports = append(transports, node.TransportOption{
			Transport: st,
			Source:    transport.PacketSourceSerial,
			Name:      "serial",
		})
	}

	if phyTr != nil {
		transports = append(transports, node.TransportOption{
			Transport: phyTr,
			Source:    transport.PacketSourcePHY,
			Name:      "phy",
		})
	}

	if mqttCfg.Broker != "" {
		mqttCfg.NodeID = nodeID
		mqttCfg.Logger = slog.Default()
		mt := mqtttransport.New(mqttCfg)
		transports = append(transports, node.TransportOption{
			Transport: mt,
			Source:    transport.PacketSourceMQTT,
			Name:      "mqtt",
		})
	}

	if udpCfg != nil {
		udpCfg.Logger = slog.Default()
		ut := udptransport.New(*udpCfg)
		transports = append(transports, node.TransportOption{
			Transport: ut,
			Source:    transport.PacketSourceUDP,
			Name:      "udp",
		})
	}

	return transports, nil
}

// loadOrCreateKey reads the node's Ed25519 seed (hex) from path, or generates a
// new key and persists its seed there on first run so the node identity is
// stable across restarts.
func loadOrCreateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		seed, decErr := hex.DecodeString(strings.TrimSpace(string(data)))
		if decErr != nil || len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf("invalid key file %s", path)
		}
		return ed25519.NewKeyFromSeed(seed), nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, []byte(hex.EncodeToString(priv.Seed())), 0o600); err != nil {
		return nil, err
	}
	slog.Info("generated new node key", "path", path)
	return priv, nil
}
