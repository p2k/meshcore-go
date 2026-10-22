package node

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kabili207/meshcore-go/core"
	"github.com/kabili207/meshcore-go/core/clock"
	"github.com/kabili207/meshcore-go/core/codec"
	"github.com/kabili207/meshcore-go/core/crypto"
	"github.com/kabili207/meshcore-go/device/ack"
	"github.com/kabili207/meshcore-go/device/advert"
	"github.com/kabili207/meshcore-go/device/connection"
	"github.com/kabili207/meshcore-go/device/contact"
	"github.com/kabili207/meshcore-go/device/event"
	"github.com/kabili207/meshcore-go/device/router"
)

// CompanionConfig configures a CompanionNode.
type CompanionConfig struct {
	// PrivateKey is the node's Ed25519 private key (64 bytes: seed + pubkey).
	PrivateKey ed25519.PrivateKey

	// Transports to connect to the mesh network.
	Transports []TransportOption

	// Contacts is the contact store. If nil, a default ContactManager is
	// created with MaxContacts=256 and OverwriteWhenFull=true.
	Contacts contact.ContactStore

	// Advertisement
	Name     string   // Node name broadcast in adverts.
	NodeType uint8    // Default: codec.NodeTypeChat.
	Lat      *float64 // Optional GPS latitude.
	Lon      *float64 // Optional GPS longitude.

	// AdvertLocalInterval is the local (zero-hop) advert interval in firmware
	// units (value * 2 minutes). Default: 0 (disabled).
	//
	// The companion_radio firmware has no automatic advert timer: companions
	// advertise only when the user triggers it. To match that, recurring adverts
	// are off by default. Set a non-zero value to opt into periodic adverts.
	AdvertLocalInterval uint8

	// AdvertFloodInterval is the flood advert interval in hours. Default: 0
	// (disabled), matching firmware. Set a non-zero value to opt in.
	AdvertFloodInterval uint8

	// NoInitialAdvert skips the flood advert Run sends on start. Default: false
	// (advert sent). The companion_radio firmware doesn't advertise on start;
	// set it to match, e.g. on a radio where each restart would flood the mesh.
	// Periodic adverts (AdvertLocalInterval, AdvertFloodInterval) and adverts
	// requested by the app are unaffected.
	NoInitialAdvert bool

	// ACKTimeout is how long to wait for an ACK before retrying. Default: 12s.
	ACKTimeout time.Duration

	// MaxRetries is how many times to resend before giving up. Default: 3.
	MaxRetries int

	// KeepAliveInterval is how often to send keep-alives to logged-in servers.
	// Default: 2 minutes.
	KeepAliveInterval time.Duration

	// ForwardPackets enables packet relaying. Default: false.
	ForwardPackets bool

	// ExtraAckTransmits sends redundant multipart ACK copies over a direct path
	// (firmware multi_acks). Default: 0 (off).
	ExtraAckTransmits int

	// EventHandlers registered during construction.
	EventHandlers []event.Handler

	// Logger for node events. Falls back to slog.Default() if nil.
	Logger *slog.Logger
}

// CompanionNode is a high-level companion/chat node for exchanging DMs on
// MeshCore networks. It composes BaseNode with text sending helpers, ACK
// tracking, message chunking, and advertisement scheduling.
//
// This is the node type used by the multi-mesh-bbs for MeshCore transport.
type CompanionNode struct {
	base        *BaseNode
	ackTracker  *ack.Tracker
	advertSched *advert.Scheduler
	connections *connection.Manager
	clk         *clock.Clock
	log         *slog.Logger

	initialAdvert  bool
	keepAliveEvery time.Duration

	pendingMu        sync.Mutex
	pendingLogins    map[core.MeshCoreID]uint32 // server -> login send time
	pendingTelemetry map[uint32]core.MeshCoreID // request tag -> peer
	pendingStatus    map[uint32]core.MeshCoreID // status request tag -> peer
}

// NewCompanion creates a CompanionNode from the given configuration.
func NewCompanion(cfg CompanionConfig) (*CompanionNode, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	nodeType := cfg.NodeType
	if nodeType == 0 {
		nodeType = codec.NodeTypeChat
	}

	ackTimeout := cfg.ACKTimeout
	if ackTimeout == 0 {
		ackTimeout = 12 * time.Second
	}
	maxRetries := cfg.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	tracker := ack.NewTracker(ack.TrackerConfig{
		ACKTimeout: ackTimeout,
		MaxRetries: maxRetries,
		Logger:     logger,
	})

	clk := clock.New()

	// Default contact store: an in-memory ContactManager. Without this, a nil
	// Contacts store would panic on first use.
	contacts := cfg.Contacts
	if contacts == nil {
		contacts = contact.NewManager(cfg.PrivateKey, contact.ManagerConfig{
			MaxContacts:       256,
			OverwriteWhenFull: true,
		})
	}

	base, err := NewBase(BaseConfig{
		PrivateKey:        cfg.PrivateKey,
		Contacts:          contacts,
		Clock:             clk,
		ACKTracker:        tracker,
		Transports:        cfg.Transports,
		ForwardPackets:    cfg.ForwardPackets,
		ExtraAckTransmits: cfg.ExtraAckTransmits,
		EventHandlers:     cfg.EventHandlers,
		Logger:            logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create base node: %w", err)
	}

	// Register the built-in "Public" channel so group messages decrypt by default.
	base.AddChannel(crypto.DefaultChannelKey)

	// Build advert scheduler
	advertBuilder := advert.NewSelfAdvertBuilder(&advert.SelfAdvertConfig{
		PrivateKey: cfg.PrivateKey,
		PublicKey:  base.PublicKey(),
		Clock:      clk,
		AppData: &codec.AdvertAppData{
			Name:     cfg.Name,
			NodeType: nodeType,
			Lat:      cfg.Lat,
			Lon:      cfg.Lon,
		},
	})

	// Companions default to no recurring adverts (both intervals 0), matching the
	// companion_radio firmware. A zero here means "disabled", not "use default",
	// so we pass the config values straight through.
	scheduler := advert.NewScheduler(base.Router, advertBuilder, advert.SchedulerConfig{
		LocalAdvertInterval: cfg.AdvertLocalInterval,
		FloodAdvertInterval: cfg.AdvertFloodInterval,
		Logger:              logger,
	})

	keepAlive := cfg.KeepAliveInterval
	if keepAlive == 0 {
		keepAlive = 2 * time.Minute
	}

	n := &CompanionNode{
		base:        base,
		ackTracker:  tracker,
		advertSched: scheduler,
		connections: connection.NewManager(connection.ManagerConfig{
			KeepAliveInterval: keepAlive,
			Logger:            logger,
		}),
		clk:              clk,
		log:              logger.WithGroup("companion"),
		initialAdvert:    !cfg.NoInitialAdvert,
		keepAliveEvery:   keepAlive,
		pendingLogins:    make(map[core.MeshCoreID]uint32),
		pendingTelemetry: make(map[uint32]core.MeshCoreID),
		pendingStatus:    make(map[uint32]core.MeshCoreID),
	}

	// Watch responses for login-OK correlation and connection liveness.
	base.OnEvent(n.onInternalEvent)

	return n, nil
}

// Run starts all components and blocks until ctx is cancelled.
// Starts: transports, router, ACK tracker, advert scheduler.
func (n *CompanionNode) Run(ctx context.Context) error {
	// Start transports
	if err := n.base.StartTransports(ctx); err != nil {
		return err
	}

	// Start router send queue
	n.base.Router.Start(ctx)

	// Start ACK tracker
	go n.ackTracker.Start(ctx)

	// Start connection timeout tracking and the keep-alive sender.
	go n.connections.Start(ctx)
	go n.keepAliveLoop(ctx)

	if n.initialAdvert {
		n.advertSched.SendNow(true)
	}

	// Run advert scheduler (blocks until ctx cancelled)
	n.advertSched.Start(ctx)

	return nil
}

// OnEvent registers an event handler. Delegates to BaseNode.
func (n *CompanionNode) OnEvent(fn event.Handler) {
	n.base.OnEvent(fn)
}

// Base returns the underlying BaseNode for advanced use.
func (n *CompanionNode) Base() *BaseNode { return n.base }

// ID returns the node's MeshCoreID.
func (n *CompanionNode) ID() core.MeshCoreID { return n.base.ID() }

// SendReply sends an encrypted response using a ReplyContext from a received event.
func (n *CompanionNode) SendReply(reply event.ReplyContext, to core.MeshCoreID, payloadType uint8, plaintext []byte) error {
	return n.base.SendReply(reply, to, payloadType, plaintext)
}

// SetSendScope scopes this node's outbound flood traffic (messages, ACKs, path
// returns) to a region. Derive the key with router.TransportKeyFromRegion.
// Adverts and direct sends stay unscoped. Call ClearSendScope to revert.
func (n *CompanionNode) SetSendScope(key router.TransportKey) {
	n.base.Router.SetSendScope(key)
}

// ClearSendScope reverts to unscoped flood sending.
func (n *CompanionNode) ClearSendScope() {
	n.base.Router.ClearSendScope()
}

// AdvertScheduler returns the advert scheduler for manual control
// (e.g., triggering an immediate advert after settings change).
func (n *CompanionNode) AdvertScheduler() *advert.Scheduler {
	return n.advertSched
}

// AddChannel registers a group channel by its shared key and returns the channel
// hash. The built-in "Public" channel is registered automatically.
func (n *CompanionNode) AddChannel(key []byte) uint8 {
	return n.base.AddChannel(key)
}

// SendChannelText sends a plain group text message on the channel identified by
// key.
func (n *CompanionNode) SendChannelText(key []byte, text string) error {
	return n.base.SendChannelText(key, text)
}

// ACKTracker returns the ACK tracker for manual tracking if needed.
func (n *CompanionNode) ACKTracker() *ack.Tracker {
	return n.ackTracker
}
