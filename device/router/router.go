// Package router provides packet routing and forwarding for MeshCore networks.
//
// The Router sits between transports (MQTT, serial) and application logic,
// making forwarding decisions for every received packet. It handles:
//   - Flood routing: appending this node's hash to the path and re-broadcasting
//   - Direct routing: forwarding packets along a specified path of node hashes
//   - Deduplication: preventing duplicate packet processing via circular hash tables
//   - Multipart reassembly: combining fragmented packets before dispatch
//   - Transport code validation: dropping packets with unrecognized region codes
//   - ACK forwarding: creating new ACK packets when relaying direct-routed ACKs
//   - TRACE forwarding: hop-by-hop path tracing with SNR collection
//   - Send queue: priority-ordered outbound packet queue with optional delay
//
// This corresponds to the firmware's Mesh class (src/Mesh.cpp).
package router

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/kabili207/meshcore-go/core"
	"github.com/kabili207/meshcore-go/core/codec"
	"github.com/kabili207/meshcore-go/core/dedupe"
	"github.com/kabili207/meshcore-go/core/multipart"
	"github.com/kabili207/meshcore-go/transport"
)

const (
	// DefaultMaxFloodHops is the maximum number of flood hops before a packet is dropped.
	DefaultMaxFloodHops = codec.MaxPathSize // 64

	// DefaultMaxUnscopedFloodHops is the default hop limit for unscoped flood packets.
	DefaultMaxUnscopedFloodHops = 64

	// DefaultMaxAdvertFloodHops is the default hop limit for flooded ADVERT packets.
	DefaultMaxAdvertFloodHops = 8

	// DefaultDrainInterval is the default interval for the send queue drain loop.
	DefaultDrainInterval = 10 * time.Millisecond

	// PathSendDelay is the delay before sending PATH packets. This gives
	// the original flood packet time to propagate before the response follows.
	// Firmware: PATH_RETURN_DELAY = 300ms.
	PathSendDelay = 300 * time.Millisecond

	// Send priorities matching firmware conventions.
	PriorityDirect      = 0 // Highest: direct-routed traffic
	PriorityFloodData   = 1 // Flood data, ACKs
	PriorityFloodPath   = 2 // Flood PATH packets
	PriorityFloodAdvert = 3 // Lowest for outbound: ADVERT packets
	PriorityTrace       = 5 // TRACE forwarding
)

// PacketHandler is called by the router when a packet is received that should
// be processed by the application layer. The handler runs synchronously before
// any forwarding decision — it may call pkt.MarkDoNotRetransmit() to suppress
// flood forwarding.
type PacketHandler func(pkt *codec.Packet, src transport.PacketSource)

// PacketMonitor is called for every valid reception BEFORE deduplication,
// regardless of routing decisions. Unlike PacketHandler, it fires for all
// packets including those that are only forwarded (e.g., direct-routed packets
// transiting this node) AND including duplicate copies of a flood that arrived
// via different paths. Observer/telemetry systems need visibility into every
// physical reception, so the path-blind dedup gate must not hide duplicates
// from them (it still gates forwarding and application processing).
//
// The monitor must not modify the packet. It runs synchronously on the
// serialized receive path, so it should return quickly or dispatch work to a
// goroutine.
type PacketMonitor func(pkt *codec.Packet, src transport.PacketSource)

// Config configures a Router.
type Config struct {
	// SelfID is this node's identity. Its Hash() (first byte of public key)
	// is used for path matching during direct routing and appended to paths
	// during flood forwarding.
	SelfID core.MeshCoreID

	// ForwardPackets enables packet forwarding (repeater mode).
	// When false (default), the router processes packets addressed to this node
	// but does not relay flood or direct traffic for other nodes.
	// This is equivalent to the firmware's allowPacketForward().
	ForwardPackets bool

	// MaxFloodHops limits how far flood packets can propagate through this node.
	// Packets with hop count >= MaxFloodHops are not forwarded.
	// Default: 64 (MaxPathSize).
	MaxFloodHops int

	// MaxUnscopedFloodHops limits how far unscoped flood packets (RouteTypeFlood,
	// i.e. without region transport codes) propagate. Scoped flood
	// (RouteTypeTransportFlood) is not affected. Firmware's flood.max.unscoped.
	// Default: 64.
	MaxUnscopedFloodHops int

	// MaxAdvertFloodHops limits how far flooded ADVERT packets propagate, to curb
	// advert storms. Firmware's flood.max.advert. Default: 8.
	MaxAdvertFloodHops int

	// PathHashMode controls the hash size used when originating flood packets.
	// 0 = 1-byte hashes (default, backward-compatible), 1 = 2-byte, 2 = 3-byte.
	PathHashMode uint8

	// LoopDetect sets the loop detection level for flood forwarding.
	// 0 = off (default), 1 = minimal, 2 = moderate, 3 = strict.
	// When enabled, packets with too many self-hash occurrences in the
	// path are dropped before forwarding.
	LoopDetect int

	// DrainInterval is how often the queue drain goroutine checks for ready
	// packets. Default: 10ms. Only used when Start() is called.
	DrainInterval time.Duration

	// ValidateTransportCode is called for packets that include transport codes.
	// If non-nil, it must return true for the packet to be processed.
	//
	// If nil, packets with transport codes pass through without validation.
	// This is the correct default for a companion/client node: like the
	// firmware companion, it accepts scoped traffic and simply processes
	// whatever is addressed to it. A repeater that forwards scoped floods must
	// set this (e.g. via NewTransportCodeValidator) so that packets whose code
	// matches no configured region are dropped, matching the firmware's
	// allowPacketForward() where an unmatched TRANSPORT_FLOOD is not relayed.
	ValidateTransportCode TransportCodeValidator

	// SendScope, when non-null, scopes outbound flood traffic to a region by
	// attaching transport codes (see SendFloodScoped / SendFloodPathScoped).
	// Adverts and direct sends are never scoped. Mirrors the firmware's
	// send_scope. Default: null key (unscoped).
	SendScope TransportKey

	// DefaultReplyScope is the fallback scope for a flooded reply whose request
	// scope cannot be determined: the request arrived DIRECT (no transport
	// codes), or its code matched no configured region. Without it such a reply
	// goes out un-scoped and is dropped at hop 0 by any repeater running
	// flood.max.unscoped=0. Mirrors the firmware's default_scope.
	//
	// A reply to a request that arrived on a known scope always mirrors that
	// scope instead, and a request that was deliberately un-scoped is answered
	// un-scoped. Falls back to SendScope when null. See ResolveReplyScope.
	DefaultReplyScope TransportKey

	// RegionMap, when set, applies region policy to flood forwarding: a scoped
	// TRANSPORT_FLOOD is forwarded only if it matches a flood-permitting region,
	// and an unscoped FLOOD is forwarded only if the wildcard region permits
	// flood (letting a repeater run region-only). This is the repeater-side
	// analog of the firmware's region_map and gates forwarding only, not
	// reception or dispatch. It is independent of ValidateTransportCode, which
	// is a pre-dispatch hard filter.
	//
	// The map is read on the receive path and is not internally synchronized;
	// callers that edit it while routing is active must synchronize those edits.
	RegionMap *RegionMap

	// Logger for routing events. Falls back to slog.Default() if nil.
	Logger *slog.Logger
}

// Router handles packet routing and forwarding for a MeshCore node.
type Router struct {
	cfg      Config
	log      *slog.Logger
	dedup    *dedupe.PacketDeduplicator
	queue    *SendQueue
	counters RouterCounters

	// recvMu serializes the transport-facing receive path. The whole
	// HandlePacket pipeline (dedup, TRACE, room/region handlers, multipart
	// reassembly) assumes single-threaded access, but transports can deliver
	// concurrently: the MQTT transport uses OrderMatters(false), and multiple
	// transports (MQTT + serial) deliver from independent goroutines. This lock
	// is taken only at the transport entry wrapper in AddTransport, so the
	// internal recursive HandlePacket call in handleMultipart does not deadlock.
	// It is separate from mu (which guards config/handler fields and is taken
	// inside the receive path) to avoid re-entrant locking.
	recvMu sync.Mutex

	mu         sync.RWMutex
	transports []transportEntry
	onPacket   PacketHandler
	onMonitor  PacketMonitor

	cancel    context.CancelFunc
	drainDone chan struct{}
	started   bool
}

type transportEntry struct {
	transport transport.Transport
	source    transport.PacketSource
}

// New creates a Router with the given configuration.
func New(cfg Config) *Router {
	if cfg.MaxFloodHops <= 0 {
		cfg.MaxFloodHops = DefaultMaxFloodHops
	}
	if cfg.MaxUnscopedFloodHops <= 0 {
		cfg.MaxUnscopedFloodHops = DefaultMaxUnscopedFloodHops
	}
	if cfg.MaxAdvertFloodHops <= 0 {
		cfg.MaxAdvertFloodHops = DefaultMaxAdvertFloodHops
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Router{
		cfg:   cfg,
		log:   logger.WithGroup("router"),
		dedup: dedupe.New(),
		queue: NewSendQueue(),
	}
}

// Start begins the queue drain goroutine. Packets pushed to the queue will
// be sent when ready. If Start is never called, enqueue falls back to
// synchronous sending.
func (r *Router) Start(ctx context.Context) {
	interval := r.cfg.DrainInterval
	if interval <= 0 {
		interval = DefaultDrainInterval
	}
	ctx, r.cancel = context.WithCancel(ctx)
	r.drainDone = make(chan struct{})
	r.started = true
	go r.drainLoop(ctx, interval)
}

// Stop cancels the drain goroutine and waits for it to finish.
func (r *Router) Stop() {
	if r.cancel != nil {
		r.cancel()
		<-r.drainDone
		r.cancel = nil
		r.started = false
	}
}

// drainLoop pops ready packets from the send queue and sends them.
func (r *Router) drainLoop(ctx context.Context, interval time.Duration) {
	defer close(r.drainDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				entry := r.queue.Pop()
				if entry == nil {
					break
				}
				if entry.SendToAll {
					r.broadcastToAllTransports(entry.Packet)
				} else {
					r.broadcastToTransports(entry.Packet, entry.ExcludeSource)
				}
				r.notifyMonitor(entry.Packet, transport.PacketSourceLocal)
			}
		}
	}
}

// enqueue adds a packet to the send queue if the drain goroutine is running,
// otherwise sends synchronously.
func (r *Router) enqueue(pkt *codec.Packet, priority uint8, delay time.Duration, excludeSource transport.PacketSource, sendToAll bool) {
	if !r.started {
		if sendToAll {
			r.broadcastToAllTransports(pkt)
		} else {
			r.broadcastToTransports(pkt, excludeSource)
		}
		r.notifyMonitor(pkt, transport.PacketSourceLocal)
		return
	}
	r.queue.Push(pkt, priority, delay, excludeSource, sendToAll)
}

// SetPacketHandler sets the callback for packets that should be processed by
// the application layer. The handler is called synchronously during HandlePacket
// before forwarding decisions are made. The handler may call
// pkt.MarkDoNotRetransmit() to suppress flood forwarding of the packet.
func (r *Router) SetPacketHandler(fn PacketHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onPacket = fn
}

// SetPacketMonitor sets a monitor callback that fires for every valid reception
// before deduplication, regardless of routing. This includes packets that are
// only forwarded through this node, never reach the application handler, or are
// duplicate copies of a flood heard via different paths. Useful for
// observer/telemetry systems that must see every physical reception.
func (r *Router) SetPacketMonitor(fn PacketMonitor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onMonitor = fn
}

// GetPathHashMode returns the current path hash mode (0, 1, or 2).
func (r *Router) GetPathHashMode() uint8 {
	return r.cfg.PathHashMode
}

// SetPathHashMode updates the path hash mode used when originating floods.
func (r *Router) SetPathHashMode(mode uint8) {
	r.cfg.PathHashMode = mode
}

// GetLoopDetect returns the current loop detection level.
func (r *Router) GetLoopDetect() int {
	return r.cfg.LoopDetect
}

// SetLoopDetect updates the loop detection level.
func (r *Router) SetLoopDetect(level int) {
	r.cfg.LoopDetect = level
}

// GetMaxFloodHops returns the maximum flood hop count before packets are dropped.
func (r *Router) GetMaxFloodHops() int {
	return r.cfg.MaxFloodHops
}

// SetMaxFloodHops updates the maximum flood hop count. A value <= 0 restores the
// default.
func (r *Router) SetMaxFloodHops(n int) {
	if n <= 0 {
		n = DefaultMaxFloodHops
	}
	r.cfg.MaxFloodHops = n
}

// GetMaxUnscopedFloodHops returns the hop limit for unscoped flood packets.
func (r *Router) GetMaxUnscopedFloodHops() int {
	return r.cfg.MaxUnscopedFloodHops
}

// SetMaxUnscopedFloodHops updates the unscoped flood hop limit. A value <= 0
// restores the default.
func (r *Router) SetMaxUnscopedFloodHops(n int) {
	if n <= 0 {
		n = DefaultMaxUnscopedFloodHops
	}
	r.cfg.MaxUnscopedFloodHops = n
}

// GetMaxAdvertFloodHops returns the hop limit for flooded ADVERT packets.
func (r *Router) GetMaxAdvertFloodHops() int {
	return r.cfg.MaxAdvertFloodHops
}

// SetMaxAdvertFloodHops updates the flooded ADVERT hop limit. A value <= 0
// restores the default.
func (r *Router) SetMaxAdvertFloodHops(n int) {
	if n <= 0 {
		n = DefaultMaxAdvertFloodHops
	}
	r.cfg.MaxAdvertFloodHops = n
}

// GetForwardPackets reports whether packet forwarding is enabled.
func (r *Router) GetForwardPackets() bool {
	return r.cfg.ForwardPackets
}

// SetForwardPackets enables or disables packet forwarding.
func (r *Router) SetForwardPackets(enabled bool) {
	r.cfg.ForwardPackets = enabled
}

// SetRegionMap sets (or clears, with nil) the region policy used for flood
// forwarding. See Config.RegionMap for the concurrency contract.
func (r *Router) SetRegionMap(rm *RegionMap) {
	r.cfg.RegionMap = rm
}

// RegionMap returns the configured region policy, or nil.
func (r *Router) RegionMap() *RegionMap {
	return r.cfg.RegionMap
}

// notifyMonitor calls the packet monitor callback if one is set.
func (r *Router) notifyMonitor(pkt *codec.Packet, src transport.PacketSource) {
	r.mu.RLock()
	monitor := r.onMonitor
	r.mu.RUnlock()
	if monitor != nil {
		monitor(pkt, src)
	}
}

// AddTransport registers a transport with the router. The router installs
// itself as the transport's packet handler so that incoming packets are
// automatically routed through HandlePacket.
func (r *Router) AddTransport(t transport.Transport, source transport.PacketSource) {
	r.mu.Lock()
	r.transports = append(r.transports, transportEntry{transport: t, source: source})
	r.mu.Unlock()

	t.SetPacketHandler(func(pkt *codec.Packet, src transport.PacketSource) {
		// Serialize the receive path across all transports and any concurrent
		// per-message delivery goroutines. The internal recursion in
		// handleMultipart calls HandlePacket directly (already under this lock),
		// so it must not be re-acquired there.
		r.recvMu.Lock()
		defer r.recvMu.Unlock()
		r.HandlePacket(pkt, src)
	})
}

// Counters returns a pointer to the router's packet counters.
func (r *Router) Counters() *RouterCounters { return &r.counters }

// HandlePacket is the main routing entry point. It processes an incoming packet,
// dispatches it to the application callback, and makes forwarding decisions.
//
// This corresponds to the firmware's Mesh::onRecvPacket() + routeRecvPacket().
func (r *Router) HandlePacket(pkt *codec.Packet, src transport.PacketSource) {
	// Gate 1: version check
	if pkt.PayloadVersion() > codec.PayloadVer1 {
		r.log.Debug("dropping packet with unsupported version",
			"version", pkt.PayloadVersion())
		return
	}

	// Gate 1.5: transport code validation (before dedup so rejected packets
	// don't consume dedup slots)
	if pkt.HasTransportCodes() && r.cfg.ValidateTransportCode != nil {
		if !r.cfg.ValidateTransportCode(pkt) {
			r.log.Debug("dropping packet with unrecognized transport code",
				"code0", pkt.TransportCodes[0],
				"code1", pkt.TransportCodes[1])
			return
		}
	}

	r.counters.PacketsRecv.Add(1)

	// Gate 2: multipart reassembly
	if pkt.PayloadType() == codec.PayloadTypeMultipart {
		r.handleMultipart(pkt, src)
		return
	}

	// Monitor hook: fires for every valid reception BEFORE dedup, regardless of
	// routing decisions. Observers must see every physical reception, including
	// duplicate copies of a flood that arrived via different paths (the dedup
	// hash is path-blind by design, so it would otherwise collapse them). This
	// matches the firmware, which captures at logRx/logRxRaw before hasSeen().
	// Only the version and transport-code gates run ahead of this, since those
	// reject malformed or foreign-scoped packets that never should be reported.
	r.notifyMonitor(pkt, src)

	// Gate 3: deduplication. HasSeen checks and records in one step; firmware
	// splits this into wasSeen/markSeen, but the whole receive path here runs
	// under recvMu, so the combined call is equivalent and avoids a second
	// lock acquisition. Dedup gates forwarding and application processing
	// only, NOT observer capture (which happened above).
	if r.dedup.HasSeen(pkt) {
		if pkt.IsFlood() {
			r.counters.FloodDups.Add(1)
		} else {
			r.counters.DirectDups.Add(1)
		}
		return
	}

	// Gate 3.5: TRACE handling (after dedup, before direct routing —
	// TRACE uses Path[] for SNR values, not relay hashes)
	if pkt.PayloadType() == codec.PayloadTypeTrace {
		r.handleTrace(pkt, src)
		return
	}

	// Gate 4: direct routing with path
	if pkt.IsDirect() && pkt.HopCount() > 0 {
		r.counters.RecvDirect.Add(1)
		r.handleDirectForward(pkt, src)
		return
	}

	// Gate 5: direct with no path (zero-hop or final destination)
	if pkt.IsDirect() && pkt.HopCount() == 0 {
		r.counters.RecvDirect.Add(1)
		r.dispatchToApp(pkt, src)
		return
	}

	// Gate 6: flood routing
	if pkt.IsFlood() {
		r.counters.RecvFlood.Add(1)
		r.handleFlood(pkt, src)
		return
	}

	// Unknown route type — drop silently
}

// handleMultipart processes a MULTIPART packet. Firmware uses MULTIPART only for
// ACKs, and each packet is self-contained: payload[0] = (remaining<<4)|inner_type
// and payload[1:] is a complete inner packet payload. `remaining` is a redundancy
// hint (how many more copies the sender will emit), NOT a fragment index — the
// fragments are not concatenated. We reconstruct the inner packet and feed it back
// through HandlePacket so it is deduplicated and handled like a regular ACK (which
// also forwards it when it arrived on a direct path). See firmware Mesh.cpp
// PAYLOAD_TYPE_MULTIPART handling and forwardMultipartDirect().
func (r *Router) handleMultipart(pkt *codec.Packet, src transport.PacketSource) {
	frag, err := multipart.ParseFragment(pkt.Payload)
	if err != nil {
		r.log.Debug("failed to parse multipart fragment", "error", err)
		return
	}

	// Only ACK is defined as an inner type today (firmware treats others as
	// future work). Guarding also prevents re-dispatch recursion.
	if frag.InnerType != codec.PayloadTypeAck {
		r.log.Debug("ignoring non-ACK multipart", "innerType", frag.InnerType)
		return
	}

	inner := &codec.Packet{
		Header: (pkt.PayloadVersion() << codec.PHVerShift) |
			(frag.InnerType << codec.PHTypeShift) | pkt.RouteType(),
		TransportCodes: pkt.TransportCodes,
		PathLen:        pkt.PathLen,
		PathHashSize:   pkt.PathHashSize,
		Path:           append([]byte(nil), pkt.Path...),
		Payload:        frag.Data,
		SNR:            pkt.SNR,
	}
	r.HandlePacket(inner, src)
}

// handleDirectForward processes a direct-routed packet with hop count >= 1.
func (r *Router) handleDirectForward(pkt *codec.Packet, src transport.PacketSource) {
	// Early ACK receive: resolve any direct ACK we hear, even if we are not the
	// next hop and even if forwarding is disabled. Firmware calls onAckRecv before
	// the next-hop / allowPacketForward checks, so a node that merely overhears an
	// ACK routed through a neighbor can resolve its own pending ACK early. Dedup
	// (run before this) ensures each unique ACK is resolved only once.
	if pkt.PayloadType() == codec.PayloadTypeAck {
		r.dispatchToApp(pkt, src)
	}

	info := pkt.PathInfo()
	hashSize := int(info.HashSize)

	// Check if we are the next hop (first hashSize bytes of path)
	if len(pkt.Path) < hashSize {
		return
	}
	if !r.cfg.SelfID.IsHashMatch(pkt.Path[:hashSize]) {
		// Not our hop — drop (an ACK was already resolved above)
		return
	}

	if !r.cfg.ForwardPackets {
		// We match, but forwarding is disabled — drop
		return
	}

	// ACK special case: already resolved above; forward a fresh copy at highest
	// priority.
	if pkt.PayloadType() == codec.PayloadTypeAck {
		removeSelfFromPath(pkt)
		r.forwardAck(pkt)
		return
	}

	// Remove ourselves from the path
	removeSelfFromPath(pkt)

	if pkt.HopCount() == 0 {
		// We were the last relay hop. The next node to receive this is the
		// final destination (identified by dest_hash in the payload, not path).
		// Forward it out with empty path.
	}

	r.enqueue(pkt, PriorityDirect, 0, src, false)
}

// forwardAck creates a new ACK packet from the forwarded packet's payload
// and queues it at the highest priority. This matches the firmware's
// routeDirectRecvAcks() behavior where new packets are created rather than
// retransmitting the original.
//
// The full payload is copied verbatim. Since v1.16 plain text-message ACKs are
// 6 bytes (hash + attempt + random); truncating to the 4-byte hash would both
// drop those bytes and change the packet hash used for deduplication along the
// path.
func (r *Router) forwardAck(pkt *codec.Packet) {
	if len(pkt.Payload) < codec.AckSize {
		return
	}

	ackPkt := &codec.Packet{
		Header:       pkt.Header,
		PathLen:      pkt.PathLen,
		PathHashSize: pkt.PathHashSize,
		Path:         make([]byte, len(pkt.Path)),
		Payload:      make([]byte, len(pkt.Payload)),
	}
	if pkt.HasTransportCodes() {
		ackPkt.TransportCodes = pkt.TransportCodes
	}
	copy(ackPkt.Path, pkt.Path)
	copy(ackPkt.Payload, pkt.Payload)

	r.enqueue(ackPkt, PriorityDirect, 0, 0, true)
}

// handleFlood processes a flood-routed packet.
func (r *Router) handleFlood(pkt *codec.Packet, src transport.PacketSource) {
	// Dispatch to app first — the app may decrypt the packet and call
	// MarkDoNotRetransmit() to suppress forwarding.
	r.dispatchToApp(pkt, src)

	// Forwarding decision
	r.routeFloodForward(pkt, src)
}

// routeFloodForward decides whether to re-broadcast a flood packet.
// This corresponds to the firmware's Mesh::routeRecvPacket().
func (r *Router) routeFloodForward(pkt *codec.Packet, src transport.PacketSource) {
	if !r.cfg.ForwardPackets {
		return
	}
	if pkt.IsMarkedDoNotRetransmit() {
		return
	}

	info := pkt.PathInfo()
	if int(info.HopCount)+1 > r.cfg.MaxFloodHops {
		return
	}
	// Tiered flood caps (firmware flood.max.unscoped / flood.max.advert).
	// These mirror the >= semantics of the MaxFloodHops check above.
	if pkt.RouteType() == codec.RouteTypeFlood && int(info.HopCount)+1 > r.cfg.MaxUnscopedFloodHops {
		return
	}
	if pkt.PayloadType() == codec.PayloadTypeAdvert && int(info.HopCount)+1 > r.cfg.MaxAdvertFloodHops {
		return
	}

	// Region policy: drop floods the RegionMap does not permit forwarding.
	if !r.regionAllowsFlood(pkt) {
		return
	}

	if r.cfg.LoopDetect > LoopDetectOff {
		selfHash := r.cfg.SelfID.HashN(int(info.HashSize))
		if detectLoop(pkt.Path, selfHash, int(info.HashSize), r.cfg.LoopDetect) {
			return
		}
	}

	// Clone the packet before modifying path for forwarding.
	// The original was already dispatched to the app.
	fwd := pkt.Clone()

	// Append our N-byte hash to the path
	selfHash := r.cfg.SelfID.HashN(int(info.HashSize))
	fwd.Path = append(fwd.Path, selfHash...)

	// Update wire byte: same mode, hop count + 1
	newInfo := codec.PathInfo{HashSize: info.HashSize, HopCount: info.HopCount + 1}
	fwd.PathLen = newInfo.ToWireByte()

	// Firmware uses hop count as priority for flood forwarding:
	// closer sources get lower (better) priority.
	r.enqueue(fwd, uint8(newInfo.HopCount), 0, src, false)
}

// regionAllowsFlood applies the RegionMap policy to a flood packet's forwarding
// decision, mirroring the firmware's filterRecvFloodPacket + allowPacketForward:
// a scoped TRANSPORT_FLOOD is forwarded only if it matches a flood-permitting
// region, and an unscoped FLOOD is forwarded only if the wildcard permits flood.
// With no RegionMap configured, all floods are allowed.
func (r *Router) regionAllowsFlood(pkt *codec.Packet) bool {
	rm := r.cfg.RegionMap
	if rm == nil {
		return true
	}
	if pkt.RouteType() == codec.RouteTypeTransportFlood {
		return rm.FindMatch(pkt, RegionDenyFlood) != nil
	}
	// Unscoped RouteTypeFlood: gated by the wildcard region's flood flag.
	return rm.Wildcard().Flags&RegionDenyFlood == 0
}

// dispatchToApp calls the registered application packet handler.
func (r *Router) dispatchToApp(pkt *codec.Packet, src transport.PacketSource) {
	r.mu.RLock()
	handler := r.onPacket
	r.mu.RUnlock()

	if handler != nil {
		handler(pkt, src)
	}
}

// broadcastToTransports sends a packet to all registered transports except the
// one identified by excludeSource. This prevents echoing a packet back to the
// transport it arrived on.
//
// A radio is never excluded: repeating over the air means sending on the radio
// the packet arrived on, like the firmware with its single radio. The radio
// doesn't hear its own transmissions, and dedup drops echoes from neighbors.
func (r *Router) broadcastToTransports(pkt *codec.Packet, excludeSource transport.PacketSource) {
	r.mu.RLock()
	entries := make([]transportEntry, len(r.transports))
	copy(entries, r.transports)
	r.mu.RUnlock()

	for _, entry := range entries {
		if entry.source == excludeSource && excludeSource != transport.PacketSourcePHY {
			continue
		}
		if !entry.transport.IsConnected() {
			continue
		}
		if err := entry.transport.SendPacket(pkt); err != nil {
			r.log.Warn("failed to send packet",
				"transport", entry.source, "error", err)
		} else {
			r.counters.PacketsSent.Add(1)
		}
	}
}

// SendFlood prepares and sends a packet in flood mode.
// The path is cleared, the packet is marked as seen (to prevent loopback),
// and it is sent to all connected transports.
func (r *Router) SendFlood(pkt *codec.Packet) {
	// Set flood route type, preserving payload type and version bits
	pkt.Header = (pkt.Header &^ codec.PHRouteMask) | codec.RouteTypeFlood

	hashSize := r.cfg.PathHashMode + 1
	pkt.PathLen = codec.PathInfo{HashSize: hashSize, HopCount: 0}.ToWireByte()
	pkt.PathHashSize = hashSize
	pkt.Path = nil

	// Mark before enqueue, so a loop-back copy is recognized as a duplicate.
	r.dedup.MarkSeen(pkt)

	r.counters.SentFlood.Add(1)
	r.enqueue(pkt, PriorityFloodData, 0, 0, true)
}

// SendTrace broadcasts a TRACE packet. TRACE routing is driven by the relay
// hashes embedded in the payload, and PathLen is a raw hop counter (0 at the
// origin) rather than a wire path byte — so unlike other sends the wire path is
// left empty and the counter is zeroed.
func (r *Router) SendTrace(pkt *codec.Packet) {
	pkt.Header = (pkt.Header &^ codec.PHRouteMask) | codec.RouteTypeDirect
	pkt.PathLen = 0
	pkt.PathHashSize = r.cfg.PathHashMode + 1
	pkt.Path = nil

	r.dedup.MarkSeen(pkt)

	r.counters.SentDirect.Add(1)
	r.enqueue(pkt, PriorityTrace, 0, 0, true)
}

// SetSendScope sets the region send scope used by SendFloodScoped and
// SendFloodPathScoped. Pass a key from TransportKeyFromRegion. A null key
// disables scoping (equivalent to ClearSendScope).
func (r *Router) SetSendScope(key TransportKey) { r.cfg.SendScope = key }

// ClearSendScope disables region scoping for outbound flood traffic.
func (r *Router) ClearSendScope() { r.cfg.SendScope = TransportKey{} }

// SendScope returns the current region send scope (null key if unscoped).
func (r *Router) SendScope() TransportKey { return r.cfg.SendScope }

// SendFloodScoped sends a flood packet scoped to the configured send scope.
// When no scope is set (SendScope is null) it is identical to SendFlood.
// Otherwise the packet is sent as a TRANSPORT_FLOOD carrying transport codes,
// restricting propagation to repeaters that recognize the region.
//
// This mirrors the firmware's BaseChatMesh::sendFloodScoped(), which routes all
// user-originated flood traffic (messages, ACKs, path returns, responses)
// through the home-region scope. Adverts remain unscoped.
func (r *Router) SendFloodScoped(pkt *codec.Packet) {
	r.sendScopedFlood(pkt, PriorityFloodData, 0)
}

// SendFloodPathScoped is the scoped counterpart of SendFloodPath: a PATH packet
// sent at PriorityFloodPath after PathSendDelay, scoped to the send scope when
// one is set.
func (r *Router) SendFloodPathScoped(pkt *codec.Packet) {
	r.sendScopedFlood(pkt, PriorityFloodPath, PathSendDelay)
}

// SendFloodReply floods a reply, scoping it to match the request in origPkt
// (see ResolveReplyScope). Use this instead of SendFloodScoped whenever the
// packet answers a received request, so the reply reaches the requester through
// the same region the request came from.
//
// Mirrors the firmware's sendFloodReply.
func (r *Router) SendFloodReply(pkt *codec.Packet, origPkt *codec.Packet) {
	r.sendFloodWithScope(pkt, r.ResolveReplyScope(origPkt), PriorityFloodData, 0)
}

// SendFloodPathReply is the PATH counterpart of SendFloodReply: a PATH return
// scoped to match the request that prompted it.
func (r *Router) SendFloodPathReply(pkt *codec.Packet, origPkt *codec.Packet) {
	r.sendFloodWithScope(pkt, r.ResolveReplyScope(origPkt), PriorityFloodPath, PathSendDelay)
}

// SendFloodWithKey floods a packet scoped to an already-resolved key, for
// callers that resolved the reply scope earlier (see event.ReplyContext). A
// zero key sends un-scoped.
func (r *Router) SendFloodWithKey(pkt *codec.Packet, key [16]byte) {
	r.sendFloodWithScope(pkt, TransportKey(key), PriorityFloodData, 0)
}

// SendFloodPathWithKey is the PATH counterpart of SendFloodWithKey.
func (r *Router) SendFloodPathWithKey(pkt *codec.Packet, key [16]byte) {
	r.sendFloodWithScope(pkt, TransportKey(key), PriorityFloodPath, PathSendDelay)
}

// sendScopedFlood is the shared implementation for the scoped flood senders.
// With a null scope it produces an ordinary RouteTypeFlood packet; with a scope
// set it produces a RouteTypeTransportFlood packet whose transport_codes[0] is
// the scope's code for this packet and transport_codes[1] is 0 (matching the
// firmware, which reserves [1] as the reply-region hint, currently unused).
func (r *Router) sendScopedFlood(pkt *codec.Packet, priority uint8, delay time.Duration) {
	r.sendFloodWithScope(pkt, r.cfg.SendScope, priority, delay)
}

// sendFloodWithScope is sendScopedFlood with the scope supplied by the caller,
// used by the reply senders to mirror a request's scope instead of this node's
// configured send scope.
func (r *Router) sendFloodWithScope(pkt *codec.Packet, scope TransportKey, priority uint8, delay time.Duration) {
	scoped := !scope.IsNull()

	routeType := uint8(codec.RouteTypeFlood)
	if scoped {
		routeType = codec.RouteTypeTransportFlood
	}
	pkt.Header = (pkt.Header &^ codec.PHRouteMask) | routeType

	hashSize := r.cfg.PathHashMode + 1
	pkt.PathLen = codec.PathInfo{HashSize: hashSize, HopCount: 0}.ToWireByte()
	pkt.PathHashSize = hashSize
	pkt.Path = nil

	if scoped {
		// Transport codes hash the payload type and payload only, so they are
		// stable regardless of the route-type change above.
		pkt.TransportCodes[0] = scope.CalcTransportCode(pkt)
		pkt.TransportCodes[1] = 0
	}

	// Mark before enqueue, so a loop-back copy is recognized as a duplicate.
	r.dedup.MarkSeen(pkt)

	r.counters.SentFlood.Add(1)
	r.enqueue(pkt, priority, delay, 0, true)
}

// SendFloodAsRelay floods a packet as though this node had already relayed it
// once: the origin path is seeded with this node's own hash and the hop count
// starts at 1, instead of the empty zero-hop path a normal origin produces.
//
// Downstream nodes then see the packet as having passed through this repeater,
// with subsequent relays appending their hashes as usual. Loop detection keys on
// the self-hash exactly as it would for a genuinely-forwarded packet, so a copy
// that loops back is dropped. Region scoping is applied like SendFloodScoped;
// the transport code hashes only the payload, so seeding the path does not
// change it.
//
// This suits a bridge injecting traffic from another network: the packet did
// pass through this node, so presenting it as a one-hop relay reflects reality
// better than a zero-hop origin (which implies an endpoint device authored it).
func (r *Router) SendFloodAsRelay(pkt *codec.Packet) {
	scope := r.cfg.SendScope
	scoped := !scope.IsNull()

	routeType := uint8(codec.RouteTypeFlood)
	if scoped {
		routeType = codec.RouteTypeTransportFlood
	}
	pkt.Header = (pkt.Header &^ codec.PHRouteMask) | routeType

	hashSize := r.cfg.PathHashMode + 1
	pkt.PathHashSize = hashSize
	pkt.Path = r.cfg.SelfID.HashN(int(hashSize))
	pkt.PathLen = codec.PathInfo{HashSize: hashSize, HopCount: 1}.ToWireByte()

	if scoped {
		pkt.TransportCodes[0] = scope.CalcTransportCode(pkt)
		pkt.TransportCodes[1] = 0
	}

	// Mark before enqueue, so a loop-back copy is recognized as a duplicate.
	r.dedup.MarkSeen(pkt)

	r.counters.SentFlood.Add(1)
	r.enqueue(pkt, PriorityFloodData, 0, 0, true)
}

// SendDirect prepares and sends a packet in direct routing mode.
// The path is set to the provided route, and the packet is marked as seen.
func (r *Router) SendDirect(pkt *codec.Packet, path []byte) {
	pkt.Header = (pkt.Header &^ codec.PHRouteMask) | codec.RouteTypeDirect

	hashSize := r.cfg.PathHashMode + 1
	hopCount := uint8(0)
	if hashSize > 0 && len(path) > 0 {
		hopCount = uint8(len(path) / int(hashSize))
	}
	pkt.PathLen = codec.PathInfo{HashSize: hashSize, HopCount: hopCount}.ToWireByte()
	pkt.PathHashSize = hashSize
	pkt.Path = make([]byte, len(path))
	copy(pkt.Path, path)

	r.dedup.MarkSeen(pkt)

	r.counters.SentDirect.Add(1)
	r.enqueue(pkt, PriorityDirect, 0, 0, true)
}

// SendFloodPath prepares and sends a PATH packet in flood mode with a delay.
// PATH packets use a lower priority than data floods and are delayed to let
// the original request propagate first. This matches the firmware's
// createPathReturn() sending behavior.
func (r *Router) SendFloodPath(pkt *codec.Packet) {
	pkt.Header = (pkt.Header &^ codec.PHRouteMask) | codec.RouteTypeFlood

	hashSize := r.cfg.PathHashMode + 1
	pkt.PathLen = codec.PathInfo{HashSize: hashSize, HopCount: 0}.ToWireByte()
	pkt.PathHashSize = hashSize
	pkt.Path = nil

	r.dedup.MarkSeen(pkt)

	r.counters.SentFlood.Add(1)
	r.enqueue(pkt, PriorityFloodPath, PathSendDelay, 0, true)
}

// SendZeroHop prepares and sends a packet as a zero-hop direct packet.
// These packets are not forwarded by relays (path is empty).
func (r *Router) SendZeroHop(pkt *codec.Packet) {
	pkt.Header = (pkt.Header &^ codec.PHRouteMask) | codec.RouteTypeDirect

	hashSize := r.cfg.PathHashMode + 1
	pkt.PathLen = codec.PathInfo{HashSize: hashSize, HopCount: 0}.ToWireByte()
	pkt.PathHashSize = hashSize
	pkt.Path = nil

	r.dedup.MarkSeen(pkt)

	r.counters.SentDirect.Add(1)
	r.enqueue(pkt, PriorityDirect, 0, 0, true)
}

// broadcastToAllTransports sends a packet to every connected transport.
// Used for outbound packets originated by this node (no source to exclude).
func (r *Router) broadcastToAllTransports(pkt *codec.Packet) {
	r.mu.RLock()
	entries := make([]transportEntry, len(r.transports))
	copy(entries, r.transports)
	r.mu.RUnlock()

	for _, entry := range entries {
		if !entry.transport.IsConnected() {
			continue
		}
		if err := entry.transport.SendPacket(pkt); err != nil {
			r.log.Warn("failed to send packet",
				"transport", entry.source, "error", err)
		} else {
			r.counters.PacketsSent.Add(1)
		}
	}
}

// removeSelfFromPath removes the first hash entry from the packet's path,
// shifting all remaining bytes left. The hash size is determined from the
// packet's path encoding. This is called when this node is the next hop
// in a direct-routed packet.
func removeSelfFromPath(pkt *codec.Packet) {
	info := pkt.PathInfo()
	if info.HopCount == 0 {
		return
	}

	hashSize := int(info.HashSize)
	// Shift path left by hashSize bytes
	copy(pkt.Path, pkt.Path[hashSize:])
	pkt.Path = pkt.Path[:len(pkt.Path)-hashSize]

	// Decrement hop count, preserve hash mode
	newInfo := codec.PathInfo{HashSize: info.HashSize, HopCount: info.HopCount - 1}
	pkt.PathLen = newInfo.ToWireByte()
}
