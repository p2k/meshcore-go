# meshcore-go Parity Plan

Prioritized plan to close the gaps found in `docs/FEATURE_PARITY.md`.

**Status (2026-08-16): 20 of 24 items are done.** Only Phase 5 (airtime / duty-cycle /
CSMA) and Phase 6 (sensor role, KISS modem) remain open. Phases 1 through 4 are
complete. The v1.17.1 firmware upgrade is tracked separately in `UPGRADE_1.17.md`.

Four correctness bugs found during the original analysis were fixed before this plan
was written (room stats size, open-room posting rights, active-path replay protection,
companion nil-contacts panic); those are recorded in the parity doc and not repeated
here.

Effort tags: **S** = a few hours, **M** = a day or two, **L** = multi-day.
"N/A (radio)" marks firmware features that only make sense on a node driving a LoRa
radio directly. meshcore-go is transport-attached (MQTT/serial/UDP); since 2026-10-18
the `transport/phy` transport drives a radio attached to the host, so these apply to
nodes using it (see Phase 5).

## Phase 1: correctness and interop bugs

These are small, high-value, and mostly version-independent. Do them first.

1. **Forged-advert forwarding (S, security).** The router re-floods adverts before
   any signature check; `contact.ProcessAdvert` verifies at the app layer after the
   rebroadcast and never marks a bad advert do-not-retransmit. Verify the advert
   signature before forwarding, and drop invalid ones. Files: `device/router/router.go`
   (flood path), `device/contact/helpers.go`, `core/crypto/advert.go`.
   **DONE** (2026-07-03): `handleAdvert` verifies before forwarding and marks
   do-not-retransmit on failure (`device/node/dispatch.go`); regression test added.
2. **Room post authorship (M, interop).** Firmware pushes stored posts as
   `SIGNED_PLAIN` carrying the original author's 4-byte pubkey prefix and the post's
   original timestamp; Go re-sends plain content with no author. Also align the
   push-ACK keying (firmware keys by recipient client pubkey over the signed payload).
   Without this, posts from a Go room are unattributed on firmware clients. Files:
   `device/room/sync.go`, `device/room/post.go`, `device/room/handlers.go`.
   **DONE** (2026-07-03): `pushPostToClient` builds a fresh `SIGNED_PLAIN` payload
   (author prefix + original timestamp + random attempt) and keys the expected ACK by
   the client pubkey. The companion (`BaseNode.handleTxtMsg`) now auto-ACKs signed
   messages with a 4-byte hash keyed by its own pubkey, so Go-to-Go push ACKs resolve.
   Tests added on both sides (`TestPushPost_SignedWithAuthor`,
   `TestHandleTxtMsg_SignedAutoACK`).
3. **Overheard direct-ACK resolution (S).** Firmware resolves a direct ACK transiting
   through it even when it is not the next hop; Go only resolves hash-matching ACKs.
   Resolve ACKs seen in transit. Files: `device/router/router.go`, `device/ack`.
   **DONE** (2026-07-03): `handleDirectForward` dispatches (resolves) any direct ACK
   before the next-hop / forwarding checks. Test added
   (`TestHandlePacket_DirectAckOverheard`).
4. **Multipart reassembler audit (S).** Confirm multipart ACK fragments are handled
   as independent complete ACKs (firmware model), not concatenated by the general
   `Reassembler`. Fix or document. Files: `core/multipart/multipart.go`, node dispatch.
   **DONE** (2026-07-03): confirmed the bug. `router.handleMultipart` now reconstructs
   the self-contained inner ACK and redispatches it through the normal pipeline
   (dedup + ACK handling) instead of concatenating; the `Reassembler` is deprecated
   and no longer used. Test rewritten to the correct model.
5. **Packet validation nits (S).** Reject reserved path-hash mode 3 in
   `PathInfoFromWireByte`; enforce the 32-byte advert appdata cap before signing.
   Files: `core/codec/pathinfo.go`, `core/codec/builder.go`, `core/crypto/advert.go`.
   **DONE** (2026-07-03): `Packet.ReadFrom` rejects reserved mode 3
   (`ErrReservedPathMode`); `BuildAdvertAppData` caps to `MaxAdvertAppDataSize` (32).
   Tests added for both.
6. **Room keep-alive parity (S).** Append the unsynced-count byte to the keep-alive
   ACK, honor `forceSince`, and restrict to direct route. Files: `device/room/handlers.go`.
   **DONE** (2026-07-03): event-path keep-alive now direct-only, honors `forceSince`
   (also fixing the ACK hash, which previously omitted it), and appends the
   unsynced-post count via a new `NodeSender.SendACKPayload`. Tests added.
7. **Login response polish (S).** Fill the random blob in the login response
   (`resp[8:12]`) for packet-hash uniqueness, and reset `out_path` on a flood-routed
   login. Files: `device/room/login.go`.
   **DONE** (2026-07-03): both login-response builders fill `resp[8:12]` with random
   bytes; the event-path login resets a stale direct path on a flood login. Test added.
8. **v1.16 constant catch-up (S).** Add `CmdSendRawPacket = 65` to the serial
   constants; note the `PAYLOAD_TYPE_GRP_DATA` (0x06) inner-format change
   (`data_type(uint16), data_len, blob`) for whenever group-data support lands.
   Files: `core/codec/serial/protocol.go`.
   **DONE** (2026-07-03): `CmdSendRawPacket = 65` added.

## Phase 2: companion completeness (full chat client)

Make `CompanionNode` a complete chat client, not just a DM client.

1. **Channel / group messages (M).** Wire the existing `crypto/group.go` into receive
   (decrypt instead of emitting raw ciphertext) and add a `SendChannelText` send path.
   Needs a channel-key store keyed by channel hash. Files: `device/node/dispatch.go`
   (`handleGrpTxt`/`handleGrpData`), `device/node/send.go`, new channel store.
   **DONE** (2026-07-03): `BaseNode` gained a channel-key store (`channel.go`:
   `AddChannel`/`RemoveChannel`, keyed by channel hash) plus `SendChannelText` /
   `SendChannelData`. `handleGrpTxt`/`handleGrpData` now look up the key, decrypt
   (MAC-verified), and emit the real message/data; messages on unregistered channels
   are dropped. The companion registers the built-in "Public" channel by default and
   exposes `AddChannel`/`SendChannelText`. Tests cover receive, unknown-channel drop,
   and send round-trip.
2. **Client-side login + keep-alive (M).** Add a helper to send an ANON_REQ login to a
   repeater/room server and manage the keep-alive session (the server side already
   exists in `device/room`). Files: new `device/node` client-login helper, reuse
   `device/connection`.
   **DONE** (2026-07-03): `CompanionNode.SendLogin` (`companion_login.go`) builds the
   ANON_REQ login with the firmware format (room adds `sync_since`, others don't) and
   sends direct/flood. A login-OK RESPONSE fires a new `event.LoginResponse` (server +
   permissions) and registers the server in a `connection.Manager`. `SendKeepAlive`
   sends a keep-alive REQ and refreshes liveness on the ACK; a keep-alive loop runs in
   `Run`, and `connection.Manager` gained `Peers()`. Tests: end-to-end companion→
   repeater login, login-response event, keepalive packet.
3. **Telemetry request helper (S).** `SendTelemetryReq(to)` building a
   `ReqTypeGetTelemetry` request; surface the response via an event. Files: `device/node/send.go`.
   **DONE** (2026-07-03): `CompanionNode.SendTelemetryReq` (`companion_request.go`)
   sends a GET_TELEMETRY REQ (mask 0 = request all); the reply is correlated by tag
   and surfaced as a new `event.TelemetryResponse` carrying the raw CayenneLPP bytes.
   Tests cover the request/response round-trip and unmatched-tag rejection.
4. **Path/trace send helpers (S-M).** `ResetPath`, `SendPathDiscovery`, and trace
   initiation (`BuildTracePayload` exists; add the send + response correlation).
   Files: `device/node`, `device/router/trace.go`.
   **DONE** (2026-07-03): `CompanionNode.ResetPath` clears a contact's out_path;
   `SendPathDiscovery` sends a base-telemetry request over forced flood (the PATH
   return re-establishes the route); `SendTrace` broadcasts a TRACE along a relay-hash
   route via a new `Router.SendTrace` (PathLen is a raw hop counter, not a wire byte).
   Completed traces surface as a new `event.TraceReceived` (tag + per-hop SNRs) via a
   `PayloadTypeTrace` case in `processPacket`. Tests cover all four.
5. **Contact persistence (M).** Provide a default persistent `ContactStore`
   (the interface exists; ship a file or pluggable-backend implementation) so shared
   secrets survive restart. Files: `device/contact/store.go` + new impl.
   **DONE** (2026-07-03): an opt-in `ContactPersistence` backend (`Load`/`Save`/
   `Delete`) on `ContactManager` (`ManagerConfig.Persistence`): the manager seeds
   itself from `Load` at construction and mirrors add/update/remove mutations to the
   backend, keeping the in-memory fast read path. Shipped `FileContactStore` (JSON,
   debounced + atomic writes, `Close()` to flush). Design informed by mesh-bridge and
   multi-mesh-bbs, which use their own DBs + advert-driven persistence, so this is
   opt-in and doesn't disrupt them. Tests cover round-trip, missing file, seed-on-
   restart (incl. shared-secret derivation), and delete.
6. **Auto-ACK piggyback + multi-acks (S).** Piggyback the ACK into the path-return for
   flood-received DMs, and support extra ACK transmits. Files: `device/node/base.go`,
   `device/node/dispatch.go`.
   **DONE** (2026-07-03): `handleTxtMsg` now piggybacks the ACK into the path-return
   for flood-received plain DMs (one packet that both teaches the sender the return
   path and acks the message — previously Go sent a standalone ACK and never returned
   the path, so senders stayed flood-only). Direct DMs still get a standalone ACK, plus
   optional redundant multipart ACKs via `ExtraAckTransmits` (firmware multi_acks,
   default 0). Tests cover flood-piggyback (verifying the embedded ACK), direct
   standalone, and multi-acks.

**Phase 2 status:** all six items done. Remaining companion gaps (the serial/BLE
frame protocol, offline message queue, radio prefs, message-signing) are N/A by design
for a headless transport-attached node — see `docs/FEATURE_PARITY.md`. (2026-10-18:
radio prefs are now applied through optional callbacks when a PHY transport is used.)

## Phase 3: repeater control plane

The data plane is already solid. Add the admin/config/stats surface, reusing the
shared ACL machinery.

**Foundation DONE** (2026-07-03): extracted a shared `device/acl` package
(`Client` + role helpers, `Store`/`MemoryStore` with non-admin LRU eviction, a
configurable `Authenticator` for password->permission). The room server was
migrated onto it: `room.ClientInfo` embeds `acl.Client`, and `resolvePermissions`
delegates to `acl.Authenticator`. All tests green (17 packages). The repeater items
below build on `device/acl`.

1. **Admin login + ACL (M).** Wire an `acl.Store` + `acl.Authenticator`
   (admin/guest passwords, guest->GUEST, no open access) onto the repeater via
   `BaseNode`'s `AnonRequestReceived` pipeline; gate CLI/requests by permission.
   Files: `device/node/repeater*.go`, `device/acl`.
   **DONE** (2026-07-03): `RepeaterConfig` gained `AdminPassword`/`GuestPassword`/
   `MaxClients`; `RepeaterNode` holds an `acl.MemoryStore` + `acl.Authenticator` and
   handles ANON_REQ logins (`repeater_admin.go`): password auth, replay check, ACL +
   contact registration, flood out_path reset, and a 13-byte login response. Also
   fixed the nil-`Contacts` panic (same bug the companion had). Tests cover
   admin/guest/wrong-password/replay. Permission gating of CLI/requests lands with
   items 2-4.
2. **Request/response handler (M).** Answer `GET_STATUS` (repeater stats: the 56-byte
   `RepeaterStats` including `total_rx_air_time_secs` + `n_recv_errors`), plus
   `GET_NEIGHBOURS` and `GET_OWNER_INFO`. Router counters already exist. Files:
   `device/node/repeater.go`, new repeater stats struct.
   **DONE (partial)** (2026-07-03): `RepeaterStats` (56-byte struct + MarshalBinary)
   and the request handler are in (`repeater_stats.go`, `repeater_request.go`),
   dispatched from `RequestReceived` and gated on ACL membership. `GET_STATUS`
   (any authenticated client; filled from router counters + uptime, radio fields
   zero) and `GET_ACCESS_LIST` (admin only) are done with tests. `GET_NEIGHBOURS`
   waits on the neighbor table (item 3). `GET_TELEMETRY` is answered via the shared
   `telemetry.Provider` (see below). Owner-info/regions/clock are ANON_REQ handlers,
   not REQ types. **DONE** (2026-07-03): implemented in `repeater_anon.go` (direct-
   routed only, `anon_limiter` 4/3min, owner reply reuses `owner.info`); see the
   Phase 3 item 4 note.
   **Telemetry provider DONE** (2026-07-03): a shared `device/telemetry` package
   defines `Provider` (`QuerySensors(mask, cayennelpp.Encoder)`), the `TELEM_PERM_*`
   mask bits + `ChannelSelf`, and `Mask`/`Encode` helpers (guests → base only,
   firmware's `~payload[1]` semantics). Both the repeater (`RepeaterConfig.Telemetry`)
   and room (`ServerConfig.Telemetry`) answer `GET_TELEMETRY` with it; the room's old
   bytes-based `TelemetryProvider` was migrated onto it. Encoding uses
   `github.com/TheThingsNetwork/go-cayenne-lib`. Tests cover mask/encode, admin-all vs
   guest-base, and the no-provider path.
3. **Neighbor table (M).** Add SNR and heard-timestamp tracking (either extend
   `ContactInfo` or a dedicated neighbor table), zero-hop/repeater-only filtering, and
   the sorted/paginated `GET_NEIGHBOURS` reply + `neighbor.remove`. Files:
   `device/contact` or new `device/router/neighbors.go`.
   **DONE** (2026-07-03): dedicated `neighborTable` (`repeater_neighbors.go`) with
   SNR + advert/heard timestamps and LRU eviction; records only zero-hop, non-share,
   repeater-type adverts (via the `AdvertReceived` event). `GET_NEIGHBOURS`
   (`repeater_request.go`) sorts by newest/oldest/strongest/weakest and paginates
   (count/offset), returning `[neighbours_count][results_count]` + `[prefix][heard_ago]
   [snr]` entries. This also closes the `GET_NEIGHBOURS` piece deferred from item 2.
   Tests cover recording filters, eviction, and the sorted request. `neighbor.remove`
   is a CLI command (item 4).
4. **Rate limiters + node discovery (M).** Port the discover/anon `RateLimiter` and
   `CTL_TYPE_NODE_DISCOVER` request/response. Files: `device/node/repeater.go`.
   **DONE** (2026-07-03): `rateLimiter` primitive (`ratelimit.go`, firmware
   RateLimiter semantics). Node discovery (`repeater_discover.go`): CONTROL packets
   arrive via the `PacketReceived` catch-all; the repeater answers `NODE_DISCOVER_REQ`
   for repeater filters with a zero-hop `RESP` (identity + inbound SNR), throttled by
   `discover_limiter` (4/2min). `SendNodeDiscover` originates a request and matching
   `RESP`s are recorded as neighbors. Tests cover respond/filter/rate-limit/record.
   The `anon_limiter` (4/3min) and the ANON_REQ regions/owner/clock handlers landed
   in `repeater_anon.go` (2026-07-03, direct-routed only). The CLI
   `setperm`/`neighbor.remove`/`discover.neighbors` commands landed in Phase 4's
   shared CLI (see the CLI review follow-up).

**Phase 3 status:** all four items done, plus the ANON_REQ regions/owner/clock
handlers (2026-07-03). Remaining repeater parity (radio config, pref persistence,
telemetry provider) is covered by Phase 6 and the N/A-radio items in
`docs/FEATURE_PARITY.md`.

## Phase 4: shared CLI and persistence framework

1. **Shared CLI dispatcher (L).** Firmware's `CommonCLI` is used by every role; Go
   only has a room-server-specific CLI. Extract a shared command dispatcher (get/set
   config keys, `reboot`, `advert`, `clock`, `password`, `stats`, `neighbors`, `region`)
   that room and repeater both mount. Files: new `device/cli` package, refactor
   `device/room/cli.go`.
   **DONE** (2026-07-03): `device/cli` package — a registry `Dispatcher` (register
   `ConfigKey` get/set closures + `CommandFunc`s + fallback + after-set hook) that
   owns `get`/`set`/command parsing. The room server migrated onto it (behavior-
   identical, validated by new room CLI tests). The repeater now has a CLI too
   (`repeater_cli.go`): admin-gated `TXT_TYPE_CLI` handling with keys (name/lat/lon,
   path.hash.mode, loop.detect, flood.max, repeat, public.key, role) and commands
   (ver, clock, advert, advert.zerohop, neighbors, password, setperm, region). Shared
   `LoopDetectName`/`ParseLoopDetectLevel` + flood/forward accessors moved to
   `device/router`. Tests cover the room CLI and an end-to-end repeater admin CLI
   round-trip (plus non-admin denial). Radio-param keys (freq/bw/sf/cr) are stored/
   reported only on a transport-attached node.
   **CLI review follow-up** (2026-07-03): reviewed the full firmware `CommonCLI`
   surface and closed the meaningful gaps. Added to both roles: `get acl`,
   `flood.max.advert`/`flood.max.unscoped`, `owner.info`, `stats-packets/core/radio`,
   and `time`/`reboot` gated behind opt-in `OnSetClock`/`OnReboot` callbacks (a bridged
   node keeps its authoritative host clock, so client-driven clock override is never on
   by default). Room-only: `password` (change admin password) and `flood.max`, evening
   the room/repeater asymmetry. Repeater-only (needs its scheduler/BaseNode, which the
   room lacks): `advert.interval`, `flood.advert.interval`, `multi.acks`,
   `neighbor.remove`, `discover.neighbors`. New supporting API: router
   `Get/SetMaxAdvert/UnscopedFloodHops` + `CountersSnapshot.String()`, scheduler
   `LocalInterval`/`FloodInterval`, `BaseNode.Get/SetExtraAckTransmits`, and
   `neighborTable.remove`. `stats-core`/`stats-radio` report only tracked data (table
   sizes / uptime; radio is `unsupported`). Remaining firmware commands are all
   hardware/radio (gps, sensor, log, ota, tempradio, bridge.*, pwrmgt.*) and stay N/A.
2. **Persistence framework (M-L).** Pluggable persistence for contacts, ACL/clients,
   and prefs so configuration and admin lists survive restart (firmware persists to
   flash). Files: `device/contact`, `device/room/clientstore*.go`, new prefs store.
   **DONE** (2026-07-03): contacts via `contact.ContactPersistence` + `FileContactStore`
   (Phase 2); ACL/admin clients via `acl.Persistence` + `acl.FileStore` (opt-in
   `acl.WithPersistence`, admin-only like firmware's `/s_contacts`, debounced + atomic
   writes). The repeater exposes `RepeaterConfig.ACLPersistence`; `setperm` persists via
   `UpdateClient`. Prefs persistence is covered by the CLI `AfterSet` hook (the app
   writes changed keys to its own config), so no separate prefs store was needed.
   Tests: ACL round-trip, admin-only filtering, demote-drops-persistence, and an
   end-to-end admin-survives-repeater-restart.
   **Room client-store DONE** (2026-07-03): `MemoryClientStore` gained the same opt-in
   `WithPersistence(acl.Persistence)` (reusing `acl.FileStore`, since `ClientInfo`
   embeds `acl.Client`). Admins are mirrored + seeded on startup; the transient sync
   state (`SyncSince`/`PushFailures`) is not persisted and resets on restart (the
   client re-establishes it on its next login/keepalive). The app wires it at store
   construction (`ServerConfig.Clients` is already pluggable), so no server config
   change was needed. Tests cover round-trip, admin-only, demote, and remove.

## Phase 5: airtime / duty-cycle / CSMA

The largest systemic gap, but it only bites a node that fronts real RF. Firmware's
`Dispatcher` runs a token-bucket airtime budget (~50% duty cycle), CAD/channel
sensing, SNR-weighted RX flood delay, and randomized rebroadcast jitter. For
meshcore-go this is **N/A (radio)** while it only bridges MQTT/serial. If a LoRa
transport is ever added:

1. **Rebroadcast jitter + airtime budget (L).** Add randomized flood forward delay and
   a per-window airtime accounting/deferral in the router send loop. Files:
   `device/router/router.go`, `device/router/queue.go`.
2. **Channel sensing hook (M).** Extend the `transport.Transport` interface with an
   optional channel-activity/airtime method radios can implement. Files:
   `transport/interfaces.go`.

**Phase 5 status** (2026-10-18): done for the airtime budget and flood jitter, in
`transport/phy` rather than the router send loop. The router sends every packet to all
transports, and MQTT/UDP must not be paced by RF rules, so pacing belongs to the
transport that drives the radio. The PHY transport keeps a token-bucket budget over a
1-hour window like `Dispatcher`, delays forwarded floods by up to
`5 * tx_delay_factor * airtime`, and queues so `SendPacket` never blocks the router.
Channel sensing lives in the SX126x driver instead of a `transport.Transport` hook: it
postpones a transmission while a packet with a valid header is being received. The
router no longer excludes `PacketSourcePHY` when rebroadcasting, so RF to RF repeating
works. Left: direct-forward jitter (the transport cannot tell forwards apart; a router
hint would fix it and stop `SendFloodAsRelay` getting flood jitter), SNR-weighted RX
delay (disabled by default in the companion firmware), CAD / RSSI interference
threshold / noise floor, and the firmware's 4s cap on waiting for a busy channel.

## Phase 6: missing roles

1. **Sensor role.** Firmware's `SensorMesh` is a repeater plus a telemetry/history/
   alerts layer: it forwards packets, has ACL/login/CLI/self-advert/regions/
   `GET_TELEMETRY` (all already shared in Go), advertises as `NodeTypeSensor`, and
   adds time-series storage, min/max/avg queries, and condition-triggered alerts.
   Hardware drivers/GPS stay N/A; for a Go node "sensor" means exposing app-supplied
   data (an API, DB metric, HA entity) as MeshCore telemetry with queryable history.

   **Phase 6a (MVP, in progress scope):** everything except alerts.
   - `TimeSeriesData` ring buffer (fixed-interval float samples) + `MinMaxAvg` — a
     direct port of firmware's `TimeSeriesData.cpp`. Pure Go, fully unit-testable.
   - LPP type metadata (`size`/`multiplier`/`signed` per LPP type) + a `PutValue`
     encoder. **Needed because go-cayenne-lib exposes neither the per-type metadata
     nor the `Voltage`/`Current`/`Power`/`Altitude` types firmware uses.** Home:
     `device/telemetry` (it already owns CayenneLPP concerns).
   - `GET_MINMAXAVG (0x04)` handler: request `start_secs_ago(4)+end_secs_ago(4)+
     res1+res2`; reply `tag(4)+now(4)+[channel,lpp_type,min,max,avg]…`, each value
     encoded via the LPP metadata (MSB-first, scaled). Gated at read-only+.
   - `SensorNode` assembly: reuse the repeater's shared scaffolding (BaseNode, acl,
     cli, advert, telemetry, ANON_REQ handlers, forwarding), advertise
     `NodeTypeSensor`, wire a `SeriesProvider` callback for min/max/avg. Open design
     point: reuse `RepeaterNode` via a node-type knob vs. a parallel type reusing the
     shared packages; will maximize reuse and extract shared request-dispatch if the
     duplication is non-trivial.
   - Self-read loop: a ticker firing an app `OnRead` callback (firmware's
     `onSensorDataRead`) so the app records samples into its `TimeSeriesData`.
   - Files: new `device/node/sensor*.go`, `device/telemetry/lpp.go`.

   **Phase 6b (alerts, deferred):** condition-triggered `Trigger` state machine that
   pushes TXT alerts to clients holding the alert-subscription permission bits
   (`PERM_RECV_ALERTS_LO`=1<<6, `HI`=1<<7), with ACK-tracked retry and
   `MAX_CONCURRENT_ALERTS`, plus subscribe-on-login. Self-contained; larger.
2. **KISS modem (M, optional).** KISS TNC serial framing + raw packet passthrough.
   Niche; only build if a raw-modem use case appears. Files: new `device/kiss`.

## Suggested ordering

Phase 1 in full first (small, corrects on-air/interop behavior). Then pick the track
that matches the immediate use case: Phase 2 if the priority is a chat client, Phase 3
if it is a repeater. Phase 4 pays off once both room and repeater need CLIs. Phase 5's
remaining items matter for nodes using the PHY transport, mainly repeaters. Phase 6 is
only worth starting when a sensor/modem use case is on the table.
</content>
