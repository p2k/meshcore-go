# meshcore-go Feature Parity vs Official Firmware

Compares meshcore-go against the official MeshCore C++ firmware, role by role, plus
the shared base layer.

## Version baseline

- **Go (this repo):** wire-compatible with `FIRMWARE_VER_CODE 13`.
- **Firmware compared:** `~/Projects/PlatformIO/MeshCore` at `d9296435`
  (`companion-v1.17.1`). `FIRMWARE_VER_CODE` is still 13, unchanged since v1.16.0, so
  the wire baseline did not move. See `UPGRADE_1.17.md` for the v1.16.0 to v1.17.1
  delta and what it required.

Everything below was re-verified against the code on 2026-08-16. Claims here have a
history of drifting behind the implementation; when in doubt, trust the code and fix
this file.

Two structural facts shape everything:

1. The Go node is **transport-attached** (MQTT / serial / UDP / PHY). Since
   2026-10-18 the `transport/phy` transport drives a LoRa radio attached to the host
   (a SX126x via `periph.io/x/devices/v3/sx126x`), so radio tuning, TX power and the
   airtime rules now have a target when that transport is used. GPS, display/UI, and
   sleep remain **N/A by design**, not gaps.
2. The firmware companion is a **phone bridge**: it exposes a serial/BLE frame
   protocol, and the real chat client is a phone app. Go implements **both** sides —
   `device/node` is the mesh/chat engine, and `device/companion` is a frame-protocol
   server a phone app can connect to over TCP or any stream.

## Roles at a glance

| Role | Firmware | Go | Overall parity |
|---|---|---|---|
| Companion / chat | `companion_radio` | `device/node/companion.go` + `device/companion` | Strong, both mesh and frame protocol |
| Repeater | `simple_repeater` | `device/node/repeater.go` | Strong; no airtime/CSMA |
| Room server | `simple_room_server` | `device/room/*` | Strong |
| Sensor | `simple_sensor` | none | Wire format + telemetry only |
| KISS modem | `kiss_modem` | none | Missing entirely |
| Secure chat demo | `simple_secure_chat` | (companion covers it) | Reference app, not a gap |

---

## Shared / base layer

**Full parity:** packet codec (header bits, payload types, variable-width path hashes,
transport-code ordering), crypto (Ed25519 identity, Curve25519 ECDH, AES-128-ECB,
2-byte truncated HMAC MAC), flood routing gate + self-hash append + hop-priority, path
append/remove/reverse, region map including the exact `/regions2` binary format,
transport keys (Go implements the private-region keystore that firmware stubs as a
hardware TODO), trace forwarding, multipart ACK handling, packet dedup.

**Airtime / duty-cycle / CSMA: implemented in `transport/phy` (2026-10-18), with
gaps.** Firmware's `Dispatcher` runs a token-bucket airtime budget (~50% duty cycle,
1-hour window), defers TX when budget is low, does CAD/channel sensing, applies
SNR-weighted RX flood delay (better-SNR nodes rebroadcast first), and randomizes flood
rebroadcast jitter. The router still drains its queue every 10ms
(`DefaultDrainInterval`) and almost always sends with `delay=0`, which is right for
MQTT/serial/UDP. The RF rules live in the PHY transport instead, so only the radio is
paced:

- **Airtime budget:** token bucket over a 1-hour window like `Dispatcher`, starting full
  and refilled at the duty cycle; TX waits while the budget is below half the airtime
  of a maximum-size packet. `Config.DutyCycle` is `1/(1+af)`, default 0.5 (firmware
  af 1.0); `SetDutyCycle` changes it at runtime.
- **Flood rebroadcast jitter:** random delay up to `5 * tx_delay_factor * airtime` for
  forwarded floods (hop count > 0), `tx_delay_factor` 0.5 as in firmware
  (`MyMesh::getRetransmitDelay`). `SendPacket` queues and returns immediately.
- **Not talking over a reception:** the SX126x driver postpones a transmission while a
  packet with a valid LoRa header is being received, until it is complete. Implicit
  headers are not covered.
- **Radio settings:** preamble per spreading factor like `preambleLengthForSF` (32
  symbols up to SF8, 16 above), low data rate optimization for symbols of 16 ms or
  longer, MeshCore's private sync word. `SetRadioParams` reconfigures at runtime,
  pausing RX/TX meanwhile.
- **RF to RF repeating:** the router no longer excludes `PacketSourcePHY` when
  rebroadcasting, so a packet heard on the radio is forwarded on the radio.

Still missing:

- **Direct-forward jitter** (`direct_tx_delay_factor`, firmware default 0.2): the
  transport cannot tell a forwarded direct packet from an originated one. For the same
  reason, `SendFloodAsRelay` packets get flood jitter as if they were forwards.
- **SNR-weighted RX delay** (`calcRxDelay`): not implemented; the companion firmware
  ships it disabled (`rx_delay_base` 0).
- **CAD and the RSSI interference threshold** (`int.thresh`), and **noise floor
  sampling**, so `NoiseFloor` in radio stats stays 0.
- **Busy-channel cap:** firmware retries every 200ms and gives up waiting after 4s;
  the driver waits until the packet being received is complete.

**Reply scoping** now matches firmware v1.17: a flooded reply mirrors the request's
transport scope, falls back to `Config.DefaultReplyScope`, and only goes un-scoped
when the requester did (`device/router/replypolicy.go`).

**Tiered flood caps** (`MaxFloodHops` / `MaxUnscopedFloodHops` / `MaxAdvertFloodHops`,
defaults 64/64/8) are a parity match as of v1.17, which added the same three tiers in
`RoutingPolicy.h`. Go had them first; this is no longer a Go-only extra.

**Shared CLI:** firmware's `CommonCLI` (~25 commands, ~40 config keys, used by all
roles) maps to a shared `device/cli` dispatcher that the room and repeater both mount.
The config/admin subset is implemented on both (get/set keys, `get acl`, `setperm`,
`clock`, `time`/`reboot` via opt-in callbacks, `region`, `stats-*`, flood caps,
`owner.info`, `advert.interval`, `multi.acks`); the repeater adds `advert`,
`advert.zerohop`, `neighbors`, `neighbor.remove`, and `discover.neighbors`. The
remaining firmware commands are hardware/radio (gps, sensor, log, ota, tempradio,
bridge.*, pwrmgt.*, radio tuning) and stay N/A. `stats-radio` returns `unsupported` in
both roles, also with a PHY transport, whose `Stats()` the CLI does not read yet.

---

## Companion / chat node

Two layers, both implemented.

### Mesh / chat engine (`device/node`)

**Full:** contact management (add/update/remove/search/favorites/eviction, plus a
transient anon pool), DM send (plain/CLI/signed, chunking, direct-vs-flood, attempt
encoding), DM receive + decrypt, ACK matching via `ack.Tracker`, path learning (flood
to direct), advert processing, channel/group messages (GRP_TXT and GRP_DATA), client
login + keep-alive to repeaters and room servers, telemetry and status requests
(`SendTelemetryReq`, `SendStatusReq`), path reset / path discovery / trace initiation,
auto-ACK piggyback and multi-acks.

**Divergences:**
- Go defaults `MaxContacts` to 32 (firmware 100), plus 8 anon slots, so 40 total.
- `CMD_SEND_SELF_ADVERT` returns OK without actually sending an advert
  (`device/companion/companion.go`). Recurring adverts correctly default to disabled
  to match firmware, so a companion currently has no working way to advertise on
  demand through the frame protocol.
- `CompanionNode.Run` sends a flood advert on every start, while the companion
  firmware only advertises when the user asks. Over a PHY transport that is a
  mesh-wide flood per restart.

### Frame protocol server (`device/companion`)

A phone app can connect to a Go node today. `ListenAndServe` runs a TCP listener and
`Serve` works over any `io.ReadWriter` (a pty, for instance). The framing is the same
`'>'`/`'<'` + uint16-LE-length format the firmware uses on USB serial and, as of
v1.17, on Ethernet — so the same client code works against either.

**Implemented:** 35 dispatch arms covering 38 `CMD_*` codes, an offline message queue
drained by `CMD_SYNC_NEXT_MESSAGE`, and unsolicited pushes to connected sessions.
Unknown opcodes return `ERR_CODE_UNSUPPORTED_CMD`.

**Divergence worth fixing:** the offline queue is **unbounded**. Firmware uses a fixed
16-slot ring. A Go node that never has an app connect will grow the queue without
limit.

**Not dispatched** (23 constants defined, no handler). Hardware-bound and N/A:
`CmdReboot`, `CmdFactoryReset`, `CmdSetDevicePin`, `CmdGetAllowedRepeatFreq`,
`CmdSetOtherParams`. Real gaps, roughly in priority order:

- `CmdSendChannelData`, `CmdSendControlData`, `CmdSendBinaryReq`, `CmdSendAnonReq`,
  `CmdSendPathDiscoveryReq` — send paths `BaseNode` already implements internally, just
  not reachable from a connected app
- `CmdSetPathHashMode`, `CmdSetDefaultFloodScope`, `CmdSetFloodScopeKey` — config the
  router supports but an app cannot set
- `CmdLogout`, `CmdHasConnection` — session lifecycle
- `CmdSignStart` / `CmdSignData` / `CmdSignFinish` — message-signing sessions
- `CmdSendRawData`, `CmdSendRawPacket` — raw packet injection
- `CmdSetCustomVar` — the getter is implemented, the setter is not
- `CmdExportPrivateKey` / `CmdImportPrivateKey` — key portability; worth a deliberate
  decision rather than a default, since it moves private keys over the wire

**Radio prefs:** `CmdSetRadioParams`, `CmdSetRadioTxPower`, and `CmdSetTuningParams`
are stored and reported back. With the optional `Config.OnRadioParams` / `OnTxPower` /
`OnTuningParams` callbacks they are applied first and rejected if that fails;
`examples/companion -phy` wires them to a PHY transport (the airtime factor sets its
duty cycle, the RX delay is ignored) and fills the radio part of `GET_STATS` (last
RSSI/SNR, TX/RX airtime) from `phy.Stats()`. `Identity.AirtimeFactor` sets what
`GET_TUNING_PARAMS` reports.

**N/A by design:** BLE transport itself (Go speaks the framing, not GATT),
BLE-pin/GPS/buzzer prefs.

---

## Repeater

Data plane and control plane both in good shape.

**Full:** flood forwarding, direct forwarding (self-hash match, path pop, ACK
re-creation), loop detection (thresholds `{4,2,1}/{2,1,1}/{1,1,1}` match), region map +
region CLI, transport keys / scoped flood, self-advert scheduling, packet counters,
dedup, TRACE forwarding, tiered flood caps.

**Control plane:** admin login + ACL, the shared `device/cli` surface (admin-gated
`TXT_TYPE_CLI`), `GET_STATUS` / `GET_ACCESS_LIST` / `GET_NEIGHBOURS` / `GET_TELEMETRY`
as REQ types, owner-info / regions / clock as direct-routed ANON_REQ types
(`repeater_anon.go`), a dedicated neighbor table with per-neighbor SNR and heard
timestamps, node discovery (`repeater_discover.go`), and rate limiting
(`ratelimit.go`, mirroring firmware's `RateLimiter`: 4-per-2min for discovery,
4-per-3min for anon requests).

**Persistence:** opt-in via hooks. `OnRegionsChanged` / `OnSettingChanged` plus
`SetConfig` / `LoadConfig` round-trip node settings, and `ACLPersistence` persists the
access list. Nothing is persisted unless the host app wires these up.

**RF:** with a PHY transport the repeater gets the airtime budget, flood rebroadcast
jitter, and RF to RF forwarding (see the shared-layer note). **Missing:** direct-forward
jitter, CAD / interference threshold, and radio stats: `stats-radio` stays
`unsupported` and the radio fields of `GET_STATUS` stay zero, as the repeater has no hook
yet to feed `phy.Stats()` in.

**Behavioral note:** the Go repeater sends a flood advert on boot
(`advertSched.SendNow(true)`); firmware sends zero-hop.

---

## Room server

**Full / close:** sync loop (round-robin, 6s post-sync delay, 3-failure cap),
GET_TELEMETRY (guest to base-only), GET_ACCESS_LIST (admin-only), push to logged-in
clients with correct authorship (`SIGNED_PLAIN` carrying the author's 4-byte pubkey
prefix and original timestamp), adverts, path management, ACK receive, active-path
replay protection, open-room posting rights, 52-byte `ServerStats`, and a keep-alive
that is direct-only, honors `forceSince`, and appends the unsynced-count byte.

**Single dispatch path.** The legacy `HandlePacket` switch was removed; the event path
(`device/room/handlers.go`, driven by `RoomNode.dispatchToServer`) is the only one. The
room tests now drive that path through a shim (`device/room/eventbridge_test.go`) that
reproduces `BaseNode`'s packet-to-event conversion, since `device/room` cannot import
`device/node`. If a room test passes while the real node misbehaves, suspect that shim
first.

**Still open:**
- On-demand `advert` CLI command. `RoomNode` does build an advert scheduler and wire
  interval hooks; only the CLI command binding is missing.
- `setperm` cannot add a brand-new admin by pubkey — it searches existing clients and
  returns `ERR: client not found`. The repeater has the same limitation.
- `MemoryClientStore` defaults to RAM-only; persistence is opt-in via
  `WithPersistence`.

---

## Sensor role (mostly missing)

Firmware `SensorMesh` is a full node: self-telemetry (battery, and as of v1.17 MCU
temperature), 14 environment sensor chip families, GPS/NMEA location, full Cayenne LPP
codec, a time-series ring buffer with min/max/avg, telemetry request/response with a
3-tier permission mask, and threshold alerts with ACK tracking.

Go has the wire format plus telemetry plumbing: `NodeTypeSensor = 0x04` so adverts
parse, and both the room server and repeater answer `GET_TELEMETRY` via the shared
`telemetry.Provider` (the host app populates a CayenneLPP encoder, gated by the
firmware permission mask), using go-cayenne-lib.

Still missing: the sensor node type itself, drivers, GPS, time-series storage
(`ReqTypeGetMinMaxAvg = 0x04` is defined but handled nowhere), and alerts.

---

## KISS modem (missing entirely)

Firmware `KissModem` is a raw radio pipe: KISS TNC serial framing (FEND/FESC), raw
packet in/out, a CSMA TX state machine, and a `SETHARDWARE` sub-protocol (~26
crypto/radio ops). `grep` for kiss/tnc across the Go tree returns zero hits. The
companion protocol's `CmdSendRawData` is unrelated (app-frame packet injection, not a
KISS TNC).

---

## Bottom line

- Wire protocol / crypto / codec: near-complete parity.
- Companion: both halves are real. The mesh engine is a solid DM/channel client, and
  the frame-protocol server means a phone app can drive a Go node over TCP. Bound the
  offline queue and make `CMD_SEND_SELF_ADVERT` actually advertise.
- Repeater: good forwarder with a working admin/config/stats surface, rate limiting,
  discovery, and opt-in persistence.
- Room server: functionally close to firmware, on a single dispatch path.
- Sensor and KISS modem: not started.
- Radio: a LoRa transport exists (`transport/phy`) with the firmware's airtime budget,
  flood jitter, and radio settings the companion app can change. Remaining RF gaps:
  direct-forward jitter, CAD / interference threshold / noise floor, and the flood
  advert a companion sends on every start.
