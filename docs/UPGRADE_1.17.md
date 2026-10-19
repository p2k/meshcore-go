# Upgrading meshcore-go to firmware v1.17.1

Analysis of upstream `meshcore-dev/MeshCore` between `companion-v1.16.0` (the
previous Go baseline) and `companion-v1.17.1`, and what it means for this repo.

Local firmware checkout: `~/Projects/PlatformIO/MeshCore`, now at `d9296435`
(`* version 1.17.1`).

## Headline: the wire protocol did not change

- `FIRMWARE_VER_CODE` is still **13**, same as v1.16.0.
- No new or changed mesh payload types. `Packet.h`'s only diff is a comment typo
  (`SNI` to `SNR`).
- No new or renumbered companion serial `CMD_*` constants. `CMD_SEND_RAW_PACKET = 65`
  is still the maximum, and it was already there in v1.16.0 (`FEATURE_PARITY.md`
  already tracks it).
- `MAX_FRAME_SIZE`, packet hash, ACK layout, region format: all unchanged.

So nothing here breaks compatibility. 367 commits landed, but the large majority are
display drivers, board/pin fixes, and nRF52 hardware crypto. The crypto work
(`USE_CC310_HW_CRYPTO` in `Utils.cpp` / `Identity.cpp`) is hardware acceleration
behind an ifdef and produces byte-identical output.

The v1.17.0 to v1.17.1 patch is 47 commits, essentially all board pin corrections
plus one routing fix (below).

## Items that actually affect this repo

Five findings, ordered by how much they matter here.

> **Status:** all five items are done. See "Remaining work" at the end for the
> commit list and what was deliberately left out.

### 1. Dedup split into `wasSeen()` / `markSeen()` — we already did this

Upstream split `MeshTables::hasSeen()` (which both tested and inserted) into separate
`wasSeen()` and `markSeen()` calls across every receive path in `Mesh.cpp`, and
reworked `SimpleMeshTables` to match.

This is the same problem commit `a738607` fixed here ("capture before dedup"), arrived
at independently. Our fix moved the observer hook ahead of the dedup gate; upstream
instead made the check non-mutating so the mark can be placed deliberately. Same
outcome for observers.

Our `PacketDeduplicator.HasSeen()` (`core/dedupe/dedupe.go:58`) is still the combined
check-and-mark, with send-side call sites discarding the return value to use it as a
bare mark. That works, but it now reads differently from firmware and the "call it and
ignore the result" idiom is easy to misread.

**Done** (`3178c50`). Added `WasSeen` (pure query) and `MarkSeen` (record only), and
converted the eight send-side mark-only call sites. The receive gate keeps the combined
`HasSeen`: it needs check-and-record as one step, and the whole receive path runs under
`recvMu`, so it is equivalent to firmware's pair without a second lock acquisition.
No behavior change; race detector clean.

### 2. `path_len` validation on the inner PATH payload — real gap

`Mesh.cpp` now rejects a bad `path_len` on the *decrypted* `PAYLOAD_TYPE_PATH` content:

```cpp
uint8_t path_len = data[k++];
if (!Packet::isValidPathLen(path_len)) {
  break;   // reject bad encoding
}
```

`Packet::isValidPathLen` (`src/Packet.cpp:13`) rejects reserved hash mode 3
(`hash_size == 4`) and enforces `hash_count * hash_size <= MAX_PATH_SIZE` (64).

We validate this on the **outer** packet header (`core/codec/packet.go:229-241`,
including `ErrReservedPathMode`) but **not** on the inner decrypted PATH content.
`ParsePathContent` (`core/codec/payload.go:465`) only bounds-checks against the
buffer, so a `path_len` of `0xFF` (mode 3, 63 hops, 252 bytes) is accepted whenever
the decrypted blob is long enough — and decrypted content can exceed 64 bytes since
`MaxPacketPayload` is 184.

The unvalidated wire byte then propagates: it is stored verbatim as a contact's
`OutPathLen` with the raw bytes as `OutPath` (`device/contact/helpers.go:180`), reached
from `device/node/dispatch.go:303` and `device/room/dispatch.go:62`, mirrored into the
room client store, and persisted. A peer can poison a stored path this way.

`parseAnonReplyPath` (`device/node/repeater_anon.go:98`) has the same shape: a
client-supplied `{path_len}{path}` with only a buffer-length check, copied into
`rc.DirectPathLen` at `:63`.

**Done** (`c9ce9da`). Added `PathInfo.IsValid` mirroring `isValidPathLen` and called it
at both sites. Confirmed the gap was real before fixing: without the check, both a
reserved-mode and a 189-byte path parsed successfully (`error = <nil>`).

### 3. Scoped reply routing (PR #3106) — new firmware behavior we lack

New `src/helpers/RoutingPolicy.h` factors three decisions into testable helpers, used
by both the repeater and room server:

- `isFloodHopLimitExceeded(pkt, flood_max, flood_max_unscoped, flood_max_advert)`
- `chooseReplyRoute(inbound_is_flood, have_supplied_path, have_out_path)`
- `chooseReplyScope(request_scope_known, request_was_unscoped_flood, default_scope_known)`

Two of these are new behavior rather than refactoring:

**Reply scope.** Firmware now mirrors the request's transport scope onto a flooded
reply, falling back to a configured `default_scope`, and only sends unscoped when the
requester did. The motivating bug: an unscoped reply is dropped at hop 0 by any
repeater running `flood.max.unscoped=0`.

We have no equivalent. `Config.SendScope` (`device/router/router.go:129`) is a single
global; the inbound packet's `TransportCodes` are validated on receive
(`router.go:422`) but never read back to scope the reply, and there is no default-scope
fallback. Nothing in `device/room/respond.go` or `device/node/base.go` consults
`origPkt.TransportCodes`.

**Reply route.** `chooseReplyRoute` gives a DIRECT login the option of replying via a
stored `out_path`, which the repeater's anon path previously could not do. Our
precedence (`device/node/base.go:253`) is flood-arrival wins, then supplied path, then
stored path, then flood — which matches `chooseReplyRoute` for the flood and
supplied-path cases. The gap is the `REPLY_ROUTE_DIRECT_OUT_PATH` case for anon
requests.

**Done** (`315bf6a`), reply-scope half only. Added `Config.DefaultReplyScope`,
a `ChooseReplyScope` port, and `SendFloodReply`/`SendFloodPathReply`.

One implementation constraint shaped the design: the transport code is an HMAC over
the request's own payload, so it can only be matched against the original packet, not
reconstructed later from a stored code. The room server resolves from the packet
directly; `BaseNode` resolves in `buildReplyContext` while the request is in hand and
carries the resolved key on `ReplyContext`. `matchScopeKey` deliberately applies no
flag mask, since a region that denies forwarding is still the region a request arrived
on. `DefaultReplyScope` falls back to `SendScope`, so existing configs are unaffected.

The reply-route half (`REPLY_ROUTE_DIRECT_OUT_PATH` for anon requests) is still open;
our precedence already matches upstream for the flood and supplied-path cases.

Note our hop-limit semantics already match: firmware `hops >= flood_max` versus our
`hopCount + 1 > MaxFloodHops` are equivalent, and the check ordering is the same. The
`docs/cli_commands.md` note about `flood.max.unscoped` defaulting to `0xFF` ("tracks
flood.max until set") is not implemented — both roles still initialize it to 64, same
as our `DefaultMaxUnscopedFloodHops`.

### 4. UTF-8 aware advert name truncation

`AdvertDataBuilder::encodeTo` now truncates the node name at a valid UTF-8 boundary via
the new `mesh::validUtf8PrefixLength` (`src/helpers/UTF8Helpers.h`), instead of copying
bytes until the buffer fills.

Ours truncates the assembled buffer with a raw byte slice
(`core/codec/builder.go:120-126`, `data = data[:MaxAdvertAppDataSize]`) and can split a
rune. The name sits at the tail of the buffer, so the budget shrinks to 19 bytes with
GPS plus both feature fields — a mid-rune cut is reachable with emoji or non-Latin
names.

This is cosmetic on the wire (both sides sign whatever bytes result, so signatures
still verify) but a node with an emoji name would advertise a mangled trailing byte and
render as a replacement character in clients. Firmware documents the new budget as
"23 bytes when location is included and 31 bytes otherwise."

**Done** (`8e1f0fc`). The name is now truncated to the remaining budget at a rune
boundary before the buffer is sized, and `FlagHasName` is only set when something
survives (matching firmware's `name_len > 0` guard). Verified the resulting budgets are
31 bytes for a name alone and 23 with location, exactly as the firmware docs state.

`truncateName` in `device/router/region.go:385` was checked and deliberately left
alone. Firmware copies region names with a byte-wise `StrHelper::strncpy`
(`RegionMap.cpp:167`) and did *not* get the UTF-8 treatment in v1.17 — only advert
names did. Making the Go side rune-aware would diverge from firmware in a fixed-width
binary field that has to round-trip through the `/regions2` format.

### 5. `MAX_ANON_CONTACTS` — we model this differently

Firmware reserved the first 8 slots of the contacts array for transient ANON_REQ
contacts (`MAX_ANON_CONTACTS = 8`), added `getTotalContactSlots()` versus
`getNumContacts()`, and made `ContactsIterator` start past the reserved region.
`onAdvertRecv` now clears a temp anon slot when a real advert arrives for that key.

We already have `DefaultMaxAnonContacts = 8` (`device/contact/manager.go:12`), but
separate the pools **by node type at eviction time** rather than by array index —
`IsTransient()` tests `Type == NodeTypeNone`. Since our persisted format is per-contact
JSON records, slot ordering does not matter for compatibility.

One behavioral difference is worth a look: our growth is gated by `MaxContacts` alone
(`manager.go:333`), so the anon headroom is never populated by the append path. Once
`len == MaxContacts`, a transient add must evict an existing transient and returns nil
if there is none, rather than using one of the 8 reserved slots.

**Action:** verify that nil-return path is intentional. Consider adopting the
firmware's "clear the anon slot when a real advert arrives for that pubkey" behavior,
which we may not do today. Low priority.

## Explicitly not actionable

- **`ConfigSerializer` / `NodePrefs` rewrite.** Prefs moved from a packed binary struct
  to a nested JSON-ish serializer with named keys. This is on-device flash storage, not
  wire format, and has no Go counterpart.
- **`radio.fem.rxgain` / `radio.fem.txgain` CLI keys and the `MeshCore.h` FEM virtuals.**
  Hardware PA/LNA control; N/A for a transport-attached node. The v1.17.1 patch even
  comments out their load/save until they can be set.
- **`cad` CLI key, `getCADEnabled()`.** Radio hardware; N/A for a transport-attached
  node. With the PHY transport (2026-10-18), CAD is one of the remaining RF gaps listed
  in `FEATURE_PARITY.md`.
- **MCU temperature in telemetry.** Firmware now adds `board.getMCUTemperature()` to
  self-telemetry. Our `telemetry.Provider` is pluggable and the host app populates it,
  so this is an app-level choice, not a library gap.
- **Display drivers, board pin fixes, nRF52 CC310 crypto.** Firmware-only.
- **`poweroff`/`shutdown` CLI commands.** Hardware; N/A.

### Ethernet and `MultiSerialInterface` — no port needed, but worth knowing

The drivers are firmware-only, but the interface they expose is not new to us.
`SerialEthernetInterface` runs a TCP server (port 5000) speaking **the companion frame
protocol, byte-for-byte identical to USB serial**: `'>'`/`'<'` marker, uint16-LE
length, no checksum. That is exactly what `core/codec/serial` implements and what
`device/companion`'s `ListenAndServe` already serves, so the same phone-app client
works against a firmware node over Ethernet or a Go node over TCP. Nothing to port.

`MultiSerialInterface` is a composite over `BaseSerialInterface` (up to 4 of BLE, USB,
WiFi, Ethernet). Writes broadcast to every enabled interface; reads poll in
registration order and return the first frame, so slot order acts as a priority. The
separate `enableBluetooth`/`disableBluetooth` helpers let the CLI toggle BLE while
leaving wired transports up.

Two behaviors to keep in mind if you ever talk to an Ethernet-equipped node:

- **`FRAME_QUEUE_SIZE` is 4.** `writeFrame` returns 0 when the queue is full, and the
  queue drains one frame per `checkRecvFrame` call, so a burst of pushes can silently
  drop frames.
- **`ETHERNET_RAW_LINE=1`** is a build-time flag that replaces framing with CRLF
  delimited lines for raw CLI use. A node built that way does not speak the frame
  protocol at all, so frame-protocol clients cannot talk to it.

## Remaining work

All five items are done, each in its own commit:

| Item | Commit |
|---|---|
| Inner-PATH `path_len` validation | `c9ce9da` |
| UTF-8 advert name truncation | `8e1f0fc` |
| Dedup `WasSeen`/`MarkSeen` split | `3178c50` |
| Anon contact pool | `add9f49` |
| Scoped reply routing | `315bf6a` |

Two transport bugs were fixed along the way, both pre-existing and unrelated to 1.17:

| Fix | Commit |
|---|---|
| Nil-logger panic on the transport receive paths | `306fd26` |
| `readLoop`/`Stop` data race (UDP and serial) | `a82ae2e` |

The nil-logger panic had been masking the race: once `TestProcessDatagram_Malformed`
stopped crashing, `-race` surfaced an unguarded read of `t.conn`/`t.port` that `Stop`
nils under the mutex. The whole suite now passes under `-race`.

Deliberately left open:

- **`REPLY_ROUTE_DIRECT_OUT_PATH`** for anon requests, the reply-route half of PR
  #3106. Our precedence already matches upstream for the flood and supplied-path cases.
- **Clearing an anon slot when a real advert arrives** for that pubkey, which firmware
  added in v1.17 (`onAdvertRecv`).
- **`truncateName`** in `device/router/region.go`, left byte-wise on purpose. See
  item 4.

## Unrelated: firmware checkout state

The fast-forward to `d9296435` succeeded. Your local modification to
`variants/heltec_v3/platformio.ini` (enabling `MESH_PACKET_LOGGING`) was preserved.

Upstream **deleted** `.vscode/extensions.json`, which you had also modified — your only
change was re-adding a comment header, no content difference. That deletion is still
unresolved in the working tree (`DU` state) because `git rm` was blocked by a
permission prompt. Resolve with:

```
git rm .vscode/extensions.json
git stash drop        # the pre-merge stash, no longer needed
```
