# meshcore-go

A Go implementation of the MeshCore protocol for mesh networking over LoRa radios.

## Overview

This library provides Go packages for building MeshCore mesh networking applications:

- **Core protocol** - Packet encoding/decoding, routing, crypto
- **Device implementations** - Room server, repeater, companion node
- **Transports** - Serial (RS232), MQTT, UDP multicast, LoRa radio (PHY)

## Packages

### core/codec

Packet encoding/decoding with support for:

- All payload types (advert, text, direct, ack, trace, etc.)
- RS232 framing with Fletcher-16 checksums
- Path hashing

### core/crypto

Encryption and authentication:

- AES-CCM for packet encryption
- Ed25519 for signing
- Group encryption

### device

Device role implementations:

- **room** - Room server (mesh routing hub)
- **node** - Repeater and companion node logic
- **contact** - Contact list management
- **router** - Packet routing with loop detection

### transport

Network transports:

- **serial** - RS232 serial connection
- **mqtt** - MQTT bridge for extending networks
- **phy** - LoRa radio attached to the host, e.g. a SX1262 via periph.io

## Usage

```go
import (
    "github.com/kabili207/meshcore-go/core/codec"
    "github.com/kabili207/meshcore-go/transport/serial"
)

cfg := serial.Config{
    Port: "/dev/ttyUSB0",
    BaudRate: 115200,
}
tr := serial.New(cfg)
```

The MQTT transport speaks to firmware forks that add MQTT bridging to MeshCore repeaters. The forks use different payload formats, so pick the one your repeaters run:

| `Framing` | Firmware | Payload | Default topic |
|-----------|----------|---------|---------------|
| `mqtt.FramingBridge` (default) | [xJARiD/MeshCore-EastMesh](https://github.com/xJARiD/MeshCore-EastMesh) | magic + Fletcher-16 + packet, XORed with `Secret` | `meshcore/bridge/packets` |
| `mqtt.FramingRaw` | [vrybdpkt/MeshCore](https://github.com/vrybdpkt/MeshCore) | bare packet bytes | `meshcore/bridge` |

For EastMesh, `Secret` is the repeater's `bridge.secret`. That firmware's bridge connects over plain TCP only, so the broker needs a non-TLS listener for the repeaters even if this side uses `UseTLS`.

The PHY transport drives a SX126x LoRa radio attached to the host, e.g. a Raspberry Pi HAT or a USB-to-SPI stick, through `periph.io/x/devices/v3/sx126x`. It configures the radio, keeps an airtime budget like the firmware, and queues packets so sending never blocks:

```go
tr := phy.New(phy.Config{
    Radio:     dev, // *sx126x.Dev, set up for your board
    Configure: phy.ConfigureSX126x(dev),
    Params:    phy.RadioParams{Frequency: 915000000, Bandwidth: 250000, SpreadingFactor: 11, CodingRate: 5, TxPower: 22},
})
```

`go run ./examples/companion -phy meshstick` (or `-phy waveshare-hat`) runs a companion node on such a radio. Pay attention to the regulations of your region, e.g. frequencies, TX power and duty cycle (`DutyCycle`, `-duty-cycle`).

## Protocol

MeshCore is a lightweight mesh routing protocol for LoRa radios. See [meshcore.io](https://meshcore.io) for the full protocol specification.

## Resources

- Firmware: [meshcore-dev/MeshCore](https://github.com/meshcore-dev/MeshCore)