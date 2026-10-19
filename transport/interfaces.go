// Package transport provides transport interfaces and implementations for
// communicating over MeshCore networks.
package transport

import (
	"context"

	"github.com/kabili207/meshcore-go/core/codec"
)

// Transport is the base interface for all transport implementations.
type Transport interface {
	// Start begins the transport's connection and message handling.
	// The provided context controls the transport's lifetime.
	Start(ctx context.Context) error
	// Stop gracefully shuts down the transport.
	Stop() error
	// IsConnected returns true if the transport is currently connected.
	IsConnected() bool
	// SetPacketHandler sets the callback for incoming MeshCore packets.
	SetPacketHandler(fn PacketHandler)
	// SetStateHandler sets the callback for transport state changes.
	SetStateHandler(fn StateHandler)
	// SendPacket encodes and transmits a packet over the transport.
	SendPacket(packet *codec.Packet) error
}

// PacketHandler is called when a MeshCore packet is received.
type PacketHandler func(packet *codec.Packet, source PacketSource)

// StateHandler is called when the transport state changes.
type StateHandler func(transport Transport, event Event)

// Event represents transport state change events.
type Event int

const (
	// EventConnected is fired when the transport connects.
	EventConnected Event = iota
	// EventDisconnected is fired when the transport disconnects.
	EventDisconnected
	// EventReconnecting is fired when the transport is attempting to reconnect.
	EventReconnecting
	// EventError is fired when an error occurs.
	EventError
)

func (e Event) String() string {
	switch e {
	case EventConnected:
		return "connected"
	case EventDisconnected:
		return "disconnected"
	case EventReconnecting:
		return "reconnecting"
	case EventError:
		return "error"
	default:
		return "unknown"
	}
}

// PacketSource indicates where a packet originated from.
type PacketSource int

const (
	// PacketSourceMQTT indicates the packet came from MQTT.
	PacketSourceMQTT PacketSource = iota
	// PacketSourceSerial indicates the packet came from a serial connection.
	PacketSourceSerial
	// PacketSourceLocal indicates the packet was originated by this node (TX).
	PacketSourceLocal
	// PacketSourceUDP indicates the packet came from a UDP multicast connection.
	PacketSourceUDP
	// PacketSourcePHY indicates the packet came from a physical LoRa chip
	PacketSourcePHY
)

func (s PacketSource) String() string {
	switch s {
	case PacketSourceMQTT:
		return "mqtt"
	case PacketSourceSerial:
		return "serial"
	case PacketSourceLocal:
		return "local"
	case PacketSourceUDP:
		return "udp"
	case PacketSourcePHY:
		return "phy"
	default:
		return "unknown"
	}
}
