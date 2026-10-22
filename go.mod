module github.com/kabili207/meshcore-go

go 1.26.0

require (
	filippo.io/edwards25519 v1.2.0
	github.com/TheThingsNetwork/go-cayenne-lib v1.2.0
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/kabili207/slog-helper v0.1.0
	go.bug.st/serial v1.6.4
	golang.org/x/crypto v0.50.0
	golang.org/x/net v0.53.0
	periph.io/x/conn/v3 v3.7.2
	periph.io/x/devices/v3 v3.7.4
	periph.io/x/host/v3 v3.8.5
)

require (
	github.com/MatusOllah/slogcolor v1.7.0 // indirect
	github.com/creack/goselect v0.1.3 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/systemd/slog-journal v0.1.2 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

// The sx126x driver from the sx126x branch of a periph fork, until it is
// merged and released (https://github.com/p2k/periph-devices/tree/sx126x).
// Update with:
//   go mod edit -replace=periph.io/x/devices/v3=github.com/p2k/periph-devices/v3@sx126x && go mod tidy
replace periph.io/x/devices/v3 => github.com/p2k/periph-devices/v3 v3.0.0-20261022140114-ec5965a1df04
