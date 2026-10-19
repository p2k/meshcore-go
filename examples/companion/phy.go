package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/kabili207/meshcore-go/device/companion"
	phytransport "github.com/kabili207/meshcore-go/transport/phy"
	"periph.io/x/conn/v3/gpio/gpioreg"
	"periph.io/x/conn/v3/physic"
	"periph.io/x/conn/v3/spi"
	"periph.io/x/conn/v3/spi/spireg"
	"periph.io/x/devices/v3/sx126x"
	"periph.io/x/host/v3"
	"periph.io/x/host/v3/gpioioctl"
)

// openRadio sets up the SX1262 of a supported board and returns it along with
// a function closing its SPI connection.
func openRadio(board string) (*sx126x.Dev, func(), error) {
	if _, err := host.Init(); err != nil {
		return nil, nil, fmt.Errorf("initializing periph: %w", err)
	}
	var (
		dev  *sx126x.Dev
		conn spi.PortCloser
		err  error
	)
	switch board {
	case "meshstick":
		dev, conn, err = openMeshstick()
	case "waveshare-hat":
		dev, conn, err = openWaveshareHAT()
	default:
		return nil, nil, fmt.Errorf("unknown radio board %q, want meshstick or waveshare-hat", board)
	}
	if err != nil {
		return nil, nil, err
	}
	dev.Reset()
	return dev, func() { conn.Close() }, nil
}

// openMeshstick sets up the Elecrow Meshstick, a USB stick with a CH341
// USB-to-SPI bridge and a SX1262 with TCXO.
//
// It needs the CH341 kernel driver from
// https://github.com/frank-zago/ch341-i2c-spi-gpio and a spidev device on CS0
// of its SPI controller, see the sx126x package's Meshstick example.
func openMeshstick() (*sx126x.Dev, spi.PortCloser, error) {
	// The numbers of the SPI controller and the GPIO chip depend on what else
	// is present on the system, so look them up.
	spis, err := filepath.Glob("/sys/bus/platform/devices/ch341-spi.*/spi_master/spi*")
	if err != nil || len(spis) == 0 {
		return nil, nil, errors.New("meshstick SPI controller not found")
	}
	var chip *gpioioctl.GPIOChip
	for _, c := range gpioioctl.Chips {
		if strings.HasPrefix(c.Label(), "ch341-gpio.") {
			chip = c
			break
		}
	}
	if chip == nil {
		return nil, nil, errors.New("meshstick GPIO chip not found")
	}
	conn, err := spireg.Open("/dev/spidev" + strings.TrimPrefix(filepath.Base(spis[0]), "spi") + ".0")
	if err != nil {
		return nil, nil, fmt.Errorf("opening meshstick SPI: %w", err)
	}

	// The CH341 can't generate interrupts on these pins, so the driver polls
	// DIO1 and BUSY.
	dev, err := sx126x.NewSPI(conn, chip.ByNumber(6), chip.ByNumber(4), chip.ByNumber(2), &sx126x.Opts{
		Variant:            sx126x.SX1262,
		Dio2AsRfSwitchCtrl: true,
		Dio3AsTcxoCtrl:     true,
		TcxoVoltage:        1800 * physic.MilliVolt,
		TcxoDelay:          5 * time.Millisecond,
		RxEn:               chip.ByNumber(1),
		RxBoostedGain:      true,
	})
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("setting up meshstick radio: %w", err)
	}
	return dev, conn, nil
}

// openWaveshareHAT sets up the WaveShare SX1262 868M/915M HAT for Raspberry
// Pi.
//
// Chip Select has to be configured to GPIO 21 by adding
// "dtoverlay=spi0-1cs,cs0_pin=21" and "dtparam=spi=on" to
// /boot/firmware/config.txt.
func openWaveshareHAT() (*sx126x.Dev, spi.PortCloser, error) {
	conn, err := spireg.Open("")
	if err != nil {
		return nil, nil, fmt.Errorf("opening HAT SPI: %w", err)
	}
	// GPIO 6 is called DIO4 and an active low TXEN in the HAT's documentation.
	// It drives the inverted input of the RF switch, so it acts as RxEn.
	dev, err := sx126x.NewSPI(conn, gpioreg.ByName("16"), gpioreg.ByName("20"), gpioreg.ByName("18"), &sx126x.Opts{
		Variant:            sx126x.SX1262,
		Dio2AsRfSwitchCtrl: true,
		RxEn:               gpioreg.ByName("6"),
	})
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("setting up HAT radio: %w", err)
	}
	return dev, conn, nil
}

// newPHY creates the phy transport for dev, which configures it with params.
func newPHY(dev *sx126x.Dev, params phytransport.RadioParams, dutyCycle float64) *phytransport.Transport {
	return phytransport.New(phytransport.Config{
		Radio:     dev,
		Configure: phytransport.ConfigureSX126x(dev),
		Params:    params,
		DutyCycle: dutyCycle,
		Logger:    slog.Default(),
	})
}

// wirePHY lets the app change the radio settings of tr and adds its radio
// statistics to GET_STATS.
func wirePHY(cfg *companion.Config, tr *phytransport.Transport) {
	cfg.Identity.AirtimeFactor = 1/tr.DutyCycle() - 1
	cfg.OnRadioParams = func(freqKHz, bwHz uint32, sf, cr uint8) error {
		p := tr.RadioParams()
		p.Frequency, p.Bandwidth, p.SpreadingFactor, p.CodingRate = freqKHz*1000, bwHz, sf, cr
		return tr.SetRadioParams(p)
	}
	cfg.OnTxPower = func(dBm int8) error {
		p := tr.RadioParams()
		p.TxPower = dBm
		return tr.SetRadioParams(p)
	}
	cfg.OnTuningParams = func(_, airtimeFactor uint32) error {
		// The RX delay is disabled in the companion firmware and not
		// implemented by phy, so only the airtime factor is applied.
		tr.SetDutyCycle(1 / (1 + float64(airtimeFactor)/1000))
		return nil
	}
	stats := cfg.Stats
	cfg.Stats = func() companion.Stats {
		var s companion.Stats
		if stats != nil {
			s = stats()
		}
		r := tr.Stats()
		s.LastRSSI = int8(max(min(math.Round(float64(r.LastRSSI)), math.MaxInt8), math.MinInt8))
		s.LastSNR = int8(max(min(math.Round(float64(r.LastSNR)*4), math.MaxInt8), math.MinInt8))
		s.TxAirSecs = uint32(r.TxAirtime / time.Second)
		s.RxAirSecs = uint32(r.RxAirtime / time.Second)
		return s
	}
}
