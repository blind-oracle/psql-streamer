package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/blind-oracle/psql-streamer/db"
	"github.com/blind-oracle/psql-streamer/sink"
	"github.com/blind-oracle/psql-streamer/source"
	"github.com/spf13/viper"
)

// Config structure
var cfg struct {
	// Path to the config file if provided
	File string

	TickerInterval time.Duration

	Bolt struct {
		Path string
	}

	HTTP struct {
		// IP:port to listen on
		Listen string
	}
}

// Read & parse the config file
func configLoad() (err error) {
	if cfg.File == "" {
		return errors.New("Config file not specified")
	}

	viper.SetConfigFile(cfg.File)

	// Read & parse config
	if err = viper.ReadInConfig(); err != nil {
		return
	}

	if cfg.TickerInterval = viper.GetDuration("tickerInterval"); cfg.TickerInterval < 0 {
		return errors.New("tickerInterval should be >= 0")
	}

	// HTTP
	if cfg.HTTP.Listen = viper.GetString("http"); cfg.HTTP.Listen == "" {
		return errors.New("You need to specify http")
	}

	// BoltDB is only required for some sources - don't init it if unspecified
	// The sources that need it should complain if boltdb is nil
	if cfg.Bolt.Path = viper.GetString("boltdb"); cfg.Bolt.Path != "" {
		if boltdb, err = db.New(cfg.Bolt.Path); err != nil {
			return fmt.Errorf("Unable to init Bolt: %s", err)
		}
	}

	// Load sources
	srcNames := viper.GetStringMap("source")
	if len(srcNames) == 0 {
		return fmt.Errorf("No sources defined")
	}

	for sn := range srcNames {
		v := viper.Sub("source." + sn)
		db.SetHandleInViper(v, boltdb)

		s, err := source.Init(sn, v)
		if err != nil {
			return fmt.Errorf("Unable to init source '%s': %s", sn, err)
		}

		log.Printf("Source '%s' loaded", sn)
		sources[sn] = s
		commonObjs = append(commonObjs, s)
	}

	// Load sinks
	snkNames := viper.GetStringMap("sink")
	if len(snkNames) == 0 {
		return fmt.Errorf("No sinks defined")
	}

	for sn := range snkNames {
		v := viper.Sub("sink." + sn)
		db.SetHandleInViper(v, boltdb)

		s, err := sink.Init(sn, v)
		if err != nil {
			return fmt.Errorf("Unable to init sink '%s': %s", sn, err)
		}

		srcs := v.GetStringSlice("sources")
		if len(srcs) == 0 {
			return fmt.Errorf("Sink '%s': no sources defined", sn)
		}

		for _, snn := range srcs {
			src, ok := sources[snn]
			if !ok {
				return fmt.Errorf("Sink '%s': source '%s' undefined", sn, snn)
			}

			src.Subscribe(s)
		}

		log.Printf("Sink '%s' loaded (subscribed to: %s)", sn, strings.Join(srcs, ", "))
		sinks[sn] = s
		commonObjs = append(commonObjs, s)
	}

	// Print summary
	log.Printf("Configuration loaded from %s: %+v", viper.ConfigFileUsed(), cfg)
	return
}
