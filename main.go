package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/db"
	"github.com/blind-oracle/psql-streamer/sink"
	"github.com/blind-oracle/psql-streamer/source"
)

var (
	wg         sync.WaitGroup
	chShutdown = make(chan struct{})

	boltdb     *db.DB
	sources    = map[string]source.Source{}
	sinks      = map[string]sink.Sink{}
	commonObjs []common.Common
)

func main() {
	var err error
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	flag.StringVar(&cfg.File, "config", "", "Path to configuration file")
	flag.Parse()

	if err = configLoad(); err != nil {
		log.Fatal(err)
	}

	// Shutdown handling stuff
	wg.Add(1)
	go handleShutdown()

	// Start the sources
	for _, s := range sources {
		s.Start()
	}

	if cfg.TickerInterval > 0 {
		wg.Add(1)
		go tickerWorker()
	}

	httpInit()

	wg.Wait()
	log.Println("Service shutdown complete")
}

// Wait for the SIGTERM or Ctrl+C and handle shutdown
func handleShutdown() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
	<-sig

	log.Printf("Performing graceful shutdown")

	httpShutdown()
	close(chShutdown)

	// Shutdown sources
	for sn, s := range sources {
		log.Printf("Closing source '%s' ...", sn)
		if err := s.Close(); err != nil {
			log.Printf("Unable to shutdown source '%s': %s", sn, err)
		} else {
			log.Printf("Source '%s' closed: %s", sn, s.Stats())
		}
	}

	// Shutdown sinks
	for sn, s := range sinks {
		log.Printf("Closing sink '%s' ...", sn)
		if err := s.Close(); err != nil {
			log.Printf("Unable to shutdown sink '%s': %s", sn, err)
		} else {
			log.Printf("Sink '%s' closed: %s", sn, s.Stats())
		}
	}

	if boltdb != nil {
		if err := boltdb.Close(); err != nil {
			log.Printf("Unable to close Bolt: %s", err)
		}
	}

	wg.Done()
}
