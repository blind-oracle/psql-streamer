package main

import (
	"log"
	"time"

	"github.com/blind-oracle/psql-streamer/common"
)

func tickerWorker() {
	defer wg.Done()

	ticker := time.NewTicker(cfg.TickerInterval)
	defer ticker.Stop()

	log.Printf("Ticker started with %s interval", cfg.TickerInterval)
	for {
		select {
		case <-chShutdown:
			return

		case <-ticker.C:
			for _, s := range gatherStats() {
				log.Print(s)
			}
		}
	}
}

func gatherStats() (s []string) {
	for _, v := range commonObjs {
		s = append(s, "(Ticker) "+common.LoggerHeader(v)+v.Stats())
	}

	return
}
