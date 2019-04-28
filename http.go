package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpServer *http.Server
)

func httpInit() {
	r := http.NewServeMux()

	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Alive and well\n")
	})

	r.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		problem := false

		body := ""
		for _, v := range commonObjs {
			err := v.Status()
			if err != nil {
				problem = true
			}

			body += common.LoggerHeader(v) + err.Error()
		}

		if problem {
			w.WriteHeader(500)
		}

		// Make linter happy
		_, _ = w.Write([]byte(body))
	})

	r.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		for _, s := range gatherStats() {
			fmt.Fprintf(w, "%s\n", s)
		}
	})

	r.HandleFunc("/debug/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			w.WriteHeader(400)
			fmt.Fprintf(w, "Request should be in form /debug/[off|on]\n")
			return
		}

		var debug bool
		switch parts[2] {
		case "on":
			debug = true
		case "off":
			debug = false
		default:
			w.WriteHeader(400)
			fmt.Fprintf(w, "Debug can be [on] or [off]\n")
			return
		}

		for _, v := range commonObjs {
			v.SetDebug(debug)
		}

		fmt.Fprintf(w, "Debug is now %t\n", debug)
	})

	r.Handle("/metrics", promhttp.Handler())

	httpServer = &http.Server{
		Addr:    cfg.HTTP.Listen,
		Handler: r,
	}

	go func() {
		if err := httpServer.ListenAndServe(); err == http.ErrServerClosed {
			log.Println("HTTP was shut down gracefully")
		} else if err != nil {
			log.Fatalf("HTTP: Failed to listen on %s: %v", cfg.HTTP.Listen, err)
		}
	}()

	time.Sleep(10 * time.Millisecond) // Give it time to die
	log.Printf("HTTP listening to %s", cfg.HTTP.Listen)
}

func httpShutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("Unable to shutdown HTTP: %s", err)
	}

	cancel()
	log.Println("HTTP shutdown finished")
}
