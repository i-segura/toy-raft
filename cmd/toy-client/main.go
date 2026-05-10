package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

func main() {
	peersFlag := flag.String("peers", "", "comma-separated data HTTP base URLs (e.g. http://127.0.0.1:18088,http://127.0.0.1:18089)")
	maxSleep := flag.Duration("max-interval", 2*time.Second, "max sleep between requests (uniform 0..max)")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request timeout")
	flag.Parse()

	if *peersFlag == "" {
		log.Fatal("flag -peers is required (comma-separated HTTP URLs)")
	}
	peers := splitPeers(*peersFlag)
	if len(peers) == 0 {
		log.Fatal("no peers after parsing -peers")
	}

	client := &http.Client{Timeout: *timeout}
	n := 0
	for {
		d := time.Duration(rand.Int63n(int64(*maxSleep) + 1))
		time.Sleep(d)

		url := strings.TrimRight(peers[rand.Intn(len(peers))], "/") + "/"
		body := randomCommand(n)
		n++

		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			cancel()
			log.Printf("new request: %v", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		cancel()
		if err != nil {
			log.Printf("POST %s: %v", url, err)
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		log.Printf("POST %s -> %d %s", url, resp.StatusCode, string(bytes.TrimSpace(b)))
	}
}

func splitPeers(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func randomCommand(seq int) []byte {
	m := map[string]any{
		"op":    "set",
		"key":   fmt.Sprintf("k%d", seq),
		"value": rand.Int63(),
	}
	b, err := json.Marshal(m)
	if err != nil {
		return []byte(`{"op":"noop"}`)
	}
	return b
}
