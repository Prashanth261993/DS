package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Hub owns the client set; only its goroutine touches it (no mutex, no races).
type Hub struct {
	clients    map[*client]struct{}
	register   chan *client
	unregister chan *client
	broadcast  chan []byte
}

type client struct {
	conn *websocket.Conn
	send chan []byte // buffered; if it fills, the client is dropped (slow consumer)
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*client]struct{}),
		register:   make(chan *client),
		unregister: make(chan *client),
		broadcast:  make(chan []byte, 256),
	}
}

func (h *Hub) run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = struct{}{}
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
		case msg := <-h.broadcast:
			for c := range h.clients {
				select {
				case c.send <- msg:
				default: // buffer full → drop slow client
					delete(h.clients, c)
					close(c.send)
				}
			}
		}
	}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (h *Hub) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &client{conn: conn, send: make(chan []byte, 64)}
	h.register <- c
	go func() { // writer
		defer conn.Close()
		for msg := range c.send {
			if conn.WriteMessage(websocket.TextMessage, msg) != nil {
				return
			}
		}
	}()
	go func() { // reader (drains pings/close); on error unregister
		defer func() { h.unregister <- c }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

// consumeBars reads bars_1s and broadcasts each bar to all WS clients.
func consumeBars(ctx context.Context, brokers string, h *Hub) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumeTopics("bars_1s"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		log.Fatalf("ws kafka: %v", err)
	}
	defer cl.Close()
	for {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		f.EachRecord(func(r *kgo.Record) {
			var b Bar
			if json.Unmarshal(r.Value, &b) == nil {
				h.broadcast <- r.Value
			}
		})
	}
}
