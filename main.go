package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/kalvin807/twitter-v2-stream/internal/stream"
)

func HandleChan(messages <-chan *stream.StreamData) {
	for message := range messages {
		fmt.Println(message.Tweet.ID)
	}
}

func main() {
	println("Hello, World!")
	token := os.Getenv("TWITTER_TOKEN")
	client := http.DefaultClient
	v2Service := stream.NewStreamService(client, token)
	params := &stream.StreamFilterParams{}
	v2, err := v2Service.Connect(params)
	if err != nil {
		panic(err)
	}
	go HandleChan(v2.Messages)

	http.ListenAndServe("0.0.0.0:8080", nil)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)
	v2.Stop()
}
