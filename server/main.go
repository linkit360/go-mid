package main

import (
	"os"
	"os/signal"
	"syscall"

	inmem_server "github.com/linkit360/go-mid/server/src"
)

func main() {
	c := make(chan os.Signal, 3)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		inmem_server.OnExit()
		os.Exit(1)
	}()

	inmem_server.Run()
}
