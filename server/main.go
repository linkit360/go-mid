package main

import (
	"os"
	"os/signal"
	"syscall"

	mid_server "github.com/linkit360/go-mid/server/src"
)

func main() {
	c := make(chan os.Signal, 3)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		mid_server.OnExit()
		os.Exit(1)
	}()

	mid_server.Run()
}
