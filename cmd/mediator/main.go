package main

import (
	"context"

	"github.com/go-baselib/new-user-task-with-event-driven/mediator"
)

func main() {
	mediator.Listen(context.Background())
}
