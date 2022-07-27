package main

import (
	"context"

	"github.com/go-baselib/new-user-task-with-event-driven/notify"
)

func main() {
	notify.Processor(context.Background())
}
