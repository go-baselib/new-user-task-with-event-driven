package main

import (
	"context"

	"github.com/go-baselib/new-user-task-with-event-driven/prom/user"

	rkentry "github.com/rookie-ninja/rk-entry/entry"
	"github.com/rookie-ninja/rk-prom"
)

func main() {
	rkentry.RegisterInternalEntriesFromConfig("./prom.yaml")

	maps := rkprom.RegisterPromEntriesWithConfig("./prom.yaml")

	entry := maps[rkprom.PromEntryNameDefault]
	entry.Bootstrap(context.Background())

	promEntry, ok := entry.(*rkprom.PromEntry)
	if !ok {
		panic("invalid type")
	}
	user.Init(promEntry.Registerer)

	rkentry.GlobalAppCtx.WaitForShutdownSig()

	// stop server
	entry.Interrupt(context.Background())
}
