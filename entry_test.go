package scheduler_test

import (
	"context"
	"testing"
	"time"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

// TestEntrySnapshot confirms entries handed out by the scheduler are copies.
func TestEntrySnapshot(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.New()

	id, err := scheduler.RegisterFunc("@every 1h", "job", func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("RegisterFunc() error = %v", err)
	}

	entries := scheduler.Entries()
	if len(entries) != 1 {
		t.Fatalf("Entries() = %d entries; want 1", len(entries))
	}

	entry := entries[0]
	entry.Next = time.Now()

	if scheduler.Entries()[0].Next.Equal(entry.Next) {
		t.Error("mutating the returned Entry changed the scheduler's state")
	}

	if entries[0].ID != id {
		t.Errorf("Entry.ID = %d; want %d", entries[0].ID, id)
	}
}
