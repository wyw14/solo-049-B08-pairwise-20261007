package scheduler

import (
	"os"
	"testing"
	"time"

	"orbitlink/internal/store"
)

func breakSnapshot(t *testing.T, path string) {
	t.Helper()
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func sampleCommands(passID string, revision int64) []PointingCommand {
	base := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	return []PointingCommand{
		{PassID: passID, AntennaID: "ANT-1", Revision: revision, ExecuteAt: base, Azimuth: 100, Elevation: 10},
		{PassID: passID, AntennaID: "ANT-1", Revision: revision, ExecuteAt: base.Add(5 * time.Minute), Azimuth: 200, Elevation: 8},
	}
}

func TestReplaceFailureLeavesQueueIntact(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	queue, err := NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Replace("pass-1", 1, sampleCommands("pass-1", 1)); err != nil {
		t.Fatal(err)
	}
	breakSnapshot(t, paths.Snapshot("pointing-commands"))
	if err := queue.Replace("pass-1", 2, sampleCommands("pass-1", 2)); err == nil {
		t.Fatal("replace unexpectedly succeeded with a broken snapshot path")
	}
	commands := queue.List("pass-1")
	if len(commands) != 2 {
		t.Fatalf("queue holds %d commands, want the original 2", len(commands))
	}
	for _, command := range commands {
		if command.Retired || command.Revision != 1 {
			t.Fatalf("failed replace mutated the queue: %#v", command)
		}
		if !queue.IsExecutable(command) {
			t.Fatalf("previously active command is no longer executable: %#v", command)
		}
	}
}

func TestRetirePassRetiresActiveCommands(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	queue, err := NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Replace("pass-1", 1, sampleCommands("pass-1", 1)); err != nil {
		t.Fatal(err)
	}
	if err := queue.RetirePass("pass-1", "replan rolled back"); err != nil {
		t.Fatal(err)
	}
	for _, command := range queue.List("pass-1") {
		if !command.Retired || command.RetireCause != "replan rolled back" {
			t.Fatalf("command was not retired with cause: %#v", command)
		}
	}
	if due := queue.Due(time.Now().UTC().Add(2 * time.Hour)); len(due) != 0 {
		t.Fatalf("retired pass still has due commands: %#v", due)
	}
}

func TestRetirePassFailureLeavesQueueIntact(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	queue, err := NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Replace("pass-1", 1, sampleCommands("pass-1", 1)); err != nil {
		t.Fatal(err)
	}
	breakSnapshot(t, paths.Snapshot("pointing-commands"))
	if err := queue.RetirePass("pass-1", "replan rolled back"); err == nil {
		t.Fatal("retire unexpectedly succeeded with a broken snapshot path")
	}
	for _, command := range queue.List("pass-1") {
		if command.Retired {
			t.Fatalf("failed retire mutated the queue: %#v", command)
		}
	}
}
