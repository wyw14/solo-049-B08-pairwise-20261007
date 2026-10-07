package scheduler

import (
	"testing"
	"time"

	"orbitlink/internal/store"
)

func testCommands(revision int64, base time.Time) []PointingCommand {
	return []PointingCommand{
		{AntennaID: "ANT-01", Revision: revision, ExecuteAt: base, Azimuth: 100, Elevation: 10},
		{AntennaID: "ANT-01", Revision: revision, ExecuteAt: base.Add(time.Minute), Azimuth: 120, Elevation: 20},
	}
}

func newTestQueue(t *testing.T) *CommandQueue {
	t.Helper()
	queue, err := NewCommandQueue(store.NewPaths(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func TestCommandTransactionCommit(t *testing.T) {
	queue := newTestQueue(t)
	base := time.Now().UTC()
	if err := queue.Replace("pass-1", 1, testCommands(1, base)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.PrepareReplacement("pass-1", 2, testCommands(2, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	// The active revision stays 1 while staged.
	if pending := queue.PendingReplacements(); len(pending) != 1 || pending[0].Committed {
		t.Fatalf("unexpected pending state before commit: %#v", pending)
	}
	if due := queue.Due(base.Add(time.Hour)); len(due) != 2 || due[0].Revision != 1 {
		t.Fatalf("staged commands leaked into the live queue: %#v", due)
	}
	if err := queue.CommitPrepared("pass-1"); err != nil {
		t.Fatal(err)
	}
	due := queue.Due(base.Add(time.Hour))
	if len(due) != 2 || due[0].Revision != 2 {
		t.Fatalf("live queue did not switch to revision 2: %#v", due)
	}
	pending := queue.PendingReplacements()
	if len(pending) != 1 || !pending[0].Committed || len(pending[0].Backup) == 0 {
		t.Fatalf("committed transaction missing live flag/backup: %#v", pending)
	}
	if err := queue.ConfirmPrepared("pass-1"); err != nil {
		t.Fatal(err)
	}
	if len(queue.PendingReplacements()) != 0 {
		t.Fatal("transaction marker survived confirmation")
	}
}

func TestCommandRollbackAfterCommitRestoresRevision(t *testing.T) {
	queue := newTestQueue(t)
	base := time.Now().UTC()
	if err := queue.Replace("pass-1", 1, testCommands(1, base)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.PrepareReplacement("pass-1", 2, testCommands(2, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := queue.CommitPrepared("pass-1"); err != nil {
		t.Fatal(err)
	}
	if err := queue.RollbackPrepared("pass-1"); err != nil {
		t.Fatal(err)
	}
	due := queue.Due(base.Add(time.Hour))
	if len(due) != 2 || due[0].Revision != 1 {
		t.Fatalf("rollback did not serve revision 1 again: %#v", due)
	}
	// Retired history comes back with the backup: all revision-1 commands are
	// executable once more and no marker remains.
	for _, command := range due {
		if !queue.IsExecutable(command) {
			t.Fatal("restored revision-1 command is not executable")
		}
	}
	if len(queue.PendingReplacements()) != 0 {
		t.Fatal("transaction marker survived rollback")
	}
}

func TestCommandRollbackBeforeCommitIsADiscard(t *testing.T) {
	queue := newTestQueue(t)
	base := time.Now().UTC()
	if err := queue.Replace("pass-1", 1, testCommands(1, base)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.PrepareReplacement("pass-1", 2, testCommands(2, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := queue.RollbackPrepared("pass-1"); err != nil {
		t.Fatal(err)
	}
	if due := queue.Due(base.Add(time.Hour)); len(due) != 2 || due[0].Revision != 1 {
		t.Fatalf("discard before commit disturbed the queue: %#v", due)
	}
}

func TestCommandTransactionRecoveredAfterReopen(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	queue, err := NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if err := queue.Replace("pass-1", 1, testCommands(1, base)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.PrepareReplacement("pass-1", 2, testCommands(2, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := queue.CommitPrepared("pass-1"); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	pending := reopened.PendingReplacements()
	if len(pending) != 1 || !pending[0].Committed {
		t.Fatalf("committed transaction lost across reopen: %#v", pending)
	}
	if err := reopened.RollbackPrepared("pass-1"); err != nil {
		t.Fatal(err)
	}
	if due := reopened.Due(base.Add(time.Hour)); len(due) != 2 || due[0].Revision != 1 {
		t.Fatalf("rollback after reopen did not restore revision 1: %#v", due)
	}
}
