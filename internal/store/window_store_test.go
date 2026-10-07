package store

import (
	"testing"
	"time"
)

func testWindowRecord(revision int64, aos time.Time) WindowRecord {
	return WindowRecord{PassID: "pass-1", Revision: revision, AOS: aos, LOS: aos.Add(8 * time.Minute)}
}

func TestWindowTransactionCommitAndConfirm(t *testing.T) {
	paths := NewPaths(t.TempDir())
	windows, err := NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if err := windows.Save(testWindowRecord(1, base)); err != nil {
		t.Fatal(err)
	}
	revised := testWindowRecord(2, base.Add(time.Second))
	if err := windows.PrepareWindow(revised); err != nil {
		t.Fatal(err)
	}
	// Staging must not touch the committed snapshot.
	if got, _ := windows.Get("pass-1"); got.Revision != 1 {
		t.Fatalf("committed window changed during prepare: %d", got.Revision)
	}
	if pending := windows.PendingWindows(); len(pending) != 1 || pending[0].Committed {
		t.Fatalf("unexpected pending state before commit: %#v", pending)
	}
	if _, err := windows.CommitPreparedWindow("pass-1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := windows.Get("pass-1"); got.Revision != 2 {
		t.Fatalf("window not committed: %d", got.Revision)
	}
	pending := windows.PendingWindows()
	if len(pending) != 1 || !pending[0].Committed || pending[0].Backup == nil || pending[0].Backup.Revision != 1 {
		t.Fatalf("committed transaction not reported with revision-1 backup: %#v", pending)
	}
	if err := windows.ConfirmPreparedWindow("pass-1"); err != nil {
		t.Fatal(err)
	}
	if len(windows.PendingWindows()) != 0 {
		t.Fatal("transaction marker survived confirmation")
	}
}

func TestWindowRollbackAfterCommitRestoresPrevious(t *testing.T) {
	paths := NewPaths(t.TempDir())
	windows, err := NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if err := windows.Save(testWindowRecord(1, base)); err != nil {
		t.Fatal(err)
	}
	if err := windows.PrepareWindow(testWindowRecord(2, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := windows.CommitPreparedWindow("pass-1"); err != nil {
		t.Fatal(err)
	}
	if err := windows.RollbackPreparedWindow("pass-1"); err != nil {
		t.Fatal(err)
	}
	got, found := windows.Get("pass-1")
	if !found || got.Revision != 1 || !got.AOS.Equal(base) {
		t.Fatalf("previous window not restored: %#v found=%v", got, found)
	}
	if len(windows.PendingWindows()) != 0 {
		t.Fatal("transaction marker survived rollback")
	}
}

func TestWindowRollbackBeforeCommitKeepsSnapshot(t *testing.T) {
	paths := NewPaths(t.TempDir())
	windows, err := NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if err := windows.Save(testWindowRecord(1, base)); err != nil {
		t.Fatal(err)
	}
	if err := windows.PrepareWindow(testWindowRecord(2, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := windows.RollbackPreparedWindow("pass-1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := windows.Get("pass-1"); got.Revision != 1 {
		t.Fatalf("rollback before commit disturbed the snapshot: %d", got.Revision)
	}
}

func TestWindowTransactionSurvivesReopen(t *testing.T) {
	paths := NewPaths(t.TempDir())
	windows, err := NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	if err := windows.Save(testWindowRecord(1, base)); err != nil {
		t.Fatal(err)
	}
	if err := windows.PrepareWindow(testWindowRecord(2, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := windows.CommitPreparedWindow("pass-1"); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	pending := reopened.PendingWindows()
	if len(pending) != 1 || !pending[0].Committed {
		t.Fatalf("reopened store lost committed transaction: %#v", pending)
	}
	if err := reopened.RollbackPreparedWindow("pass-1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := reopened.Get("pass-1"); got.Revision != 1 {
		t.Fatalf("rollback after reopen did not restore revision 1: %d", got.Revision)
	}
}
