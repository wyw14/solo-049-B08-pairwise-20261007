package store

import (
	"os"
	"testing"
	"time"
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

func TestWindowStoreSaveFailureRestoresPreviousWindow(t *testing.T) {
	paths := NewPaths(t.TempDir())
	windows, err := NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	first := WindowRecord{PassID: "pass-1", AOS: base, LOS: base.Add(8 * time.Minute), Revision: 1}
	if err := windows.Save(first); err != nil {
		t.Fatal(err)
	}
	breakSnapshot(t, paths.Snapshot("pass-windows"))
	second := WindowRecord{PassID: "pass-1", AOS: base.Add(time.Second), LOS: base.Add(8 * time.Minute), Revision: 2}
	if err := windows.Save(second); err == nil {
		t.Fatal("save unexpectedly succeeded with a broken snapshot path")
	}
	current, found := windows.Get("pass-1")
	if !found || current.Revision != 1 || !current.AOS.Equal(first.AOS) {
		t.Fatalf("failed save diverged the in-memory window: %#v found=%v", current, found)
	}
}

func TestWindowStoreSaveFailureDropsUncommittedWindow(t *testing.T) {
	paths := NewPaths(t.TempDir())
	windows, err := NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	breakSnapshot(t, paths.Snapshot("pass-windows"))
	base := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	record := WindowRecord{PassID: "pass-1", AOS: base, LOS: base.Add(8 * time.Minute), Revision: 1}
	if err := windows.Save(record); err == nil {
		t.Fatal("save unexpectedly succeeded with a broken snapshot path")
	}
	if _, found := windows.Get("pass-1"); found {
		t.Fatal("failed save left an uncommitted window in memory")
	}
}
