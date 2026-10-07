package pass

import (
	"os"
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/recorder"
	"orbitlink/internal/store"
)

func TestUpdateFailureRestoresPreviousPass(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	manager, err := recorder.NewManager(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(paths, manager, trail)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	value, err := service.Create(CreateRequest{
		SatelliteID: "SAT-1", AntennaID: "ANT-1",
		RFChains: []string{"RF-1"}, RequiredReceiverChains: []string{"RF-1"},
		AOS: base, LOS: base.Add(8 * time.Minute), Revision: 1, OwnerStation: "north",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := paths.Snapshot("passes")
	if err := os.RemoveAll(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(snapshot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := service.Fail(value.ID, "simulated failure"); err == nil {
		t.Fatal("update unexpectedly succeeded with a broken snapshot path")
	}
	current, found := service.Get(value.ID)
	if !found {
		t.Fatal("pass disappeared after failed update")
	}
	if current.Phase != Planned || current.FailureReason != "" {
		t.Fatalf("failed update diverged the in-memory pass: phase=%s reason=%q", current.Phase, current.FailureReason)
	}
}
