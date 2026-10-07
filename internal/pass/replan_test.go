package pass

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/recorder"
	"orbitlink/internal/satellite"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

type replanHarness struct {
	paths     store.Paths
	trail     *audit.Trail
	passes    *Service
	windows   *WindowRecomputer
	queue     *scheduler.CommandQueue
	replanner *Replanner
}

func newReplanHarness(t *testing.T) *replanHarness {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	log := store.NewAppendLog(paths)
	trail := audit.NewTrail(log)
	manager, err := recorder.NewManager(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	passes, err := NewService(paths, manager, trail)
	if err != nil {
		t.Fatal(err)
	}
	windowStore, err := store.NewWindowStore(paths)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := scheduler.NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	windows := NewWindowRecomputer(windowStore, trail)
	return &replanHarness{paths: paths, trail: trail, passes: passes, windows: windows, queue: queue, replanner: NewReplanner(passes, windows, queue, trail)}
}

func (h *replanHarness) createPlannedPass(t *testing.T, aos time.Time, revision int64) Pass {
	t.Helper()
	value, err := h.passes.Create(CreateRequest{SatelliteID: "SAT-A12", AntennaID: "ANT-01", RFChains: []string{"RF-01"}, RequiredReceiverChains: []string{"RF-01"}, AOS: aos, LOS: aos.Add(8 * time.Minute), Revision: revision, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.windows.Apply(value.ID, value.Window); err != nil {
		t.Fatal(err)
	}
	if err := h.replanner.QueueInitial(value.ID); err != nil {
		t.Fatal(err)
	}
	return value
}

func ephemeris(revision int64) satellite.Ephemeris {
	return satellite.Ephemeris{SatelliteID: "SAT-A12", Revision: revision, Epoch: time.Now().UTC(), Line1: "one", Line2: "two"}
}

func (h *replanHarness) assertRevision(t *testing.T, passID string, revision int64) {
	t.Helper()
	value, found := h.passes.Get(passID)
	if !found {
		t.Fatal("pass missing")
	}
	if value.Window.Revision != revision {
		t.Fatalf("pass revision = %d, want %d", value.Window.Revision, revision)
	}
	window, found := h.windows.store.Get(passID)
	if !found || window.Revision != revision {
		t.Fatalf("window store revision = %d (found=%v), want %d", window.Revision, found, revision)
	}
	if active := h.activeRevision(passID); active != revision {
		t.Fatalf("active command revision = %d, want %d", active, revision)
	}
}

func (h *replanHarness) activeRevision(passID string) int64 {
	active := int64(-1)
	for _, command := range h.queue.List(passID) {
		if !command.Retired && command.Revision > active {
			active = command.Revision
		}
	}
	return active
}

func TestApplyRevisionCommitsEverywhereAndConfirms(t *testing.T) {
	h := newReplanHarness(t)
	value := h.createPlannedPass(t, time.Now().UTC().Add(5*time.Minute), 1)

	h.replanner.ApplyRevision(ephemeris(1), ephemeris(2))

	h.assertRevision(t, value.ID, 2)
	if len(h.windows.PendingWindows()) != 0 || len(h.queue.PendingReplacements()) != 0 {
		t.Fatal("transaction markers survived a successful replan")
	}
	events := h.trail.List(audit.Query{Component: "pass", Subject: value.ID})
	var applied bool
	for _, event := range events {
		if event.Action == "replan.applied" {
			applied = true
		}
	}
	if !applied {
		t.Fatal("successful replan was not audited")
	}
}

func TestUnrecoverableMarkerParksPassForReviewKeepingOldPlan(t *testing.T) {
	h := newReplanHarness(t)
	value := h.createPlannedPass(t, time.Now().UTC().Add(5*time.Minute), 1)
	// A corrupt window transaction marker cannot be reconciled automatically.
	markerPath := filepath.Join(filepath.Dir(h.paths.Snapshot("pass-windows")), "pass-windows.tx-"+store.SafeName(value.ID)+".json")
	if err := os.WriteFile(markerPath, []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.replanner.ApplyRevision(ephemeris(1), ephemeris(2))

	got, _ := h.passes.Get(value.ID)
	if got.Phase != ReplanReview {
		t.Fatalf("pass phase = %s, want replan_review", got.Phase)
	}
	// The committed window and commands must stay on the old revision.
	if record, ok := h.windows.store.Get(value.ID); !ok || record.Revision != 1 {
		t.Fatalf("window store left on revision %d", record.Revision)
	}
	if active := h.activeRevision(value.ID); active != 1 {
		t.Fatalf("command queue left on revision %d", active)
	}
}

func TestCompensateRestoresPreviousWindowAndCommands(t *testing.T) {
	h := newReplanHarness(t)
	value := h.createPlannedPass(t, time.Now().UTC().Add(5*time.Minute), 1)
	window := Window{AOS: value.Window.AOS.Add(time.Second), LOS: value.Window.LOS.Add(time.Second), Revision: 2}
	if err := h.windows.Prepare(value.ID, window); err != nil {
		t.Fatal(err)
	}
	if _, err := h.queue.PrepareReplacement(value.ID, 2, h.queue.List(value.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.windows.CommitPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.CommitPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	// Both stores switched; pretend the pass snapshot could not advance.
	h.replanner.compensate(value.ID, 2, "advance.pass", assertError("disk full"))

	h.assertRevision(t, value.ID, 1)
	if got, _ := h.passes.Get(value.ID); got.Phase != Planned {
		t.Fatalf("pass phase = %s, want planned", got.Phase)
	}
	if len(h.windows.PendingWindows()) != 0 || len(h.queue.PendingReplacements()) != 0 {
		t.Fatal("compensation left transaction markers behind")
	}
}

func TestCompensateFailureParksPassForReview(t *testing.T) {
	h := newReplanHarness(t)
	value := h.createPlannedPass(t, time.Now().UTC().Add(5*time.Minute), 1)
	window := Window{AOS: value.Window.AOS.Add(time.Second), LOS: value.Window.LOS.Add(time.Second), Revision: 2}
	if err := h.windows.Prepare(value.ID, window); err != nil {
		t.Fatal(err)
	}
	if _, err := h.queue.PrepareReplacement(value.ID, 2, h.queue.List(value.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.windows.CommitPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.CommitPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	// Drop the window marker: the previous window can no longer be restored.
	if err := h.windows.DiscardPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	h.replanner.compensate(value.ID, 2, "commit.commands", assertError("disk full"))

	got, _ := h.passes.Get(value.ID)
	if got.Phase != ReplanReview || got.ReplanRevision != 2 {
		t.Fatalf("pass = phase %s revision %d, want replan_review pending revision 2", got.Phase, got.ReplanRevision)
	}
	if got.ReplanReason == "" {
		t.Fatal("review pass carries no reason")
	}
	// Operator resolution brings the pass back into service.
	if err := h.passes.KeepCurrentWindow(value.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := h.passes.Get(value.ID); got.Phase != Planned {
		t.Fatalf("pass phase after keep = %s", got.Phase)
	}
}

func TestRecoverConfirmsFullyCommittedReplan(t *testing.T) {
	h := newReplanHarness(t)
	value := h.createPlannedPass(t, time.Now().UTC().Add(5*time.Minute), 1)
	window := Window{AOS: value.Window.AOS.Add(time.Second), LOS: value.Window.LOS.Add(time.Second), Revision: 2}
	if err := h.windows.Prepare(value.ID, window); err != nil {
		t.Fatal(err)
	}
	if _, err := h.queue.PrepareReplacement(value.ID, 2, h.queue.List(value.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.windows.CommitPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.CommitPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.passes.update(value.ID, func(pass *Pass) error { pass.Window = window; return nil }); err != nil {
		t.Fatal(err)
	}
	// Crash before confirmation: markers persist, every state is at revision 2.
	h.replanner.Recover()
	h.assertRevision(t, value.ID, 2)
	if len(h.windows.PendingWindows()) != 0 || len(h.queue.PendingReplacements()) != 0 {
		t.Fatal("recovery did not clear the dangling markers")
	}
}

func TestRecoverRollsBackCommittedStoresWhenPassStayedOld(t *testing.T) {
	h := newReplanHarness(t)
	value := h.createPlannedPass(t, time.Now().UTC().Add(5*time.Minute), 1)
	window := Window{AOS: value.Window.AOS.Add(time.Second), LOS: value.Window.LOS.Add(time.Second), Revision: 2}
	if err := h.windows.Prepare(value.ID, window); err != nil {
		t.Fatal(err)
	}
	if _, err := h.queue.PrepareReplacement(value.ID, 2, h.queue.List(value.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.windows.CommitPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.CommitPrepared(value.ID); err != nil {
		t.Fatal(err)
	}
	// Pass snapshot never advanced (crash before that step).
	h.replanner.Recover()
	h.assertRevision(t, value.ID, 1)
	if len(h.windows.PendingWindows()) != 0 || len(h.queue.PendingReplacements()) != 0 {
		t.Fatal("recovery left transaction markers behind")
	}
}

func TestResolveReviewAcceptsRevisedWindow(t *testing.T) {
	h := newReplanHarness(t)
	value := h.createPlannedPass(t, time.Now().UTC().Add(5*time.Minute), 1)
	if err := h.passes.MarkReplanReview(value.ID, 2, "synthetic review"); err != nil {
		t.Fatal(err)
	}
	resolved, err := h.replanner.ResolveReview(value.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Phase != Planned || resolved.Window.Revision != 2 {
		t.Fatalf("resolved pass = phase %s revision %d", resolved.Phase, resolved.Window.Revision)
	}
	h.assertRevision(t, value.ID, 2)
}

func TestResolveReviewKeepCurrentRestoresPinnedPlan(t *testing.T) {
	h := newReplanHarness(t)
	value := h.createPlannedPass(t, time.Now().UTC().Add(5*time.Minute), 1)
	window := Window{AOS: value.Window.AOS.Add(time.Second), LOS: value.Window.LOS.Add(time.Second), Revision: 2}
	// Both stores sit on revision 2 while the pass was parked for review and
	// the transaction markers are gone, so automatic compensation is over.
	if err := h.windows.Apply(value.ID, window); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.Replace(value.ID, 2, buildTestCommands(t, h, value, window)); err != nil {
		t.Fatal(err)
	}
	if err := h.passes.MarkReplanReview(value.ID, 2, "unrecoverable rollback"); err != nil {
		t.Fatal(err)
	}
	// Operator pins the old window: ResolveReview(false) repins both stores.
	if _, err := h.replanner.ResolveReview(value.ID, false); err != nil {
		t.Fatal(err)
	}
	h.assertRevision(t, value.ID, 1)
	if got, _ := h.passes.Get(value.ID); got.Phase != Planned {
		t.Fatalf("pass phase after keep = %s", got.Phase)
	}
}

func buildTestCommands(t *testing.T, h *replanHarness, value Pass, window Window) []scheduler.PointingCommand {
	t.Helper()
	commands, err := antenna.BuildCommands(value.ID, value.AntennaID, window.Revision, revisedTrackPoints(window))
	if err != nil {
		t.Fatal(err)
	}
	return commands
}

type assertError string

func (e assertError) Error() string { return string(e) }
