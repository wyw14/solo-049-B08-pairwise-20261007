package pass

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/satellite"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

type fakePassStore struct {
	passes      map[string]Pass
	updateErr   error
	failFirst   int // fail only the first N update calls; 0 = fail all
	updateCalls int
}

func (f *fakePassStore) List() []Pass {
	values := make([]Pass, 0, len(f.passes))
	for _, value := range f.passes {
		values = append(values, value)
	}
	return values
}

func (f *fakePassStore) Get(id string) (Pass, bool) {
	value, found := f.passes[id]
	return value, found
}

func (f *fakePassStore) update(passID string, apply func(*Pass) error) error {
	f.updateCalls++
	if f.updateErr != nil && (f.failFirst == 0 || f.updateCalls <= f.failFirst) {
		return f.updateErr
	}
	value, found := f.passes[passID]
	if !found {
		return fmt.Errorf("pass %s not found", passID)
	}
	if err := apply(&value); err != nil {
		return err
	}
	f.passes[passID] = value
	return nil
}

type fakeWindowStore struct {
	windows    map[string]Window
	applyErr   error
	restoreErr error
	applied    []Window
	restored   []Window
}

func (f *fakeWindowStore) Apply(passID string, window Window) error {
	if f.applyErr != nil {
		return f.applyErr
	}
	f.windows[passID] = window
	f.applied = append(f.applied, window)
	return nil
}

func (f *fakeWindowStore) Get(passID string) (Window, bool) {
	window, found := f.windows[passID]
	return window, found
}

func (f *fakeWindowStore) Restore(passID string, window Window) error {
	if f.restoreErr != nil {
		return f.restoreErr
	}
	f.windows[passID] = window
	f.restored = append(f.restored, window)
	return nil
}

type fakeCommandQueue struct {
	active        map[string][]scheduler.PointingCommand
	revisions     []int64
	calls         int
	failAll       bool
	failFrom      int // Replace calls beyond this many successes fail; 0 = never
	retireErr     error
	retiredPasses []string
}

func (f *fakeCommandQueue) Replace(passID string, revision int64, commands []scheduler.PointingCommand) error {
	f.calls++
	if f.failAll || (f.failFrom > 0 && f.calls > f.failFrom) {
		return errors.New("queue write failed")
	}
	stamped := make([]scheduler.PointingCommand, 0, len(commands))
	for _, command := range commands {
		command.PassID = passID
		command.Revision = revision
		stamped = append(stamped, command)
	}
	f.active[passID] = stamped
	f.revisions = append(f.revisions, revision)
	return nil
}

func (f *fakeCommandQueue) RetirePass(passID, cause string) error {
	if f.retireErr != nil {
		return f.retireErr
	}
	delete(f.active, passID)
	f.retiredPasses = append(f.retiredPasses, passID)
	return nil
}

func (f *fakeCommandQueue) List(passID string) []scheduler.PointingCommand {
	return append([]scheduler.PointingCommand(nil), f.active[passID]...)
}

type replanFixture struct {
	replanner *Replanner
	passes    *fakePassStore
	windows   *fakeWindowStore
	queue     *fakeCommandQueue
	trail     *audit.Trail
	pass      Pass
}

func newReplanFixture(t *testing.T) *replanFixture {
	t.Helper()
	base := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	value := Pass{
		ID: "pass-1", SatelliteID: "SAT-1", AntennaID: "ANT-1",
		RFChains: []string{"RF-1"}, RequiredReceiverChains: []string{"RF-1"},
		Window:       Window{AOS: base, LOS: base.Add(8 * time.Minute), Revision: 1},
		Phase:        Planned,
		OwnerStation: "north", OwnerGeneration: 1,
	}
	passes := &fakePassStore{passes: map[string]Pass{value.ID: value}}
	windows := &fakeWindowStore{windows: map[string]Window{value.ID: value.Window}}
	queue := &fakeCommandQueue{active: map[string][]scheduler.PointingCommand{
		value.ID: {
			{ID: "cmd-1", PassID: value.ID, AntennaID: value.AntennaID, Revision: 1, ExecuteAt: base, Azimuth: 100, Elevation: 10},
			{ID: "cmd-2", PassID: value.ID, AntennaID: value.AntennaID, Revision: 1, ExecuteAt: base.Add(8 * time.Minute), Azimuth: 250, Elevation: 7},
		},
	}}
	trail := audit.NewTrail(store.NewAppendLog(store.NewPaths(t.TempDir())))
	replanner := NewReplanner(passes, windows, queue, trail)
	return &replanFixture{replanner: replanner, passes: passes, windows: windows, queue: queue, trail: trail, pass: value}
}

func (f *replanFixture) apply() {
	f.replanner.ApplyRevision(
		satellite.Ephemeris{SatelliteID: "SAT-1", Revision: 1, Epoch: time.Now().UTC()},
		satellite.Ephemeris{SatelliteID: "SAT-1", Revision: 2, Epoch: time.Now().UTC()},
	)
}

func (f *replanFixture) auditActions() map[string]int {
	actions := make(map[string]int)
	for _, event := range f.trail.List(audit.Query{Component: "pass"}) {
		actions[event.Action]++
	}
	return actions
}

func (f *replanFixture) auditSeverity(action string) audit.Severity {
	for _, event := range f.trail.List(audit.Query{Component: "pass"}) {
		if event.Action == action {
			return event.Severity
		}
	}
	return ""
}

func TestApplyRevisionMigratesWholePlan(t *testing.T) {
	fixture := newReplanFixture(t)
	fixture.apply()

	window := fixture.windows.windows[fixture.pass.ID]
	if window.Revision != 2 {
		t.Fatalf("window store revision = %d, want 2", window.Revision)
	}
	if !window.AOS.Equal(fixture.pass.Window.AOS.Add(time.Second)) {
		t.Fatalf("window AOS = %s, want old AOS shifted by 1s", window.AOS)
	}
	commands := fixture.queue.active[fixture.pass.ID]
	if len(commands) == 0 || commands[0].Revision != 2 {
		t.Fatalf("queue active commands not at revision 2: %#v", commands)
	}
	if got := fixture.passes.passes[fixture.pass.ID].Window.Revision; got != 2 {
		t.Fatalf("pass window revision = %d, want 2", got)
	}
	if actions := fixture.auditActions(); actions["replan.rolled-back"] != 0 || actions["replan.needs-review"] != 0 || actions["replan.failed"] != 0 {
		t.Fatalf("unexpected rollback/review audit events: %#v", actions)
	}
}

func TestApplyRevisionWindowFailureLeavesPlanUntouched(t *testing.T) {
	fixture := newReplanFixture(t)
	fixture.windows.applyErr = errors.New("window store write failed")
	fixture.apply()

	if fixture.queue.calls != 0 {
		t.Fatalf("queue was mutated despite window failure (%d replace calls)", fixture.queue.calls)
	}
	if got := fixture.passes.passes[fixture.pass.ID].Window.Revision; got != 1 {
		t.Fatalf("pass window revision = %d, want 1", got)
	}
	if len(fixture.windows.restored) != 0 {
		t.Fatal("window restore ran although nothing was applied")
	}
	if actions := fixture.auditActions(); actions["replan.failed"] != 1 {
		t.Fatalf("expected one replan.failed event: %#v", actions)
	}
}

func TestApplyRevisionQueueFailureRestoresWindow(t *testing.T) {
	fixture := newReplanFixture(t)
	fixture.queue.failAll = true
	fixture.apply()

	if len(fixture.windows.restored) != 1 || fixture.windows.restored[0].Revision != 1 {
		t.Fatalf("window was not restored to revision 1: %#v", fixture.windows.restored)
	}
	if got := fixture.windows.windows[fixture.pass.ID].Revision; got != 1 {
		t.Fatalf("window store revision = %d, want 1", got)
	}
	if got := fixture.passes.passes[fixture.pass.ID].Window.Revision; got != 1 {
		t.Fatalf("pass window revision = %d, want 1", got)
	}
	if actions := fixture.auditActions(); actions["replan.rolled-back"] != 1 || actions["replan.needs-review"] != 0 {
		t.Fatalf("expected one rollback and no review flag: %#v", actions)
	}
}

func TestApplyRevisionPassFailureRestoresQueueAndWindow(t *testing.T) {
	fixture := newReplanFixture(t)
	fixture.passes.updateErr = errors.New("pass snapshot write failed")
	fixture.apply()

	if len(fixture.queue.revisions) != 2 || fixture.queue.revisions[0] != 2 || fixture.queue.revisions[1] != 1 {
		t.Fatalf("queue replace revisions = %#v, want [2 1] (apply then restore)", fixture.queue.revisions)
	}
	commands := fixture.queue.active[fixture.pass.ID]
	if len(commands) != 2 || commands[0].Revision != 1 {
		t.Fatalf("queue active commands not restored to revision 1: %#v", commands)
	}
	if len(fixture.windows.restored) != 1 || fixture.windows.restored[0].Revision != 1 {
		t.Fatalf("window was not restored to revision 1: %#v", fixture.windows.restored)
	}
	if actions := fixture.auditActions(); actions["replan.rolled-back"] != 1 || actions["replan.needs-review"] != 0 {
		t.Fatalf("expected one rollback and no review flag: %#v", actions)
	}
}

func TestApplyRevisionQueueRestoreFailureFlagsReview(t *testing.T) {
	fixture := newReplanFixture(t)
	fixture.passes.updateErr = errors.New("pass snapshot write failed")
	fixture.passes.failFirst = 1 // stage-4 commit fails, the review flag update succeeds
	fixture.queue.failFrom = 1   // initial replace succeeds, compensation replace fails
	fixture.apply()

	if actions := fixture.auditActions(); actions["replan.needs-review"] != 1 || actions["replan.rolled-back"] != 0 {
		t.Fatalf("expected one needs-review and no rollback: %#v", actions)
	}
	if severity := fixture.auditSeverity("replan.needs-review"); severity != audit.Critical {
		t.Fatalf("needs-review severity = %s, want critical", severity)
	}
	reason := fixture.passes.passes[fixture.pass.ID].FailureReason
	if !strings.Contains(reason, "manual review") {
		t.Fatalf("pass failure reason %q does not flag manual review", reason)
	}
}

func TestApplyRevisionWindowRestoreFailureFlagsReview(t *testing.T) {
	fixture := newReplanFixture(t)
	fixture.queue.failAll = true // queue replace fails after the window was applied
	fixture.windows.restoreErr = errors.New("window restore write failed")
	fixture.apply()

	if actions := fixture.auditActions(); actions["replan.needs-review"] != 1 {
		t.Fatalf("expected one needs-review event: %#v", actions)
	}
	if reason := fixture.passes.passes[fixture.pass.ID].FailureReason; !strings.Contains(reason, "manual review") {
		t.Fatalf("pass failure reason %q does not flag manual review", reason)
	}
}

func TestApplyRevisionWithoutPriorCommandsRetiresQueueOnRollback(t *testing.T) {
	fixture := newReplanFixture(t)
	fixture.queue.active[fixture.pass.ID] = nil // pass never had commands queued
	fixture.passes.updateErr = errors.New("pass snapshot write failed")
	fixture.apply()

	if len(fixture.queue.retiredPasses) != 1 || fixture.queue.retiredPasses[0] != fixture.pass.ID {
		t.Fatalf("queue retire calls = %#v, want the pass retired once", fixture.queue.retiredPasses)
	}
	if len(fixture.windows.restored) != 1 {
		t.Fatalf("window was not restored: %#v", fixture.windows.restored)
	}
	if actions := fixture.auditActions(); actions["replan.rolled-back"] != 1 || actions["replan.needs-review"] != 0 {
		t.Fatalf("expected clean rollback: %#v", actions)
	}
}

func TestApplyRevisionRetireFailureFlagsReview(t *testing.T) {
	fixture := newReplanFixture(t)
	fixture.queue.active[fixture.pass.ID] = nil // pass never had commands queued
	fixture.passes.updateErr = errors.New("pass snapshot write failed")
	fixture.passes.failFirst = 1 // stage-4 commit fails, the review flag update succeeds
	fixture.queue.retireErr = errors.New("queue retire write failed")
	fixture.apply()

	if actions := fixture.auditActions(); actions["replan.needs-review"] != 1 {
		t.Fatalf("expected one needs-review event: %#v", actions)
	}
	if reason := fixture.passes.passes[fixture.pass.ID].FailureReason; !strings.Contains(reason, "manual review") {
		t.Fatalf("pass failure reason %q does not flag manual review", reason)
	}
}

func TestApplyRevisionSkipsPassAlreadyAtRevision(t *testing.T) {
	fixture := newReplanFixture(t)
	value := fixture.passes.passes[fixture.pass.ID]
	value.Window.Revision = 2
	fixture.passes.passes[fixture.pass.ID] = value
	fixture.apply()

	if len(fixture.windows.applied) != 0 || fixture.queue.calls != 0 {
		t.Fatalf("replan touched an up-to-date pass: applied=%#v queue calls=%d", fixture.windows.applied, fixture.queue.calls)
	}
}
