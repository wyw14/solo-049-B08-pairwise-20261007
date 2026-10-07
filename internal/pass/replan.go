package pass

import (
	"fmt"
	"sync"
	"time"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/satellite"
	"orbitlink/internal/scheduler"
)

type Replanner struct {
	passes  *Service
	windows *WindowRecomputer
	queue   *scheduler.CommandQueue
	audit   *audit.Trail
	// mu serializes revision applications so a newer ephemeris revision can
	// never interleave its window/command transaction with an older one.
	mu sync.Mutex
}

func NewReplanner(passes *Service, windows *WindowRecomputer, queue *scheduler.CommandQueue, trail *audit.Trail) *Replanner {
	return &Replanner{passes: passes, windows: windows, queue: queue, audit: trail}
}

func (r *Replanner) ApplyRevision(previous, current satellite.Ephemeris) {
	if current.Revision <= previous.Revision {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, value := range r.passes.List() {
		if value.SatelliteID != current.SatelliteID || value.Phase != Planned {
			continue
		}
		r.applyOne(value, current)
	}
}

// applyOne moves a single planned pass through the replan transaction:
//
//	stage window + commands -> commit window -> commit commands ->
//	advance the pass snapshot -> confirm both transaction markers.
//
// Any failure before the pass snapshot advances is compensated: the previous
// window and command set are restored from the backups captured at commit
// time. When compensation cannot be guaranteed, the pass is parked in
// ReplanReview instead of being left across two revisions.
func (r *Replanner) applyOne(value Pass, current satellite.Ephemeris) {
	passID := value.ID
	// Clear a transaction a previous process could not finish before staging
	// a new revision for the same pass.
	if !r.reconcilePendingLocked(passID) {
		return
	}
	shift := time.Duration(current.Revision-value.Window.Revision) * time.Second
	window := Window{AOS: value.Window.AOS.Add(shift), LOS: value.Window.LOS.Add(shift), Revision: current.Revision}

	// Stage 1: tentative window. Nothing committed yet.
	if err := r.windows.Prepare(passID, window); err != nil {
		r.auditFailure(passID, current.Revision, "prepare.window", err, audit.Warning)
		return
	}
	// Stage 2: build the pointing plan (pure computation, no side effects).
	commands, err := antenna.BuildCommands(passID, value.AntennaID, current.Revision, revisedTrackPoints(window))
	if err != nil {
		r.abortPrepared(passID, current.Revision, "build.commands", err)
		return
	}
	// Stage 3: tentative commands.
	if _, err := r.queue.PrepareReplacement(passID, current.Revision, commands); err != nil {
		r.abortPrepared(passID, current.Revision, "prepare.commands", err)
		return
	}

	// Commit window. The main snapshot switches, but the previous record is
	// held in the transaction marker for compensation.
	if _, err := r.windows.CommitPrepared(passID); err != nil {
		r.abortWindowCommitFailure(passID, current.Revision, err)
		return
	}
	// Commit commands.
	if err := r.queue.CommitPrepared(passID); err != nil {
		r.compensate(passID, current.Revision, "commit.commands", err)
		return
	}
	// Advance the pass snapshot last. Both stores are committed; if this
	// cannot take effect the whole revision is rolled back.
	if err := r.passes.update(passID, func(pass *Pass) error {
		if pass.Phase != Planned || pass.Window.Revision != value.Window.Revision {
			return fmt.Errorf("pass moved on while replan was committing (phase=%s revision=%d)", pass.Phase, pass.Window.Revision)
		}
		pass.Window = window
		return nil
	}); err != nil {
		r.compensate(passID, current.Revision, "advance.pass", err)
		return
	}

	// Confirm. The new revision is now the committed state everywhere; a
	// marker cleanup failure leaves consistent state and is reconciled by
	// Recover on the next startup.
	if err := r.windows.ConfirmPrepared(passID); err != nil {
		r.auditFailure(passID, current.Revision, "confirm.window", err, audit.Warning)
	}
	if err := r.queue.ConfirmPrepared(passID); err != nil {
		r.auditFailure(passID, current.Revision, "confirm.commands", err, audit.Warning)
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.applied", Subject: passID, Fields: map[string]any{"revision": current.Revision, "aos": window.AOS, "los": window.LOS}})
}

// abortPrepared discards both staged-but-uncommitted transactions. The
// committed window and queue are untouched.
func (r *Replanner) abortPrepared(passID string, revision int64, stage string, cause error) {
	_ = r.windows.DiscardPrepared(passID)
	_ = r.queue.DiscardPrepared(passID)
	r.auditFailure(passID, revision, stage, cause, audit.Warning)
}

// abortWindowCommitFailure handles a window commit that may or may not have
// switched the snapshot: roll back when a backup exists, otherwise just drop
// the marker. Commands were never committed.
func (r *Replanner) abortWindowCommitFailure(passID string, revision int64, cause error) {
	if err := r.windows.RollbackPrepared(passID); err != nil {
		_ = r.windows.DiscardPrepared(passID)
	}
	_ = r.queue.DiscardPrepared(passID)
	r.auditFailure(passID, revision, "commit.window", cause, audit.Warning)
}

// compensate undoes committed window and command changes using the backups
// captured at commit time. The stores are afterwards checked against the
// revision the pass snapshot still holds; anything that cannot be proven
// restored parks the pass for operator review instead of leaving a half plan.
func (r *Replanner) compensate(passID string, revision int64, stage string, cause error) {
	windowErr := r.windows.RollbackPrepared(passID)
	queueErr := r.queue.RollbackPrepared(passID)
	current, found := r.passes.Get(passID)
	restored := found && windowErr == nil && queueErr == nil
	if restored {
		if record, ok := r.windows.store.Get(passID); !ok || record.Revision != current.Window.Revision {
			restored = false
			windowErr = fmt.Errorf("window store did not return to revision %d", current.Window.Revision)
		}
		if restored && r.activeCommandRevision(passID) != current.Window.Revision {
			restored = false
			queueErr = fmt.Errorf("command queue did not return to revision %d", current.Window.Revision)
		}
	}
	if !restored {
		reason := fmt.Sprintf("%s failed: %v (rollback window: %v, commands: %v)", stage, cause, windowErr, queueErr)
		r.markReviewOrEscalate(passID, revision, "rollback.required-review", reason)
		return
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.rolled_back", Subject: passID, Severity: audit.Warning, Fields: map[string]any{"revision": revision, "stage": stage, "error": cause.Error()}})
}

func (r *Replanner) activeCommandRevision(passID string) int64 {
	active := int64(-1)
	for _, command := range r.queue.List(passID) {
		if !command.Retired && command.Revision > active {
			active = command.Revision
		}
	}
	return active
}

func (r *Replanner) auditFailure(passID string, revision int64, stage string, cause error, severity audit.Severity) {
	fields := map[string]any{"revision": revision, "stage": stage}
	if cause != nil {
		fields["error"] = cause.Error()
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.failed", Subject: passID, Severity: severity, Fields: fields})
}

// Recover finishes transactions left behind by a crash or an earlier failed
// confirmation, and is also invoked per pass before a new revision is staged.
func (r *Replanner) Recover() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	passIDs := make(map[string]bool)
	for _, pending := range r.windows.PendingWindows() {
		passIDs[pending.PassID] = true
	}
	for _, pending := range r.queue.PendingReplacements() {
		passIDs[pending.PassID] = true
	}
	recovered := make([]string, 0, len(passIDs))
	for passID := range passIDs {
		if r.reconcilePendingLocked(passID) {
			recovered = append(recovered, passID)
		}
	}
	return recovered
}

func (r *Replanner) reconcilePendingLocked(passID string) bool {
	if !r.windows.HasPrepared(passID) && !r.queue.HasPrepared(passID) {
		return true
	}
	var windowRevision int64
	windowCommitted := false
	windowPresent := false
	for _, pending := range r.windows.PendingWindows() {
		if pending.PassID == passID {
			windowPresent, windowCommitted, windowRevision = true, pending.Committed, pending.Window.Revision
		}
	}
	queuePresent, queueCommitted := false, false
	for _, pending := range r.queue.PendingReplacements() {
		if pending.PassID == passID {
			queuePresent, queueCommitted = true, pending.Committed
			if !windowPresent {
				windowRevision = pending.Revision
			}
		}
	}
	value, found := r.passes.Get(passID)
	windowLive, windowHasLive := r.windows.store.Get(passID)
	windowLiveAtRevision := windowHasLive && windowLive.Revision == windowRevision
	queueLiveAtRevision := r.activeCommandRevision(passID) == windowRevision
	if found && value.Window.Revision == windowRevision &&
		(!windowPresent || windowCommitted && windowLiveAtRevision) &&
		(!queuePresent || queueCommitted && queueLiveAtRevision) &&
		(windowPresent || windowLiveAtRevision) && (queuePresent || queueLiveAtRevision) {
		// Every participant and the pass snapshot reached the new revision;
		// only the confirmation of one marker was missing.
		if windowPresent {
			if err := r.windows.ConfirmPrepared(passID); err != nil {
				r.auditFailure(passID, windowRevision, "confirm.window", err, audit.Warning)
			}
		}
		if queuePresent {
			if err := r.queue.ConfirmPrepared(passID); err != nil {
				r.auditFailure(passID, windowRevision, "confirm.commands", err, audit.Warning)
			}
		}
		_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.recovered", Subject: passID, Fields: map[string]any{"revision": windowRevision, "outcome": "committed"}})
		return true
	}
	if found && value.Window.Revision == windowRevision {
		// The pass snapshot reached a revision whose stores cannot both be
		// proven committed: restoring either side would fork the plan, so a
		// human must choose the authoritative window.
		reason := fmt.Sprintf("replan for revision %d reached the pass snapshot but window/command commits disagree", windowRevision)
		return r.markReviewOrEscalate(passID, windowRevision, "recover.required-review", reason)
	}
	// Pass snapshot stayed on the previous revision: undo whatever reached
	// the two stores. Rollback is a no-op for participants never committed.
	windowErr := r.windows.RollbackPrepared(passID)
	queueErr := r.queue.RollbackPrepared(passID)
	if windowErr != nil || queueErr != nil {
		reason := fmt.Sprintf("recovery could not roll back revision %d (window: %v, commands: %v)", windowRevision, windowErr, queueErr)
		return r.markReviewOrEscalate(passID, windowRevision, "recover.required-review", reason)
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.recovered", Subject: passID, Severity: audit.Warning, Fields: map[string]any{"revision": windowRevision, "outcome": "rolled_back"}})
	return true
}

func (r *Replanner) markReviewOrEscalate(passID string, revision int64, action, reason string) bool {
	if err := r.passes.MarkReplanReview(passID, revision, reason); err != nil {
		r.auditFailure(passID, revision, action, fmt.Errorf("%s; mark review: %w", reason, err), audit.Critical)
		return false
	}
	r.auditFailure(passID, revision, action, fmt.Errorf("%s", reason), audit.Critical)
	return false
}

func revisedTrackPoints(window Window) []antenna.TrackPoint {
	return []antenna.TrackPoint{
		{At: window.AOS.Add(-20 * time.Second), Azimuth: 120, Elevation: 10},
		{At: window.AOS, Azimuth: 135, Elevation: 20},
		{At: window.LOS, Azimuth: 240, Elevation: 8},
	}
}

// ResolveReview takes an operator decision for a pass parked in ReplanReview.
// accept=true rebuilds window and queue at the pending revision; accept=false
// pins both stores back to the revision the pass still holds. The pass
// snapshot is advanced last, so a failure leaves the pass in review and the
// resolution can be retried.
func (r *Replanner) ResolveReview(passID string, accept bool) (Pass, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, found := r.passes.Get(passID)
	if !found {
		return Pass{}, fmt.Errorf("pass %s not found", passID)
	}
	if value.Phase != ReplanReview {
		return Pass{}, fmt.Errorf("pass %s is not awaiting replan review", passID)
	}
	revision := value.Window.Revision
	window := value.Window
	if accept {
		shift := time.Duration(value.ReplanRevision-value.Window.Revision) * time.Second
		revision = value.ReplanRevision
		window = Window{AOS: value.Window.AOS.Add(shift), LOS: value.Window.LOS.Add(shift), Revision: revision}
	}
	points := revisedTrackPoints(window)
	commands, err := antenna.BuildCommands(passID, value.AntennaID, revision, points)
	if err != nil {
		return Pass{}, err
	}
	// Leftover markers described the abandoned attempt; the operator decision
	// now writes the chosen state directly.
	_ = r.windows.DiscardPrepared(passID)
	_ = r.queue.DiscardPrepared(passID)
	if err := r.windows.Apply(passID, window); err != nil {
		return Pass{}, err
	}
	if err := r.queue.Replace(passID, revision, commands); err != nil {
		return Pass{}, err
	}
	if accept {
		if err := r.passes.AcceptReviewedWindow(passID, window); err != nil {
			return Pass{}, err
		}
	} else if err := r.passes.KeepCurrentWindow(passID); err != nil {
		return Pass{}, err
	}
	outcome := "kept"
	if accept {
		outcome = "accepted"
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.review-resolved", Subject: passID, Fields: map[string]any{"revision": revision, "outcome": outcome}})
	resolved, _ := r.passes.Get(passID)
	return resolved, nil
}

func (r *Replanner) QueueInitial(passID string) error {
	value, found := r.passes.Get(passID)
	if !found {
		return fmt.Errorf("pass %s not found", passID)
	}
	points := []antenna.TrackPoint{
		{At: value.Window.AOS.Add(-20 * time.Second), Azimuth: 100, Elevation: 10},
		{At: value.Window.AOS, Azimuth: 125, Elevation: 25},
		{At: value.Window.LOS, Azimuth: 250, Elevation: 7},
	}
	commands, err := antenna.BuildCommands(value.ID, value.AntennaID, value.Window.Revision, points)
	if err != nil {
		return err
	}
	return r.queue.Replace(value.ID, value.Window.Revision, commands)
}
