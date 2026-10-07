package pass

import (
	"fmt"
	"time"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/satellite"
	"orbitlink/internal/scheduler"
)

// passStore is the part of the pass service the replanner relies on.
type passStore interface {
	List() []Pass
	Get(id string) (Pass, bool)
	update(passID string, apply func(*Pass) error) error
}

// windowStore publishes revision-bound pass windows.
type windowStore interface {
	Apply(passID string, window Window) error
	Get(passID string) (Window, bool)
	Restore(passID string, window Window) error
}

// commandQueue holds the active pointing plan per pass.
type commandQueue interface {
	Replace(passID string, revision int64, commands []scheduler.PointingCommand) error
	RetirePass(passID, cause string) error
	List(passID string) []scheduler.PointingCommand
}

type Replanner struct {
	passes  passStore
	windows windowStore
	queue   commandQueue
	audit   *audit.Trail
}

func NewReplanner(passes passStore, windows windowStore, queue commandQueue, trail *audit.Trail) *Replanner {
	return &Replanner{passes: passes, windows: windows, queue: queue, audit: trail}
}

// ApplyRevision shifts every planned pass of a satellite to a newer ephemeris
// revision. Each pass migrates as a compensating transaction: the window
// store, the pointing queue and the pass record either all move to the new
// revision, or every completed step is rolled back so no half-applied plan
// survives. If a rollback itself fails, the pass is flagged for manual review.
func (r *Replanner) ApplyRevision(previous, current satellite.Ephemeris) {
	if current.Revision <= previous.Revision {
		return
	}
	for _, value := range r.passes.List() {
		if value.SatelliteID != current.SatelliteID || value.Phase != Planned {
			continue
		}
		r.applyToPass(value, previous, current)
	}
}

func (r *Replanner) applyToPass(value Pass, previous, current satellite.Ephemeris) {
	if value.Window.Revision >= current.Revision {
		return
	}
	shift := time.Duration(current.Revision-previous.Revision) * time.Second
	window := Window{AOS: value.Window.AOS.Add(shift), LOS: value.Window.LOS.Add(shift), Revision: current.Revision}

	// Build the pointing plan before mutating anything, so a build failure
	// leaves the old plan fully intact.
	points := []antenna.TrackPoint{
		{At: window.AOS.Add(-20 * time.Second), Azimuth: 120, Elevation: 10},
		{At: window.AOS, Azimuth: 135, Elevation: 20},
		{At: window.LOS, Azimuth: 240, Elevation: 8},
	}
	commands, err := antenna.BuildCommands(value.ID, value.AntennaID, current.Revision, points)
	if err != nil {
		r.auditFailed(value.ID, current.Revision, "build commands", err)
		return
	}

	// Snapshot the current plan for compensation.
	previousWindow, found := r.windows.Get(value.ID)
	if !found {
		previousWindow = value.Window
	}
	var active []scheduler.PointingCommand
	for _, command := range r.queue.List(value.ID) {
		if !command.Retired {
			active = append(active, command)
		}
	}

	if err := r.windows.Apply(value.ID, window); err != nil {
		r.auditFailed(value.ID, current.Revision, "apply window", err)
		return
	}
	if err := r.queue.Replace(value.ID, current.Revision, commands); err != nil {
		r.compensate(value, previousWindow, active, false, current.Revision, fmt.Errorf("replace pointing commands: %w", err))
		return
	}
	if err := r.passes.update(value.ID, func(pass *Pass) error {
		if pass.Phase != Planned {
			return fmt.Errorf("pass left phase %s during replan", Planned)
		}
		pass.Window = window
		return nil
	}); err != nil {
		r.compensate(value, previousWindow, active, true, current.Revision, fmt.Errorf("update pass window: %w", err))
		return
	}
}

// compensate rolls a partially applied revision back to the previous plan.
// queueCommitted reports whether the pointing queue already holds the new
// revision and must be restored as well. If any rollback step fails, the pass
// is flagged for manual review instead of being left silently diverged.
func (r *Replanner) compensate(value Pass, previousWindow Window, active []scheduler.PointingCommand, queueCommitted bool, targetRevision int64, cause error) {
	var problems []string
	if queueCommitted {
		if len(active) > 0 {
			if err := r.queue.Replace(value.ID, previousWindow.Revision, active); err != nil {
				problems = append(problems, "restore pointing commands: "+err.Error())
			}
		} else if err := r.queue.RetirePass(value.ID, "replan rolled back"); err != nil {
			problems = append(problems, "retire pointing commands: "+err.Error())
		}
	}
	if err := r.windows.Restore(value.ID, previousWindow); err != nil {
		problems = append(problems, "restore window: "+err.Error())
	}
	if len(problems) == 0 {
		_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.rolled-back", Subject: value.ID, Severity: audit.Warning, Fields: map[string]any{"target_revision": targetRevision, "restored_revision": previousWindow.Revision, "cause": cause.Error()}})
		return
	}
	reason := fmt.Sprintf("replan to revision %d diverged, manual review required: %s", targetRevision, cause.Error())
	if err := r.passes.update(value.ID, func(pass *Pass) error {
		pass.FailureReason = reason
		return nil
	}); err != nil {
		problems = append(problems, "flag pass for review: "+err.Error())
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.needs-review", Subject: value.ID, Severity: audit.Critical, Fields: map[string]any{"target_revision": targetRevision, "cause": cause.Error(), "rollback_errors": problems}})
}

func (r *Replanner) auditFailed(passID string, revision int64, stage string, err error) {
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "replan.failed", Subject: passID, Severity: audit.Warning, Fields: map[string]any{"stage": stage, "revision": revision, "error": err.Error()}})
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
