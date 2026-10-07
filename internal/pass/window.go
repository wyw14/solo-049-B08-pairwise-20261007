package pass

import (
	"fmt"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

type WindowRecomputer struct {
	store *store.WindowStore
	audit *audit.Trail
}

func NewWindowRecomputer(windowStore *store.WindowStore, trail *audit.Trail) *WindowRecomputer {
	return &WindowRecomputer{store: windowStore, audit: trail}
}

func (r *WindowRecomputer) Apply(passID string, window Window) error {
	if err := r.save(passID, window); err != nil {
		return err
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "window.recomputed", Subject: passID, Fields: map[string]any{"revision": window.Revision, "aos": window.AOS, "los": window.LOS}})
	return nil
}

// Get returns the window currently published for a pass, so a replan can
// snapshot it for compensation before writing a new revision.
func (r *WindowRecomputer) Get(passID string) (Window, bool) {
	record, found := r.store.Get(passID)
	if !found {
		return Window{}, false
	}
	return Window{AOS: record.AOS, LOS: record.LOS, Revision: record.Revision}, true
}

// Restore republishes a previous window after a failed replan. The distinct
// audit action keeps the recomputed-then-restored pair visible in the trail.
func (r *WindowRecomputer) Restore(passID string, window Window) error {
	if err := r.save(passID, window); err != nil {
		return err
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "window.restored", Subject: passID, Severity: audit.Warning, Fields: map[string]any{"revision": window.Revision, "aos": window.AOS, "los": window.LOS}})
	return nil
}

func (r *WindowRecomputer) save(passID string, window Window) error {
	if err := window.Validate(); err != nil {
		return err
	}
	if passID == "" {
		return fmt.Errorf("pass ID is required")
	}
	record := store.WindowRecord{PassID: passID, AOS: window.AOS, LOS: window.LOS, Revision: window.Revision, Updated: time.Now().UTC()}
	return r.store.Save(record)
}
