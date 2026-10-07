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
	if err := window.Validate(); err != nil {
		return err
	}
	if passID == "" {
		return fmt.Errorf("pass ID is required")
	}
	if err := r.store.Save(windowRecord(passID, window)); err != nil {
		return err
	}
	_, _ = r.audit.Record(audit.Event{Component: "pass", Action: "window.recomputed", Subject: passID, Fields: map[string]any{"revision": window.Revision, "aos": window.AOS, "los": window.LOS}})
	return nil
}

// Prepare stages a replacement window without touching the committed
// snapshot.
func (r *WindowRecomputer) Prepare(passID string, window Window) error {
	if err := window.Validate(); err != nil {
		return err
	}
	if passID == "" {
		return fmt.Errorf("pass ID is required")
	}
	return r.store.PrepareWindow(windowRecord(passID, window))
}

// CommitPrepared publishes the staged window. The previous record stays
// recoverable through RollbackPrepared until ConfirmPrepared is called.
func (r *WindowRecomputer) CommitPrepared(passID string) (Window, error) {
	record, err := r.store.CommitPreparedWindow(passID)
	if err != nil {
		return Window{}, err
	}
	return windowFromRecord(record), nil
}

// RollbackPrepared restores the previous window captured at commit time.
func (r *WindowRecomputer) RollbackPrepared(passID string) error {
	return r.store.RollbackPreparedWindow(passID)
}

// ConfirmPrepared drops the transaction marker after a successful replan.
func (r *WindowRecomputer) ConfirmPrepared(passID string) error {
	return r.store.ConfirmPreparedWindow(passID)
}

// DiscardPrepared removes a staged-but-uncommitted window.
func (r *WindowRecomputer) DiscardPrepared(passID string) error {
	return r.store.DiscardPreparedWindow(passID)
}

// PendingWindows exposes unfinished window transactions for crash recovery.
func (r *WindowRecomputer) PendingWindows() []store.PendingWindow {
	return r.store.PendingWindows()
}

// HasPrepared reports whether a window transaction marker exists for passID.
func (r *WindowRecomputer) HasPrepared(passID string) bool {
	return r.store.HasPreparedWindow(passID)
}

func windowRecord(passID string, window Window) store.WindowRecord {
	return store.WindowRecord{PassID: passID, AOS: window.AOS, LOS: window.LOS, Revision: window.Revision, Updated: time.Now().UTC()}
}

func windowFromRecord(record store.WindowRecord) Window {
	return Window{AOS: record.AOS, LOS: record.LOS, Revision: record.Revision}
}
