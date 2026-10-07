package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type WindowRecord struct {
	PassID   string    `json:"pass_id"`
	AOS      time.Time `json:"aos"`
	LOS      time.Time `json:"los"`
	Revision int64     `json:"revision"`
	Updated  time.Time `json:"updated"`
}

// PendingWindow describes a window revision that was staged for a replan but
// never reached a confirmed end state. It is reconstructed from a transaction
// sidecar after a crash or a failed commit. Committed is inferred from the
// live snapshot, so it stays correct when a crash lands between the snapshot
// switch and the transaction marker update.
type PendingWindow struct {
	PassID    string
	Window    WindowRecord
	Backup    *WindowRecord
	Committed bool
	OpenedAt  time.Time
}

// WindowStore publishes whole revision-bound windows under one lock.
type WindowStore struct {
	mu      sync.RWMutex
	path    string
	windows map[string]WindowRecord
}

func NewWindowStore(paths Paths) (*WindowStore, error) {
	store := &WindowStore{
		path:    paths.Snapshot("pass-windows"),
		windows: make(map[string]WindowRecord),
	}
	var values []WindowRecord
	found, err := ReadJSON(store.path, &values)
	if err != nil {
		return nil, err
	}
	if found {
		for _, value := range values {
			store.windows[value.PassID] = value
		}
	}
	return store, nil
}

func (s *WindowStore) Save(window WindowRecord) error {
	if err := validateWindow(window); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	window.Updated = time.Now().UTC()
	s.windows[window.PassID] = window
	return s.flushLocked()
}

func (s *WindowStore) Get(passID string) (WindowRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	window, found := s.windows[passID]
	return window, found
}

func (s *WindowStore) List() []WindowRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]WindowRecord, 0, len(s.windows))
	for _, window := range s.windows {
		values = append(values, window)
	}
	return values
}

func (s *WindowStore) flushLocked() error {
	values := make([]WindowRecord, 0, len(s.windows))
	for _, window := range s.windows {
		values = append(values, window)
	}
	return WriteJSON(s.path, values)
}

// windowTx is the on-disk transaction marker for one pass window revision.
// Backup stays nil between PrepareWindow and CommitPreparedWindow; once it is
// published the main snapshot may be switched, and Rollback can always restore
// the previous record.
type windowTx struct {
	PassID   string        `json:"pass_id"`
	Window   WindowRecord  `json:"window"`
	Backup   *WindowRecord `json:"backup,omitempty"`
	HadOld   bool          `json:"had_old"`
	OpenedAt time.Time     `json:"opened_at"`
}

func (s *WindowStore) txPath(passID string) string {
	return filepath.Join(filepath.Dir(s.path), "pass-windows.tx-"+SafeName(passID)+".json")
}

// HasPreparedWindow reports whether a window transaction marker exists for
// passID, including one that is not currently parseable.
func (s *WindowStore) HasPreparedWindow(passID string) bool {
	_, err := os.Stat(s.txPath(passID))
	return err == nil
}

func validateWindow(window WindowRecord) error {
	if window.PassID == "" || window.Revision < 1 || !window.LOS.After(window.AOS) {
		return fmt.Errorf("invalid revision-bound pass window")
	}
	return nil
}

// PrepareWindow stages a new window in a sidecar without touching the
// committed snapshot. A pending transaction for the same pass must be cleared
// first.
func (s *WindowStore) PrepareWindow(window WindowRecord) error {
	if err := validateWindow(window); err != nil {
		return err
	}
	txPath := s.txPath(window.PassID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if found, err := ReadJSON(txPath, &windowTx{}); err != nil {
		return err
	} else if found {
		return fmt.Errorf("window replan for pass %s is already in progress", window.PassID)
	}
	window.Updated = time.Now().UTC()
	return WriteJSON(txPath, windowTx{PassID: window.PassID, Window: window, OpenedAt: time.Now().UTC()})
}

// CommitPreparedWindow publishes the staged window. The previous record is
// written into the sidecar before the snapshot is switched, so a failure at
// any point afterwards remains recoverable through RollbackPreparedWindow.
func (s *WindowStore) CommitPreparedWindow(passID string) (WindowRecord, error) {
	txPath := s.txPath(passID)
	s.mu.Lock()
	defer s.mu.Unlock()
	var tx windowTx
	found, err := ReadJSON(txPath, &tx)
	if err != nil {
		return WindowRecord{}, fmt.Errorf("no staged window for pass %s: %w", passID, err)
	}
	if !found {
		return WindowRecord{}, fmt.Errorf("no staged window for pass %s", passID)
	}
	old, hadOld := s.windows[passID]
	tx.Backup = &old
	tx.HadOld = hadOld
	if err := WriteJSON(txPath, tx); err != nil {
		return WindowRecord{}, err
	}
	tx.Window.Updated = time.Now().UTC()
	s.windows[passID] = tx.Window
	if err := s.flushLocked(); err != nil {
		// Disk still holds the previous snapshot; keep memory consistent
		// with it so the failure can be compensated within the process.
		if hadOld {
			s.windows[passID] = old
		} else {
			delete(s.windows, passID)
		}
		return WindowRecord{}, err
	}
	return tx.Window, nil
}

// RollbackPreparedWindow restores the record captured at commit time (or
// removes the entry when there was none) and clears the sidecar. When the
// window was staged but never committed (no backup), the committed snapshot
// is left untouched.
func (s *WindowStore) RollbackPreparedWindow(passID string) error {
	txPath := s.txPath(passID)
	s.mu.Lock()
	defer s.mu.Unlock()
	var tx windowTx
	found, err := ReadJSON(txPath, &tx)
	if err != nil {
		return fmt.Errorf("no staged window for pass %s: %w", passID, err)
	}
	if !found {
		// Nothing staged: nothing to undo.
		return nil
	}
	live, hasLive := s.windows[passID]
	stageIsLive := hasLive && live.Revision == tx.Window.Revision && live.AOS.Equal(tx.Window.AOS) && live.LOS.Equal(tx.Window.LOS)
	if stageIsLive && tx.Backup == nil {
		return fmt.Errorf("committed window for pass %s has no backup to restore", passID)
	}
	if tx.Backup != nil {
		if tx.HadOld {
			s.windows[passID] = *tx.Backup
		} else {
			delete(s.windows, passID)
		}
		if err := s.flushLocked(); err != nil {
			return err
		}
	}
	return RemoveFile(txPath)
}

// ConfirmPreparedWindow drops the transaction marker after every participant
// has committed and the pass snapshot was advanced.
func (s *WindowStore) ConfirmPreparedWindow(passID string) error {
	return RemoveFile(s.txPath(passID))
}

// DiscardPreparedWindow removes a staged-but-uncommitted sidecar without
// changing the committed snapshot.
func (s *WindowStore) DiscardPreparedWindow(passID string) error {
	return RemoveFile(s.txPath(passID))
}

// PendingWindows lists unfinished window transactions left on disk.
func (s *WindowStore) PendingWindows() []PendingWindow {
	dir := filepath.Dir(s.path)
	entries, err := ReadDirNames(dir)
	if err != nil {
		return nil
	}
	pending := make([]PendingWindow, 0, len(entries))
	for _, name := range entries {
		if !strings.HasPrefix(name, "pass-windows.tx-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		var tx windowTx
		found, err := ReadJSON(filepath.Join(dir, name), &tx)
		if err != nil || !found {
			continue
		}
		s.mu.RLock()
		live, ok := s.windows[tx.PassID]
		s.mu.RUnlock()
		committed := ok && live.Revision == tx.Window.Revision && live.AOS.Equal(tx.Window.AOS) && live.LOS.Equal(tx.Window.LOS)
		pending = append(pending, PendingWindow{PassID: tx.PassID, Window: tx.Window, Backup: tx.Backup, Committed: committed, OpenedAt: tx.OpenedAt})
	}
	return pending
}
