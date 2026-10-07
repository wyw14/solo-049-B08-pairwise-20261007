package scheduler

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/store"
)

type PointingCommand struct {
	ID          string    `json:"id"`
	PassID      string    `json:"pass_id"`
	AntennaID   string    `json:"antenna_id"`
	Revision    int64     `json:"revision"`
	ExecuteAt   time.Time `json:"execute_at"`
	Azimuth     float64   `json:"azimuth"`
	Elevation   float64   `json:"elevation"`
	Retired     bool      `json:"retired"`
	RetireCause string    `json:"retire_cause,omitempty"`
}

type CommandQueue struct {
	mu       sync.RWMutex
	path     string
	commands []PointingCommand
	active   map[string]int64
}

// PendingCommandRevision carries the staged command set for a replan,
// reconstructed from a transaction sidecar after a crash or failed commit.
type PendingCommandRevision struct {
	PassID    string
	Revision  int64
	Commands  []PointingCommand
	Backup    []PointingCommand
	Committed bool
	OpenedAt  time.Time
}

func NewCommandQueue(paths store.Paths) (*CommandQueue, error) {
	queue := &CommandQueue{path: paths.Snapshot("pointing-commands"), active: make(map[string]int64)}
	found, err := store.ReadJSON(queue.path, &queue.commands)
	if err != nil {
		return nil, err
	}
	if found {
		for _, command := range queue.commands {
			if !command.Retired && command.Revision > queue.active[command.PassID] {
				queue.active[command.PassID] = command.Revision
			}
		}
	}
	return queue, nil
}

func (q *CommandQueue) Replace(passID string, revision int64, commands []PointingCommand) error {
	if passID == "" || revision < 1 || len(commands) == 0 {
		return fmt.Errorf("pass, revision and pointing commands are required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for index := range q.commands {
		if q.commands[index].PassID == passID && !q.commands[index].Retired {
			q.commands[index].Retired = true
			q.commands[index].RetireCause = fmt.Sprintf("replaced by revision %d", revision)
		}
	}
	for _, command := range commands {
		command.ID = uuid.NewString()
		command.PassID = passID
		command.Revision = revision
		q.commands = append(q.commands, command)
	}
	q.active[passID] = revision
	return store.WriteJSON(q.path, q.commands)
}

// commandTx is the on-disk transaction marker for one queue replacement.
// Before CommitPrepared the main queue is untouched and Backup is empty; after
// commit the backup lets Rollback restore the exact previous slice.
type commandTx struct {
	PassID    string            `json:"pass_id"`
	Revision  int64             `json:"revision"`
	Commands  []PointingCommand `json:"commands"`
	Backup    []PointingCommand `json:"backup,omitempty"`
	Committed bool              `json:"committed"`
	OpenedAt  time.Time         `json:"opened_at"`
}

func (q *CommandQueue) txPath(passID string) string {
	return filepath.Join(filepath.Dir(q.path), "pointing-commands.tx-"+store.SafeName(passID)+".json")
}

// PrepareReplacement validates and stages the new command set in a sidecar,
// assigning durable IDs, without touching the committed queue.
func (q *CommandQueue) PrepareReplacement(passID string, revision int64, commands []PointingCommand) ([]PointingCommand, error) {
	if passID == "" || revision < 1 || len(commands) == 0 {
		return nil, fmt.Errorf("pass, revision and pointing commands are required")
	}
	staged := make([]PointingCommand, len(commands))
	for index, command := range commands {
		command.ID = uuid.NewString()
		command.PassID = passID
		command.Revision = revision
		command.Retired = false
		command.RetireCause = ""
		staged[index] = command
	}
	txPath := q.txPath(passID)
	q.mu.Lock()
	defer q.mu.Unlock()
	if found, err := store.ReadJSON(txPath, &commandTx{}); err != nil {
		return nil, err
	} else if found {
		return nil, fmt.Errorf("command replacement for pass %s is already in progress", passID)
	}
	if err := store.WriteJSON(txPath, commandTx{PassID: passID, Revision: revision, Commands: staged, OpenedAt: time.Now().UTC()}); err != nil {
		return nil, err
	}
	return staged, nil
}

// HasPreparedReplacement reports whether a command transaction marker exists
// for passID, including one that is not currently parseable.
func (q *CommandQueue) HasPreparedReplacement(passID string) bool {
	_, err := os.Stat(q.txPath(passID))
	return err == nil
}

// HasPrepared is the short alias used by transaction coordination code.
func (q *CommandQueue) HasPrepared(passID string) bool {
	return q.HasPreparedReplacement(passID)
}

// CommitPrepared retires the currently active commands for the pass and
// appends the staged set. The previous slice is captured in the sidecar
// first, so Rollback can always undo the switch.
func (q *CommandQueue) CommitPrepared(passID string) error {
	txPath := q.txPath(passID)
	q.mu.Lock()
	defer q.mu.Unlock()
	var tx commandTx
	found, err := store.ReadJSON(txPath, &tx)
	if err != nil {
		return fmt.Errorf("no staged commands for pass %s: %w", passID, err)
	}
	if !found {
		return fmt.Errorf("no staged commands for pass %s", passID)
	}
	tx.Backup = cloneCommands(q.commands)
	tx.Committed = false
	if err := store.WriteJSON(txPath, tx); err != nil {
		return err
	}
	previous := cloneCommands(q.commands)
	for index := range q.commands {
		if q.commands[index].PassID == passID && !q.commands[index].Retired {
			q.commands[index].Retired = true
			q.commands[index].RetireCause = fmt.Sprintf("replaced by revision %d", tx.Revision)
		}
	}
	q.commands = append(q.commands, tx.Commands...)
	q.active[passID] = tx.Revision
	if err := store.WriteJSON(q.path, q.commands); err != nil {
		// Disk still holds the previous queue; keep memory consistent with it.
		q.commands = previous
		q.active = rebuildActive(q.commands)
		return err
	}
	tx.Committed = true
	return store.WriteJSON(txPath, tx)
}

// RollbackPrepared restores the command slice captured at commit time and
// clears the sidecar. Whether the switch happened is inferred from the live
// queue (the staged set being served), not from the marker flag, because a
// crash can land between the snapshot rename and the flag update.
func (q *CommandQueue) RollbackPrepared(passID string) error {
	txPath := q.txPath(passID)
	q.mu.Lock()
	defer q.mu.Unlock()
	var tx commandTx
	found, err := store.ReadJSON(txPath, &tx)
	if err != nil {
		return fmt.Errorf("no staged commands for pass %s: %w", passID, err)
	}
	if !found {
		// Nothing staged: nothing to undo.
		return nil
	}
	if q.stageIsLiveLocked(tx) {
		if len(tx.Backup) == 0 {
			return fmt.Errorf("committed replacement for pass %s has no backup to restore", passID)
		}
		q.commands = cloneCommands(tx.Backup)
		q.active = rebuildActive(q.commands)
		if err := store.WriteJSON(q.path, q.commands); err != nil {
			return err
		}
	}
	return store.RemoveFile(txPath)
}

// ConfirmPrepared drops the transaction marker after the window revision and
// pass snapshot are also confirmed.
func (q *CommandQueue) ConfirmPrepared(passID string) error {
	return store.RemoveFile(q.txPath(passID))
}

// DiscardPrepared removes a staged-but-uncommitted sidecar.
func (q *CommandQueue) DiscardPrepared(passID string) error {
	return store.RemoveFile(q.txPath(passID))
}

// PendingReplacements lists unfinished command transactions left on disk.
// Committed is inferred from the queue snapshot itself (staged command IDs
// present and active) rather than the marker, because a crash between the
// snapshot switch and the marker update would otherwise be misread.
func (q *CommandQueue) PendingReplacements() []PendingCommandRevision {
	dir := filepath.Dir(q.path)
	entries, err := store.ReadDirNames(dir)
	if err != nil {
		return nil
	}
	prefix := "pointing-commands.tx-"
	pending := make([]PendingCommandRevision, 0)
	for _, name := range entries {
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".json") {
			continue
		}
		var tx commandTx
		found, err := store.ReadJSON(filepath.Join(dir, name), &tx)
		if err != nil || !found {
			continue
		}
		pending = append(pending, PendingCommandRevision{PassID: tx.PassID, Revision: tx.Revision, Commands: tx.Commands, Backup: tx.Backup, Committed: q.stageIsLive(tx), OpenedAt: tx.OpenedAt})
	}
	return pending
}

func (q *CommandQueue) stageIsLive(tx commandTx) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.stageIsLiveLocked(tx)
}

// stageIsLiveLocked reports whether the staged set of tx is what the committed
// queue currently serves: every staged command is present with its assigned
// ID, not retired, and the active revision for the pass matches.
func (q *CommandQueue) stageIsLiveLocked(tx commandTx) bool {
	if q.active[tx.PassID] != tx.Revision {
		return false
	}
	byID := make(map[string]PointingCommand, len(q.commands))
	for _, command := range q.commands {
		byID[command.ID] = command
	}
	for _, staged := range tx.Commands {
		live, ok := byID[staged.ID]
		if !ok || live.Retired || live.Revision != tx.Revision {
			return false
		}
	}
	return true
}

func cloneCommands(commands []PointingCommand) []PointingCommand {
	return append([]PointingCommand(nil), commands...)
}

func rebuildActive(commands []PointingCommand) map[string]int64 {
	active := make(map[string]int64)
	for _, command := range commands {
		if !command.Retired && command.Revision > active[command.PassID] {
			active[command.PassID] = command.Revision
		}
	}
	return active
}

func (q *CommandQueue) Due(now time.Time) []PointingCommand {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var values []PointingCommand
	for _, command := range q.commands {
		if !command.Retired && command.Revision == q.active[command.PassID] && !command.ExecuteAt.After(now) {
			values = append(values, command)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ExecuteAt.Before(values[j].ExecuteAt) })
	return values
}

func (q *CommandQueue) IsExecutable(command PointingCommand) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return !command.Retired && command.Revision == q.active[command.PassID]
}

func (q *CommandQueue) List(passID string) []PointingCommand {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var values []PointingCommand
	for _, command := range q.commands {
		if passID == "" || command.PassID == passID {
			values = append(values, command)
		}
	}
	return values
}
