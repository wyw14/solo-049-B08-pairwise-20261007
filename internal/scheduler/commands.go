package scheduler

import (
	"fmt"
	"sort"
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

// Replace swaps the active plan for a pass. The in-memory queue is only
// committed after the snapshot write succeeds, so a failed replace leaves
// the previous plan fully intact for compensation.
func (q *CommandQueue) Replace(passID string, revision int64, commands []PointingCommand) error {
	if passID == "" || revision < 1 || len(commands) == 0 {
		return fmt.Errorf("pass, revision and pointing commands are required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	updated := make([]PointingCommand, len(q.commands), len(q.commands)+len(commands))
	copy(updated, q.commands)
	for index := range updated {
		if updated[index].PassID == passID && !updated[index].Retired {
			updated[index].Retired = true
			updated[index].RetireCause = fmt.Sprintf("replaced by revision %d", revision)
		}
	}
	for _, command := range commands {
		command.ID = uuid.NewString()
		command.PassID = passID
		command.Revision = revision
		updated = append(updated, command)
	}
	if err := store.WriteJSON(q.path, updated); err != nil {
		return err
	}
	q.commands = updated
	q.active[passID] = revision
	return nil
}

// RetirePass retires every live command of a pass and clears its active
// revision. It rolls a queued plan back when there is no earlier revision to
// restore. Like Replace, it commits in memory only after the snapshot write.
func (q *CommandQueue) RetirePass(passID, cause string) error {
	if passID == "" || cause == "" {
		return fmt.Errorf("pass and retire cause are required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	updated := make([]PointingCommand, len(q.commands))
	copy(updated, q.commands)
	for index := range updated {
		if updated[index].PassID == passID && !updated[index].Retired {
			updated[index].Retired = true
			updated[index].RetireCause = cause
		}
	}
	if err := store.WriteJSON(q.path, updated); err != nil {
		return err
	}
	q.commands = updated
	delete(q.active, passID)
	return nil
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
