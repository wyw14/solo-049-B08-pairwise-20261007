package store

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Paths keeps every durable document below one station state directory.
type Paths struct {
	Root string
}

func NewPaths(root string) Paths {
	return Paths{Root: filepath.Clean(root)}
}

func (p Paths) Events(stream string) string {
	return filepath.Join(p.Root, "events", SafeName(stream)+".jsonl")
}

func (p Paths) Snapshot(component string) string {
	return filepath.Join(p.Root, "snapshots", SafeName(component)+".json")
}

func (p Paths) Recording(id string) string {
	return filepath.Join(p.Root, "recordings", SafeName(id)+".frames")
}

func (p Paths) Validate() error {
	if p.Root == "." || strings.TrimSpace(p.Root) == "" {
		return fmt.Errorf("state root is required")
	}
	return nil
}

// SafeName makes an identifier safe to embed in a state file name.
func SafeName(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "..", "-")
	value = strings.NewReplacer("/", "-", "\\", "-", ":", "-").Replace(value)
	if value == "" {
		return "default"
	}
	return value
}
