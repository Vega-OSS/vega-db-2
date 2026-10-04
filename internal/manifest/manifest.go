package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Vega-OSS/vega-db/internal/vlog"
)

// Table describes a registered SSTable tracked by the manifest.
type Table struct {
	ID       uint64 `json:"id"`
	Level    int    `json:"level"`
	Smallest []byte `json:"smallest"`
	Largest  []byte `json:"largest"`
	Size     int64  `json:"size"`
}

// State represents the complete persisted manifest state.
type State struct {
	NextFileID uint64        `json:"nextFileID"`
	VlogHead   vlog.Position `json:"vlogHead"`
	Tables     []Table       `json:"tables"`
}

// Manifest coordinates atomic updates to the database manifest file.
type Manifest struct {
	dir   string
	mu    sync.Mutex // serializes Apply; held across temp file write
	state State
}

func cloneState(s State) State {
	cp := State{
		NextFileID: s.NextFileID,
		VlogHead:   s.VlogHead,
	}
	if len(s.Tables) > 0 {
		cp.Tables = make([]Table, len(s.Tables))
		for i, t := range s.Tables {
			tblCopy := Table{
				ID:    t.ID,
				Level: t.Level,
				Size:  t.Size,
			}
			if len(t.Smallest) > 0 {
				tblCopy.Smallest = make([]byte, len(t.Smallest))
				copy(tblCopy.Smallest, t.Smallest)
			}
			if len(t.Largest) > 0 {
				tblCopy.Largest = make([]byte, len(t.Largest))
				copy(tblCopy.Largest, t.Largest)
			}
			cp.Tables[i] = tblCopy
		}
	}
	return cp
}

// Open reads or initializes the MANIFEST file in dir, cleaning up any leftover MANIFEST.tmp.
func Open(dir string) (*Manifest, State, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, State{}, fmt.Errorf("manifest: mkdir %s: %w", dir, err)
	}

	tmpPath := filepath.Join(dir, "MANIFEST.tmp")
	_ = os.Remove(tmpPath) // delete leftover crash artifact

	manifestPath := filepath.Join(dir, "MANIFEST")
	m := &Manifest{dir: dir}

	data, err := os.ReadFile(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		initialState := State{
			NextFileID: 1,
			VlogHead:   vlog.Position{File: 1, Off: 0},
			Tables:     nil,
		}
		if err := m.writeAtomic(manifestPath, tmpPath, initialState); err != nil {
			return nil, State{}, err
		}
		m.state = initialState
		return m, cloneState(initialState), nil
	}
	if err != nil {
		return nil, State{}, fmt.Errorf("manifest: read %s: %w", manifestPath, err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, State{}, fmt.Errorf("manifest: unmarshal json: %w", err)
	}

	if state.NextFileID == 0 {
		state.NextFileID = 1
	}
	if state.VlogHead.File == 0 {
		state.VlogHead.File = 1
	}

	m.state = state
	return m, cloneState(state), nil
}

func (m *Manifest) writeAtomic(targetPath, tmpPath string, st State) error {
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest: marshal: %w", err)
	}

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("manifest: open tmp %s: %w", tmpPath, err)
	}

	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		return fmt.Errorf("manifest: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("manifest: sync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest: close tmp: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		return fmt.Errorf("manifest: rename %s -> %s: %w", tmpPath, targetPath, err)
	}

	// fsync the directory
	d, err := os.Open(m.dir)
	if err != nil {
		return fmt.Errorf("manifest: open dir %s: %w", m.dir, err)
	}
	_ = d.Sync()
	_ = d.Close()

	return nil
}

// Apply executes an edit on a copy of the state, atomically persists it to disk, and updates current state.
func (m *Manifest) Apply(edit func(*State)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	newState := cloneState(m.state)
	edit(&newState)

	targetPath := filepath.Join(m.dir, "MANIFEST")
	tmpPath := filepath.Join(m.dir, "MANIFEST.tmp")

	if err := m.writeAtomic(targetPath, tmpPath, newState); err != nil {
		return err
	}

	m.state = newState
	return nil
}
