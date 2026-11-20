// Package publish provides a generic, recoverable implementation of
// billy.Publisher on top of any billy.Filesystem.
//
// The protocol uses a recognizable layout inside the set root:
//
//	CURRENT            switch point: names the visible generation
//	gen-00000001-xxxx  committed generations, immutable once visible
//	.staging-<gen>     same-volume staging area of an in-flight prepare
//	.journal-<gen>     commit journal, exists only while a commit runs
//	.current-xxx       temporary switch-point file, renamed onto CURRENT
//
// A prepare writes every member of the set into the staging area and syncs
// it. A commit records its intent in the journal, renames the staging area
// into its generation directory (a single same-volume rename) and then
// atomically replaces CURRENT, which is the only moment the new set
// becomes visible. Recover uses the journal to tell an interrupted commit
// apart from a generation kept for rollback, and removes anything that was
// never visible.
package publish

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/util"
)

const (
	currentFile   = "CURRENT"
	lockFile      = ".publish-lock"
	genPrefix     = "gen-"
	stagingPrefix = ".staging-"
	journalPrefix = ".journal-"
	tmpPrefix     = ".current-"

	dirMode  = 0o755
	fileMode = 0o644
)

// New returns a billy.Publisher managing sets rooted at dir within fsys.
// When fsys implements billy.PublisherProvider its native implementation
// is used; otherwise the generic protocol described in the package
// documentation is applied on top of fsys.
func New(fsys billy.Filesystem, dir string) (billy.Publisher, error) {
	if p, ok := fsys.(billy.PublisherProvider); ok {
		return p.Publisher(dir)
	}

	return newGeneric(fsys, dir)
}

// StageFile is a convenience wrapper around s.Stage for []byte contents.
func StageFile(ctx context.Context, s billy.PublishSession, name string, data []byte, perm fs.FileMode) error {
	return s.Stage(ctx, name, bytes.NewReader(data), perm)
}

type publisher struct {
	fsys billy.Filesystem
	dir  string

	mu     sync.Mutex
	active *session
}

func newGeneric(fsys billy.Filesystem, dir string) (*publisher, error) {
	if dir == "" || dir == "." || dir == "/" {
		return nil, fmt.Errorf("publish: invalid set root %q", dir)
	}

	fi, err := fsys.Lstat(dir)
	switch {
	case err == nil:
		if fi.Mode()&fs.ModeSymlink != 0 {
			return nil, fmt.Errorf("publish: set root %q: %w", dir, billy.ErrPublishSymlink)
		}
		if !fi.IsDir() {
			return nil, fmt.Errorf("publish: set root %q is not a directory", dir)
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}

	return &publisher{fsys: fsys, dir: dir}, nil
}

func (p *publisher) join(elem ...string) string {
	return p.fsys.Join(append([]string{p.dir}, elem...)...)
}

// Begin implements billy.Publisher.
func (p *publisher) Begin(ctx context.Context) (billy.PublishSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active != nil {
		return nil, billy.ErrPublishSessionActive
	}

	lock, err := p.lock()
	if err != nil {
		return nil, err
	}

	if err := p.recoverLocked(); err != nil {
		unlock(lock)
		return nil, err
	}

	gen, err := p.nextGeneration()
	if err != nil {
		unlock(lock)
		return nil, err
	}

	staging := p.join(stagingPrefix + gen)
	if err := p.fsys.MkdirAll(staging, dirMode); err != nil {
		unlock(lock)
		return nil, err
	}

	s := &session{p: p, gen: gen, staging: staging, lock: lock}
	p.active = s
	return s, nil
}

// Current implements billy.Publisher.
func (p *publisher) Current(context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.currentLocked()
}

// Recover implements billy.Publisher.
func (p *publisher) Recover(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active != nil {
		return billy.ErrPublishSessionActive
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	lock, err := p.lock()
	if err != nil {
		return err
	}
	defer unlock(lock)

	return p.recoverLocked()
}

// Rollback implements billy.Publisher.
func (p *publisher) Rollback(ctx context.Context) (billy.PublishResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active != nil {
		return billy.PublishResult{}, billy.ErrPublishSessionActive
	}
	if err := ctx.Err(); err != nil {
		return billy.PublishResult{}, err
	}

	lock, err := p.lock()
	if err != nil {
		return billy.PublishResult{}, err
	}
	defer unlock(lock)

	gens, err := p.generations()
	if err != nil {
		return billy.PublishResult{}, err
	}

	current, err := p.currentLocked()
	if err != nil {
		return billy.PublishResult{}, err
	}

	idx := slices.Index(gens, current)
	if idx <= 0 {
		return billy.PublishResult{Generation: current, Visible: current != ""},
			billy.ErrNoPreviousGeneration
	}

	prev := gens[idx-1]
	if err := p.flip(prev); err != nil {
		return billy.PublishResult{}, err
	}

	return billy.PublishResult{Generation: prev, Visible: true}, nil
}

// Prune implements billy.Publisher.
func (p *publisher) Prune(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active != nil {
		return billy.ErrPublishSessionActive
	}

	lock, err := p.lock()
	if err != nil {
		return err
	}
	defer unlock(lock)

	gens, err := p.generations()
	if err != nil {
		return err
	}

	current, err := p.currentLocked()
	if err != nil {
		return err
	}

	idx := slices.Index(gens, current)
	if idx < 0 {
		// Nothing visible: without a switch point there is no way to tell
		// kept generations apart from leftovers, so keep everything.
		return nil
	}

	// Only generations strictly older than the visible one's predecessor
	// are removed. The predecessor is the rollback target, and
	// generations newer than the visible one are what a rollback came
	// back from: all of them may still be needed and must survive even
	// if the context is cancelled halfway through.
	for _, g := range gens[:max(idx-1, 0)] {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := util.RemoveAll(p.fsys, p.join(g)); err != nil {
			return err
		}
	}

	return nil
}

// recoverLocked undoes interrupted prepares and commits. Anything it
// removes was never reachable through the switch point, so readers always
// saw a consistent set. It is idempotent: a second run finds nothing to do.
func (p *publisher) recoverLocked() error {
	entries, err := p.fsys.ReadDir(p.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	current, err := p.currentLocked()
	if err != nil {
		return err
	}

	var journals []string
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasPrefix(name, stagingPrefix),
			strings.HasPrefix(name, tmpPrefix):
			// Interrupted prepare or interrupted switch-point flip: never
			// visible to readers, safe to drop.
			if err := util.RemoveAll(p.fsys, p.join(name)); err != nil {
				return err
			}
		case strings.HasPrefix(name, journalPrefix):
			journals = append(journals, name)
		}
	}

	for _, j := range journals {
		gen := strings.TrimPrefix(j, journalPrefix)
		if gen == current {
			// The commit flipped the switch point but exited before
			// removing the journal: the commit is complete.
			if err := p.remove(j); err != nil {
				return err
			}
			continue
		}
		// Commit interrupted before the flip: the generation was never
		// visible, remove it together with its journal.
		if err := util.RemoveAll(p.fsys, p.join(gen)); err != nil {
			return err
		}
		if err := p.remove(j); err != nil {
			return err
		}
	}

	return nil
}

func (p *publisher) remove(name string) error {
	if err := p.fsys.Remove(p.join(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (p *publisher) currentLocked() (string, error) {
	data, err := util.ReadFile(p.fsys, p.join(currentFile))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(data)), nil
}

// generations returns the committed generation directories, oldest first.
func (p *publisher) generations() ([]string, error) {
	entries, err := p.fsys.ReadDir(p.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var gens []string
	for _, e := range entries {
		if _, ok := parseGen(e.Name()); ok && e.IsDir() {
			gens = append(gens, e.Name())
		}
	}
	slices.Sort(gens)

	return gens, nil
}

func (p *publisher) nextGeneration() (string, error) {
	gens, err := p.generations()
	if err != nil {
		return "", err
	}

	seq := 0
	if len(gens) > 0 {
		seq, _ = parseGen(gens[len(gens)-1])
	}

	return fmt.Sprintf("%s%08d-%s", genPrefix, seq+1, randSuffix()), nil
}

// parseGen extracts the sequence number of a generation directory name.
func parseGen(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, genPrefix)
	if !ok {
		return 0, false
	}
	seq, suffix, ok := strings.Cut(rest, "-")
	if !ok || suffix == "" {
		return 0, false
	}
	n, err := strconv.Atoi(seq)
	if err != nil || n <= 0 {
		return 0, false
	}

	return n, true
}

// flip atomically points the switch point at gen by renaming a synced
// temporary file onto it. Readers resolve the set through the switch
// point, so they only ever observe the previous or the new generation.
func (p *publisher) flip(gen string) error {
	tmp := p.join(tmpPrefix + randSuffix())
	if err := util.WriteFile(p.fsys, tmp, []byte(gen+"\n"), fileMode); err != nil {
		return err
	}
	if err := p.fsys.Rename(tmp, p.join(currentFile)); err != nil {
		_ = p.fsys.Remove(tmp)
		return err
	}
	syncDir(p.fsys, p.dir)

	return nil
}

// lock takes the set's advisory lock, serializing writers across
// processes. Backings without billy.Locker support proceed unlocked.
func (p *publisher) lock() (billy.File, error) {
	if err := p.fsys.MkdirAll(p.dir, dirMode); err != nil {
		return nil, err
	}

	f, err := p.fsys.OpenFile(p.join(lockFile), os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return nil, err
	}

	if l, ok := f.(billy.Locker); ok {
		if err := l.Lock(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("publish: acquiring lock: %w", err)
		}
	}

	return f, nil
}

func unlock(f billy.File) {
	if l, ok := f.(billy.Locker); ok {
		_ = l.Unlock()
	}
	_ = f.Close()
}

// syncDir best-effort syncs a directory to stable storage. Backings that
// cannot sync directories are silently tolerated.
func syncDir(fsys billy.Filesystem, dir string) {
	f, err := fsys.Open(dir)
	if err != nil {
		return
	}
	if sf, ok := f.(billy.Syncer); ok {
		_ = sf.Sync()
	}
	_ = f.Close()
}

func randSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}

	return hex.EncodeToString(b[:])
}

type session struct {
	p       *publisher
	gen     string
	staging string
	lock    billy.File

	mu        sync.Mutex
	committed bool
	aborted   bool
	result    billy.PublishResult
}

// Stage implements billy.PublishSession.
func (s *session) Stage(ctx context.Context, name string, r io.Reader, perm fs.FileMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.committed || s.aborted {
		return billy.ErrPublishSessionClosed
	}
	if err := ctx.Err(); err != nil {
		s.abortLocked()
		return err
	}

	rel, err := cleanPath(name)
	if err != nil {
		s.abortLocked()
		return err
	}
	if err := s.checkSymlinks(rel); err != nil {
		s.abortLocked()
		return err
	}

	if dir := path.Dir(rel); dir != "." {
		if err := s.p.fsys.MkdirAll(s.p.fsys.Join(s.staging, dir), dirMode); err != nil {
			s.abortLocked()
			return err
		}
	}

	target := s.p.fsys.Join(s.staging, rel)
	f, err := s.p.fsys.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		s.abortLocked()
		return err
	}

	if _, err = io.Copy(f, r); err == nil {
		if sf, ok := f.(billy.Syncer); ok {
			err = sf.Sync()
		}
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		s.abortLocked()
		return err
	}

	return nil
}

// Commit implements billy.PublishSession.
func (s *session) Commit(ctx context.Context) (billy.PublishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.committed {
		// Idempotent: report the outcome of the first commit.
		return s.result, nil
	}
	if s.aborted {
		return billy.PublishResult{}, billy.ErrPublishAborted
	}
	if err := ctx.Err(); err != nil {
		s.abortLocked()
		return billy.PublishResult{}, err
	}

	p := s.p
	genPath := p.join(s.gen)
	journal := p.join(journalPrefix + s.gen)

	// Make the staged entries durable before they can become visible.
	syncDir(p.fsys, s.staging)

	// Record the commit intent before moving any data: recovery uses the
	// journal to tell an interrupted commit apart from a kept generation.
	if err := util.WriteFile(p.fsys, journal, []byte(s.gen+"\n"), fileMode); err != nil {
		return s.fail(err)
	}

	// Same-volume rename: the whole staged set becomes a generation in a
	// single step.
	if err := p.fsys.Rename(s.staging, genPath); err != nil {
		return s.fail(err)
	}

	if err := ctx.Err(); err != nil {
		// Cancelled before the flip: the set was never visible, drop it.
		_ = util.RemoveAll(p.fsys, genPath)
		_ = p.remove(journalPrefix + s.gen)
		s.abortLocked()
		return billy.PublishResult{}, err
	}

	// Atomic switch-point flip: the only moment the new set becomes
	// visible to readers.
	if err := p.flip(s.gen); err != nil {
		return s.fail(err)
	}

	// The generation is visible; the journal is no longer needed.
	_ = p.remove(journalPrefix + s.gen)

	s.committed = true
	s.result = billy.PublishResult{Generation: s.gen, Visible: true}
	s.closeLocked()

	return s.result, nil
}

// Abort implements billy.PublishSession.
func (s *session) Abort(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Idempotent: aborting a finished session is a no-op and never
	// removes committed data.
	if s.committed || s.aborted {
		return nil
	}

	s.abortLocked()
	return nil
}

// fail cleans up all staged or half-committed data after an error that
// happened before the switch point was flipped, leaving the previously
// visible set untouched, and aborts the session.
func (s *session) fail(err error) (billy.PublishResult, error) {
	_ = util.RemoveAll(s.p.fsys, s.staging)
	_ = util.RemoveAll(s.p.fsys, s.p.join(s.gen))
	_ = s.p.remove(journalPrefix + s.gen)
	s.abortLocked()

	return billy.PublishResult{Generation: s.gen, Visible: false}, err
}

func (s *session) abortLocked() {
	if s.committed || s.aborted {
		return
	}
	s.aborted = true
	_ = util.RemoveAll(s.p.fsys, s.staging)
	s.closeLocked()
}

func (s *session) closeLocked() {
	if s.lock != nil {
		unlock(s.lock)
		s.lock = nil
	}

	s.p.mu.Lock()
	if s.p.active == s {
		s.p.active = nil
	}
	s.p.mu.Unlock()
}

// cleanPath validates a staged path and returns it in cleaned,
// set-root-relative form. Absolute paths and paths escaping the set root
// are rejected.
func cleanPath(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("publish: empty path: %w", billy.ErrPublishPathEscape)
	}
	if strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("publish: path %q contains NUL: %w", name, billy.ErrPublishPathEscape)
	}
	if path.IsAbs(name) {
		return "", fmt.Errorf("publish: absolute path %q: %w", name, billy.ErrPublishPathEscape)
	}
	clean := path.Clean(name)
	switch {
	case clean == ".":
		return "", fmt.Errorf("publish: path %q is not a file: %w", name, billy.ErrPublishPathEscape)
	case clean == ".." || strings.HasPrefix(clean, "../"):
		return "", fmt.Errorf("publish: path %q escapes the set root: %w", name, billy.ErrPublishPathEscape)
	}

	return clean, nil
}

// checkSymlinks rejects staged paths that traverse a symbolic link within
// the staging area, so a planted link cannot redirect a write outside the
// set.
func (s *session) checkSymlinks(rel string) error {
	parts := strings.Split(rel, "/")
	cur := s.staging
	for i, part := range parts {
		cur = s.p.fsys.Join(cur, part)
		fi, err := s.p.fsys.Lstat(cur)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("publish: path %q: %w", rel, billy.ErrPublishSymlink)
		}
		if i < len(parts)-1 && !fi.IsDir() {
			return fmt.Errorf("publish: path %q: %q is not a directory", rel, part)
		}
	}

	return nil
}
