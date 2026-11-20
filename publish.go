package billy

import (
	"context"
	"errors"
	"io"
	"io/fs"
)

var (
	// ErrPublishSessionActive is returned when a publish session is already
	// in flight for the same set.
	ErrPublishSessionActive = errors.New("publish session already active")
	// ErrPublishSessionClosed is returned when staging into a session that
	// was already committed or aborted.
	ErrPublishSessionClosed = errors.New("publish session already closed")
	// ErrPublishAborted is returned when committing a session that was
	// already aborted.
	ErrPublishAborted = errors.New("publish session aborted")
	// ErrPublishPathEscape is returned when a staged path is absolute or
	// escapes the set root.
	ErrPublishPathEscape = errors.New("publish path escapes the set root")
	// ErrPublishSymlink is returned when a staged path traverses a symbolic
	// link.
	ErrPublishSymlink = errors.New("publish path contains a symbolic link")
	// ErrNoPreviousGeneration is returned by Rollback when there is no
	// previous generation to roll back to.
	ErrNoPreviousGeneration = errors.New("no previous generation to roll back to")
)

// PublishResult describes the outcome of a commit or rollback.
type PublishResult struct {
	// Generation identifies the generation the operation refers to.
	Generation string
	// Visible reports whether Generation is the set readers currently
	// observe. It is false when a commit failed before the switch point
	// was flipped, in which case the previously committed generation
	// remains visible and all staged data was discarded.
	Visible bool
}

// PublishSession is an in-flight atomic publish. A session is prepared by
// staging every member of the set and is then either committed or aborted.
// Sessions are not safe for concurrent use beyond what the implementation
// documents; the generic implementation serializes all methods.
type PublishSession interface {
	// Stage writes name, relative to the set root, into the same-volume
	// staging area and syncs it to stable storage. Absolute paths, paths
	// escaping the set root and paths traversing symbolic links are
	// rejected. Any failure cleans up all staged data and aborts the
	// session, leaving the previously published set untouched.
	Stage(ctx context.Context, name string, r io.Reader, perm fs.FileMode) error
	// Commit atomically publishes every staged file as a new generation.
	// Readers only ever observe the previous or the new set, never a mix.
	// Commit is idempotent: calling it again returns the same result. If
	// Commit fails, the returned result reports whether the new set
	// became visible.
	Commit(ctx context.Context) (PublishResult, error)
	// Abort discards all staged data. It is idempotent and never removes
	// data that was already committed.
	Abort(ctx context.Context) error
}

// Publisher manages the atomic, recoverable publication of a set of files
// as a single unit, for example a manifest, its checksums and the data
// files they describe. Implementations prepare the new set in a staging
// area on the same volume, then commit it through a recognizable
// generation directory or an equivalent switch point, so that readers
// only ever see the old set or the new set.
//
// A process that exits between operations leaves enough state behind for
// Recover to finish or undo the interrupted operation; recovery is
// idempotent. Closing, cancelling a context or aborting never removes
// generations that may still be needed for a rollback.
type Publisher interface {
	// Begin recovers any interrupted session and starts a new one. Only
	// one session may be active at a time.
	Begin(ctx context.Context) (PublishSession, error)
	// Current returns the identifier of the visible generation, or ""
	// when no generation has been committed yet.
	Current(ctx context.Context) (string, error)
	// Recover finishes or undoes any interrupted prepare or commit. It is
	// idempotent and is also called implicitly by Begin.
	Recover(ctx context.Context) error
	// Rollback flips the switch point back to the generation committed
	// before the visible one.
	Rollback(ctx context.Context) (PublishResult, error)
	// Prune removes committed generations older than the visible one and
	// its predecessor. The predecessor is kept as the rollback target and
	// is never removed, even if the context is cancelled mid-prune.
	Prune(ctx context.Context) error
}

// PublisherProvider may be implemented by filesystems that offer a native
// atomic publish implementation. It is discovered with a type assertion,
// for example by helper/publish.New, which falls back to a generic
// protocol when the filesystem does not provide one.
type PublisherProvider interface {
	// Publisher returns a Publisher managing sets rooted at dir.
	Publisher(dir string) (Publisher, error)
}
