package publish

import (
	"context"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type publishSuite struct {
	suite.Suite
	fs   billy.Filesystem
	dir  string
	pub  billy.Publisher
	path func(elem ...string) string
}

func (s *publishSuite) setup(newFS func() (billy.Filesystem, string)) {
	s.fs, s.dir = newFS()
	s.path = func(elem ...string) string {
		return s.fs.Join(append([]string{s.dir}, elem...)...)
	}

	var err error
	s.pub, err = New(s.fs, s.dir)
	s.Require().NoError(err)
}

func (s *publishSuite) names() []string {
	entries, err := s.fs.ReadDir(s.dir)
	if os.IsNotExist(err) {
		return nil
	}
	s.Require().NoError(err)

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func (s *publishSuite) generations() []string {
	var gens []string
	for _, n := range s.names() {
		if strings.HasPrefix(n, genPrefix) {
			gens = append(gens, n)
		}
	}
	slices.Sort(gens)
	return gens
}

func (s *publishSuite) readSet(gen, name string) string {
	data, err := util.ReadFile(s.fs, s.path(gen, name))
	s.Require().NoError(err)
	return string(data)
}

func (s *publishSuite) commitSet(files map[string]string) billy.PublishResult {
	sess, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)
	for name, data := range files {
		s.Require().NoError(StageFile(context.Background(), sess, name, []byte(data), 0o644))
	}
	res, err := sess.Commit(context.Background())
	s.Require().NoError(err)
	s.Require().True(res.Visible)
	return res
}

func (s *publishSuite) TestCommitMakesWholeSetVisible() {
	res := s.commitSet(map[string]string{
		"manifest.json": `{"v":1}`,
		"checksums.txt": "deadbeef",
		"data/blob.bin": "payload",
	})

	current, err := s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(res.Generation, current)
	s.True(strings.HasPrefix(current, genPrefix))

	// The whole set is readable through the visible generation.
	s.Equal(`{"v":1}`, s.readSet(current, "manifest.json"))
	s.Equal("deadbeef", s.readSet(current, "checksums.txt"))
	s.Equal("payload", s.readSet(current, "data/blob.bin"))

	// No staging or journal state is left behind.
	for _, n := range s.names() {
		s.False(strings.HasPrefix(n, stagingPrefix), n)
		s.False(strings.HasPrefix(n, journalPrefix), n)
		s.False(strings.HasPrefix(n, tmpPrefix), n)
	}
}

func (s *publishSuite) TestReadersSeeOldOrNewSetOnly() {
	first := s.commitSet(map[string]string{
		"manifest.json": "v1",
		"data.bin":      "one",
	})

	sess, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(StageFile(context.Background(), sess, "manifest.json", []byte("v2"), 0o644))
	s.Require().NoError(StageFile(context.Background(), sess, "data.bin", []byte("two"), 0o644))

	// Before the commit the previous set is still the visible one.
	current, err := s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(first.Generation, current)
	s.Equal("v1", s.readSet(current, "manifest.json"))

	second, err := sess.Commit(context.Background())
	s.Require().NoError(err)
	s.Require().True(second.Visible)

	current, err = s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(second.Generation, current)
	s.Equal("v2", s.readSet(current, "manifest.json"))
	s.Equal("two", s.readSet(current, "data.bin"))

	// The old generation is kept intact for rollback.
	s.Equal("v1", s.readSet(first.Generation, "manifest.json"))
}

func (s *publishSuite) TestAbortCleansStagingAndKeepsOriginal() {
	first := s.commitSet(map[string]string{"manifest.json": "v1"})

	sess, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(StageFile(context.Background(), sess, "manifest.json", []byte("v2"), 0o644))
	s.Require().NoError(sess.Abort(context.Background()))
	// Repeated aborts are idempotent.
	s.Require().NoError(sess.Abort(context.Background()))

	for _, n := range s.names() {
		s.False(strings.HasPrefix(n, stagingPrefix), n)
	}

	current, err := s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(first.Generation, current)
	s.Equal("v1", s.readSet(current, "manifest.json"))

	// Committing an aborted session fails clearly.
	_, err = sess.Commit(context.Background())
	s.ErrorIs(err, billy.ErrPublishAborted)
}

func (s *publishSuite) TestStageFailureCleansStaging() {
	first := s.commitSet(map[string]string{"manifest.json": "v1"})

	sess, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(StageFile(context.Background(), sess, "ok.txt", []byte("ok"), 0o644))

	err = StageFile(context.Background(), sess, "../evil.txt", []byte("x"), 0o644)
	s.ErrorIs(err, billy.ErrPublishPathEscape)

	// The failure cleaned up every staged byte; the old set is untouched.
	for _, n := range s.names() {
		s.False(strings.HasPrefix(n, stagingPrefix), n)
	}
	s.Equal([]string{first.Generation}, s.generations())
	s.Equal("v1", s.readSet(first.Generation, "manifest.json"))
}

func (s *publishSuite) TestPathEscapeRejected() {
	for _, name := range []string{
		"", ".", "..", "../x", "a/../../b", "/abs", "/",
		"\x00nul",
	} {
		sess, err := s.pub.Begin(context.Background())
		s.Require().NoError(err)

		err = StageFile(context.Background(), sess, name, []byte("x"), 0o644)
		s.ErrorIs(err, billy.ErrPublishPathEscape, "path %q", name)

		// The failed stage aborted the session.
		_, err = sess.Commit(context.Background())
		s.ErrorIs(err, billy.ErrPublishAborted)
	}
}

func (s *publishSuite) TestSymlinkTargetRejected() {
	sess, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)

	// Plant a symlink inside the active staging area.
	var staging string
	for _, n := range s.names() {
		if strings.HasPrefix(n, stagingPrefix) {
			staging = n
		}
	}
	s.Require().NotEmpty(staging)
	s.Require().NoError(s.fs.Symlink("../..", s.path(staging, "link")))

	err = StageFile(context.Background(), sess, "link/evil.txt", []byte("x"), 0o644)
	s.ErrorIs(err, billy.ErrPublishSymlink)
}

func (s *publishSuite) TestSymlinkSetRootRejected() {
	target := s.fs.Join("real-set")
	s.Require().NoError(s.fs.MkdirAll(target, 0o755))
	link := s.fs.Join("link-set")
	s.Require().NoError(s.fs.Symlink(target, link))

	_, err := New(s.fs, link)
	s.ErrorIs(err, billy.ErrPublishSymlink)
}

func (s *publishSuite) TestRecoverInterruptedPrepare() {
	s.Require().NoError(s.fs.MkdirAll(s.path(stagingPrefix+"gen-00000001-deadbeef", "sub"), 0o755))
	s.Require().NoError(util.WriteFile(s.fs, s.path(stagingPrefix+"gen-00000001-deadbeef", "sub", "f"), []byte("x"), 0o644))

	s.Require().NoError(s.pub.Recover(context.Background()))
	// Recovery is idempotent.
	s.Require().NoError(s.pub.Recover(context.Background()))

	for _, n := range s.names() {
		s.False(strings.HasPrefix(n, stagingPrefix), n)
	}
}

func (s *publishSuite) TestRecoverCommitInterruptedBeforeFlip() {
	// A journaled generation that CURRENT does not point at was never
	// visible and must be removed.
	old := s.commitSet(map[string]string{"manifest.json": "v1"})

	gen := genPrefix + "00000002-deadbeef"
	s.Require().NoError(s.fs.MkdirAll(s.path(gen), 0o755))
	s.Require().NoError(util.WriteFile(s.fs, s.path(gen, "manifest.json"), []byte("v2"), 0o644))
	s.Require().NoError(util.WriteFile(s.fs, s.path(journalPrefix+gen), []byte(gen+"\n"), 0o644))

	s.Require().NoError(s.pub.Recover(context.Background()))
	s.Require().NoError(s.pub.Recover(context.Background()))

	s.Equal([]string{old.Generation}, s.generations())
	for _, n := range s.names() {
		s.False(strings.HasPrefix(n, journalPrefix), n)
	}
	current, err := s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(old.Generation, current)
	s.Equal("v1", s.readSet(current, "manifest.json"))
}

func (s *publishSuite) TestRecoverCommitInterruptedAfterFlip() {
	// A journaled generation that CURRENT already points at is fully
	// committed; only the journal is dropped.
	res := s.commitSet(map[string]string{"manifest.json": "v1"})
	s.Require().NoError(util.WriteFile(s.fs, s.path(journalPrefix+res.Generation), []byte(res.Generation+"\n"), 0o644))

	s.Require().NoError(s.pub.Recover(context.Background()))
	s.Require().NoError(s.pub.Recover(context.Background()))

	s.Equal([]string{res.Generation}, s.generations())
	current, err := s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(res.Generation, current)
	s.Equal("v1", s.readSet(current, "manifest.json"))
}

func (s *publishSuite) TestCommitAndCancelAreIdempotent() {
	sess, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(StageFile(context.Background(), sess, "manifest.json", []byte("v1"), 0o644))

	res, err := sess.Commit(context.Background())
	s.Require().NoError(err)
	s.Require().True(res.Visible)

	// Repeated commits return the same result.
	again, err := sess.Commit(context.Background())
	s.Require().NoError(err)
	s.Equal(res, again)

	// Abort after commit is a no-op and keeps the committed data.
	s.Require().NoError(sess.Abort(context.Background()))
	s.Equal("v1", s.readSet(res.Generation, "manifest.json"))

	// Staging into a finished session is rejected.
	s.ErrorIs(StageFile(context.Background(), sess, "x", []byte("y"), 0o644), billy.ErrPublishSessionClosed)
}

func (s *publishSuite) TestOnlyOneSessionAtATime() {
	sess, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)

	_, err = s.pub.Begin(context.Background())
	s.ErrorIs(err, billy.ErrPublishSessionActive)

	s.Require().NoError(sess.Abort(context.Background()))

	sess2, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(sess2.Abort(context.Background()))
}

func (s *publishSuite) TestRollback() {
	first := s.commitSet(map[string]string{"manifest.json": "v1"})
	second := s.commitSet(map[string]string{"manifest.json": "v2"})

	res, err := s.pub.Rollback(context.Background())
	s.Require().NoError(err)
	s.Require().True(res.Visible)
	s.Equal(first.Generation, res.Generation)

	current, err := s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(first.Generation, current)
	s.Equal("v1", s.readSet(current, "manifest.json"))

	// Both generations still exist: the newer data is not lost.
	s.ElementsMatch([]string{first.Generation, second.Generation}, s.generations())

	// No generation before the first one.
	_, err = s.pub.Rollback(context.Background())
	s.ErrorIs(err, billy.ErrNoPreviousGeneration)
}

func (s *publishSuite) TestPruneKeepsCurrentAndRollbackTarget() {
	s.commitSet(map[string]string{"manifest.json": "v1"})
	second := s.commitSet(map[string]string{"manifest.json": "v2"})
	third := s.commitSet(map[string]string{"manifest.json": "v3"})

	s.Require().NoError(s.pub.Prune(context.Background()))

	s.Equal([]string{second.Generation, third.Generation}, s.generations())
	current, err := s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(third.Generation, current)

	// The rollback target survived the prune.
	res, err := s.pub.Rollback(context.Background())
	s.Require().NoError(err)
	s.Equal(second.Generation, res.Generation)

	// Pruning again is a no-op.
	s.Require().NoError(s.pub.Prune(context.Background()))
	s.Equal([]string{second.Generation, third.Generation}, s.generations())
}

func (s *publishSuite) TestPruneCancelledKeepsRollbackData() {
	s.commitSet(map[string]string{"manifest.json": "v1"})
	second := s.commitSet(map[string]string{"manifest.json": "v2"})
	third := s.commitSet(map[string]string{"manifest.json": "v3"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.pub.Prune(ctx)
	s.ErrorIs(err, context.Canceled)

	// Cancellation never removes the visible generation or its
	// predecessor.
	gens := s.generations()
	s.Contains(gens, second.Generation)
	s.Contains(gens, third.Generation)
}

func (s *publishSuite) TestCommitWithCancelledContextNotVisible() {
	first := s.commitSet(map[string]string{"manifest.json": "v1"})

	sess, err := s.pub.Begin(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(StageFile(context.Background(), sess, "manifest.json", []byte("v2"), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := sess.Commit(ctx)
	s.ErrorIs(err, context.Canceled)
	s.False(res.Visible)

	// The cancelled publish left no trace; the old set is still visible.
	s.Equal([]string{first.Generation}, s.generations())
	current, err := s.pub.Current(context.Background())
	s.Require().NoError(err)
	s.Equal(first.Generation, current)
}

// memfs variant.

type memfsPublishSuite struct {
	publishSuite
}

func TestMemfsPublishSuite(t *testing.T) {
	suite.Run(t, new(memfsPublishSuite))
}

func (s *memfsPublishSuite) SetupTest() {
	s.setup(func() (billy.Filesystem, string) {
		return memfs.New(), "releases"
	})
}

// osfs variant, exercising real renames, syncs and locks.

type osfsPublishSuite struct {
	publishSuite
}

func TestOsfsPublishSuite(t *testing.T) {
	suite.Run(t, new(osfsPublishSuite))
}

func (s *osfsPublishSuite) SetupTest() {
	base := s.T().TempDir()
	s.setup(func() (billy.Filesystem, string) {
		return osfs.New(base), "releases"
	})
}

func TestStageFilePerm(t *testing.T) {
	fsys := memfs.New()
	pub, err := New(fsys, "releases")
	require.NoError(t, err)

	sess, err := pub.Begin(context.Background())
	require.NoError(t, err)
	require.NoError(t, StageFile(context.Background(), sess, "a/b/c.txt", []byte("nested"), 0o640))
	res, err := sess.Commit(context.Background())
	require.NoError(t, err)

	fi, err := fsys.Stat(fsys.Join("releases", res.Generation, "a/b/c.txt"))
	require.NoError(t, err)
	require.Equal(t, fs.FileMode(0o640), fi.Mode().Perm())
}
