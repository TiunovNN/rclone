package operations

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/rclone/rclone/lib/random"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoMultiThreadCopy(t *testing.T) {
	ctx := context.Background()
	ci := fs.GetConfig(ctx)
	f, err := mockfs.NewFs(ctx, "potato", "", nil)
	require.NoError(t, err)
	src := mockobject.New("file.txt").WithContent([]byte(random.String(100)), mockobject.SeekModeNone)
	srcFs, err := mockfs.NewFs(ctx, "sausage", "", nil)
	require.NoError(t, err)
	src.SetFs(srcFs)

	oldStreams := ci.MultiThreadStreams
	oldCutoff := ci.MultiThreadCutoff
	oldIsSet := ci.MultiThreadSet
	defer func() {
		ci.MultiThreadStreams = oldStreams
		ci.MultiThreadCutoff = oldCutoff
		ci.MultiThreadSet = oldIsSet
	}()

	ci.MultiThreadStreams, ci.MultiThreadCutoff = 4, 50
	ci.MultiThreadSet = false

	nullWriterAt := func(ctx context.Context, remote string, size int64) (fs.WriterAtCloser, error) {
		panic("don't call me")
	}
	f.Features().OpenWriterAt = nullWriterAt

	assert.True(t, doMultiThreadCopy(ctx, f, src))

	ci.MultiThreadStreams = 0
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	ci.MultiThreadStreams = 1
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	ci.MultiThreadStreams = 2
	assert.True(t, doMultiThreadCopy(ctx, f, src))

	ci.MultiThreadCutoff = 200
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	ci.MultiThreadCutoff = 101
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	ci.MultiThreadCutoff = 100
	assert.True(t, doMultiThreadCopy(ctx, f, src))

	f.Features().OpenWriterAt = nil
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	f.Features().OpenWriterAt = nullWriterAt
	assert.True(t, doMultiThreadCopy(ctx, f, src))

	f.Features().IsLocal = true
	srcFs.Features().IsLocal = true
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	ci.MultiThreadSet = true
	assert.True(t, doMultiThreadCopy(ctx, f, src))
	ci.MultiThreadSet = false
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	srcFs.Features().IsLocal = false
	assert.True(t, doMultiThreadCopy(ctx, f, src))
	srcFs.Features().IsLocal = true
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	f.Features().IsLocal = false
	assert.True(t, doMultiThreadCopy(ctx, f, src))
	srcFs.Features().IsLocal = false
	assert.True(t, doMultiThreadCopy(ctx, f, src))

	srcFs.Features().NoMultiThreading = true
	assert.False(t, doMultiThreadCopy(ctx, f, src))
	srcFs.Features().NoMultiThreading = false
	assert.True(t, doMultiThreadCopy(ctx, f, src))
}

func TestMultithreadCalculateNumChunks(t *testing.T) {
	for _, test := range []struct {
		size          int64
		chunkSize     int64
		wantNumChunks int
	}{
		{size: 1, chunkSize: multithreadChunkSize, wantNumChunks: 1},
		{size: 1 << 20, chunkSize: 1, wantNumChunks: 1 << 20},
		{size: 1 << 20, chunkSize: 2, wantNumChunks: 1 << 19},
		{size: (1 << 20) + 1, chunkSize: 2, wantNumChunks: (1 << 19) + 1},
		{size: (1 << 20) - 1, chunkSize: 2, wantNumChunks: 1 << 19},
	} {
		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			mc := &multiThreadCopyState{}
			mc.numChunks = calculateNumChunks(test.size, test.chunkSize)
			assert.Equal(t, test.wantNumChunks, mc.numChunks)
		})
	}
}

// Skip if not multithread, returning the chunkSize otherwise
func skipIfNotMultithread(ctx context.Context, t *testing.T, r *fstest.Run) int {
	features := r.Fremote.Features()
	if features.OpenChunkWriter == nil && features.OpenWriterAt == nil {
		t.Skip("multithread writing not supported")
	}

	// Only support one hash for the local backend otherwise we end up spending a huge amount of CPU on hashing!
	if r.Fremote.Features().IsLocal {
		oldHashes := hash.SupportOnly([]hash.Type{r.Fremote.Hashes().GetOne()})
		t.Cleanup(func() {
			_ = hash.SupportOnly(oldHashes)
		})
	}

	ci := fs.GetConfig(ctx)
	chunkSize := int(ci.MultiThreadChunkSize)
	if features.OpenChunkWriter != nil {
		//OpenChunkWriter func(ctx context.Context, remote string, src ObjectInfo, options ...OpenOption) (info ChunkWriterInfo, writer ChunkWriter, err error)
		const fileName = "chunksize-probe"
		src := object.NewStaticObjectInfo(fileName, time.Now(), int64(100*fs.Mebi), true, nil, nil)
		info, writer, err := features.OpenChunkWriter(ctx, fileName, src)
		if err != nil {
			// If the probe fails because the file is too small, skip
			if errors.Is(err, fs.ErrorFileTooSmall) {
				t.Skipf("probe file too small for multipart upload: %v", err)
			}
			require.NoError(t, err)
		}
		chunkSize = int(info.ChunkSize)
		err = writer.Abort(ctx)
		require.NoError(t, err)
	}
	return chunkSize
}

func TestMultithreadCopy(t *testing.T) {
	r := fstest.NewRun(t)
	ctx := context.Background()
	chunkSize := skipIfNotMultithread(ctx, t, r)
	// Check every other transfer for metadata
	checkMetadata := false
	ctx, ci := fs.AddConfig(ctx)

	for _, upload := range []bool{false, true} {
		for _, test := range []struct {
			size    int
			streams int
		}{
			{size: chunkSize*2 - 1, streams: 2},
			{size: chunkSize * 2, streams: 2},
			{size: chunkSize*2 + 1, streams: 2},
		} {
			checkMetadata = !checkMetadata
			ci.Metadata = checkMetadata
			fileName := fmt.Sprintf("test-multithread-copy-%v-%d-%d", upload, test.size, test.streams)
			t.Run(fmt.Sprintf("upload=%v,size=%v,streams=%v", upload, test.size, test.streams), func(t *testing.T) {
				if *fstest.SizeLimit > 0 && int64(test.size) > *fstest.SizeLimit {
					t.Skipf("exceeded file size limit %d > %d", test.size, *fstest.SizeLimit)
				}
				var (
					contents     = random.String(test.size)
					t1           = fstest.Time("2001-02-03T04:05:06.499999999Z")
					file1        fstest.Item
					src, dst     fs.Object
					err          error
					testMetadata = fs.Metadata{
						// System metadata supported by all backends
						"mtime": t1.Format(time.RFC3339Nano),
						// User metadata
						"potato": "jersey",
					}
				)

				var fSrc, fDst fs.Fs
				if upload {
					file1 = r.WriteFile(fileName, contents, t1)
					r.CheckRemoteItems(t)
					r.CheckLocalItems(t, file1)
					fDst, fSrc = r.Fremote, r.Flocal
				} else {
					file1 = r.WriteObject(ctx, fileName, contents, t1)
					r.CheckRemoteItems(t, file1)
					r.CheckLocalItems(t)
					fDst, fSrc = r.Flocal, r.Fremote
				}
				src, err = fSrc.NewObject(ctx, fileName)
				require.NoError(t, err)

				do, canSetMetadata := src.(fs.SetMetadataer)
				if checkMetadata && canSetMetadata {
					// Set metadata on the source if required
					err := do.SetMetadata(ctx, testMetadata)
					if err == fs.ErrorNotImplemented {
						canSetMetadata = false
					} else {
						require.NoError(t, err)
						fstest.CheckEntryMetadata(ctx, t, r.Flocal, src, testMetadata)
					}
				}

				accounting.GlobalStats().ResetCounters()
				tr := accounting.GlobalStats().NewTransfer(src, nil)

				defer func() {
					tr.Done(ctx, err)
				}()

				dst, err = multiThreadCopy(ctx, fDst, fileName, src, test.streams, tr)
				if errors.Is(err, fs.ErrorFileTooSmall) {
					require.NoError(t, src.Remove(ctx))
					t.Skipf("file too small for multipart upload: %v", err)
				}
				require.NoError(t, err)

				assert.Equal(t, src.Size(), dst.Size())
				assert.Equal(t, fileName, dst.Remote())
				fstest.CheckListingWithPrecision(t, fSrc, []fstest.Item{file1}, nil, fs.GetModifyWindow(ctx, fDst, fSrc))
				fstest.CheckListingWithPrecision(t, fDst, []fstest.Item{file1}, nil, fs.GetModifyWindow(ctx, fDst, fSrc))

				if checkMetadata && canSetMetadata && fDst.Features().ReadMetadata {
					fstest.CheckEntryMetadata(ctx, t, fDst, dst, testMetadata)
				}

				require.NoError(t, dst.Remove(ctx))
				require.NoError(t, src.Remove(ctx))

			})
		}
	}
}

type errorObject struct {
	fs.Object
	size int64
	wg   *sync.WaitGroup
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
//
// Remember this is called multiple times whenever the backend seeks (eg having read checksum)
func (o errorObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.Debugf(nil, "Open with options = %v", options)
	rc, err := o.Object.Open(ctx, options...)
	if err != nil {
		return nil, err
	}
	// Return an error reader for the second segment
	for _, option := range options {
		if ropt, ok := option.(*fs.RangeOption); ok {
			end := ropt.End + 1
			if end >= o.size {
				// Give the other chunks a chance to start
				time.Sleep(time.Second)
				// Wait for chunks to upload first
				o.wg.Wait()
				fs.Debugf(nil, "Returning error reader")
				return errorReadCloser{rc}, nil
			}
		}
	}
	o.wg.Add(1)
	return wgReadCloser{rc, o.wg}, nil
}

type errorReadCloser struct {
	io.ReadCloser
}

func (rc errorReadCloser) Read(p []byte) (n int, err error) {
	fs.Debugf(nil, "BOOM: simulated read failure")
	return 0, errors.New("BOOM: simulated read failure")
}

type wgReadCloser struct {
	io.ReadCloser
	wg *sync.WaitGroup
}

func (rc wgReadCloser) Close() (err error) {
	rc.wg.Done()
	return rc.ReadCloser.Close()
}

// Make sure aborting the multi-thread copy doesn't overwrite an existing file.
func TestMultithreadCopyAbort(t *testing.T) {
	r := fstest.NewRun(t)
	ctx := context.Background()
	chunkSize := skipIfNotMultithread(ctx, t, r)
	size := 2*chunkSize + 1
	if *fstest.SizeLimit > 0 && int64(size) > *fstest.SizeLimit {
		t.Skipf("exceeded file size limit %d > %d", size, *fstest.SizeLimit)
	}

	// first write a canary file which we are trying not to overwrite
	const fileName = "test-multithread-abort"
	contents := random.String(100)
	t1 := fstest.Time("2001-02-03T04:05:06.499999999Z")
	canary := r.WriteObject(ctx, fileName, contents, t1)
	r.CheckRemoteItems(t, canary)

	// Now write a local file to upload
	file1 := r.WriteFile(fileName, random.String(size), t1)
	r.CheckLocalItems(t, file1)

	src, err := r.Flocal.NewObject(ctx, fileName)
	require.NoError(t, err)
	accounting.GlobalStats().ResetCounters()
	tr := accounting.GlobalStats().NewTransfer(src, nil)

	defer func() {
		tr.Done(ctx, err)
	}()
	wg := new(sync.WaitGroup)
	dst, err := multiThreadCopy(ctx, r.Fremote, fileName, errorObject{src, int64(size), wg}, 1, tr)
	if errors.Is(err, fs.ErrorFileTooSmall) {
		t.Skipf("file too small for multipart upload: %v", err)
	}
	assert.Error(t, err)
	assert.Nil(t, dst)

	if r.Fremote.Features().PartialUploads {
		r.CheckRemoteItems(t)

	} else {
		r.CheckRemoteItems(t, canary)
		o, err := r.Fremote.NewObject(ctx, fileName)
		require.NoError(t, err)
		require.NoError(t, o.Remove(ctx))
	}
}

type errorWriterAtCloser struct {
	writeErr error
	closeErr error
	closed   bool
}

func (w *errorWriterAtCloser) WriteAt(p []byte, _ int64) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return len(p), nil
}

func (w *errorWriterAtCloser) Close() error {
	w.closed = true
	return w.closeErr
}

// Classification belongs to the backend; this checks propagation through the consumer.
func TestMultithreadCopyWriterAtErrors(t *testing.T) {
	for _, stage := range []string{"open", "write", "close"} {
		for _, fatal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fatal=%v", stage, fatal), func(t *testing.T) {
				ctx, ci := fs.AddConfig(context.Background())
				ci.MultiThreadChunkSize = 4
				ci.MultiThreadWriteBufferSize = 0
				ci.MultiThreadStreams = 2
				ci.MultiThreadSet = true
				f, err := mockfs.NewFs(ctx, "destination", "", nil)
				require.NoError(t, err)
				src := mockobject.New("file.txt").WithContent([]byte("0123456789abcdef"), mockobject.SeekModeNone)
				cause := errors.New("disk full")
				injected := cause
				if fatal {
					injected = fserrors.FatalError(injected)
				}
				writer := &errorWriterAtCloser{}
				f.Features().OpenWriterAt = func(context.Context, string, int64) (fs.WriterAtCloser, error) {
					switch stage {
					case "open":
						return nil, injected
					case "write":
						writer.writeErr = injected
					case "close":
						writer.closeErr = injected
					}
					return writer, nil
				}
				tr := accounting.GlobalStats().NewTransfer(src, nil)
				_, err = multiThreadCopy(ctx, f, src.Remote(), src, 2, tr)
				tr.Done(ctx, err)
				require.ErrorIs(t, err, cause)
				assert.Equal(t, fatal, fserrors.IsFatalError(err))
				assert.Equal(t, stage != "open", writer.closed)
			})
		}
	}
}

type testChunkWriter struct {
	mu           sync.Mutex
	chunks       map[int][]byte
	active       atomic.Int32
	maxActive    atomic.Int32
	writes       atomic.Int32
	writesReady  chan struct{}
	releaseWrite chan struct{}
}

func (w *testChunkWriter) WriteChunk(_ context.Context, chunk int, reader io.ReadSeeker) (int64, error) {
	active := w.active.Add(1)
	defer w.active.Add(-1)
	for {
		old := w.maxActive.Load()
		if active <= old || w.maxActive.CompareAndSwap(old, active) {
			break
		}
	}
	if w.writes.Add(1) == 4 && w.writesReady != nil {
		close(w.writesReady)
	}
	if w.releaseWrite != nil {
		<-w.releaseWrite
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	if w.chunks == nil {
		w.chunks = map[int][]byte{}
	}
	w.chunks[chunk] = data
	w.mu.Unlock()
	return int64(len(data)), nil
}

func (w *testChunkWriter) Close(context.Context) error { return nil }
func (w *testChunkWriter) Abort(context.Context) error { return nil }

type flakyRangeObject struct {
	fs.Object
	failStart int64
	failures  int32
	attempts  atomic.Int32
}

type failingOpenObject struct {
	fs.Object
	attempts atomic.Int32
	err      error
}

type failingReadObject struct {
	fs.Object
	attempts atomic.Int32
}

type retryingErrorReadCloser struct {
	io.ReadCloser
}

type readingPutFs struct {
	fs.Fs
}

func (r retryingErrorReadCloser) Read([]byte) (int, error) {
	return 0, fserrors.RetryError(errors.New("temporary source read failure"))
}

func (f *readingPutFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	_, err := io.Copy(io.Discard, in)
	return nil, err
}

func (o *failingOpenObject) Open(context.Context, ...fs.OpenOption) (io.ReadCloser, error) {
	o.attempts.Add(1)
	if o.err != nil {
		return nil, o.err
	}
	return nil, fserrors.RetryError(errors.New("temporary source open failure"))
}

func (o *failingReadObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	o.attempts.Add(1)
	rc, err := o.Object.Open(ctx, options...)
	if err != nil {
		return nil, err
	}
	return retryingErrorReadCloser{ReadCloser: rc}, nil
}

func (o *flakyRangeObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	for _, option := range options {
		if rangeOption, ok := option.(*fs.RangeOption); ok && rangeOption.Start == o.failStart {
			if o.attempts.Add(1) <= max(o.failures, 1) {
				return nil, fserrors.RetryError(errors.New("temporary source failure"))
			}
		}
	}
	return o.Object.Open(ctx, options...)
}

type blockingRangeObject struct {
	fs.Object
	active       atomic.Int32
	maxActive    atomic.Int32
	readsReady   chan struct{}
	readsOnce    sync.Once
	releaseReads chan struct{}
}

func (o *blockingRangeObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	active := o.active.Add(1)
	defer o.active.Add(-1)
	for {
		old := o.maxActive.Load()
		if active <= old || o.maxActive.CompareAndSwap(old, active) {
			break
		}
	}
	if active >= 2 {
		o.readsOnce.Do(func() { close(o.readsReady) })
	}
	select {
	case <-o.releaseReads:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return o.Object.Open(ctx, options...)
}

func newMultiThreadTest(t *testing.T, content []byte, chunkSize int64, backendConcurrency int) (context.Context, fs.Fs, *mockobject.ContentMockObject, *testChunkWriter) {
	t.Helper()
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	ci.Retries = 2
	ci.RetriesInterval = 0
	ci.MultiThreadReadRetries = 0
	ci.MultiThreadStreams = 2
	ci.MultiThreadSet = true

	srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	src := mockobject.New("source.bin").WithContent(content, mockobject.SeekModeNone)
	src.SetFs(srcFs)

	dstFs, err := mockfs.NewFs(ctx, "destination", "", nil)
	require.NoError(t, err)
	dst := mockobject.New("destination.bin").WithContent(content, mockobject.SeekModeNone)
	dstFs.(*mockfs.Fs).AddObject(dst)

	writer := &testChunkWriter{}
	dstFs.Features().OpenChunkWriter = func(context.Context, string, fs.ObjectInfo, ...fs.OpenOption) (fs.ChunkWriterInfo, fs.ChunkWriter, error) {
		return fs.ChunkWriterInfo{ChunkSize: chunkSize, Concurrency: backendConcurrency}, writer, nil
	}
	return ctx, dstFs, src, writer
}

func TestMultithreadCopyRetriesFailedSourceChunk(t *testing.T) {
	content := []byte("0123456789ab")
	ctx, dstFs, src, writer := newMultiThreadTest(t, content, 4, 2)
	flaky := &flakyRangeObject{Object: src, failStart: 4}
	tr := accounting.GlobalStats().NewTransfer(flaky, nil)

	dst, err := multiThreadCopy(ctx, dstFs, "destination.bin", flaky, 2, tr)
	tr.Done(ctx, err)
	require.NoError(t, err)
	require.NotNil(t, dst)
	assert.Equal(t, int32(2), flaky.attempts.Load())
	assert.Equal(t, []byte("4567"), writer.chunks[1])
}

func TestMultithreadCopyUsesDedicatedSourceReadRetries(t *testing.T) {
	content := []byte("0123456789ab")
	ctx, dstFs, src, writer := newMultiThreadTest(t, content, 4, 2)
	ci := fs.GetConfig(ctx)
	ci.Retries = 1
	ci.MultiThreadReadRetries = 4
	flaky := &flakyRangeObject{Object: src, failStart: 4, failures: 3}
	tr := accounting.GlobalStats().NewTransfer(flaky, nil)

	dst, err := multiThreadCopy(ctx, dstFs, "destination.bin", flaky, 2, tr)
	tr.Done(ctx, err)
	require.NoError(t, err)
	require.NotNil(t, dst)
	assert.Equal(t, int32(4), flaky.attempts.Load())
	assert.Equal(t, int32(3), writer.writes.Load())
	assert.Equal(t, []byte("4567"), writer.chunks[1])
}

func TestMultithreadCopyExhaustedSourceReadIsNotRetriedAtCommandLevel(t *testing.T) {
	content := []byte("0123456789ab")
	ctx, dstFs, src, _ := newMultiThreadTest(t, content, 4, 2)
	ci := fs.GetConfig(ctx)
	ci.MultiThreadReadRetries = 3
	flaky := &flakyRangeObject{Object: src, failStart: 4, failures: 100}
	tr := accounting.GlobalStats().NewTransfer(flaky, nil)

	_, err := multiThreadCopy(ctx, dstFs, "destination.bin", flaky, 2, tr)
	tr.Done(ctx, err)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoRetryError(err))
	assert.True(t, fserrors.IsNoLowLevelRetryError(err))
	assert.ErrorContains(t, err, "source read retries exhausted for chunk 2/3 after 3 attempts")
	assert.Equal(t, int32(3), flaky.attempts.Load())
}

func TestCopyExhaustedSourceOpenIsNotRetriedAtCommandLevel(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 3
	ci.MultiThreadStreams = 0
	srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	src := mockobject.New("source.bin").WithContent([]byte("content"), mockobject.SeekModeNone)
	src.SetFs(srcFs)
	failing := &failingOpenObject{Object: src}
	dstFs, err := mockfs.NewFs(ctx, "destination", "", nil)
	require.NoError(t, err)

	_, err = Copy(ctx, dstFs, nil, "destination.bin", failing)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoRetryError(err))
	assert.True(t, fserrors.IsNoLowLevelRetryError(err))
	assert.ErrorContains(t, err, "source read retries exhausted after 3 attempts")
	assert.Equal(t, int32(3), failing.attempts.Load())
}

func TestCopyExhaustedMidStreamSourceReadUsesDedicatedRetryLimit(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	ci.MultiThreadReadRetries = 10
	ci.MultiThreadStreams = 0
	ci.RetriesInterval = 0
	srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	src := mockobject.New("source.bin").WithContent([]byte("content"), mockobject.SeekModeNone)
	src.SetFs(srcFs)
	failing := &failingReadObject{Object: src}
	dstFsBase, err := mockfs.NewFs(ctx, "destination", "", nil)
	require.NoError(t, err)
	dstFs := &readingPutFs{Fs: dstFsBase}

	_, err = Copy(ctx, dstFs, nil, "destination.bin", failing)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoRetryError(err))
	assert.True(t, fserrors.IsNoLowLevelRetryError(err))
	assert.ErrorContains(t, err, "source read retries exhausted after 10 attempts")
	assert.Equal(t, int32(10), failing.attempts.Load())
}

func TestCopyPermanentSourceOpenErrorIsNotQuarantinable(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 3
	ci.MultiThreadStreams = 0
	srcFs, err := mockfs.NewFs(ctx, "source", "", nil)
	require.NoError(t, err)
	src := mockobject.New("source.bin").WithContent([]byte("content"), mockobject.SeekModeNone)
	src.SetFs(srcFs)
	failing := &failingOpenObject{Object: src, err: errors.New("permanent source open failure")}
	dstFs, err := mockfs.NewFs(ctx, "destination", "", nil)
	require.NoError(t, err)

	_, err = Copy(ctx, dstFs, nil, "destination.bin", failing)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "source read retries exhausted")
	assert.Equal(t, int32(1), failing.attempts.Load())
}

func TestMultithreadCopySeparatesReadAndWriteConcurrency(t *testing.T) {
	content := []byte("0123456789abcdef")
	ctx, dstFs, src, writer := newMultiThreadTest(t, content, 4, 4)
	readsReady := make(chan struct{})
	releaseReads := make(chan struct{})
	blocking := &blockingRangeObject{
		Object:       src,
		readsReady:   readsReady,
		releaseReads: releaseReads,
	}
	writer.writesReady = make(chan struct{})
	writer.releaseWrite = make(chan struct{})
	tr := accounting.GlobalStats().NewTransfer(blocking, nil)
	done := make(chan error, 1)
	go func() {
		_, err := multiThreadCopy(ctx, dstFs, "destination.bin", blocking, 2, tr)
		tr.Done(ctx, err)
		done <- err
	}()

	select {
	case <-readsReady:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for source reads")
	}
	time.Sleep(20 * time.Millisecond)
	assert.LessOrEqual(t, blocking.maxActive.Load(), int32(2), "backend upload concurrency must not increase source read concurrency")
	close(releaseReads)

	select {
	case <-writer.writesReady:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for concurrent destination writes")
	}
	assert.Equal(t, int32(4), writer.maxActive.Load())
	close(writer.releaseWrite)
	require.NoError(t, <-done)
}
