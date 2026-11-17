package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/mulgadc/maggie/internal/beads"
	"github.com/mulgadc/maggie/internal/store"
)

// staticSource feeds the store a fixed export with the journal off, so the
// store loads by polling and Sync re-exports.
type staticSource struct {
	mu     sync.Mutex
	export string
}

func (s *staticSource) Export(context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []byte(s.export), nil
}

func (s *staticSource) set(v string) {
	s.mu.Lock()
	s.export = v
	s.mu.Unlock()
}

func (*staticSource) JournalEnabled(context.Context) (bool, error) { return false, nil }

func (*staticSource) Events(context.Context, int64, bool, func([]byte) error) error {
	return nil
}

const storeExport = `{"_type":"issue","id":"mg-1","title":"From store","status":"open","priority":1,"issue_type":"task","comments":[{"id":"c1","text":"stored comment"}]}
{"_type":"issue","id":"mg-2","title":"Child","status":"open","priority":2,"issue_type":"task","dependencies":[{"issue_id":"mg-2","depends_on_id":"mg-1","type":"parent-child"}]}
`

// newStoreServer is newServer with a loaded store in front of the stub bd.
func newStoreServer(t *testing.T) (*http.Client, string, string, *staticSource, *store.Store) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	argv := filepath.Join(t.TempDir(), "argv.json")
	t.Setenv(stubEnv, "1")
	t.Setenv(stubArgvEnv, argv)
	bd := beads.New(self, t.TempDir(), 5*time.Second)

	src := &staticSource{export: storeExport}
	st := store.New(src, store.Options{Poll: time.Hour})
	ctx, cancel := context.WithCancel(t.Context())
	go st.Run(ctx)
	t.Cleanup(func() {
		cancel()
		<-st.Done()
	})
	deadline := time.Now().Add(5 * time.Second)
	for !st.Loaded() {
		if time.Now().After(deadline) {
			t.Fatal("store never loaded")
		}
		time.Sleep(5 * time.Millisecond)
	}

	srv := httptest.NewServer(routes(bd, st, nil, fstest.MapFS{"index.html": {Data: []byte("spa")}}))
	t.Cleanup(srv.Close)
	return srv.Client(), srv.URL, argv, src, st
}

func assertNoBd(t *testing.T, argv string) {
	t.Helper()
	if _, err := os.Stat(argv); err == nil {
		t.Errorf("bd ran (%q) for a read the store should answer", readArgv(t, argv))
	}
}

func TestStoreAnswersReadsWithoutBd(t *testing.T) {
	c, url, argv, _, _ := newStoreServer(t)
	for _, tc := range []struct{ path, want string }{
		{"/api/issues?all=true&limit=100000", `"parent":"mg-1"`},
		{"/api/issue?id=mg-1", "stored comment"},
		{"/api/graph", `{"from":"mg-1","to":"mg-2","dashed":true}`},
		{"/api/comments", `{"mg-1":["stored comment"]}`},
	} {
		resp := get(t, c, url+tc.path)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d", tc.path, resp.StatusCode)
		}
		if b := body(t, resp); !strings.Contains(b, tc.want) {
			t.Errorf("%s = %s, want it to contain %s", tc.path, b, tc.want)
		}
	}
	assertNoBd(t, argv)
}

// A filtered or truncated list is bd's to answer; the store only serves the
// everything-list the UI asks for.
func TestFilteredListGoesToBd(t *testing.T) {
	c, url, argv, _, _ := newStoreServer(t)
	t.Setenv(stubOutEnv, `[]`)
	get(t, c, url+"/api/issues?all=true&status=open")
	if got := readArgv(t, argv); !slices.Contains(got, "--status") {
		t.Errorf("argv = %q, want bd list --status", got)
	}
}

func TestUnknownIssueFallsBackToBd(t *testing.T) {
	c, url, argv, _, _ := newStoreServer(t)
	t.Setenv(stubOutEnv, `[{"id":"mg-9"}]`)
	resp := get(t, c, url+"/api/issue?id=mg-9")
	if b := body(t, resp); b != `[{"id":"mg-9"}]` {
		t.Errorf("body = %s, want bd's answer", b)
	}
	if got := readArgv(t, argv); got[0] != "show" {
		t.Errorf("argv = %q, want bd show", got)
	}
}

func TestReadyIsMemoisedUntilAChange(t *testing.T) {
	c, url, argv, src, st := newStoreServer(t)
	t.Setenv(stubOutEnv, `[{"id":"mg-1"}]`)
	get(t, c, url+"/api/ready")
	if _, err := os.Stat(argv); err != nil {
		t.Fatal("first ready should ask bd")
	}
	_ = os.Remove(argv)
	if b := body(t, get(t, c, url+"/api/ready")); b != `[{"id":"mg-1"}]` {
		t.Errorf("cached ready = %s", b)
	}
	assertNoBd(t, argv)

	src.set(strings.Replace(storeExport, "From store", "Changed", 1))
	if err := st.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	get(t, c, url+"/api/ready")
	if _, err := os.Stat(argv); err != nil {
		t.Error("ready after a change should ask bd again")
	}
}

// The write's reply comes from the synced store, not a second bd process.
func TestWriteRepliesFromTheStore(t *testing.T) {
	c, url, argv, src, _ := newStoreServer(t)
	t.Setenv(stubOutEnv, `[{"id":"mg-1","title":"from bd show"}]`)
	src.set(strings.Replace(storeExport, "From store", "Written", 1))
	resp := post(t, c, url+"/api/issue/update", `{"id":"mg-1","actor":"ana","notes":"n"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got[0]["title"] != "Written" {
		t.Errorf("reply = %v, want the store's synced copy", got[0])
	}
	if a := readArgv(t, argv); a[0] != "update" {
		t.Errorf("last bd call = %q, want only the update", a)
	}
}

func TestStreamPushesChanges(t *testing.T) {
	c, url, _, src, st := newStoreServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/api/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q", ct)
	}
	lines := make(chan string, 32)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	expect := func(want string) {
		t.Helper()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case l, ok := <-lines:
				if !ok {
					t.Fatalf("stream closed before %q", want)
				}
				if l == want {
					return
				}
			case <-timeout:
				t.Fatalf("no %q on the stream", want)
			}
		}
	}
	expect("event: hello")
	src.set(strings.Replace(storeExport, "From store", "Pushed", 1))
	if err := st.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	expect("event: change")
}
