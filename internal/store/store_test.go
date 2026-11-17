package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSource stands in for bd: a mutable export, a journal of records, and a
// channel the follower reads newly committed records from.
type fakeSource struct {
	mu      sync.Mutex
	export  string
	journal bool
	records []string
	live    chan string
	exports int
	// onExport runs inside Export, to commit records mid-export.
	onExport func()
}

func newFake(export string, journal bool) *fakeSource {
	return &fakeSource{export: export, journal: journal, live: make(chan string, 16)}
}

func (f *fakeSource) Export(context.Context) ([]byte, error) {
	f.mu.Lock()
	f.exports++
	hook := f.onExport
	f.onExport = nil
	out := f.export
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return []byte(out), nil
}

func (f *fakeSource) JournalEnabled(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.journal, nil
}

func seqOf(line string) int64 {
	var r struct {
		Seq int64 `json:"seq"`
	}
	_ = json.Unmarshal([]byte(line), &r)
	return r.Seq
}

func (f *fakeSource) Events(ctx context.Context, since int64, follow bool, fn func([]byte) error) error {
	f.mu.Lock()
	recs := slices.Clone(f.records)
	f.mu.Unlock()
	for _, r := range recs {
		if seqOf(r) > since {
			if err := fn([]byte(r)); err != nil {
				return err
			}
		}
	}
	if !follow {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-f.live:
			if err := fn([]byte(r)); err != nil {
				return err
			}
		}
	}
}

// commit appends a record to the journal and hands it to a running follower.
func (f *fakeSource) commit(r string) {
	f.mu.Lock()
	f.records = append(f.records, r)
	f.mu.Unlock()
	f.live <- r
}

func (f *fakeSource) setExport(s string) {
	f.mu.Lock()
	f.export = s
	f.mu.Unlock()
}

const exportJSONL = `{"_type":"issue","id":"mg-1","title":"Parent","status":"open","priority":1,"issue_type":"epic","created_at":"2026-01-01T00:00:00Z","dependency_count":0,"dependent_count":0,"comment_count":1,"comments":[{"id":"c1","issue_id":"mg-1","author":"ana","text":"hello there","created_at":"2026-01-02T00:00:00Z"}]}
{"_type":"issue","id":"mg-1.1","title":"Child","status":"open","priority":2,"issue_type":"task","created_at":"2026-01-03T00:00:00Z","dependencies":[{"issue_id":"mg-1.1","depends_on_id":"mg-1","type":"parent-child","metadata":"{}"},{"issue_id":"mg-1.1","depends_on_id":"mg-2","type":"blocks","metadata":"{}"}]}
{"_type":"issue","id":"mg-2","title":"Blocker","status":"in_progress","priority":2,"issue_type":"bug","created_at":"2026-01-04T00:00:00Z","description":"long text","labels":["db"]}
{"_type":"issue","id":"mg-3","title":"Done","status":"closed","priority":0,"issue_type":"task","created_at":"2026-01-05T00:00:00Z","dependencies":[{"issue_id":"mg-3","depends_on_id":"mg-2","type":"blocks","metadata":"{}"}]}
{"_type":"issue","id":"mg-4","title":"Later","status":"deferred","priority":3,"issue_type":"task","created_at":"2026-01-06T00:00:00Z","dependencies":[{"issue_id":"mg-4","depends_on_id":"mg-2","type":"blocks","metadata":"{}"},{"issue_id":"mg-4","depends_on_id":"mg-1","type":"relates-to","metadata":"{}"}]}
{"_type":"memory","id":"mem-1","title":"not an issue"}
`

func mustSnap(t *testing.T) map[string]*issue {
	t.Helper()
	snap, err := parseExport([]byte(exportJSONL))
	if err != nil {
		t.Fatalf("parseExport: %v", err)
	}
	return snap
}

func decodeList(t *testing.T, b []byte) []map[string]any {
	t.Helper()
	var rows []map[string]any
	if err := json.Unmarshal(b, &rows); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return rows
}

func byID(rows []map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, r := range rows {
		out[r["id"].(string)] = r
	}
	return out
}

func TestParseExportSkipsNonIssues(t *testing.T) {
	snap := mustSnap(t)
	if _, ok := snap["mem-1"]; ok {
		t.Error("memory record was loaded as an issue")
	}
	if len(snap) != 5 {
		t.Errorf("loaded %d issues, want 5", len(snap))
	}
}

func TestParseExportRejectsMalformedLine(t *testing.T) {
	if _, err := parseExport([]byte("{nope")); err == nil {
		t.Error("want an error for malformed JSONL")
	}
}

// The list must look like `bd list --json --all`: bd's order, derived counts
// that only count blocks edges, and the parent shortcut.
func TestRenderListMatchesBdShape(t *testing.T) {
	rows := decodeList(t, must(renderList(mustSnap(t))))
	var ids []string
	for _, r := range rows {
		ids = append(ids, r["id"].(string))
	}
	if want := []string{"mg-3", "mg-1", "mg-2", "mg-1.1", "mg-4"}; !slices.Equal(ids, want) {
		t.Errorf("order = %v, want %v (priority, then newest first)", ids, want)
	}
	m := byID(rows)
	if got := m["mg-1.1"]["parent"]; got != "mg-1" {
		t.Errorf("parent = %v, want mg-1", got)
	}
	if got := m["mg-1.1"]["dependency_count"]; got != 1.0 {
		t.Errorf("mg-1.1 dependency_count = %v, want 1 (parent-child not counted)", got)
	}
	if got := m["mg-2"]["dependent_count"]; got != 3.0 {
		t.Errorf("mg-2 dependent_count = %v, want 3", got)
	}
	if got := m["mg-1"]["dependent_count"]; got != 0.0 {
		t.Errorf("mg-1 dependent_count = %v, want 0 (children are not dependents)", got)
	}
	if got := m["mg-1"]["comment_count"]; got != 1.0 {
		t.Errorf("comment_count = %v, want 1", got)
	}
	if _, ok := m["mg-1"]["comments"]; ok {
		t.Error("list rows must not carry comment bodies")
	}
	if _, ok := m["mg-1"]["_type"]; ok {
		t.Error("export's _type leaked into the list")
	}
	if got := m["mg-2"]["description"]; got != "long text" {
		t.Errorf("description = %v, want it kept", got)
	}
}

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}

func TestRenderShowResolvesBothDirections(t *testing.T) {
	snap := mustSnap(t)
	show := decodeList(t, must(renderShow(snap, "mg-2")))[0]
	dents, _ := show["dependents"].([]any)
	var got []string
	for _, d := range dents {
		m := d.(map[string]any)
		got = append(got, m["id"].(string)+":"+m["dependency_type"].(string)+":"+m["title"].(string))
	}
	want := []string{"mg-1.1:blocks:Child", "mg-3:blocks:Done", "mg-4:blocks:Later"}
	if !slices.Equal(got, want) {
		t.Errorf("dependents = %v, want %v", got, want)
	}

	child := decodeList(t, must(renderShow(snap, "mg-1.1")))[0]
	deps := child["dependencies"].([]any)
	first := deps[0].(map[string]any)
	if first["id"] != "mg-1" || first["dependency_type"] != "parent-child" {
		t.Errorf("first dependency = %v", first)
	}
	blocker := deps[1].(map[string]any)
	if _, ok := blocker["description"]; ok {
		t.Error("a dependency reference should not carry the target's description")
	}

	parent := decodeList(t, must(renderShow(snap, "mg-1")))[0]
	cms := parent["comments"].([]any)
	if len(cms) != 1 || cms[0].(map[string]any)["text"] != "hello there" {
		t.Errorf("comments = %v", cms)
	}
}

// bd graph --all draws blocks and parent-child edges among issues that are
// neither closed nor deferred, from the blocker or parent to the dependent.
func TestRenderGraphMatchesBdGraph(t *testing.T) {
	var edges []Edge
	if err := json.Unmarshal(must(renderGraph(mustSnap(t))), &edges); err != nil {
		t.Fatal(err)
	}
	want := []Edge{
		{From: "mg-1", To: "mg-1.1", Dashed: true},
		{From: "mg-2", To: "mg-1.1", Dashed: false},
	}
	if !slices.Equal(edges, want) {
		t.Errorf("edges = %+v, want %+v", edges, want)
	}
}

func TestRenderGraphEncodesEmptyAsArray(t *testing.T) {
	if got := string(must(renderGraph(map[string]*issue{}))); got != "[]" {
		t.Errorf("empty graph = %s, want []", got)
	}
}

func TestRenderComments(t *testing.T) {
	var got map[string][]string
	if err := json.Unmarshal(must(renderComments(mustSnap(t))), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !slices.Equal(got["mg-1"], []string{"hello there"}) {
		t.Errorf("comments = %v", got)
	}
}

func rec(seq int64, op, id, issue string, extra string) string {
	if issue == "" {
		issue = "null"
	}
	if extra != "" {
		extra = "," + extra
	}
	return fmt.Sprintf(`{"seq":%d,"ts":"2026-02-01T00:00:00Z","op":%q,"issue_id":%q,"actor":"bo","issue":%s%s}`,
		seq, op, id, issue, extra)
}

func applyAll(t *testing.T, snap map[string]*issue, lines ...string) {
	t.Helper()
	for _, l := range lines {
		var r record
		if err := json.Unmarshal([]byte(l), &r); err != nil {
			t.Fatalf("record %s: %v", l, err)
		}
		applyRecord(snap, r)
	}
}

// An update record's payload omits dependencies and comments, which arrive as
// their own records, so applying it must not wipe them.
func TestApplyUpdateKeepsDepsAndComments(t *testing.T) {
	snap := mustSnap(t)
	applyAll(t, snap,
		rec(1, "update", "mg-1.1", `{"id":"mg-1.1","title":"Renamed","status":"blocked","priority":2,"issue_type":"task","is_blocked":true}`, ""),
		rec(2, "update", "mg-1", `{"id":"mg-1","title":"Parent","status":"open","priority":1,"issue_type":"epic"}`, ""),
	)
	if got := stringField(snap["mg-1.1"].fields, "title"); got != "Renamed" {
		t.Errorf("title = %q", got)
	}
	if len(snap["mg-1.1"].deps) != 2 {
		t.Errorf("deps = %d, want 2 kept", len(snap["mg-1.1"].deps))
	}
	if len(snap["mg-1"].comments) != 1 {
		t.Errorf("comments = %d, want 1 kept", len(snap["mg-1"].comments))
	}
	if _, ok := snap["mg-1.1"].fields["is_blocked"]; ok {
		t.Error("journal-only is_blocked should be dropped, as an export never has it")
	}
}

func TestApplyCreateDeleteAndDeps(t *testing.T) {
	snap := mustSnap(t)
	applyAll(t, snap,
		rec(1, "create", "mg-9", `{"id":"mg-9","title":"New","status":"open","priority":2,"issue_type":"task"}`, ""),
		rec(2, "dep_add", "mg-9", `{"id":"mg-9","title":"New","status":"open","priority":2,"issue_type":"task"}`,
			`"dep":{"kind":"blocks","target":"mg-2","metadata":"{}"}`),
		// A re-add of the same pair is an upsert, not a second edge.
		rec(3, "dep_add", "mg-9", `{"id":"mg-9","title":"New","status":"open","priority":2,"issue_type":"task"}`,
			`"dep":{"kind":"relates-to","target":"mg-2","metadata":"{}"}`),
		rec(4, "dep_remove", "mg-1.1", `{"id":"mg-1.1","title":"Child","status":"open","priority":2,"issue_type":"task"}`,
			`"dep":{"kind":"blocks","target":"mg-2","metadata":"{}"}`),
		rec(5, "delete", "mg-3", "", ""),
	)
	if d := snap["mg-9"].deps; len(d) != 1 || d[0].kind != "relates-to" || d[0].target != "mg-2" {
		t.Errorf("mg-9 deps = %+v, want one relates-to edge", d)
	}
	var raw map[string]any
	_ = json.Unmarshal(snap["mg-9"].deps[0].raw, &raw)
	if raw["issue_id"] != "mg-9" || raw["depends_on_id"] != "mg-2" || raw["metadata"] != "{}" {
		t.Errorf("synthesised dependency row = %v", raw)
	}
	if d := snap["mg-1.1"].deps; len(d) != 1 || d[0].target != "mg-1" {
		t.Errorf("mg-1.1 deps = %+v, want only the parent", d)
	}
	if _, ok := snap["mg-3"]; ok {
		t.Error("deleted issue still present")
	}
}

func TestApplyCommentIsIdempotent(t *testing.T) {
	snap := mustSnap(t)
	c := rec(1, "comment", "mg-2", `{"id":"mg-2","title":"Blocker","status":"in_progress","priority":2,"issue_type":"bug"}`,
		`"comment":{"id":"c9","author":"bo","text":"new note","created_at":"2026-02-01T00:00:00Z","source":"structured"}`)
	applyAll(t, snap, c, c)
	cms := snap["mg-2"].comments
	if len(cms) != 1 || cms[0].txt != "new note" {
		t.Fatalf("comments = %+v, want one", cms)
	}
	var raw map[string]any
	_ = json.Unmarshal(cms[0].raw, &raw)
	if raw["issue_id"] != "mg-2" {
		t.Errorf("comment row lacks issue_id: %v", raw)
	}
	if _, ok := raw["source"]; ok {
		t.Error("journal-only source field leaked into the comment")
	}
}

// startStore runs s until the test ends and waits for its first load.
func startStore(t *testing.T, s *Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		<-s.Done()
	})
	go s.Run(ctx)
	waitFor(t, "load", s.Loaded)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func title(t *testing.T, s *Store, id string) string {
	t.Helper()
	out, err := s.Show(id)
	if err != nil {
		return ""
	}
	return decodeList(t, out)[0]["title"].(string)
}

func TestFollowAppliesLiveRecordsAndNotifies(t *testing.T) {
	src := newFake(exportJSONL, true)
	// Already in the export; the follower resumes after it.
	src.records = []string{rec(1, "create", "mg-1", `{"id":"mg-1","title":"Parent","status":"open","priority":1,"issue_type":"epic"}`, "")}
	s := New(src, Options{Resync: time.Hour})
	startStore(t, s)
	if !s.Journal() {
		t.Fatal("store should follow the journal")
	}
	ch, stop := s.Subscribe()
	defer stop()
	before := s.Gen()

	src.commit(rec(2, "update", "mg-2", `{"id":"mg-2","title":"Live","status":"open","priority":2,"issue_type":"bug"}`, ""))
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("no change notification")
	}
	if got := title(t, s, "mg-2"); got != "Live" {
		t.Errorf("title = %q, want Live", got)
	}
	if s.Gen() == before {
		t.Error("gen did not move")
	}
	if got := title(t, s, "mg-1"); got != "Parent" {
		t.Errorf("baseline lost: mg-1 title = %q", got)
	}
}

// A write through maggie must be visible in the reply, so Sync pulls the
// journal tail itself rather than waiting on the follower.
func TestSyncCatchesUpFromTheJournal(t *testing.T) {
	src := newFake(exportJSONL, true)
	s := New(src, Options{Resync: time.Hour})
	startStore(t, s)
	src.mu.Lock()
	src.records = append(src.records, rec(1, "update", "mg-2", `{"id":"mg-2","title":"Synced","status":"open","priority":2,"issue_type":"bug"}`, ""))
	src.mu.Unlock()
	if err := s.Sync(t.Context()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := title(t, s, "mg-2"); got != "Synced" {
		t.Errorf("title = %q, want Synced", got)
	}
}

// A record the follower applies while a re-export is running must survive the
// swap to the new snapshot, which was taken before it.
func TestResyncReplaysRecordsAppliedDuringExport(t *testing.T) {
	src := newFake(exportJSONL, true)
	s := New(src, Options{Resync: time.Hour})
	startStore(t, s)

	// Something outside the journal changed mg-4, as a dolt pull would.
	src.setExport(strings.Replace(exportJSONL, `"title":"Later"`, `"title":"Pulled"`, 1))
	src.mu.Lock()
	src.onExport = func() {
		_ = s.applyLine([]byte(rec(1, "update", "mg-2", `{"id":"mg-2","title":"During","status":"open","priority":2,"issue_type":"bug"}`, "")))
	}
	src.mu.Unlock()
	if err := s.resync(t.Context()); err != nil {
		t.Fatalf("resync: %v", err)
	}
	if got := title(t, s, "mg-4"); got != "Pulled" {
		t.Errorf("mg-4 = %q, want the re-exported Pulled", got)
	}
	if got := title(t, s, "mg-2"); got != "During" {
		t.Errorf("mg-2 = %q, want the record applied mid-export", got)
	}
}

func TestResyncWithNoChangeKeepsGen(t *testing.T) {
	src := newFake(exportJSONL, true)
	s := New(src, Options{Resync: time.Hour})
	startStore(t, s)
	gen := s.Gen()
	if err := s.resync(t.Context()); err != nil {
		t.Fatalf("resync: %v", err)
	}
	if s.Gen() != gen {
		t.Error("an identical export bumped gen, which would refetch every page for nothing")
	}
}

func TestPollModeRefreshesOnlyOnChange(t *testing.T) {
	src := newFake(exportJSONL, false)
	s := New(src, Options{Poll: 10 * time.Millisecond, Resync: time.Hour})
	startStore(t, s)
	if s.Journal() {
		t.Fatal("store should poll without the journal")
	}
	_, stop := s.Subscribe() // a reader keeps polling alive
	defer stop()
	gen := s.Gen()
	waitFor(t, "a few polls", func() bool {
		src.mu.Lock()
		defer src.mu.Unlock()
		return src.exports >= 3
	})
	if s.Gen() != gen {
		t.Error("an unchanged export bumped gen")
	}
	src.setExport(strings.Replace(exportJSONL, `"title":"Later"`, `"title":"Polled"`, 1))
	waitFor(t, "poll to pick up the change", func() bool { return title(t, s, "mg-4") == "Polled" })
}

func TestPollModeIdlesWithoutReaders(t *testing.T) {
	src := newFake(exportJSONL, false)
	s := New(src, Options{Poll: 5 * time.Millisecond, Idle: time.Nanosecond, Resync: time.Hour})
	startStore(t, s)
	time.Sleep(50 * time.Millisecond)
	src.mu.Lock()
	n := src.exports
	src.mu.Unlock()
	if n > 1 {
		t.Errorf("exported %d times with nobody reading, want only the initial load", n)
	}
}

func TestSyncInPollModeReExports(t *testing.T) {
	src := newFake(exportJSONL, false)
	s := New(src, Options{Poll: time.Hour, Resync: time.Hour})
	startStore(t, s)
	src.setExport(strings.Replace(exportJSONL, `"title":"Later"`, `"title":"Written"`, 1))
	if err := s.Sync(t.Context()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := title(t, s, "mg-4"); got != "Written" {
		t.Errorf("mg-4 = %q, want Written", got)
	}
}

func TestSyncBeforeLoadFails(t *testing.T) {
	s := New(newFake(exportJSONL, true), Options{})
	if err := s.Sync(t.Context()); err == nil {
		t.Error("Sync on an unloaded store should fail so callers fall back to bd")
	}
}

func TestShowUnknownIsNotFound(t *testing.T) {
	s := New(newFake(exportJSONL, true), Options{Resync: time.Hour})
	startStore(t, s)
	if _, err := s.Show("mg-404"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestViewsAreCachedPerGeneration(t *testing.T) {
	src := newFake(exportJSONL, true)
	s := New(src, Options{Resync: time.Hour})
	startStore(t, s)
	a, _ := s.List()
	b, _ := s.List()
	if &a[0] != &b[0] {
		t.Error("second List re-rendered instead of using the cache")
	}
	src.commit(rec(1, "update", "mg-2", `{"id":"mg-2","title":"Changed","status":"open","priority":2,"issue_type":"bug"}`, ""))
	waitFor(t, "list to change", func() bool {
		c, _ := s.List()
		return strings.Contains(string(c), "Changed")
	})
}

func TestMemoRunsOncePerGeneration(t *testing.T) {
	src := newFake(exportJSONL, true)
	s := New(src, Options{Resync: time.Hour})
	startStore(t, s)
	calls := 0
	fetch := func(context.Context) ([]byte, error) {
		calls++
		return []byte(fmt.Sprintf("%d", calls)), nil
	}
	for range 3 {
		if _, err := s.Memo(t.Context(), "ready", fetch); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Errorf("fetch ran %d times, want 1", calls)
	}
	gen := s.Gen()
	src.commit(rec(1, "update", "mg-2", `{"id":"mg-2","title":"x","status":"open","priority":2,"issue_type":"bug"}`, ""))
	waitFor(t, "gen to move", func() bool { return s.Gen() != gen })
	out, _ := s.Memo(t.Context(), "ready", fetch)
	if string(out) != "2" {
		t.Errorf("memo = %s after a change, want a fresh fetch", out)
	}
}

func TestMemoDoesNotCacheErrors(t *testing.T) {
	s := New(newFake(exportJSONL, true), Options{Resync: time.Hour})
	startStore(t, s)
	boom := errors.New("boom")
	if _, err := s.Memo(t.Context(), "ready", func(context.Context) ([]byte, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	out, err := s.Memo(t.Context(), "ready", func(context.Context) ([]byte, error) { return []byte("ok"), nil })
	if err != nil || string(out) != "ok" {
		t.Errorf("after an error got %q, %v; want a retry", out, err)
	}
}

// A checkpoint pruned out of the journal makes bd fail; the baseline falls
// back to reading from the start.
type prunedSource struct{ *fakeSource }

func (p prunedSource) Events(ctx context.Context, since int64, follow bool, fn func([]byte) error) error {
	if since > 0 && !follow {
		return errors.New("events_journal_truncated")
	}
	return p.fakeSource.Events(ctx, since, follow, fn)
}

func TestBaselineRecoversFromATruncatedCheckpoint(t *testing.T) {
	src := newFake(exportJSONL, true)
	src.records = []string{rec(7, "update", "mg-2", `{"id":"mg-2","title":"x","status":"open","priority":2,"issue_type":"bug"}`, "")}
	s := New(prunedSource{src}, Options{Resync: time.Hour})
	s.seq = 3
	if err := s.baseline(t.Context()); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	if s.currentSeq() != 7 {
		t.Errorf("seq = %d, want the head 7", s.currentSeq())
	}
}
