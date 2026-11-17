// Package store keeps an in-memory copy of the beads workspace so reads are
// answered without spawning bd.
//
// The copy is seeded from `bd export`, which carries every issue with its
// labels, dependencies and comments. When the workspace has the events journal
// on, a single long-lived `bd events tail --follow` keeps it current: each
// record holds the issue's state after the mutation, so it is applied in place
// with no further query. Without the journal the store re-exports on a timer
// while someone is looking. Either way a periodic re-export picks up changes
// the journal never sees, such as a `bd dolt pull`.
//
// Every change bumps a generation number; rendered responses are cached per
// generation and subscribers are told when it moves.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Source is the slice of the bd client the store reads from.
type Source interface {
	Export(ctx context.Context) ([]byte, error)
	JournalEnabled(ctx context.Context) (bool, error)
	Events(ctx context.Context, since int64, follow bool, fn func([]byte) error) error
}

// Options tunes the refresh cadence. Zero values take the defaults.
type Options struct {
	// Resync is how often a journal-fed store re-exports to pick up changes
	// the journal does not record.
	Resync time.Duration
	// Poll is how often a store without the journal re-exports while it has
	// readers.
	Poll time.Duration
	// Idle is how long after the last read a store without the journal stops
	// polling.
	Idle time.Duration
}

const (
	defaultResync = 5 * time.Minute
	defaultPoll   = 10 * time.Second
	defaultIdle   = 2 * time.Minute
	maxBackoff    = time.Minute
)

// Store is the in-memory workspace. The zero value is not usable; call New.
type Store struct {
	src  Source
	opts Options

	mu      sync.RWMutex
	issues  map[string]*issue
	seq     int64 // highest journal seq applied
	gen     uint64
	loaded  bool
	journal bool
	// pending collects the records applied while a re-export is in flight,
	// so they can be replayed onto the fresh snapshot before it is swapped in.
	pending *[]record
	// exportSum spots a poll that changed nothing, which must not bump gen.
	exportSum [32]byte

	viewMu sync.Mutex
	views  map[string][]byte
	viewAt uint64

	subMu sync.Mutex
	subs  map[chan struct{}]struct{}

	lastRead atomic.Int64 // unix nanos of the last read, for poll mode
	done     chan struct{}
}

// New returns an empty, unloaded store over src.
func New(src Source, opts Options) *Store {
	if opts.Resync <= 0 {
		opts.Resync = defaultResync
	}
	if opts.Poll <= 0 {
		opts.Poll = defaultPoll
	}
	if opts.Idle <= 0 {
		opts.Idle = defaultIdle
	}
	return &Store{
		src:    src,
		opts:   opts,
		issues: map[string]*issue{},
		views:  map[string][]byte{},
		subs:   map[chan struct{}]struct{}{},
		done:   make(chan struct{}),
	}
}

// Loaded reports whether the store holds a snapshot. Until it does, callers
// should go to bd directly.
func (s *Store) Loaded() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loaded
}

// Journal reports whether the store is fed by the events journal.
func (s *Store) Journal() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.journal
}

// Gen returns the current generation; it changes whenever the data does.
func (s *Store) Gen() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gen
}

// Done is closed once Run returns, so long-lived readers can let go.
func (s *Store) Done() <-chan struct{} { return s.done }

// Subscribe returns a channel that receives a value after each change, with
// bursts coalesced, and a func to stop. The channel is never closed.
func (s *Store) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.subMu.Lock()
	s.subs[ch] = struct{}{}
	s.subMu.Unlock()
	return ch, func() {
		s.subMu.Lock()
		delete(s.subs, ch)
		s.subMu.Unlock()
	}
}

func (s *Store) subscribers() int {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	return len(s.subs)
}

func (s *Store) notify() {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for ch := range s.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// bumpLocked records a change; s.mu must be held for writing.
func (s *Store) bumpLocked() {
	s.gen++
}

// Run loads the store and keeps it current until ctx ends. It retries with
// backoff when bd fails, so a workspace that is briefly unavailable recovers.
func (s *Store) Run(ctx context.Context) {
	defer close(s.done)
	backoff := time.Second
	for ctx.Err() == nil {
		err := s.cycle(ctx)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("store: refresh loop restarting", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// cycle runs one load-then-follow (or load-then-poll) pass. It returns only on
// failure or when ctx ends.
func (s *Store) cycle(ctx context.Context) error {
	journal, err := s.src.JournalEnabled(ctx)
	if err != nil {
		return fmt.Errorf("journal check: %w", err)
	}
	s.mu.Lock()
	s.journal = journal
	s.mu.Unlock()
	if !journal {
		return s.poll(ctx)
	}
	return s.follow(ctx)
}

// follow loads a baseline and then applies journal records as they arrive,
// re-exporting every Resync to catch changes the journal does not record.
func (s *Store) follow(ctx context.Context) error {
	if err := s.baseline(ctx); err != nil {
		return err
	}
	slog.Info("store: following events journal", "seq", s.currentSeq())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		errc <- s.src.Events(ctx, s.currentSeq(), true, s.applyLine)
	}()
	tick := time.NewTicker(s.opts.Resync)
	defer tick.Stop()
	for {
		select {
		case err := <-errc:
			if err == nil {
				err = errors.New("events follower exited")
			}
			return err
		case <-tick.C:
			if err := s.resync(ctx); err != nil {
				slog.Warn("store: resync failed", "err", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// baseline loads a snapshot that the follower can safely resume from. The
// journal head is read before the export, so every mutation the export misses
// has a higher seq and will be replayed. Replaying one the export already holds
// is harmless: each record carries the issue's full state, so applying the tail
// in order converges on the same result.
func (s *Store) baseline(ctx context.Context) error {
	since := s.currentSeq()
	head, err := s.journalHead(ctx, since)
	if err != nil && since > 0 {
		// The checkpoint may have been pruned while the follower was down;
		// read from the start instead.
		head, err = s.journalHead(ctx, 0)
	}
	if err != nil {
		return fmt.Errorf("journal head: %w", err)
	}
	out, err := s.src.Export(ctx)
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}
	snap, err := parseExport(out)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.issues = snap
	s.seq = head
	s.loaded = true
	s.exportSum = sum(out)
	s.bumpLocked()
	s.mu.Unlock()
	s.notify()
	return nil
}

// journalHead returns the highest seq after since, or since if there is none.
func (s *Store) journalHead(ctx context.Context, since int64) (int64, error) {
	head := since
	err := s.src.Events(ctx, since, false, func(line []byte) error {
		var r struct {
			Seq int64 `json:"seq"`
		}
		if err := json.Unmarshal(line, &r); err != nil {
			return fmt.Errorf("events: %w", err)
		}
		head = max(head, r.Seq)
		return nil
	})
	return head, err
}

// resync re-exports while the follower keeps running. Records the follower
// applies meanwhile are captured and replayed onto the new snapshot, so none
// is lost to the swap.
func (s *Store) resync(ctx context.Context) error {
	s.mu.Lock()
	captured := []record{}
	s.pending = &captured
	s.mu.Unlock()

	out, err := s.src.Export(ctx)

	var snap map[string]*issue
	if err == nil {
		snap, err = parseExport(out)
	}

	s.mu.Lock()
	replay := *s.pending
	s.pending = nil
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("resync: %w", err)
	}
	sm := sum(out)
	if sm == s.exportSum && len(replay) == 0 {
		s.mu.Unlock()
		return nil
	}
	for _, r := range replay {
		applyRecord(snap, r)
	}
	s.issues = snap
	s.exportSum = sm
	s.bumpLocked()
	s.mu.Unlock()
	s.notify()
	return nil
}

// poll keeps a journal-less store current by re-exporting while it is read.
func (s *Store) poll(ctx context.Context) error {
	if err := s.refresh(ctx); err != nil {
		return err
	}
	slog.Info("store: events journal is off; polling bd export",
		"every", s.opts.Poll,
		"hint", "bd config set events-journal true")
	tick := time.NewTicker(s.opts.Poll)
	defer tick.Stop()
	recheck := time.NewTicker(s.opts.Resync)
	defer recheck.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-recheck.C:
			// Pick up the journal being switched on without a restart.
			if on, err := s.src.JournalEnabled(ctx); err == nil && on {
				return errors.New("events journal enabled; switching to follow")
			}
		case <-tick.C:
			idle := time.Since(time.Unix(0, s.lastRead.Load())) > s.opts.Idle
			if idle && s.subscribers() == 0 {
				continue
			}
			if err := s.refresh(ctx); err != nil {
				slog.Warn("store: poll failed", "err", err)
			}
		}
	}
}

// refresh replaces the snapshot with a fresh export, bumping gen only when the
// export differs from the last one.
func (s *Store) refresh(ctx context.Context) error {
	out, err := s.src.Export(ctx)
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}
	sm := sum(out)
	s.mu.RLock()
	same := s.loaded && sm == s.exportSum
	s.mu.RUnlock()
	if same {
		return nil
	}
	snap, err := parseExport(out)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.issues = snap
	s.loaded = true
	s.exportSum = sm
	s.bumpLocked()
	s.mu.Unlock()
	s.notify()
	return nil
}

// Sync brings the store up to date after a write maggie made itself, so the
// response to that write, and the next read, already reflect it.
func (s *Store) Sync(ctx context.Context) error {
	s.mu.RLock()
	loaded, journal, since := s.loaded, s.journal, s.seq
	s.mu.RUnlock()
	if !loaded {
		return errors.New("store not loaded")
	}
	if !journal {
		return s.refresh(ctx)
	}
	return s.src.Events(ctx, since, false, s.applyLine)
}

func (s *Store) currentSeq() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seq
}

// applyLine decodes one journal line and applies it if it is new.
func (s *Store) applyLine(line []byte) error {
	var r record
	if err := json.Unmarshal(line, &r); err != nil {
		return fmt.Errorf("events: %w", err)
	}
	s.mu.Lock()
	if r.Seq <= s.seq {
		s.mu.Unlock()
		return nil
	}
	applyRecord(s.issues, r)
	s.seq = r.Seq
	if s.pending != nil {
		*s.pending = append(*s.pending, r)
	}
	s.bumpLocked()
	s.mu.Unlock()
	s.notify()
	return nil
}

func (s *Store) touch() { s.lastRead.Store(time.Now().UnixNano()) }

// view returns the cached rendering for key at the current generation,
// building it with render on a miss.
func (s *Store) view(key string, render func(map[string]*issue) ([]byte, error)) ([]byte, error) {
	s.touch()
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	if s.viewAt != s.gen {
		clear(s.views)
		s.viewAt = s.gen
	}
	if b, ok := s.views[key]; ok {
		return b, nil
	}
	b, err := render(s.issues)
	if err != nil {
		return nil, err
	}
	s.views[key] = b
	return b, nil
}

// Memo caches the result of fetch for the current generation. It is for reads
// the store cannot answer itself, such as bd's ready computation, so they run
// once per change rather than once per request.
func (s *Store) Memo(ctx context.Context, key string, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	gen := s.Gen()
	s.viewMu.Lock()
	if s.viewAt == gen {
		if b, ok := s.views[key]; ok {
			s.viewMu.Unlock()
			return b, nil
		}
	}
	s.viewMu.Unlock()
	b, err := fetch(ctx)
	if err != nil {
		return nil, err
	}
	// Read gen before taking viewMu: view() takes mu then viewMu, so taking
	// them the other way round here could deadlock.
	now := s.Gen()
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	// Only cache if nothing changed while bd ran; otherwise the answer may
	// already be stale and the next request should ask again.
	if now == gen {
		if s.viewAt != gen {
			clear(s.views)
			s.viewAt = gen
		}
		s.views[key] = b
	}
	return b, nil
}

// List renders every issue in the shape of `bd list --json --all`.
func (s *Store) List() ([]byte, error) { return s.view("list", renderList) }

// Comments renders comment bodies keyed by issue id.
func (s *Store) Comments() ([]byte, error) { return s.view("comments", renderComments) }

// Graph renders the dependency edges `bd graph --all` draws.
func (s *Store) Graph() ([]byte, error) { return s.view("graph", renderGraph) }

// ErrNotFound is returned by Show for an id the store does not hold.
var ErrNotFound = errors.New("issue not found")

// Show renders one issue in the shape of `bd show --json --include-dependents
// --include-comments`.
func (s *Store) Show(id string) ([]byte, error) {
	s.touch()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.issues[id]; !ok {
		return nil, ErrNotFound
	}
	return renderShow(s.issues, id)
}

func sum(b []byte) [32]byte { return sha256.Sum256(b) }

// --- data model ---

// issue holds bd's fields verbatim, so a field maggie does not know about is
// still served, with dependencies and comments split out because the journal
// updates them separately from the rest.
type issue struct {
	fields   map[string]json.RawMessage
	deps     []dep
	comments []comment
}

type dep struct {
	target string
	kind   string
	raw    json.RawMessage
}

type comment struct {
	id  string
	raw json.RawMessage
	txt string
}

// derived are the keys maggie computes rather than stores: counts, the parent
// shortcut, and journal-only hints that an export would not repeat.
var derived = []string{
	"_type", "dependencies", "dependents", "comments",
	"dependency_count", "dependent_count", "comment_count",
	"parent", "is_blocked", "revision",
}

func parseFields(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var f map[string]json.RawMessage
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, err
	}
	return f, nil
}

func stringField(f map[string]json.RawMessage, key string) string {
	var v string
	_ = json.Unmarshal(f[key], &v)
	return v
}

func parseDeps(raw json.RawMessage) ([]dep, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	out := make([]dep, 0, len(rows))
	for _, r := range rows {
		var d struct {
			DependsOn string `json:"depends_on_id"`
			Type      string `json:"type"`
		}
		if err := json.Unmarshal(r, &d); err != nil {
			return nil, err
		}
		out = append(out, dep{target: d.DependsOn, kind: d.Type, raw: r})
	}
	return out, nil
}

func parseComments(raw json.RawMessage) ([]comment, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	out := make([]comment, 0, len(rows))
	for _, r := range rows {
		var c struct {
			ID   string `json:"id"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(r, &c); err != nil {
			return nil, err
		}
		out = append(out, comment{id: c.ID, raw: r, txt: c.Text})
	}
	return out, nil
}

// parseExport builds a snapshot from `bd export` JSONL.
func parseExport(out []byte) (map[string]*issue, error) {
	snap := map[string]*issue{}
	for line := range bytes.SplitSeq(out, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		f, err := parseFields(line)
		if err != nil {
			return nil, fmt.Errorf("export: %w", err)
		}
		if t := stringField(f, "_type"); t != "" && t != "issue" {
			continue
		}
		id := stringField(f, "id")
		if id == "" {
			continue
		}
		deps, err := parseDeps(f["dependencies"])
		if err != nil {
			return nil, fmt.Errorf("export %s dependencies: %w", id, err)
		}
		cms, err := parseComments(f["comments"])
		if err != nil {
			return nil, fmt.Errorf("export %s comments: %w", id, err)
		}
		for _, k := range derived {
			delete(f, k)
		}
		snap[id] = &issue{fields: f, deps: deps, comments: cms}
	}
	return snap, nil
}

// record is one events journal line.
type record struct {
	Seq     int64           `json:"seq"`
	TS      string          `json:"ts"`
	Op      string          `json:"op"`
	IssueID string          `json:"issue_id"`
	Actor   string          `json:"actor"`
	Issue   json.RawMessage `json:"issue"`
	Dep     *struct {
		Kind     string          `json:"kind"`
		Target   string          `json:"target"`
		Metadata json.RawMessage `json:"metadata"`
	} `json:"dep"`
	Comment json.RawMessage `json:"comment"`
}

// applyRecord folds one journal record into snap. It is idempotent per record,
// and replaying a suffix of the journal in order converges on the state after
// its last record, which is what lets a snapshot be patched without knowing
// exactly which records it already reflects.
func applyRecord(snap map[string]*issue, r record) {
	if r.Op == "delete" {
		delete(snap, r.IssueID)
		return
	}
	it := snap[r.IssueID]
	if len(r.Issue) > 0 && string(r.Issue) != "null" {
		f, err := parseFields(r.Issue)
		if err != nil {
			slog.Warn("store: bad issue in journal record", "seq", r.Seq, "err", err)
			return
		}
		if it == nil {
			it = &issue{}
			snap[r.IssueID] = it
		}
		// The payload is the full state, minus dependencies and comments,
		// which arrive as their own records.
		if deps, err := parseDeps(f["dependencies"]); err == nil && deps != nil {
			it.deps = deps
		}
		if cms, err := parseComments(f["comments"]); err == nil && cms != nil {
			it.comments = cms
		}
		for _, k := range derived {
			delete(f, k)
		}
		it.fields = f
	}
	if it == nil {
		return
	}
	switch r.Op {
	case "dep_add":
		if r.Dep == nil {
			return
		}
		raw := marshal(map[string]any{
			"issue_id":      r.IssueID,
			"depends_on_id": r.Dep.Target,
			"type":          r.Dep.Kind,
			"created_at":    r.TS,
			"created_by":    r.Actor,
			"metadata":      metadataString(r.Dep.Metadata),
		})
		d := dep{target: r.Dep.Target, kind: r.Dep.Kind, raw: raw}
		// bd keys a dependency on the pair, so a re-add replaces the edge.
		if i := slices.IndexFunc(it.deps, func(x dep) bool { return x.target == d.target }); i >= 0 {
			it.deps[i] = d
		} else {
			it.deps = append(it.deps, d)
		}
	case "dep_remove":
		if r.Dep == nil {
			return
		}
		it.deps = slices.DeleteFunc(it.deps, func(x dep) bool {
			return x.target == r.Dep.Target
		})
	case "comment":
		if len(r.Comment) == 0 {
			return
		}
		f, err := parseFields(r.Comment)
		if err != nil {
			return
		}
		f["issue_id"] = marshal(r.IssueID)
		delete(f, "source")
		c := comment{id: stringField(f, "id"), txt: stringField(f, "text"), raw: marshal(f)}
		if slices.ContainsFunc(it.comments, func(x comment) bool { return x.id == c.id }) {
			return
		}
		it.comments = append(it.comments, c)
	}
}

// metadataString keeps the export's encoding of metadata, a JSON string.
func metadataString(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "{}"
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

func marshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		// Only maps of strings and raw JSON reach here.
		panic(err)
	}
	return b
}

// --- rendering ---

// Dependency kinds bd counts and draws: only blocks edges count towards
// dependency_count, and the graph draws blocks and parent-child edges.
const (
	blocks      = "blocks"
	parentChild = "parent-child"
)

// dependents indexes the reverse of every dependency: target -> sources.
func dependents(snap map[string]*issue) map[string][]depFrom {
	rev := map[string][]depFrom{}
	for _, id := range sortedIDs(snap) {
		for _, d := range snap[id].deps {
			rev[d.target] = append(rev[d.target], depFrom{from: id, kind: d.kind})
		}
	}
	return rev
}

type depFrom struct {
	from string
	kind string
}

func sortedIDs(snap map[string]*issue) []string {
	return slices.Sorted(maps.Keys(snap))
}

func countBlocks(deps []dep) int {
	n := 0
	for _, d := range deps {
		if d.kind == blocks {
			n++
		}
	}
	return n
}

func countBlockFrom(ds []depFrom) int {
	n := 0
	for _, d := range ds {
		if d.kind == blocks {
			n++
		}
	}
	return n
}

// base returns the issue's stored fields plus the counts bd derives.
func base(it *issue, rev []depFrom) map[string]json.RawMessage {
	out := maps.Clone(it.fields)
	out["dependency_count"] = marshal(countBlocks(it.deps))
	out["dependent_count"] = marshal(countBlockFrom(rev))
	out["comment_count"] = marshal(len(it.comments))
	return out
}

// listOrder matches bd: priority, then newest first, then id.
func listOrder(snap map[string]*issue) []string {
	ids := sortedIDs(snap)
	type key struct {
		pri     int
		created string
	}
	keys := make(map[string]key, len(ids))
	for _, id := range ids {
		var p int
		_ = json.Unmarshal(snap[id].fields["priority"], &p)
		keys[id] = key{pri: p, created: stringField(snap[id].fields, "created_at")}
	}
	slices.SortStableFunc(ids, func(a, b string) int {
		ka, kb := keys[a], keys[b]
		if ka.pri != kb.pri {
			return ka.pri - kb.pri
		}
		return strings.Compare(kb.created, ka.created)
	})
	return ids
}

func renderList(snap map[string]*issue) ([]byte, error) {
	rev := dependents(snap)
	rows := make([]map[string]json.RawMessage, 0, len(snap))
	for _, id := range listOrder(snap) {
		it := snap[id]
		row := base(it, rev[id])
		if len(it.deps) > 0 {
			raws := make([]json.RawMessage, len(it.deps))
			for i, d := range it.deps {
				raws[i] = d.raw
				if d.kind == parentChild {
					row["parent"] = marshal(d.target)
				}
			}
			row["dependencies"] = marshal(raws)
		}
		rows = append(rows, row)
	}
	return json.Marshal(rows)
}

// longText is left out of a dependency reference, which only names its target.
var longText = []string{"description", "notes", "acceptance_criteria", "design"}

func depRef(snap map[string]*issue, id, kind string) map[string]json.RawMessage {
	ref := map[string]json.RawMessage{"id": marshal(id)}
	if t, ok := snap[id]; ok {
		ref = maps.Clone(t.fields)
		for _, k := range longText {
			delete(ref, k)
		}
	}
	ref["dependency_type"] = marshal(kind)
	return ref
}

func renderShow(snap map[string]*issue, id string) ([]byte, error) {
	it := snap[id]
	rev := dependents(snap)[id]
	row := base(it, rev)
	if len(it.deps) > 0 {
		refs := make([]map[string]json.RawMessage, len(it.deps))
		for i, d := range it.deps {
			refs[i] = depRef(snap, d.target, d.kind)
		}
		row["dependencies"] = marshal(refs)
	}
	if len(rev) > 0 {
		refs := make([]map[string]json.RawMessage, len(rev))
		for i, d := range rev {
			refs[i] = depRef(snap, d.from, d.kind)
		}
		row["dependents"] = marshal(refs)
	}
	if len(it.comments) > 0 {
		raws := make([]json.RawMessage, len(it.comments))
		for i, c := range it.comments {
			raws[i] = c.raw
		}
		row["comments"] = marshal(raws)
	}
	return json.Marshal([]map[string]json.RawMessage{row})
}

func renderComments(snap map[string]*issue) ([]byte, error) {
	out := map[string][]string{}
	for id, it := range snap {
		if len(it.comments) == 0 {
			continue
		}
		texts := make([]string, len(it.comments))
		for i, c := range it.comments {
			texts[i] = c.txt
		}
		out[id] = texts
	}
	return json.Marshal(out)
}

// Edge mirrors beads.Edge: from blocks (or parents) to.
type Edge struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Dashed bool   `json:"dashed"`
}

// renderGraph draws what `bd graph --all` does: blocking and parent-child
// edges between active issues (neither closed nor deferred), pointing from the
// blocker or parent to the dependent.
func renderGraph(snap map[string]*issue) ([]byte, error) {
	open := func(id string) bool {
		it, ok := snap[id]
		if !ok {
			return false
		}
		st := stringField(it.fields, "status")
		return st != "closed" && st != "deferred"
	}
	edges := []Edge{}
	for _, id := range sortedIDs(snap) {
		if !open(id) {
			continue
		}
		for _, d := range snap[id].deps {
			if (d.kind != blocks && d.kind != parentChild) || !open(d.target) {
				continue
			}
			edges = append(edges, Edge{From: d.target, To: id, Dashed: d.kind == parentChild})
		}
	}
	return json.Marshal(edges)
}
