// Command maggie serves a web UI over the Beads `bd` CLI.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mulgadc/maggie/internal/beads"
	"github.com/mulgadc/maggie/internal/store"
)

// Version is set via ldflags at build time.
// Example: go build -ldflags "-X main.Version=v1.0.0".
var Version = "dev"

//go:embed web
var webFS embed.FS

var (
	idRe       = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	statusRe   = regexp.MustCompile(`^(open|in_progress|blocked|deferred|closed)$`)
	priorityRe = regexp.MustCompile(`^[0-4]$`)
	actorRe    = regexp.MustCompile(`^[A-Za-z0-9-]{1,39}$`) // GitHub username shape
	labelRe    = regexp.MustCompile(`^[A-Za-z0-9._:/-]{1,64}$`)
	depTypeRe  = regexp.MustCompile(`^(blocks|relates-to|parent)$`)
)

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("maggie", Version)
		return
	}

	if err := setupLogging(os.Stdout, envOr("MAGGIE_LOG_LEVEL", "info")); err != nil {
		fmt.Fprintln(os.Stderr, "maggie:", err)
		os.Exit(1)
	}

	addr := envOr("MAGGIE_ADDR", ":8088")
	dir := envOr("MAGGIE_BEADS_DIR", ".")
	bin := envOr("MAGGIE_BD_BIN", "bd")

	bd := beads.New(bin, dir, bdTimeout)
	actors := splitActors(os.Getenv("MAGGIE_ACTORS"))
	resync, err := time.ParseDuration(envOr("MAGGIE_RESYNC", "5m"))
	if err != nil || resync <= 0 {
		slog.Error("MAGGIE_RESYNC must be a positive duration", "value", os.Getenv("MAGGIE_RESYNC"))
		os.Exit(1)
	}
	st := store.New(bd, store.Options{Resync: resync})

	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		slog.Error("embed web", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go st.Run(ctx)

	slog.Info("maggie listening", "addr", addr, "beads_dir", dir)
	srv := httpServer(addr, handlerChain(routes(bd, st, actors, sub)))
	if err := serve(ctx, srv); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}

// setupLogging installs the process logger: JSON on stdout, matching the other
// mulga services so journald and log shippers get structured records rather than
// slog's default text. name is a level (debug|info|warn|error, case-insensitive,
// with slog's +/-N offsets); an unusable one is a startup error rather than a
// silent fall back to info, which would hide a misconfigured deployment.
func setupLogging(w io.Writer, name string) error {
	var level slog.Level
	if err := level.UnmarshalText([]byte(name)); err != nil {
		return fmt.Errorf("MAGGIE_LOG_LEVEL %q: %w", name, err)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})))
	return nil
}

// bdTimeout caps a single bd invocation. The server's WriteTimeout is derived
// from it, so a bd call that runs long fails as a bd error rather than as a
// truncated response.
const bdTimeout = 15 * time.Second

// handlerChain wraps the route table in the middleware every response passes
// through. Compression sits inside the header middleware so the security
// headers are set once, on the outermost writer.
func handlerChain(h http.Handler) http.Handler {
	return securityHeaders(compress(h))
}

// httpServer builds the listener's server with every timeout set. WriteTimeout
// clears bdTimeout so a slow bd call still gets to answer; the rest bound how
// long an idle or trickling connection can hold a slot.
func httpServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      bdTimeout + 15*time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// shutdownGrace bounds the wait for in-flight requests once a signal arrives. A
// bd write holds its own 15s timeout, so the grace period clears it and the
// mutation lands rather than being killed halfway through.
const shutdownGrace = 30 * time.Second

// unixPrefix marks a MAGGIE_ADDR that names a Unix socket rather than a TCP
// host:port, e.g. unix:/run/user/1000/maggie.sock.
const unixPrefix = "unix:"

// listenAddr opens the listener for addr: a TCP host:port, or unix:PATH for a
// Unix socket. A socket is created mode 0600, so only maggie's own user can
// connect, which is the access control a local or ssh-forwarded UI needs
// without opening a port. Go removes the socket file when the listener closes.
func listenAddr(addr string) (net.Listener, error) {
	path, ok := strings.CutPrefix(addr, unixPrefix)
	if !ok {
		return net.Listen("tcp", addr)
	}
	if path == "" {
		return nil, errors.New("MAGGIE_ADDR unix: needs a socket path")
	}
	if err := clearStaleSocket(path); err != nil {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("restrict %s: %w", path, err)
	}
	return ln, nil
}

// clearStaleSocket removes a socket left behind by a maggie that did not shut
// down cleanly, so a restart does not fail with "address already in use". It
// refuses to touch a socket something still answers on, or a path that is not
// a socket at all.
func clearStaleSocket(path string) error {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if fi.Mode()&fs.ModeSocket == 0 {
		return fmt.Errorf("%s exists and is not a socket", path)
	}
	if conn, err := net.DialTimeout("unix", path, time.Second); err == nil {
		_ = conn.Close()
		return fmt.Errorf("%s is in use by another server", path)
	}
	return os.Remove(path)
}

// serve runs srv until it fails or ctx is cancelled, then drains in-flight
// requests. ErrServerClosed is Shutdown's documented return once the drain has
// been asked for, so it reports success rather than a failure to exit non-zero.
// srv.Addr is read by listenAddr, so it may name a Unix socket.
func serve(ctx context.Context, srv *http.Server) error {
	ln, err := listenAddr(srv.Addr)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	slog.Info("shutdown signal received, draining in-flight requests")
	drainCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return <-errCh
}

// routes wires the bd-backed API and the embedded SPA onto a mux. Reads are
// answered from st once it has loaded, and go to bd directly until then.
func routes(bd *beads.Client, st *store.Store, actors []string, web fs.FS) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/", spaHandler(web))
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/api/stream", streamHandler(st))
	mux.HandleFunc("/api/ready", jsonHandler(func(r *http.Request) ([]byte, error) {
		if st.Loaded() {
			// bd computes readiness (blockers, deferral, parents), so the
			// store only caches its answer until the next change.
			return st.Memo(r.Context(), "ready", bd.Ready)
		}
		return bd.Ready(r.Context())
	}))
	mux.HandleFunc("/api/issues", jsonHandler(func(r *http.Request) ([]byte, error) {
		o := parseList(r)
		if st.Loaded() && o.All && o.Status == "" && o.Priority == "" &&
			(o.Limit == 0 || o.Limit == maxListLimit) {
			return st.List()
		}
		return bd.List(r.Context(), o)
	}))
	mux.HandleFunc("/api/issue", jsonHandler(func(r *http.Request) ([]byte, error) {
		id := r.URL.Query().Get("id")
		if !idRe.MatchString(id) {
			return nil, errBadRequest("invalid id")
		}
		if st.Loaded() {
			if out, err := st.Show(id); err == nil {
				return out, nil
			}
		}
		return bd.Show(r.Context(), id)
	}))
	mux.HandleFunc("/api/comments", jsonHandler(func(r *http.Request) ([]byte, error) {
		if st.Loaded() {
			return st.Comments()
		}
		byID, err := bd.Comments(r.Context())
		if err != nil {
			return nil, err
		}
		return json.Marshal(byID)
	}))
	mux.HandleFunc("/api/actors", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(actors); err != nil {
			slog.Error("actors encode", "err", err)
		}
	})
	mux.HandleFunc("/api/issue/update", writeHandler(func(r *http.Request) (string, error) {
		var b updateBody
		if err := decode(r, &b); err != nil {
			return "", err
		}
		if !idRe.MatchString(b.ID) {
			return "", errBadRequest("invalid id")
		}
		f, err := b.fields()
		if err != nil {
			return "", err
		}
		return b.ID, bd.Update(r.Context(), b.ID, f, b.Actor)
	}, bd, st))
	mux.HandleFunc("/api/issue/comment", writeHandler(func(r *http.Request) (string, error) {
		var b commentBody
		if err := decode(r, &b); err != nil {
			return "", err
		}
		if !idRe.MatchString(b.ID) {
			return "", errBadRequest("invalid id")
		}
		if strings.TrimSpace(b.Text) == "" {
			return "", errBadRequest("empty comment")
		}
		return b.ID, bd.AddComment(r.Context(), b.ID, b.Text, b.Actor)
	}, bd, st))
	mux.HandleFunc("/api/issue/dep", writeHandler(func(r *http.Request) (string, error) {
		var b depBody
		if err := decode(r, &b); err != nil {
			return "", err
		}
		if !idRe.MatchString(b.ID) || !idRe.MatchString(b.DependsOn) {
			return "", errBadRequest("invalid id")
		}
		if !depTypeRe.MatchString(b.Type) {
			return "", errBadRequest("invalid dep type")
		}
		if b.ID == b.DependsOn {
			return "", errBadRequest("cannot depend on self")
		}
		return b.ID, bd.AddDep(r.Context(), b.ID, b.DependsOn, b.Type, b.Actor)
	}, bd, st))
	mux.HandleFunc("/api/graph", func(w http.ResponseWriter, r *http.Request) {
		if st.Loaded() {
			out, err := st.Graph()
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(out)
				return
			}
			slog.Error("graph render", "err", err)
		}
		edges, err := bd.GraphEdges(r.Context())
		if err != nil {
			slog.Error("graph", "err", err)
			http.Error(w, "graph failed", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(edges); err != nil {
			slog.Error("graph encode", "err", err)
		}
	})
	return mux
}

// Content-Security-Policy header. Everything the SPA needs is same-origin,
// fonts included, so the page makes no third-party request. No HSTS or
// upgrade-insecure-requests: maggie serves plain HTTP and expects a reverse
// proxy to terminate TLS, so either would break a direct http:// deployment.
const csp = "default-src 'self'; script-src 'self'; style-src 'self'; " +
	"img-src 'self' data:; font-src 'self'; " +
	"connect-src 'self'; object-src 'none'; base-uri 'self'; " +
	"form-action 'self'; frame-ancestors 'none';"

// securityHeaders applies the response headers that bound what the page is
// allowed to do. Bead text is rendered as markdown with raw HTML disabled, so
// this is defence in depth rather than the only control.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", csp)
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), browsing-topics=()")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

// healthHandler answers a liveness probe: the process is up and serving. It
// deliberately does not shell out to bd, so a probe cannot be turned into an
// unauthenticated way to spawn subprocesses. /api/ready is beads' ready-to-work
// list, not a readiness probe, despite the name.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": Version,
	}); err != nil {
		slog.Error("health encode", "err", err)
	}
}

// spaHandler serves embedded static assets, falling back to index.html for
// client-side routes (paths with no file extension that are not found).
func spaHandler(sub fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := path.Clean(strings.TrimPrefix(r.URL.Path, "/"))
		if p == "." {
			p = "index.html"
		}
		if _, err := fs.Stat(sub, p); err != nil {
			// A missing path that names a file is a real 404, and it is served
			// without a cache directive so the miss is not remembered.
			if path.Ext(p) != "" {
				fileServer.ServeHTTP(w, r)
				return
			}
			r = r.Clone(r.Context())
			r.URL.Path = "/"
			p = "index.html"
		}
		w.Header().Set("Cache-Control", assetCacheControl(p))
		fileServer.ServeHTTP(w, r)
	})
}

// embed.FS reports a zero ModTime and FileServer sets no ETag, so a revalidated
// response has no validator to answer 304 with. The content hash under assets/
// is that validator: the name addresses one build, so it can be cached forever.
// Everything else is revalidated, index.html above all — it is what points at
// the current build's hashed names.
func assetCacheControl(p string) string {
	if strings.HasPrefix(p, "assets/") {
		return "public, max-age=31536000, immutable"
	}
	return "no-cache"
}

// parseList builds ListOpts from allowlisted, validated query params.
// maxListLimit is the largest list the API asks bd for; the UI asks for it to
// mean "everything".
const maxListLimit = 100000

func parseList(r *http.Request) beads.ListOpts {
	q := r.URL.Query()
	o := beads.ListOpts{All: q.Get("all") == "true"}
	if s := q.Get("status"); statusRe.MatchString(s) {
		o.Status = s
	}
	if p := q.Get("priority"); priorityRe.MatchString(p) {
		o.Priority = p
	}
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 && n <= maxListLimit {
		o.Limit = n
	}
	return o
}

// Write request bodies. Assignee/Description are pointers so "absent" (leave
// unchanged) is distinct from "" (clear). Every write requires a valid actor.
type updateBody struct {
	ID           string   `json:"id"`
	Actor        string   `json:"actor"`
	Status       string   `json:"status"`
	Priority     string   `json:"priority"`
	Assignee     *string  `json:"assignee"`
	Description  *string  `json:"description"`
	Notes        *string  `json:"notes"`
	Acceptance   *string  `json:"acceptance"`
	AddLabels    []string `json:"add_labels"`
	RemoveLabels []string `json:"remove_labels"`
}

type commentBody struct {
	ID    string `json:"id"`
	Actor string `json:"actor"`
	Text  string `json:"text"`
}

type depBody struct {
	ID        string `json:"id"`
	Actor     string `json:"actor"`
	DependsOn string `json:"depends_on"`
	Type      string `json:"type"`
}

// fields validates the update payload and builds beads.UpdateFields.
func (b updateBody) fields() (beads.UpdateFields, error) {
	var f beads.UpdateFields
	if b.Status != "" {
		if !statusRe.MatchString(b.Status) {
			return f, errBadRequest("invalid status")
		}
		f.Status = b.Status
	}
	if b.Priority != "" {
		if !priorityRe.MatchString(b.Priority) {
			return f, errBadRequest("invalid priority")
		}
		f.Priority = b.Priority
	}
	if b.Assignee != nil {
		if *b.Assignee != "" && !actorRe.MatchString(*b.Assignee) {
			return f, errBadRequest("invalid assignee")
		}
		f.Assignee = b.Assignee
	}
	f.Description = b.Description
	f.Notes = b.Notes
	f.Acceptance = b.Acceptance
	for _, l := range append(b.AddLabels, b.RemoveLabels...) {
		if !labelRe.MatchString(l) {
			return f, errBadRequest("invalid label")
		}
	}
	f.AddLabels = b.AddLabels
	f.RemoveLabels = b.RemoveLabels
	return f, nil
}

// decode enforces POST + JSON body and rejects a missing/invalid actor up front.
func decode(r *http.Request, v interface{ actor() string }) error {
	if r.Method != http.MethodPost {
		return errBadRequest("method not allowed")
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(v); err != nil {
		return errBadRequest("invalid body")
	}
	if !actorRe.MatchString(v.actor()) {
		return errBadRequest("invalid actor")
	}
	return nil
}

func (b updateBody) actor() string  { return b.Actor }
func (b commentBody) actor() string { return b.Actor }
func (b depBody) actor() string     { return b.Actor }

// writeHandler runs a bd mutation then returns the fresh issue JSON so clients
// can update their cache without a round-trip. The store is brought up to date
// first, so the reply and every read after it include the write; bd show is the
// fallback when it cannot be.
func writeHandler(fn func(*http.Request) (string, error), bd *beads.Client, st *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := fn(r)
		if err != nil {
			if _, ok := errors.AsType[badRequestError](err); ok {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			slog.Error("write", "path", r.URL.Path, "err", err)
			http.Error(w, "upstream bd failed", http.StatusBadGateway)
			return
		}
		if st.Loaded() {
			if err := st.Sync(r.Context()); err != nil {
				slog.Warn("write sync", "id", id, "err", err)
			} else if out, err := st.Show(id); err == nil {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(out)
				return
			}
		}
		out, err := bd.Show(r.Context(), id)
		if err != nil {
			slog.Error("write show", "id", id, "err", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	}
}

type badRequestError struct{ msg string }

func (e badRequestError) Error() string { return e.msg }
func errBadRequest(m string) error      { return badRequestError{m} }

// jsonHandler adapts a bd-backed fetch into an HTTP handler, forwarding raw JSON.
func jsonHandler(fn func(*http.Request) ([]byte, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		out, err := fn(r)
		if err != nil {
			if _, ok := errors.AsType[badRequestError](err); ok {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			slog.Error("api", "path", r.URL.Path, "err", err)
			http.Error(w, "upstream bd failed", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	}
}

// splitActors parses MAGGIE_ACTORS, the comma-separated roster offered by the
// identity picker. Always non-nil so /api/actors encodes [] rather than null;
// an empty roster just means users type their own name.
func splitActors(s string) []string {
	out := []string{}
	for a := range strings.SplitSeq(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// streamHeartbeat keeps an idle event stream from being cut by proxies.
const streamHeartbeat = 25 * time.Second

// streamHandler pushes a server-sent "change" event whenever the store's data
// changes, so the page refetches straight away instead of waiting to poll. The
// event carries only the generation; the page re-reads what it shows.
func streamHandler(st *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		// The stream outlives the server's WriteTimeout by design.
		if err := rc.SetWriteDeadline(time.Time{}); err != nil {
			slog.Debug("stream deadline", "err", err)
		}
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-store")
		h.Set("X-Accel-Buffering", "no")
		ch, stop := st.Subscribe()
		defer stop()
		send := func(format string, args ...any) bool {
			if _, err := fmt.Fprintf(w, format, args...); err != nil {
				return false
			}
			return rc.Flush() == nil
		}
		if !send("retry: 3000\nevent: hello\ndata: %d\n\n", st.Gen()) {
			return
		}
		beat := time.NewTicker(streamHeartbeat)
		defer beat.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-st.Done():
				return
			case <-ch:
				if !send("event: change\ndata: %d\n\n", st.Gen()) {
					return
				}
			case <-beat.C:
				if !send(": ping\n\n") {
					return
				}
			}
		}
	}
}
