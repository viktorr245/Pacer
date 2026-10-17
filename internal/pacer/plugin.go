// Package pacer adapts the host ABI. Routing, quota observation, policy state and
// the management page have separate owners; inference never queries quota APIs.
package pacer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/floris3456/cliproxyapi-quota-handoff/internal/capacity"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/history"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/historyreports"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/management"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/measurement"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/observation"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/policyadapter"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/policyevidence"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/prefixevidence"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/routing"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/tracker"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/usagecoverage"
	_ "github.com/mattn/go-sqlite3"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

type Host func(string, any) (json.RawMessage, error)
type Config struct {
	ForwardReportPath string       `yaml:"forward_report_path" json:"-"`
	ForwardReportBook string       `yaml:"forward_report_book" json:"-"`
	ReportPath        string       `yaml:"report_path"`
	CachePolicy       PolicyConfig `yaml:"cache_policy"`
	Enabled           bool         `yaml:"enabled"`
	Priority          int          `yaml:"priority"`
	StatePath         string       `yaml:"state_path"`
	HistoryPath       string       `yaml:"history_path"`
	CompactOnSwitch   bool         `yaml:"compact_on_switch"`
	CompactionModels  []string     `yaml:"compaction_models"`
	UsageAuthURL      string       `yaml:"usage_auth_url"`
	Routing           struct {
		Mode      string   `yaml:"mode"`
		Providers []string `yaml:"providers"`
	} `yaml:"routing"`
}
type credential struct {
	ID, Index, Provider, Account, Alias string
	AccountVerified                     bool // Host billing identity, not a credential-only fallback.
	ExactIdentity                       bool
	CoverageMembership                  string
}
type attempt struct {
	Model, PreviousAccount     string
	Claim                      *tracker.Claim
	Caller, Session, Key, Auth string
	// History is the request's history start (historyKey), "" without one.
	History         string
	Headers         http.Header
	Decision        routing.Decision
	RoutingChoice   pluginapi.SchedulerPickResponse
	RoutingProvider string
	Binding         tracker.Binding
	// Client is what this request's client declared (X-Pacer-Client); Served
	// is the credential CPA picked, kept when the client asked for quota.
	Client      clientCaps
	Served      credential
	ServedModel string
}
type Plugin struct {
	forwardManagementMu    sync.Mutex
	forwardManagementEpoch uint64
	forwardManagement      *forwardManagementBinding
	// Optional deterministic lease barrier, set only before starting the plugin.
	forwardManagementLeaseHook func(context.Context)

	forward            *forwardPublisher
	forwardOptions     *forwardOptions
	coverage           *usagecoverage.Consumer
	coverageGroups     map[string]coverageGroup
	coverageMu         sync.Mutex
	coverageCursor     string
	coverageGeneration uint64
	coverageStats      coverageDiagnostics
	policyEvidence     *policyevidence.Store
	prefixEvidence     *prefixevidence.Store
	prefixDB           *sql.DB
	capacityStore      *capacity.Store
	capacityDB         *sql.DB
	capacityViews      map[string]capacityState
	reports            *historyreports.Reader
	reportSlots        chan struct{}
	policy             *policyadapter.Adapter
	selections         map[string]pluginapi.CachePolicy
	history            *history.Store
	observationMu      sync.Mutex
	measurement        *measurement.Store
	measurements       map[string]measurement.Report
	measurementErrors  int64
	measurementSource  string
	usageLive          usageLive
	usageChanges       usageChanges
	mu                 sync.Mutex
	Host               Host
	now                func() time.Time
	db                 *sql.DB
	chats              *tracker.Store
	observer           *observation.Store
	cfg                Config
	seedCfg            Config   // CPA's YAML before page settings apply
	settings           Settings // effective page settings (settings.go)
	strikes            map[string]strike
	creds              map[string]credential
	usageCreds         map[string]credential // usage page only (see usage_accounts.go)
	recordStore        *historyreports.RecordStore
	record             *historyreports.Record
	priceSync          priceSync
	requests           map[string]attempt
	previews           map[string]routingPreviewInput
	decisionErrors     int64
	workerOnce         sync.Once
	workerDone         chan struct{}
	workerWait         sync.WaitGroup
	refreshMu          sync.Mutex
	discoverMu         sync.Mutex
	lastDiscover       time.Time
	stopped            bool
}

func New(host Host) *Plugin {
	return &Plugin{Host: host, now: time.Now, settings: defaultSettings(), strikes: map[string]strike{}, creds: map[string]credential{}, requests: map[string]attempt{}, selections: map[string]pluginapi.CachePolicy{}, workerDone: make(chan struct{}), reportSlots: make(chan struct{}, 2)}
}

func registration(policyEnabled ...bool) any {
	schema := 6
	enabled := len(policyEnabled) > 0 && policyEnabled[0]
	if enabled || len(policyEnabled) > 1 && policyEnabled[1] {
		schema = 7
	}
	return map[string]any{"schema_version": schema, "metadata": pluginapi.Metadata{Name: "pacer", Version: "0.2.0-dev", Author: "floris3456", GitHubRepository: "https://github.com/floris3456/cliproxyapi-quota-handoff"}, "capabilities": map[string]bool{"scheduler": true, "scheduler_across_priorities": true, "request_interceptor": true, "request_lifecycle_plugin": true, "response_interceptor": true, "response_stream_interceptor": true, "usage_plugin": true, "management_api": true, "cache_policy": enabled}}
}

func (p *Plugin) configure(raw []byte) (any, error) {
	var request struct {
		ConfigYAML    []byte `json:"config_yaml"`
		SchemaVersion int    `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	var cfg Config
	if len(request.ConfigYAML) == 0 {
		return nil, errors.New("state_path is required")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(request.ConfigYAML))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	if cfg.StatePath == "" {
		return nil, errors.New("state_path is required")
	}
	policyOpts, err := p.policyOptions(cfg.CachePolicy)
	if err != nil {
		return nil, err
	}
	if cfg.CachePolicy.Enabled && request.SchemaVersion < 7 {
		return nil, errors.New("managed cache policy requires CPA schema 7")
	}
	usageURL, err := validateUsageURL(cfg.UsageAuthURL)
	if err != nil {
		return nil, err
	}
	cfg.UsageAuthURL = usageURL
	cfg.Routing.Mode = routingMode(cfg.Routing.Mode)
	if cfg.Routing.Mode == "" {
		return nil, errors.New("routing.mode must be off or on")
	}
	if len(cfg.Routing.Providers) == 0 {
		cfg.Routing.Providers = []string{"claude"}
	}
	for _, provider := range cfg.Routing.Providers {
		if !routableProvider(provider) {
			return nil, errors.New("unsupported routing provider")
		}
	}
	if err := seedSettings(cfg).validate(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil, errors.New("Pacer is quiesced; create a new instance for reload")
	}
	if p.db != nil {
		if p.cfg.ForwardReportPath != cfg.ForwardReportPath || p.cfg.ForwardReportBook != cfg.ForwardReportBook {
			return nil, errors.New("forward report configuration changes require idle reload")
		}
		if p.cfg.ReportPath != cfg.ReportPath {
			return nil, errors.New("report_path change requires idle reload")
		}
		if !reflect.DeepEqual(p.cfg.CachePolicy, cfg.CachePolicy) {
			return nil, errors.New("cache_policy changes require an idle reload")
		}
		if p.cfg.HistoryPath != cfg.HistoryPath {
			return nil, errors.New("history_path change requires idle reload")
		}
		if p.cfg.StatePath != cfg.StatePath {
			return nil, errors.New("state_path change requires idle reload")
		}
		mode, err := reconcileMode(p.db, cfg.Routing.Mode)
		if err != nil {
			return nil, err
		}
		settings, err := reconcileSettings(p.db, seedSettings(cfg))
		if err != nil {
			return nil, err
		}
		cfg.Routing.Mode = mode
		p.cfg, p.seedCfg = cfg, cfg
		p.applySettingsLocked(settings)
		return registration(p.policy != nil), nil
	}
	forward, err := p.openForward(cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if forward != nil && p.forward != forward {
			forward.close()
		}
	}()
	if err := os.MkdirAll(filepath.Dir(cfg.StatePath), 0700); err != nil {
		return nil, err
	}
	flags := os.O_CREATE | os.O_RDWR
	if cfg.CachePolicy.Enabled {
		flags = os.O_RDWR
	}
	file, err := os.OpenFile(cfg.StatePath, flags, 0600)
	if err != nil {
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", cfg.StatePath+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	var policy *policyadapter.Adapter
	if cfg.CachePolicy.Enabled {
		policy, err = policyadapter.Open(context.Background(), db, policyOpts)
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	if err := openDecisions(db); err != nil {
		db.Close()
		return nil, err
	}
	mode, err := reconcileMode(db, cfg.Routing.Mode)
	if err != nil {
		db.Close()
		return nil, err
	}
	settings, err := reconcileSettings(db, seedSettings(cfg))
	if err != nil {
		db.Close()
		return nil, err
	}
	cfg.Routing.Mode = mode
	chats, err := tracker.Open(db, p.now)
	if err != nil {
		db.Close()
		return nil, err
	}
	observer, err := observation.Open(db, p.now)
	if err != nil {
		db.Close()
		return nil, err
	}
	// Optional marker telemetry does not authorize policy operations and must
	// never turn an evidence-store failure into a policy reset or fake readback.
	// Its compiled namespace must precede the guarded measurement feed boundary.
	p.policyEvidence, err = policyevidence.Open(db, p.now)
	if err != nil {
		p.measurementErrors++
	}
	measure, err := p.openMeasurements(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	p.measurement = measure
	p.measurements = map[string]measurement.Report{}
	if cfg.HistoryPath != "" {
		if _, err = os.Stat(cfg.HistoryPath); err != nil {
			db.Close()
			return nil, errors.New("configured history archive is unavailable; import it offline first")
		}
		h, err := history.Open(cfg.HistoryPath)
		if err != nil {
			db.Close()
			return nil, errors.New("configured history archive is incompatible")
		}
		p.history = h
	}
	if cfg.ReportPath != "" {
		r, err := historyreports.Open(context.Background(), cfg.ReportPath)
		if err != nil {
			db.Close()
			if p.history != nil {
				p.history.Close()
				p.history = nil
			}
			return nil, errors.New("configured frozen report artifact is unavailable; prepare it offline first")
		}
		p.reports = r
		// The usage record file is optional: without it the page shows the
		// imported history only (see usage_live.go).
		if store, err := historyreports.OpenRecordStore(context.Background(), usageRecordPath(cfg.StatePath)); err == nil {
			p.recordStore, p.record = store, historyreports.NewRecord(r, store)
		} else {
			p.measurementErrors++
		}
	}
	p.db, p.chats, p.cfg, p.seedCfg = db, chats, cfg, cfg
	p.applySettingsLocked(settings)
	p.observer = observer
	p.policy = policy
	p.forward = forward
	// Prefix evidence is equally optional: an open failure only counts as a
	// measurement error and leaves the policy path untouched. It opens last, after
	// every field above is assigned, so no earlier error return can leave its
	// database open. It lives in its own SQLite file: adding its tables to the
	// state database would change that database's DDL generation after the guarded
	// measurement feed boundary, which the feed integrity witness (correctly) refuses.
	if p.prefixDB != nil {
		_ = p.prefixDB.Close()
	}
	p.prefixDB, p.prefixEvidence = openPrefixEvidence(cfg.StatePath, p.now)
	if p.prefixEvidence == nil {
		p.measurementErrors++
	}
	// The capacity ledger is optional in the same way and for the same reason.
	if p.capacityDB != nil {
		_ = p.capacityDB.Close()
	}
	p.capacityDB, p.capacityStore = openCapacityEvidence(cfg.StatePath, p.now)
	p.capacityViews = map[string]capacityState{}
	if p.capacityStore == nil {
		p.measurementErrors++
	}
	return registration(p.policy != nil), nil
}

func (p *Plugin) Close() {
	p.mu.Lock()
	if !p.stopped {
		p.stopped = true
		close(p.workerDone)
	}
	p.mu.Unlock()
	p.stopForward(true)
	p.workerWait.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.prefixDB != nil {
		_ = p.prefixDB.Close()
		p.prefixDB, p.prefixEvidence = nil, nil
	}
	if p.capacityDB != nil {
		_ = p.capacityDB.Close()
		p.capacityDB, p.capacityStore = nil, nil
	}
	if p.db != nil {
		_ = p.db.Close()
		p.db = nil
	}
	if p.history != nil {
		_ = p.history.Close()
		p.history = nil
	}
	if p.reports != nil {
		_ = p.reports.Close()
		p.reports = nil
	}
	if p.recordStore != nil {
		_ = p.recordStore.Close()
		p.recordStore, p.record = nil, nil
	}
}

func (p *Plugin) Handle(method string, raw []byte) (any, error) {
	if method == pluginabi.MethodPluginRegister || method == pluginabi.MethodPluginReconfigure {
		p.revokeForwardManagement()
		result, err := p.configure(raw)
		p.revokeForwardManagement()
		if err == nil {
			p.mu.Lock()
			result = registration(p.policy != nil, p.forward != nil)
			p.mu.Unlock()
			p.discover()
			p.startWorker()
			p.startForward()
		}
		return result, err
	}
	if method == pluginabi.MethodPluginShutdown || method == pluginabi.MethodPluginQuiesce {
		p.revokeForwardManagement()
		p.mu.Lock()
		if !p.stopped {
			p.stopped = true
			close(p.workerDone)
		}
		p.mu.Unlock()
		p.stopForward(false)
		return struct{}{}, nil
	}
	// Forward dispatch is capability- and lease-gated before the legacy handler.
	// Handle has no host IO context; the lease still inherits publisher shutdown.
	if method == pluginabi.MethodManagementHandle && forwardManagementRPC(raw) {
		return p.handleForwardManagement(context.Background(), raw), nil
	}
	p.mu.Lock()
	ready := p.db != nil && !p.stopped
	p.mu.Unlock()
	if !ready {
		return nil, errors.New("Pacer state unavailable")
	}
	switch method {
	case pluginabi.MethodCachePolicyHandle, pluginabi.MethodCachePolicyResolve, pluginabi.MethodCachePolicyAttempt:
		if p.policy == nil {
			return nil, pluginabi.NewError("pacer_cache_unavailable", "Managed cache policy is not configured", 503)
		}
		result, err := p.policy.Dispatch(context.Background(), method, raw)
		// Settlement is observed only after a successful dispatch and never
		// changes its result.
		if method == pluginabi.MethodCachePolicyAttempt && err == nil {
			p.settlePrefixAttempt(raw)
		}
		if method == pluginabi.MethodCachePolicyHandle && err == nil && p.policyEvidence != nil {
			var request pluginapi.CachePolicyRequest
			if json.Unmarshal(raw, &request) == nil && request.Method == http.MethodGet {
				if response, ok := result.(pluginapi.CachePolicyResponse); ok && response.StatusCode == http.StatusOK {
					body, evidenceErr := p.policyEvidence.AugmentAuthorized(context.Background(), request.Scope.CallerScope, response.Body)
					if evidenceErr == nil {
						response.Body = body
						result = response
					} else {
						p.metricError(evidenceErr)
					}
				}
			}
		}
		// Runs after the marker readback, starting from its body, with the same gating.
		if method == pluginabi.MethodCachePolicyHandle && err == nil {
			result = p.augmentPrefixReadback(raw, result)
		}
		if method == pluginabi.MethodCachePolicyResolve && err == nil {
			var request struct {
				RequestID string `json:"request_id"`
			}
			_ = json.Unmarshal(raw, &request)
			if selection, ok := result.(pluginapi.CachePolicy); ok {
				p.mu.Lock()
				p.selections[request.RequestID] = selection
				p.mu.Unlock()
			}
		}
		return result, err
	case pluginabi.MethodManagementRegister:
		result := map[string]any{"resources": []map[string]string{{"Path": "/index.html", "Menu": "Pacer", "Description": "Quota-aware routing and usage"}, {"Path": "/events", "Description": "Compatible capability-authenticated usage"}, {"Path": "/cache", "Description": "A chat's own prompt-cache requests, capability-authenticated"}}, "routes": append([]map[string]string{{"Method": "GET", "Path": "/plugins/pacer/state"}, {"Method": "PATCH", "Path": "/plugins/pacer/routing"}, {"Method": "PATCH", "Path": "/plugins/pacer/settings"}, {"Method": "GET", "Path": "/plugins/pacer/observations"}, {"Method": "POST", "Path": "/plugins/pacer/observations/reconcile"}, {"Method": "GET", "Path": "/plugins/pacer/keys"}, {"Method": "PATCH", "Path": "/plugins/pacer/keys"}}, usageRoutes()...)}
		result["routes"] = append(result["routes"].([]map[string]string), p.registerForwardManagement(raw)...)
		return result, nil
	case pluginabi.MethodManagementHandle:
		var req pluginapi.ManagementRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		return p.manage(req), nil
	case pluginabi.MethodRequestInterceptBefore:
		var req pluginapi.RequestInterceptRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		q, err := claim(req.Headers)
		if err != nil {
			return refuse("quota_protocol_invalid", "Invalid quota coordination header"), nil
		}
		caller := text(req.Metadata, "caller_scope")
		if q != nil && caller == "" {
			return refuse("quota_identity_missing", "Quota routing requires authenticated caller identity"), nil
		}
		history := historyKey(req.SourceFormat, req.Body)
		p.mu.Lock()
		p.requests[req.RequestID] = attempt{Claim: q, Caller: caller, Session: session(req.Headers, req.Metadata), History: history, Headers: req.Headers, Client: clientDeclaration(req.Headers)}
		p.mu.Unlock()
		return pluginapi.RequestInterceptResponse{Headers: http.Header{requestHeader: {req.RequestID}}}, nil
	case pluginabi.MethodSchedulerPick:
		var req pluginapi.SchedulerPickRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		return p.pick(req), nil
	case pluginabi.MethodRequestInterceptAfter:
		var req pluginapi.RequestInterceptRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		return p.after(req), nil
	case pluginabi.MethodResponseInterceptAfter:
		var req pluginapi.ResponseInterceptRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		p.observe(req.RequestID, req.Metadata, req.ResponseHeaders, p.now())
		return pluginapi.ResponseInterceptResponse{Headers: p.ack(req.RequestID), ClearHeaders: quotaAckHeaders()}, nil
	case pluginabi.MethodResponseInterceptStreamChunk:
		var req pluginapi.StreamChunkInterceptRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		if req.ChunkIndex == -1 {
			p.observe(req.RequestID, req.Metadata, req.ResponseHeaders, p.now())
			return pluginapi.StreamChunkInterceptResponse{Headers: p.ack(req.RequestID), ClearHeaders: quotaAckHeaders()}, nil
		}
		return pluginapi.StreamChunkInterceptResponse{}, nil
	case pluginabi.MethodRequestComplete:
		if p.policy != nil {
			if _, err := p.policy.Dispatch(context.Background(), method, raw); err != nil {
				return nil, err
			}
		}
		var req pluginapi.RequestCompletion
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
		p.chats.Complete(req.RequestID)
		p.mu.Lock()
		// A request that succeeded ends its chat's transient strikes. Admission
		// alone does not: it precedes the upstream call that may fail again.
		if t := p.requests[req.RequestID]; req.Outcome == pluginapi.RequestCompletionSucceeded && t.Key != "" {
			delete(p.strikes, t.Key)
		}
		delete(p.requests, req.RequestID)
		delete(p.selections, req.RequestID)
		p.mu.Unlock()
		return struct{}{}, nil
	case pluginabi.MethodUsageHandle:
		defer p.WakeForward()
		captured, err := p.recordUsageCaptured(raw)
		if err != nil {
			return nil, pluginabi.NewError("pacer_usage_unavailable", "Native usage could not be durably recorded", 503)
		}
		var req struct {
			pluginapi.UsageRecord
			ResponseHeadersObservedAt map[string]time.Time
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			p.coverageFailure("usage_envelope_invalid", captured.Account)
			return nil, pluginabi.NewError("pacer_usage_unavailable", "Native usage envelope is invalid", 503)
		}
		if len(req.ResponseHeadersObservedAt) > 0 {
			p.observeUsageCaptured(captured, req.ResponseHeaders, req.ResponseHeadersObservedAt, req.RequestedAt)
			return struct{}{}, nil
		}
		// Older hosts do not supply the response-capture clock. Absolute headers
		// still carry useful evidence; strip countdowns rather than re-anchor a
		// delayed stream's relative reset to callback time.
		h := req.ResponseHeaders.Clone()
		for key := range h {
			if strings.HasSuffix(strings.ToLower(key), "-reset-after-seconds") {
				delete(h, key)
			}
		}
		p.observeUsageCaptured(captured, h, nil, req.RequestedAt)
		return struct{}{}, nil
	}
	return nil, fmt.Errorf("unsupported Pacer method %s", method)
}

// pick places a chat by 7-day pace (docs/pace-routing.md) when routing is on.
// Primary requests and compactions are routed; other auxiliaries are left to
// CPA. Warm chats stay on their account; a compaction never rebinds the chat.
// When routing is off, CPA places chats, but a bound chat stays on its account.
// A chat is known by its claim or session, else by its history (historyKey),
// and a new name with a warm history stays where that history is warm.
func (p *Plugin) pick(req pluginapi.SchedulerPickRequest) pluginapi.SchedulerPickResponse {
	p.mu.Lock()
	cfg := p.cfg
	id := header(http.Header(req.Options.Headers), requestHeader)
	t, tracked := p.requests[id]
	selection, managed := p.selections[id]
	// Every scheduler retry supersedes the previous proposal, including fallbacks.
	t.RoutingChoice, t.Decision = pluginapi.SchedulerPickResponse{}, routing.Decision{}
	if tracked {
		p.requests[id] = t
	}
	p.mu.Unlock()
	provider := req.Provider
	for _, candidate := range req.Candidates {
		if provider == "" {
			provider = candidate.Provider
		}
		if candidate.Provider != provider {
			return pluginapi.SchedulerPickResponse{}
		}
	}
	enabled := false
	for _, v := range cfg.Routing.Providers {
		if v == provider {
			enabled = true
		}
	}
	kind := "primary"
	if t.Claim != nil {
		kind = t.Claim.Kind
	}
	if !cfg.Enabled || !enabled || !tracked {
		return pluginapi.SchedulerPickResponse{}
	}
	if cfg.Routing.Mode != "on" {
		return p.pinToChat(id, t, req, provider, routing.KeepAccount)
	}
	// Other auxiliaries, and whatever a managed cache policy does not select as
	// primary, are never placed by pace; on the chat's model they stay with it.
	if (kind != "primary" && kind != "compaction") || (managed && selection.Kind != "primary") {
		return p.pinToChat(id, t, req, provider, routing.WarmAuxiliary)
	}
	p.mu.Lock()
	unknown := false
	for _, candidate := range req.Candidates {
		_, _, known := p.routingCredLocked(candidate.ID)
		unknown = unknown || !known
	}
	p.mu.Unlock()
	if unknown {
		// As in pinToChat: CPA may finish loading credentials after plugin
		// registration. Routing without them leaves the chat to CPA, whose
		// affinity a restart cleared, and a warm chat would be moved.
		p.discover(true)
	}
	key, history := t.keys(provider, req.Model)
	if key == "" {
		return pluginapi.SchedulerPickResponse{}
	}
	b, active, err := p.chats.Get(key)
	if err != nil {
		return pluginapi.SchedulerPickResponse{}
	}
	if b.Key == "" && history != key {
		// A chat new by its name but with a history warm on an account (a fork,
		// a client restart) stays where that cache is.
		if b, active, err = p.rescueByHistory(key, history, provider, req.Model, t.Claim); err != nil {
			return pluginapi.SchedulerPickResponse{}
		}
	}
	if kind == "compaction" && b.Key == "" {
		return pluginapi.SchedulerPickResponse{}
	}
	if b.Key == "" && p.Host != nil && t.Session != "" {
		raw, lookupErr := p.Host("host.affinity.lookup", pluginapi.HostAffinityLookupRequest{Provider: provider, Model: req.Model, SessionID: t.Session})
		var affinity pluginapi.HostAffinityLookupResponse
		// CPA answers "unsupported" whenever a plugin scheduler (Pacer) is
		// registered: it reports no binding for Pacer to adopt, so the chat is
		// new to Pacer and placed by pace like any other.
		if lookupErr != nil || json.Unmarshal(raw, &affinity) != nil || affinity.Status == "ambiguous" {
			return pluginapi.SchedulerPickResponse{}
		}
		if affinity.Status == "bound" {
			p.mu.Lock()
			for _, c := range p.creds {
				if c.Index == affinity.AuthIndex {
					err = p.chats.Seed(key, tracker.Selection{Auth: c.ID, Account: c.Account, Alias: c.Alias, Provider: provider, Model: req.Model}, "")
					break
				}
			}
			p.mu.Unlock()
			if err != nil {
				return pluginapi.SchedulerPickResponse{}
			}
			b, active, err = p.chats.Get(key)
			if err != nil || b.Key == "" {
				return pluginapi.SchedulerPickResponse{}
			}
		}
	}
	now := p.now()
	p.mu.Lock()
	settings := p.settings
	p.mu.Unlock()
	chat := routing.Chat{Account: b.Account, Warm: warm(b, active, now, settings.warmFor())}
	switch {
	case kind == "compaction":
		// The prompt cache is per model: a compaction on the chat's own model
		// reads the warm cache; one on another model is placed like a cold chat.
		chat.Warm = req.Model == b.Model
	case t.Claim != nil && b.Key != "" && t.Claim.Checkpoint != b.Consumed:
		// The first request after a compaction starts from a new checkpoint.
		chat.Warm = false
	}
	ids := make([]string, 0, len(req.Candidates))
	for _, candidate := range req.Candidates {
		ids = append(ids, candidate.ID)
	}
	p.mu.Lock()
	input := routingPreviewInput{At: now, Provider: provider, Model: req.Model, Candidates: ids, Selection: selection, Managed: managed}
	if kind == "primary" {
		p.rememberPreviewLocked(key, input)
	}
	accounts, candidates := p.routingInputsLocked(input, now, b.Account)
	p.mu.Unlock()
	d := routing.Pick(settings.routing(), now, chat, accounts)
	p.mu.Lock()
	t.Key = key
	t.Decision = d
	t.Model, t.PreviousAccount = req.Model, b.Account
	t.RoutingProvider = provider
	hold := d.Unoffered && p.holdLocked(key, now, settings.TransientRetries)
	if hold {
		t.Decision.Handled, t.Decision.Reason, t.Decision.Auth, t.Decision.Account = false, TransientWait, "", ""
		t.RoutingChoice = pluginapi.SchedulerPickResponse{Handled: true, Reject: true, RejectCode: "auth_unavailable", RejectReason: "Pacer: this chat's account failed briefly; retry to keep its warm cache"}
	} else {
		t.RoutingChoice = routingAction(req, d, candidates)
	}
	p.requests[id] = t
	p.mu.Unlock()
	if hold {
		p.recordDecision(id, t, credential{Provider: provider}, TransientWait)
	}
	return t.RoutingChoice
}

// rescueByHistory seeds a chat that is new by its key with the account where its
// history is warm, and returns the chat's binding: still empty without a warm
// history on this provider and model. The same history start is the same
// prefix, so the chat continues from its claimed checkpoint, warm.
func (p *Plugin) rescueByHistory(key, history, provider, model string, q *tracker.Claim) (tracker.Binding, bool, error) {
	if history == "" {
		return tracker.Binding{}, false, nil
	}
	h, active, err := p.chats.Get(history)
	if err != nil {
		return tracker.Binding{}, false, err
	}
	p.mu.Lock()
	warmFor := p.settings.warmFor()
	p.mu.Unlock()
	if h.Key == "" || h.Provider != provider || h.Model != model || !warm(h, active, p.now(), warmFor) {
		return tracker.Binding{}, false, nil
	}
	checkpoint := ""
	if q != nil {
		checkpoint = q.Checkpoint
	}
	if err = p.chats.Seed(key, tracker.Selection{Auth: h.Auth, Account: h.Account, Alias: h.Alias, Provider: provider, Model: model}, checkpoint); err != nil {
		return tracker.Binding{}, false, err
	}
	return p.chats.Get(key)
}

// pinToChat keeps a request on the chat's model on the chat's account, where
// it reads and renews the chat's prompt cache. With routing on it holds the
// auxiliaries (a warming ping, a generation from the chat's context,
// WarmAuxiliary): left to CPA, they would follow CPA's own session affinity,
// which does not know where Pacer put the chat, and warm another account. With
// routing off it holds every request of the chat (KeepAccount): CPA's affinity
// lives in memory only, and after a CPA restart it would move warm chats.
// It never places or rebinds a chat: without a binding on this model, or when
// CPA does not offer the chat's account, CPA decides.
func (p *Plugin) pinToChat(id string, t attempt, req pluginapi.SchedulerPickRequest, provider, reason string) pluginapi.SchedulerPickResponse {
	key, _ := t.keys(provider, req.Model)
	if key == "" {
		return pluginapi.SchedulerPickResponse{}
	}
	b, _, err := p.chats.Get(key)
	if err != nil || b.Key == "" || b.Provider != provider || b.Model != req.Model {
		return pluginapi.SchedulerPickResponse{}
	}
	auth, candidates, unknown := p.chatCredential(req, b)
	if unknown {
		// CPA may finish loading credentials after plugin registration (after()
		// refreshes too). Left to CPA, whose affinity a restart cleared, the
		// chat would move to another account.
		p.discover(true)
		auth, candidates, _ = p.chatCredential(req, b)
	}
	d := routing.Decision{Handled: auth != "", Auth: auth, Account: b.Account, Reason: reason}
	choice := routingAction(req, d, candidates)
	if !choice.Handled {
		return pluginapi.SchedulerPickResponse{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	t.Key, t.Decision, t.RoutingChoice = key, d, choice
	t.Model, t.PreviousAccount, t.RoutingProvider = req.Model, b.Account, provider
	p.requests[id] = t
	return choice
}

// chatCredential returns the chat's own credential among the offered ones,
// else an offered alias of its account, the offered credentials Pacer knows,
// and whether any offered ID is missing from its inventory.
func (p *Plugin) chatCredential(req pluginapi.SchedulerPickRequest, b tracker.Binding) (string, []credential, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	auth, unknown := "", false
	candidates := make([]credential, 0, len(req.Candidates))
	for _, candidate := range req.Candidates {
		c, _, ok := p.routingCredLocked(candidate.ID)
		if !ok {
			unknown = true
			continue
		}
		candidates = append(candidates, c)
		if c.Account == b.Account && (auth == "" || c.ID == b.Auth || (auth != b.Auth && c.ID < auth)) {
			auth = c.ID
		}
	}
	return auth, candidates, unknown
}

// TransientWait: a warm chat's account was left out although its meters can
// serve; the request ends with a retryable error instead of moving the chat
// (CPA answers with the upstream error that left it out, else 503).
const TransientWait = "transient_wait"

type strike struct {
	n  int
	at time.Time
}

// holdLocked counts one refusal for the chat while it is under the retry
// setting. Strikes expire transientWindow after the first, and end when a
// request of the chat succeeds.
func (p *Plugin) holdLocked(key string, now time.Time, limit int) bool {
	if limit <= 0 {
		return false
	}
	s := p.strikes[key]
	if s.n == 0 || now.Sub(s.at) > transientWindow {
		s = strike{at: now}
	}
	if s.n >= limit {
		return false
	}
	s.n++
	if len(p.strikes) >= 4096 {
		for k, v := range p.strikes {
			if now.Sub(v.at) > transientWindow {
				delete(p.strikes, k)
			}
		}
	}
	p.strikes[key] = s
	return true
}

func (p *Plugin) after(req pluginapi.RequestInterceptRequest) pluginapi.RequestInterceptResponse {
	auth := text(req.Metadata, "selected_auth_id")
	p.mu.Lock()
	t, ok := p.requests[req.RequestID]
	c, usageOnly, known := p.routingCredLocked(auth)
	gate := p.cfg.CompactOnSwitch
	selectedPolicy, managed := p.selections[req.RequestID]
	managedAux := managed && selectedPolicy.Kind != "primary"
	p.mu.Unlock()
	if !known && auth != "" {
		// CPA may finish loading credentials after plugin registration. Refresh
		// its local inventory on a newly selected ID; never call analytics here.
		p.discover(true)
		p.mu.Lock()
		c, usageOnly, known = p.routingCredLocked(auth)
		p.mu.Unlock()
	}
	if ok && !known && (managed || t.Claim != nil) {
		return refuse("quota_state_unavailable", "Selected credential identity could not be verified")
	}
	if ok && t.Client.Quota && auth != "" {
		// The reply carries this credential's quota (X-Pacer-Quota).
		if served, found := p.servedCredential(auth); found {
			t.Served, t.ServedModel = served, req.Model
			p.mu.Lock()
			if current, exists := p.requests[req.RequestID]; exists {
				current.Served, current.ServedModel = served, req.Model
				p.requests[req.RequestID] = current
			}
			p.mu.Unlock()
		}
	}
	if ok && known {
		// A routed usage-only provider's usage is recorded on its own path.
		if !usageOnly {
			if err := p.captureUsageCredential(req.RequestID, c); err != nil {
				return refuse("quota_state_unavailable", "Pacer usage attribution could not be recorded")
			}
		}
		// For an alias, CPA's pick carries the alias and this hook the upstream
		// model, with the alias as the requested one: the chat is bound under the
		// name it was routed on. Any other model change stays a deviation.
		model := req.Model
		if t.Model != "" && req.RequestedModel == t.Model {
			model = t.Model
		}
		key, history := t.keys(c.Provider, model)
		kind := "primary"
		if t.Claim != nil {
			kind = t.Claim.Kind
		}
		if history != key && !managedAux && (kind == "primary" || kind == "generate") {
			// The history follows its chat's account, so the same history under a
			// new name finds that account (rescueByHistory). Advisory: a failed
			// write never refuses the request.
			if err := p.chats.Follow(history, selectionIntent(t, c, model)); err != nil {
				p.metricError(err)
			}
		}
		if key != "" && !managedAux {
			b, err := p.chats.Admit(req.RequestID, key, selectionIntent(t, c, model), t.Claim, gate)
			if err != nil {
				if e, yes := err.(*tracker.Refusal); yes {
					p.recordDecision(req.RequestID, t, c, e.Code)
					if e.Code == compactRequired {
						p.mu.Lock()
						models := append([]string{}, p.settings.CompactionModels...)
						p.mu.Unlock()
						return refuseCompact(e.Message, models)
					}
					return refuse(e.Code, e.Message)
				}
				return refuse("quota_state_unavailable", "Pacer binding state is unavailable")
			}
			p.mu.Lock()
			t.Key, t.Auth, t.Binding = key, auth, b
			p.requests[req.RequestID] = t
			p.mu.Unlock()
			p.recordDecision(req.RequestID, t, c, "admitted")
		}
	}
	clear := []string{controlHeader, requestHeader, clientHeader, "X-Quota-Binding", "X-Quota-Protocol"}
	for key := range req.Headers {
		if strings.HasPrefix(strings.ToLower(key), "x-quota-") {
			clear = append(clear, key)
		}
	}
	return pluginapi.RequestInterceptResponse{ClearHeaders: clear}
}

func (p *Plugin) ack(id string) http.Header {
	p.mu.Lock()
	t, observer := p.requests[id], p.observer
	p.mu.Unlock()
	h := handoffAck(t)
	if t.Client.Quota && t.Served.ID != "" && observer != nil {
		if v, ok := p.quotaReplyValue(observer, t.Served, t.ServedModel); ok {
			if h == nil {
				h = http.Header{}
			}
			h.Set(quotaReplyHeader, v)
		}
	}
	return h
}

// handoffAck acknowledges an X-Quota-Handoff claim: the binding, or the
// helper request a first auxiliary made.
func handoffAck(t attempt) http.Header {
	if t.Claim == nil || t.Key == "" || t.Auth == "" {
		return nil
	}
	if t.Binding.Key == "" {
		// First auxiliaries on a separate provider route have no primary binding
		// to acknowledge. Admit has authenticated their claim without creating
		// one. Prove only this helper request; never invent a generation/alias.
		switch t.Claim.Kind {
		case "compaction", "title", "generate":
			raw, _ := json.Marshal(map[string]any{"protocol": 1, "kind": t.Claim.Kind, "request": t.Claim.Request})
			return http.Header{"X-Quota-Protocol": {"1"}, "X-Quota-Auxiliary": {base64.RawURLEncoding.EncodeToString(raw)}}
		}
		return nil
	}
	raw, _ := json.Marshal(map[string]any{"protocol": 1, "generation": t.Binding.Generation, "alias": t.Binding.Alias})
	return http.Header{"X-Quota-Protocol": {"1"}, "X-Quota-Binding": {base64.RawURLEncoding.EncodeToString(raw)}}
}

// Upstream responses cannot supply coordinator acknowledgements. The host
// removes these before applying the exact headers from this admitted attempt.
func quotaAckHeaders() []string {
	return []string{"X-Quota-Protocol", "X-Quota-Binding", "X-Quota-Auxiliary", quotaReplyHeader}
}

func refuse(code, message string) pluginapi.RequestInterceptResponse {
	return refusal(map[string]any{"code": code, "message": message, "type": "quota_handoff"})
}

const compactRequired = "quota_compact_required"

// refuseCompact asks the client to compact before the move. compaction_models
// (CPA model names, in order) write the summary; absent, the client decides.
func refuseCompact(message string, models []string) pluginapi.RequestInterceptResponse {
	body := map[string]any{"code": compactRequired, "message": message, "type": "quota_handoff"}
	if len(models) > 0 {
		body["compaction_models"] = models
	}
	return refusal(body)
}

func refusal(body map[string]any) pluginapi.RequestInterceptResponse {
	raw, _ := json.Marshal(map[string]any{"error": body})
	return pluginapi.RequestInterceptResponse{Terminate: true, StatusCode: 409, ResponseHeaders: http.Header{"Content-Type": {"application/json"}, "X-Quota-Protocol": {"1"}}, ResponseBody: raw}
}

func resource(status int, body any) pluginapi.ManagementResponse {
	raw, _ := json.Marshal(body)
	return pluginapi.ManagementResponse{StatusCode: status, Headers: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}}, Body: raw}
}

func (p *Plugin) manage(req pluginapi.ManagementRequest) pluginapi.ManagementResponse {
	if strings.HasPrefix(req.Path, usageRoot) {
		return p.usageAPI(req)
	}
	if strings.HasPrefix(req.Path, modelsRoot) {
		return p.modelsAPI(req)
	}
	if req.Method == "PATCH" && req.Path == "/v0/management/plugins/pacer/keys" {
		return p.editKeys(req)
	}
	if req.Method == "GET" && req.Path == "/v0/management/plugins/pacer/keys" {
		return p.keysView()
	}
	if (req.Method == "GET" && req.Path == "/v0/management/plugins/pacer/observations") || (req.Method == "POST" && req.Path == "/v0/management/plugins/pacer/observations/reconcile") {
		return p.quotaAPI(req)
	}
	if req.Method == "GET" && req.Path == "/v0/resource/plugins/pacer/index.html" {
		return pluginapi.ManagementResponse{StatusCode: 200, Headers: http.Header{"Content-Type": {"text/html; charset=utf-8"}, "Cache-Control": {"no-store"}, "Content-Security-Policy": {"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'self'; base-uri 'none'; form-action 'self'"}}, Body: management.Page}
	}
	if req.Method == "GET" && req.Path == "/v0/resource/plugins/pacer/events" {
		return p.events(req)
	}
	if req.Method == "GET" && req.Path == chatCachePath {
		return p.chatCache(req)
	}
	// Exact management paths only: the public resource namespace cannot reach
	// these branches. CPA authenticates management routes before ABI dispatch.
	if req.Method == "GET" && req.Path == "/v0/management/plugins/pacer/state" {
		p.discover() // local inventory only, bounded separately from quota reconciliation
		p.mu.Lock()
		defer p.mu.Unlock()
		accounts := []map[string]any{}
		decisions, err := p.decisionView()
		if err != nil {
			return resource(503, map[string]string{"error": "decision_state_unavailable"})
		}
		for _, c := range p.creds {
			windows, err := p.observer.ReadWindows(c.Account)
			if err != nil {
				return resource(503, map[string]string{"error": "observation_unavailable"})
			}
			views := []map[string]any{}
			for _, w := range windows {
				state := windowState(w, p.now())
				views = append(views, map[string]any{"scope": w.Scope, "seconds": w.Seconds, "remaining": w.Remaining, "reset": w.Reset, "state": state, "source": w.Usage.Source, "usage_evidence": w.Usage, "reset_evidence": w.ResetEvidence})
			}
			job, _, err := p.observer.Status(c.Account)
			if err != nil {
				return resource(503, map[string]string{"error": "observation_unavailable"})
			}
			row := map[string]any{"id": c.ID, "auth_index": c.Index, "alias": c.Alias, "provider": c.Provider, "state": accountState(c.Provider, windows, p.now()), "windows": views, "reconciliation": map[string]any{"next": job.Next, "blocked": job.Blocked}}
			row["pace_unit"] = "%/h"
			if report, ok := p.measurements[c.Account]; ok {
				row["measurement"] = map[string]any{"as_of": report.AsOf, "five_hour": report.FiveHour, "seven_day": report.SevenDay, "warm_burn": report.WarmBurn, "coverage": report.Coverage}
			}
			row["account"] = c.Account
			row["paused"] = p.settings.paused(c.Account)
			if pace, ok := routing.Pace(p.settings.routing(), p.now(), routing.Account{Auth: c.ID, Account: c.Account, Budgets: routingBudgets(c.Provider, "", windows, p.now())}); ok {
				row["pace"] = pace
			}
			// Additive readback only: a lower bound never reaches routing.
			row["capacity"] = p.capacityView(c)
			accounts = append(accounts, row)
		}
		sort.Slice(accounts, func(i, j int) bool { return fmt.Sprint(accounts[i]["id"]) < fmt.Sprint(accounts[j]["id"]) })
		historyCoverage := "No prepared native historical report artifact is configured. Keep CPAMP and its database available."
		if p.reports != nil {
			pin := p.reports.DefaultPin()
			historyCoverage = fmt.Sprintf("Frozen reports at %s, event checkpoint %d. Native future usage is separate. Legacy views, real-data migration and retirement gates remain unverified.", time.UnixMilli(pin.At).UTC().Format(time.RFC3339), pin.Through)
		}
		return resource(200, map[string]any{"history_available": p.reports != nil, "history_coverage": historyCoverage, "routing": map[string]any{"mode": p.cfg.Routing.Mode, "modes": []string{"off", "on"}, "providers": p.cfg.Routing.Providers}, "settings": p.settingsViewLocked(), "accounts": accounts, "decisions": decisions, "decision_write_errors": p.decisionErrors, "measurement": map[string]any{"delivery": "native-acknowledged-scoped", "coverage": "No account size is ever known. accounts[].capacity holds a measured lower bound only where a quiescent sample pair proves membership and carry-in; otherwise it is unknown with reasons.", "errors": p.measurementErrors, "capacity_known": false, "source_checkpoint": map[string]any{"calls": p.coverageStats.Calls, "errors": p.coverageStats.Errors, "stale": p.coverageStats.Stale, "reason": p.coverageStats.Reason, "domain": "request_start_only", "account_membership_complete": false, "physical_carry_in_proven": false}}})
	}
	if req.Method == "PATCH" && req.Path == "/v0/management/plugins/pacer/settings" {
		p.mu.Lock()
		current := p.settings
		p.mu.Unlock()
		next, err := decodeSettingsPatch(req.Body, current)
		if err != nil {
			return resource(400, map[string]string{"error": "Settings not saved: " + err.Error(), "code": "settings_invalid"})
		}
		if err := p.setSettings(next); err != nil {
			return resource(503, map[string]string{"error": "settings_unavailable"})
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		return resource(200, p.settingsViewLocked())
	}
	if req.Method == "PATCH" && req.Path == "/v0/management/plugins/pacer/routing" {
		if len(req.Body) > 1024 {
			return resource(413, map[string]string{"error": "body_too_large"})
		}
		var body struct {
			Mode string `json:"mode"`
		}
		decoder := json.NewDecoder(bytes.NewReader(req.Body))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF || (body.Mode != "off" && body.Mode != "on") {
			return resource(400, map[string]string{"error": "routing_mode_invalid", "message": "Routing mode is off or on"})
		}
		if err := p.setMode(body.Mode); err != nil {
			return resource(503, map[string]string{"error": "routing_state_unavailable"})
		}
		return resource(200, map[string]string{"mode": body.Mode})
	}
	return resource(404, map[string]string{"error": "not_found"})
}
