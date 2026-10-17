package pacer

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/floris3456/cliproxyapi-quota-handoff/internal/measurement"
	"github.com/floris3456/cliproxyapi-quota-handoff/internal/tracker"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// The cache resource shows one chat its own requests as CPA sent them to the
// provider: what each read from and wrote to the prompt cache, and CPA's
// fingerprint of each cache marker. A client compares two requests to tell a
// prompt that changed after it left the client from one the provider lost.
//
// It authenticates like the events resource, with the chat's capability. A
// record is returned only when its caller, with the queried installation,
// session and route, rebuilds the key of the chat that capability belongs to.
// Tags are CPA's content-free HMACs; they compare only within one key ID,
// which a CPA restart rotates.

const chatCachePath = "/v0/resource/plugins/pacer/cache"

const (
	chatCacheLimit    = 100
	chatCacheMaxLimit = 500
	// Enough to tell tags apart; the rest of CPA's HMAC stays in the store.
	chatCacheTagLength = 16
	chatCacheTimeout   = 10 * time.Second
)

type chatCacheRequest struct {
	StartedAt time.Time        `json:"started_at"`
	Kind      string           `json:"kind"`
	Model     string           `json:"model"`
	Account   string           `json:"account,omitempty"`
	Success   bool             `json:"success"`
	Uncached  int64            `json:"uncached"`
	Read      int64            `json:"read"`
	Written   int64            `json:"written"`
	Prefix    *chatCachePrefix `json:"prefix,omitempty"`
}

type chatCachePrefix struct {
	Key      string            `json:"key"`
	Settings string            `json:"settings"`
	Markers  []chatCacheMarker `json:"markers"`
}

type chatCacheMarker struct {
	Section string `json:"section"`
	TTL     string `json:"ttl"`
	Tag     string `json:"tag"`
}

func (p *Plugin) chatCache(req pluginapi.ManagementRequest) pluginapi.ManagementResponse {
	authorization := req.Headers.Get("Authorization")
	token := strings.TrimPrefix(authorization, "Bearer ")
	if token == authorization || len(token) < 40 || len(token) > 128 {
		return resource(401, map[string]string{"error": "unauthorized"})
	}
	chat := tracker.Claim{Installation: req.Query.Get("installation"), Session: req.Query.Get("session"), Route: req.Query.Get("route")}
	limit, before, ok := chatCachePage(req, p.now())
	if !ok || !chatCacheID(chat.Installation) || !chatCacheID(chat.Session) || !chatCacheID(chat.Route) {
		return resource(400, map[string]string{"error": "invalid_cache_query"})
	}
	binding, err := p.chats.ByCapability(token, chat.Route)
	if err != nil || binding.Provider == "" {
		return resource(401, map[string]string{"error": "unauthorized"})
	}
	p.mu.Lock()
	store := p.measurement
	aliases := map[string]string{}
	for _, c := range p.creds {
		if c.Alias != "" {
			aliases[c.Account] = c.Alias
		}
	}
	p.mu.Unlock()
	if store == nil {
		return resource(503, map[string]string{"error": "cache_records_unavailable"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), chatCacheTimeout)
	defer cancel()
	records, more, err := store.FindRecords(ctx, binding.Provider, chat.Session, before, limit, func(r measurement.Record) bool {
		return sameSession(r.Attribution.SessionID, chat.Session) && tracker.Key(r.Attribution.CallerID, binding.Provider, chat) == binding.Key
	})
	if err != nil {
		return resource(503, map[string]string{"error": "cache_records_unavailable"})
	}
	requests := make([]chatCacheRequest, 0, len(records))
	for _, r := range records {
		requests = append(requests, chatCacheView(r, aliases[r.Attribution.AccountID]))
	}
	return resource(200, map[string]any{"protocol": 1, "requests": requests, "more": more})
}

// chatCachePage reads `limit` and `before`, an RFC 3339 time that pages to
// older requests; absent, the newest ones are returned.
func chatCachePage(req pluginapi.ManagementRequest, now time.Time) (int, time.Time, bool) {
	limit, before := chatCacheLimit, now
	if raw := req.Query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > chatCacheMaxLimit {
			return 0, time.Time{}, false
		}
		limit = n
	}
	if raw := req.Query.Get("before"); raw != "" {
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return 0, time.Time{}, false
		}
		before = t
	}
	return limit, before, true
}

func chatCacheID(s string) bool {
	return s != "" && len(s) <= 256 && !strings.ContainsAny(s, "\r\n\x00")
}

// sameSession matches CPA's session ID, which names where CPA read the
// client's ID ("affinity:<id>"), with the client's own.
func sameSession(recorded, session string) bool {
	_, id, found := strings.Cut(recorded, ":")
	return recorded == session || found && id == session
}

func chatCacheView(r measurement.Record, alias string) chatCacheRequest {
	view := chatCacheRequest{
		StartedAt: r.StartedAt,
		Kind:      r.Kind,
		Model:     r.Attribution.Model,
		Account:   alias,
		Success:   r.Success,
		Uncached:  r.Usage.NonCacheInput,
		Read:      r.Usage.CacheRead,
		Written:   r.Usage.CacheCreate,
	}
	if r.Native != nil && r.Native.PrefixSnapshot != nil {
		s := r.Native.PrefixSnapshot
		prefix := &chatCachePrefix{Key: s.KeyID, Settings: shortTag(s.SettingsTag), Markers: make([]chatCacheMarker, 0, len(s.Endpoints))}
		for _, e := range s.Endpoints {
			prefix.Markers = append(prefix.Markers, chatCacheMarker{Section: e.Section, TTL: e.TTL, Tag: shortTag(e.Tag)})
		}
		view.Prefix = prefix
	}
	return view
}

func shortTag(tag string) string {
	if len(tag) > chatCacheTagLength {
		return tag[:chatCacheTagLength]
	}
	return tag
}
