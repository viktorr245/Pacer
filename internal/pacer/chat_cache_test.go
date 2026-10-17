package pacer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/floris3456/cliproxyapi-quota-handoff/internal/measurement"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type chatCacheResponse struct {
	Requests []chatCacheRequest `json:"requests"`
	More     bool               `json:"more"`
}

func readChatCache(t *testing.T, f *routingFixture, capability string, query url.Values) (int, chatCacheResponse) {
	t.Helper()
	response := f.p.chatCache(pluginapi.ManagementRequest{Method: "GET", Path: chatCachePath, Headers: http.Header{"Authorization": {"Bearer " + capability}}, Query: query})
	var value chatCacheResponse
	if response.StatusCode == 200 && json.Unmarshal(response.Body, &value) != nil {
		t.Fatal(string(response.Body))
	}
	return response.StatusCode, value
}

func putChatRecord(t *testing.T, f *routingFixture, id, caller, session string, started time.Time, read int64, systemTag string) {
	t.Helper()
	tag := func(c string) string { return strings.Repeat(c, 64) }
	r := measurement.Record{
		EventID: "event-" + id, SourceID: "source", AttemptID: id, Revision: 1, Kind: "other",
		Attribution:    measurement.Attribution{Provider: "claude", AccountID: f.p.creds["a"].Account, CallerID: caller, SessionID: session, Model: "claude-opus-5-5"},
		StartedAt:      started,
		StartTimeBasis: "requested_at_unverified",
		EndedAt:        started.Add(time.Second),
		Final:          true,
		Success:        true,
		Usage:          measurement.Usage{NonCacheInput: 2, CacheRead: read, CacheCreate: 100},
		Native: &measurement.NativeMetadata{PrefixSnapshot: &measurement.PrefixSnapshot{Version: 1, KeyID: "0123456789abcdef", SettingsTag: tag("5"), Shape: "unsupported", Endpoints: []measurement.PrefixEndpoint{
			{Ordinal: 0, Section: "tools", TTL: "5m", Tag: tag("a")},
			{Ordinal: 1, Section: "system", TTL: "5m", Tag: systemTag},
		}}},
	}
	if _, err := f.p.measurement.PutRecord(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}

func TestChatCacheShowsOnlyThisChatsRequestsNewestFirst(t *testing.T) {
	f := previewFixture(t)
	now := f.p.now()
	query := url.Values{"installation": {f.q.Installation}, "session": {f.q.Session}, "route": {f.q.Route}}
	putChatRecord(t, f, "old", "fixture-caller", "affinity:"+f.q.Session, now.Add(-2*time.Minute), 4000, strings.Repeat("b", 64))
	putChatRecord(t, f, "new", "fixture-caller", "affinity:"+f.q.Session, now.Add(-time.Minute), 5000, strings.Repeat("c", 64))
	// The same session ID under another inference key, and another session of this caller.
	putChatRecord(t, f, "other-caller", "another-caller", "affinity:"+f.q.Session, now.Add(-time.Minute), 1, strings.Repeat("d", 64))
	putChatRecord(t, f, "other-session", "fixture-caller", "affinity:another-chat", now.Add(-time.Minute), 1, strings.Repeat("e", 64))

	status, value := readChatCache(t, f, previewCapability, query)
	if status != 200 || len(value.Requests) != 2 || value.More {
		t.Fatal(status, value)
	}
	newest, oldest := value.Requests[0], value.Requests[1]
	if newest.Read != 5000 || oldest.Read != 4000 || newest.Written != 100 || newest.Uncached != 2 || newest.Model != "claude-opus-5-5" || !newest.Success {
		t.Fatal(value.Requests)
	}
	// Tags are cut short and still tell the changed system prompt apart.
	if p := newest.Prefix; p == nil || p.Key != "0123456789abcdef" || len(p.Markers) != 2 || p.Markers[0].Tag != strings.Repeat("a", 16) || p.Markers[1].Tag == oldest.Prefix.Markers[1].Tag || p.Markers[1].Section != "system" {
		t.Fatal(newest.Prefix, oldest.Prefix)
	}

	// Pages to older requests.
	status, page := readChatCache(t, f, previewCapability, url.Values{"installation": query["installation"], "session": query["session"], "route": query["route"], "limit": {"1"}})
	if status != 200 || len(page.Requests) != 1 || page.Requests[0].Read != 5000 || !page.More {
		t.Fatal(status, page)
	}
	status, page = readChatCache(t, f, previewCapability, url.Values{"installation": query["installation"], "session": query["session"], "route": query["route"], "limit": {"1"}, "before": {page.Requests[0].StartedAt.Format(time.RFC3339Nano)}})
	if status != 200 || len(page.Requests) != 1 || page.Requests[0].Read != 4000 || page.More {
		t.Fatal(status, page)
	}
}

func TestChatCacheNeedsTheChatsCapabilityAndIdentity(t *testing.T) {
	f := previewFixture(t)
	putChatRecord(t, f, "one", "fixture-caller", "affinity:"+f.q.Session, f.p.now().Add(-time.Minute), 4000, strings.Repeat("b", 64))
	query := url.Values{"installation": {f.q.Installation}, "session": {f.q.Session}, "route": {f.q.Route}}

	if status, _ := readChatCache(t, f, strings.Repeat("x", 48), query); status != 401 {
		t.Fatal("unknown capability", status)
	}
	// The capability is right, but these identifiers do not rebuild its chat.
	status, value := readChatCache(t, f, previewCapability, url.Values{"installation": {"someone-else"}, "session": query["session"], "route": query["route"]})
	if status != 200 || len(value.Requests) != 0 {
		t.Fatal("foreign installation", status, value)
	}
	for _, bad := range []url.Values{{"session": query["session"], "route": query["route"]}, {"installation": query["installation"], "session": query["session"], "route": query["route"], "limit": {"501"}}} {
		if status, _ := readChatCache(t, f, previewCapability, bad); status != 400 {
			t.Fatal("invalid query", bad, status)
		}
	}
}
