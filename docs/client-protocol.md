# Pacer and its clients

Pacer works with any client. A client that knows Pacer (the opencode plugin, on stock opencode, opencode V2 or
OpenCodePlus) declares on each request what it handles, and Pacer only asks a client for what it declared. A client
without the plugin gets CPA's ordinary behaviour: when its chat moves to another account it simply continues there
and the new account builds its cache from scratch.

## Discovery

`GET /v0/resource/plugins/pacer/events` without `Authorization` answers, among the older fields:

```json
{"protocol":1,"pacer":true,
 "client":{"version":1,"request_header":"X-Pacer-Client","caps":["quota"],
            "quota_header":"X-Pacer-Quota","views":["usage","prices","usage-watch"]}}
```

Behind an SSO proxy, this exact GET path has to reach CPA without SSO, like the older quota-handoff path
([caddy.md](caddy.md)).

## Declaring a client: `X-Pacer-Client`

Sent on each model request, never per API key (one key is often shared by clients with and without the plugin):

```text
X-Pacer-Client: v=1; caps=quota
```

- `caps=quota`: the reply carries `X-Pacer-Quota`.
- Unknown capabilities are ignored; an unknown version or a malformed value counts as no declaration. A request is
  never refused because of this header.
- CPA removes the header before the request goes upstream.

Compacting before an account switch is separate: only a client that can compact at a step boundary sends the
`X-Quota-Handoff` claim with `compact_on_switch` (OpenCodePlus). Pacer answers `409 quota_compact_required` only to
such requests.

## Quota on replies: `X-Pacer-Quota`

For a declared request, the reply (or the first chunk of a stream) carries the serving account's quota windows. Pacer
builds it after reading the reply's own quota headers, so it already includes them. The value is base64url (no
padding) of:

```json
{"v":1,"provider":"claude","auth":"claude-x.json","alias":"X","at_ms":1791587233865,
 "windows":[{"scope":"all","seconds":18000,"left":60,"reset_ms":1791594433000,
             "reset_quality":"absolute","state":"current","read_ms":1791587233865,
             "source":"header","applies":true}]}
```

- `left` is the % left. Times are Unix milliseconds. `reset_quality` is `absolute` (the provider's timestamp) or
  `estimated` (from a countdown).
- `state`: `current`, `estimated`, `stale` (balance older than an hour), `dormant`, `partial`, `unknown`,
  `awaiting-confirmation` (the reset time passed and no reading confirmed the new window yet), `ambiguous`,
  `conflicting`. Show a countdown to `reset_ms`; when it reaches zero, show "reset due" until a reading confirms it.
  Never invent a full window.
- `applies` says whether the window limits the request's model.
- Windows the provider reports as not applying to the account are left out. Antigravity and OpenCode Go accounts are
  included; their windows come from Pacer's meter reads (`source: api`).
- Upstreams cannot set this header: CPA removes any upstream `X-Pacer-Quota` before Pacer adds its own.

## Views (`Authorization: Bearer <CPA API key>`, `auth=api-key`)

Pacer checks the key against CPA's own loopback `/v1/models`; reads never call a provider.

- `?view=usage&auth=api-key&all=true&v=1`: every account with a meter (Claude, Codex, Antigravity, OpenCode Go), with
  IDs, aliases and windows in the `X-Pacer-Quota` format (all states, including stale and not-yet-reset windows).
  Without `v=1` the windows use the older format, which leaves out windows without a fresh balance and reset. This is
  a shared-pool view, not per-tenant.
- `?view=prices&auth=api-key`: Pacer's price for every model the key can use, as on Model Settings (manual, else the
  imported book, else models.dev), so a client's cost display matches Pacer's usage page:

  ```json
  {"protocol":1,"view":"prices","unit":"usd_per_million_tokens","revision":3,"synced_at_ms":1791500000000,
   "models":{"claude-opus-5-5":{"input":5,"output":25,"cache_read":0.5,"cache_write":6.25,"source":"imported"}}}
  ```

  `cache_write` is the 5-minute write price; `context_tiers` and `fast` appear when the model has them. Models without
  a price are left out.

### Shared credential change subscription

The usage view includes `now_ms` (millisecond snapshot time) and an opaque
`cursor`. To subscribe, repeat the authenticated usage GET with
`&cursor=<last cursor>&wait_ms=20000`. Pacer returns immediately when any caller's
response headers or a scheduled provider read updates its stored observations.
No model request from the viewer is needed, and the caller contributing headers
does not need to declare `X-Pacer-Client`. Primary/last-used identity stays local
to the viewer's chat; shared quota changes do not change it.

Native resource responses are finite JSON, so this is a bounded long poll, not
SSE. An initial/mismatched cursor returns a full snapshot immediately. An
unchanged cursor returns a heartbeat snapshot after at most 20 seconds. Cursors
are instance-specific, so restart/reconnect cannot silently lose updates.
Capture the returned cursor and rearm the request; coalesced changes are covered
by the next full snapshot. Do not run overlapping waits. Retry failures with
backoff; older servers without a cursor require ordinary cached-view polling.

Notifications occur after storing observations, before the rest of measurement
processing. They never block on viewers, perform provider requests, or run on
payload chunks. Authentication still uses CPA's local model-list boundary.
The native ABI cannot cancel a held call on HTTP disconnect; held calls therefore
have the 20-second lifetime and a global limit of 256 concurrent waiters.

## How often Pacer reads meters

### Per-chat next-use preview

`GET /v0/resource/plugins/pacer/events?view=usage&v=1&model=<client-provider/model>`
with `Authorization: Bearer <private chat capability>` adds `next_use` to the usage
view. It is scoped by the existing binding capability and route; the shared API-key
view does not expose other chats' routing preferences. The raw capability is never
returned. Older clients can ignore this additive field.

```json
{"v":1,"at_ms":1791587233865,"expires_ms":1791587263865,
 "candidates_at_ms":1791587200000,"mode":"active","provider":"claude",
 "model":"claude-opus-5-5","reason":"cold_move","warm":true,
 "auth":"claude-y.json","alias":"Y","forced":false,
 "force_auth":"claude-z.json","force_alias":"Z","force_reason":"soon"}
```

`mode` is `active` when routing is on and `off` otherwise (older Pacer versions
also sent `dry-run`). This is a **proposal, not a reservation or a promise of
CPA's next selection**. Each read reruns the same pace routing rule with current
observed quotas, as the chat's next routing moments would:

- `auth`/`alias` (**NEXT USE**): the account the chat moves to once its cache is
  cold (`cold_move`), or now when its account cannot serve (`forced_move`,
  `forced:true`). Shown while the chat is still warm (`warm:true`, Pacer's view of
  its cache); absent when the chat would stay.
- `force_auth`/`force_alias`/`force_reason` (**FORCE USE**): the account a forced
  move sends the chat to: `now` when its account cannot serve, `soon` when one of
  its meters is within the page's forced-move warning (default 5 points) of its
  floor. A forced move asks a declaring client to compact first.

An account CPA left out whose meters can serve is waited for, not left, so it
yields neither. Missing meters never become a move. Clients show FORCE USE instead
of NEXT USE when both name the same account.

Candidate eligibility comes from that chat's last primary scheduler invocation
(`candidates_at_ms`), intersected with the current discovered credential inventory.
CPA revalidates cooldowns, model eligibility and availability on actual dispatch.
No candidate snapshot after restart, one older than 24 hours, missing quota
evidence, or routing off means no target. The in-memory snapshot cache is bounded.
The `auth`, `alias` and `force_*` fields are absent whenever no replacement is proposed.

Render **NEXT USE** and **FORCE USE** separately from **LAST USED**. Hide a preview after
`expires_ms`, on a failed read, or when switching chat/model. Visible sidebars may
refresh every 15 seconds. Reads only consult local state: they do not enroll,
admit, move, compact, reserve capacity or fetch provider quotas.

### Live request percentages

The response interceptor observes the upstream percentage headers **before**
building `X-Pacer-Quota`. For streaming requests this happens at header init,
before the first content chunk. Payload chunks do no quota work. Clients should
apply the header immediately and push their local UI update rather than waiting
for the usage poll; auxiliary replies update meters but not the chat's LAST USED
identity. This adds no network request per inference response.

Reads of a provider's quota endpoint are Pacer's alone; clients and pages only read Pacer's copy
([observation-contract.md](observation-contract.md)). Claude and Codex report quota with their responses, so an
account in use needs no reads; an idle one is read every 30 minutes, and once just after a reset. Antigravity and
OpenCode Go report nothing with responses: they are read every 30 minutes and 5 minutes after a burst of requests.
No account is read more than once in 15 minutes.

## A chat's prompt-cache requests

`GET /v0/resource/plugins/pacer/cache?installation=<id>&session=<id>&route=<client-provider/model>`
with `Authorization: Bearer <private chat capability>` returns the chat's own requests as CPA
sent them to the provider, newest first. `installation`, `session` and `route` are the claim's
values. A request is included only when its inference caller, with these three, rebuilds the
key of the chat the capability belongs to: the same session under another API key, or another
session, is never returned. An unknown capability is 401.

```json
{"protocol":1,"more":true,"requests":[
 {"started_at":"2026-10-17T14:53:17.2Z","kind":"other","model":"claude-haiku-5-5",
  "account":"Y","success":true,"uncached":4,"read":4773,"written":1040,
  "prefix":{"key":"1525aa62688d2064","settings":"9c1d0e7a52b4f6a3","markers":[
   {"section":"tools","ttl":"5m","tag":"e78f5c30a1b2c3d4"},
   {"section":"system","ttl":"5m","tag":"2560ab8a0f1e2d3c"},
   {"section":"messages","ttl":"5m","tag":"1f76650b9a8b7c6d"},
   {"section":"messages","ttl":"5m","tag":"499f59c1e2d3c4b5"}]}}]}
```

`read`, `written` and `uncached` are the provider's own counts. `prefix` is present for Claude
requests: one tag per cache marker in final-wire order, covering the prompt up to that marker
after CPA's own changes, and `settings` for the request settings that also key the cache. Tags
are the first 16 hex characters of CPA's content-free HMACs. They compare only between requests
with the same `key`; a CPA restart rotates it. When the previous request read or wrote up to a
marker and this one misses, the first marker whose tag changed names what changed; equal tags
mean the provider lost the entry. ChatGPT reports no cache writes and CPA no markers for it, so
its requests carry counts only. `account` is the account's alias, when it has one.

`limit` (1–500, default 100) bounds a page. `more:true` means older requests remain; pass the
oldest `started_at` as `before` (RFC 3339) for the next page. Records are kept as long as
Pacer's measurement store keeps them. Reads only consult the store and never change routing.
