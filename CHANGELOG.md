# Changelog

## [0.1.1](https://github.com/bedrock-python/grpc-client-kit/compare/grpc-client-kit-v0.1.0...grpc-client-kit-v0.1.1) (2026-09-06)


### Bug Fixes

* missing root exports, and three guides that described the wrong behaviour ([#17](https://github.com/bedrock-python/grpc-client-kit/issues/17)) ([b43d8cb](https://github.com/bedrock-python/grpc-client-kit/commit/b43d8cb390ebf3fa86a6049312e65de8baff8705))

## 0.1.0 (2026-08-13)

Initial release of grpc-client-kit.

### Features

* `ChannelPool` — channels pooled by their full identity (target, security, credentials, options, compression, interceptor chain); idle connections parked by gRPC core, health verdicts flag pooled channels
* `GrpcClient` — a cheap stub factory over the pool: picks a target, asks for the matching channel, hands out a stub
* `GrpcClientFactory` — maps one settings object onto the pool, balancer, health checker and per-target interceptor chains; eager settings validation, first-health-pass gate on entry, graceful `shutdown_grace` on exit
* Streaming-aware interceptors — one `around_call` async-generator seam expands into all four `grpc.aio` adapter classes, and write()-style streaming calls return promptly
* Canonical chain: logging, tracing, metrics, timeout, deadline budget, wait-for-ready, retry, circuit breaker — ordered so every layer sees what it needs
* Timeout as the budget of the whole call, divided between retry attempts rather than granted afresh to each one
* Deadline-budget propagation (`deadline` extra) — outgoing calls trimmed to what the caller's request has left
* Wait-for-ready policy per method, with an optional deadline requirement
* Retries with budget arithmetic, idempotency whitelists, jitter and retry metrics — and a warning when a native `retryPolicy` would multiply them
* Circuit breaker per method and per target — half-open trials, bounded LRU state with OPEN circuits pinned, rejections reported as `status="rejected"`, never `error`
* Client-side load balancing — round-robin, random, weighted; health-aware narrowing and passive quarantine that takes a target out of rotation on the first real transport failure
* Active `grpc.health.v1` monitoring (`health` extra) on the checker's own channels, with `wait_until_ready` and per-service probing
* Uniform observability — structured logs with sensitive-metadata redaction, Prometheus metrics, OpenTelemetry spans that parent correctly, all four RPC kinds covered
* `GrpcClientKitError` family separating the kit's own refusals from server failures, while RPC-shaped ones remain `AioRpcError`s
* Connectivity tuning in seconds — keepalive and reconnect backoff, part of channel identity
* TLS by default; zero-dependency core (`grpcio` only) — every integration is an opt-in extra
