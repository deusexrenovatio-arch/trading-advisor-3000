# Red-team checklist for acceptance review

Use this checklist to try to disprove closure.

## Universal checks

- [ ] Does the phase rely on descriptive files instead of executable proof?
- [ ] Does the phase claim “full” closure where the registry still shows `partial` or `scaffold`?
- [ ] Can a green result be produced without booting the claimed runtime?
- [ ] Are there explicit negative tests that would fail if only proxy evidence existed?
- [ ] Does CI install the dependencies required for the claimed technology?

## Delta

- [ ] Are there real Delta tables with `_delta_log`?
- [ ] Can they be read back by a Delta runtime?
- [ ] If `_delta_log` is removed, do tests fail?

## Spark

- [ ] Is a real Spark runtime invoked?
- [ ] Are outputs produced by execution rather than static SQL plan strings?

## Dagster

- [ ] Are assets actually materialized?
- [ ] Is there a real `Definitions` object and runnable job/selection?

## Runtime durability

- [ ] Does staging/prod profile refuse to start without Postgres?
- [ ] After restart, is runtime state restored from durable store?
- [ ] Is in-memory path explicit and test-only?

## FastAPI

- [ ] Is there an ASGI app?
- [ ] Is there an HTTP smoke test?
- [ ] Is plain class API being misrepresented as a service?

## aiogram

- [ ] Is aiogram actually imported and used?
- [ ] Is there a mocked Telegram API proof?

## Sidecar

- [ ] Is there a real `.sln/.csproj` in repo?
- [ ] Does `dotnet build/test/publish` succeed?
- [ ] Does Python talk to a compiled sidecar process?

## Observability

- [ ] If OpenTelemetry is claimed, is OTel actually exported?
- [ ] If not, was the spec formally changed?

## Final verdict rule

If any answer reveals “docs/manifest/stub/sample artifact only”, deny closure.
