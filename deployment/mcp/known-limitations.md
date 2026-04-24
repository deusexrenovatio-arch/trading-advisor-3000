# MCP Known Limitations

1. MCP rollout is profile-scoped for local/staging operations and is not a production execution plane.
2. `mempalace` is local-state only and depends on a host-level MemPalace install plus local palace configuration.
3. PostgreSQL and DuckDB integrations are read-only by contract in this wave.
4. Credentials are env-only temporary and require manual rotation/expiration discipline.
5. Runtime smoke validates command readiness and handshake; it does not validate full business workflows.
6. Hosted CI validates static contract and secret hygiene only, not local MCP command execution.
