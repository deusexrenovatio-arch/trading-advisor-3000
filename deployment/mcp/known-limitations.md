# MCP Known Limitations

1. MCP rollout is profile-scoped for local/staging operations and is not a production execution plane.
2. PostgreSQL and DuckDB integrations are read-only by contract in this wave.
3. Credentials are env-only temporary and require manual rotation/expiration discipline.
4. Runtime smoke validates command readiness and handshake; it does not validate full business workflows.
5. Hosted CI validates static contract and secret hygiene only, not local MCP command execution.
