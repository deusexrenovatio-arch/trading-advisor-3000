## Summary
Describe what changed and why.

## Change Surface
- [ ] `shell`
- [ ] `product-plane`
- [ ] `mixed`

## Boundary Checklist
- [ ] I kept trading/business logic out of shell control-plane paths.
- [ ] I kept product-plane runtime changes inside isolated app/deployment paths.
- [ ] If `mixed`, I documented why one coherent outcome required both surfaces.

## Validation Evidence
- [ ] Loop gate executed for local fast feedback (`python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`)
- [ ] PR gate executed (`python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`), including PR size validation
- [ ] Product tests executed when product-plane paths changed (`python -m pytest tests/product-plane -q`)

## Notes For Reviewers
List boundary-sensitive decisions, tradeoffs, and follow-ups.

## Architecture Impact
Classify `none`, `within-contract`, or `contract-change` using
[Architecture Review](docs/agent/architecture-review.md), with its single
`architecture-review` JSON block here. `none`: short reason and scope binding;
no mandatory document/hash. New or revised durable architectural choices:
save/update the owning explanation, pin `decision.record`, and include both
independent reviews. PR size and routine implementation details do not qualify
by themselves. Decision acceptance, implementation and deployment are separate.
