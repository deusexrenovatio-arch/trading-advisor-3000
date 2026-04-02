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
- [ ] Loop gate executed (`python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`)
- [ ] PR gate executed (`python scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`)
- [ ] Product tests executed when product-plane paths changed (`python -m pytest tests/app -q`)

## Notes For Reviewers
List boundary-sensitive decisions, tradeoffs, and follow-ups.
