## pr gate

- Primary surface: `contracts`
- Surfaces: `contracts, architecture, app-runtime, app, governance, mixed`
- Docs only: `False`
- Changed files: `102`
- Snapshot mode: `changed-files`
- Profile: `none`

### Commands
- `C:\Users\Admin\AppData\Local\Programs\Python\Python311\python.exe scripts/run_loop_gate.py --mapping configs/change_surface_mapping.yaml --skip-session-check --snapshot-mode changed-files --profile none --enforce-explicit-markers --from-git --git-ref HEAD`
- `C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe scripts/validate_task_outcomes.py --require-terminal-outcome --stdin`
- `C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe -m pytest tests/process/test_phase1_governance_shell.py`
- `C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe -m pytest tests/process/test_validate_plans_contract.py -q`
- `C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe -m pytest tests/process/test_validate_task_request_contract.py -q`
- `C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe -m pytest tests/process/test_critical_contours.py -q`
- `C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe -m pytest tests/architecture/test_context_coverage.py -q`
- `C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe -m pytest tests/architecture -q`
- `C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe scripts/run_surface_pr_matrix.py --stdin`
