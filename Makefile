.PHONY: boring boring-fix type boring-full

boring:
	python scripts/run_boring_checks.py --profile quick --scope changed

boring-fix:
	python scripts/run_boring_checks.py --profile code --scope changed --fix

type:
	python scripts/run_boring_checks.py --profile type --scope all

boring-full:
	python scripts/run_boring_checks.py --profile full --scope all
