# Issue Tracker: GitHub

Issues and PRDs for this repo live in GitHub Issues for `deusexrenovatio-arch/trading-advisor-3000`.

Use the `gh` CLI from inside this clone so the repo is inferred from `git remote -v`.

## Conventions

- Create an issue: `gh issue create --title "..." --body "..."`
- Read an issue: `gh issue view <number> --comments`
- List issues: `gh issue list --state open --json number,title,body,labels,comments`
- Comment: `gh issue comment <number> --body "..."`
- Add or remove labels: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- Close: `gh issue close <number> --comment "..."`

Use heredocs or files for multiline bodies.

## Pull Requests As A Triage Surface

PRs as a request surface: no.

Ordinary TA3000 PRs stay in PR workflow unless the user explicitly asks to treat one as an incoming issue.
