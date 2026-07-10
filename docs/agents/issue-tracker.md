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

Do not pull ordinary TA3000 PRs into the `/triage` queue. Treat PRs as PR workflow unless the user explicitly asks to triage a PR as an incoming request.

## When A Skill Says "Publish To The Issue Tracker"

Create a GitHub issue.

## When A Skill Says "Fetch The Relevant Ticket"

Run `gh issue view <number> --comments`.

## Wayfinding Operations

For `/wayfinder`, use one GitHub issue as the map and child issues as investigation or implementation tickets.

- Map issue label: `wayfinder:map`
- Child ticket labels: `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling`, or `wayfinder:task`
- Use GitHub sub-issues and native dependencies when available.
- If native dependencies are unavailable, put `Blocked by: #<n>, #<n>` at the top of the child issue body.
- A frontier ticket is open, unassigned, and has no open blockers.
