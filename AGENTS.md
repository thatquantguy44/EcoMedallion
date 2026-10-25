# Project Agent Guidance

## Pinned engineering workflow library

The complete agent workflow library is pinned as a Git submodule at
`vendor/agent-skills`. Initialize it after cloning with:

```bash
git submodule update --init --recursive
```

For non-trivial build, support, debugging, review, documentation, or release
work, first read `vendor/agent-skills/skills/using-agent-skills/SKILL.md`, then
read only the `SKILL.md` files it routes to for the current task. Do not load
all skills into context. Apply the library incrementally because this is an
established codebase: preserve repository conventions, characterize existing
behavior before risky changes, and use the project's own tests and runbooks as
the final authority.

The library's root `AGENTS.md` governs maintenance inside the submodule only;
it is not a project-wide instruction file. User, system, developer, and this
repository's instructions take precedence over vendored guidance.

Do not edit vendored files from the parent repository. Pin upgrades by moving
the submodule commit in a dedicated reviewed change, following
`docs/development/agent_library.md`.
