# Git Hooks

This checkout uses repo-local hooks to keep commits authored by Joshua
Lutkemuller rather than by coding agents.

Install for this clone:

```bash
git config core.hooksPath .githooks
git config user.name "Joshua Lutkemuller"
git config user.email "110635594+joshualutkemuller@users.noreply.github.com"
```

The hooks enforce both author and committer identity and reject commit messages
that add Claude, Codex, OpenAI, Anthropic, ChatGPT, Copilot, assistant, or agent
as co-authors.
