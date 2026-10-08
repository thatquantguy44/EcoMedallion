# Git Hooks

This checkout uses repo-local hooks to keep commits authored by the configured
Git identity rather than by coding agents.

Install for this clone:

```bash
git config core.hooksPath .githooks
git config user.name "Your Name"
git config user.email "you@example.com"
```

The hooks enforce both author and committer identity and reject commit messages
that add Claude, Codex, OpenAI, Anthropic, ChatGPT, Copilot, assistant, or agent
as co-authors.
