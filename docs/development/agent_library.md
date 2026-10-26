# Pinned Agent Library

Last verified: 2026-10-25

The full `agent-skills` library is available at `vendor/agent-skills` for
future feature builds, debugging, operational support, review, and shipping
work. It is a Git submodule, so the parent repository records one exact library
commit instead of copying or silently updating its files.

## Current pin

| Field | Value |
| --- | --- |
| Submodule remote | Repository-relative `../agent-skills.git` |
| Commit | `f63ec56a3cc936408d792956ae583c3c96a825bd` |
| License | MIT |

The repository-relative remote keeps the project portable without embedding an
account-specific GitHub URL. Git resolves it relative to the parent repository
remote; the recorded commit remains the exact reproducible library pin.

## Initialize and verify

After cloning the pipeline repository:

```bash
git submodule update --init --recursive
git submodule status vendor/agent-skills
git -C vendor/agent-skills rev-parse HEAD
node vendor/agent-skills/scripts/validate-skills.js
node vendor/agent-skills/scripts/validate-reference-links.js
```

The reported commit must match the gitlink recorded by the parent repository.
CI and support automation should fail rather than silently using another
revision when the submodule is absent.

## Use

Start with `vendor/agent-skills/skills/using-agent-skills/SKILL.md`. Select only
the workflow relevant to the current phase; for example:

- feature definition: `spec-driven-development`;
- implementation: `incremental-implementation` and
  `test-driven-development`;
- incidents and unexpected behavior: `debugging-and-error-recovery`;
- pre-merge review: `code-review-and-quality`;
- production rollout: `shipping-and-launch`.

The pinned library supplements this repository's specifications, tests, and
runbooks. It does not authorize external writes, deployments, or overrides of
project-specific safety gates.

## Update the pin

Update only in an isolated branch after reviewing upstream changes:

```bash
git -C vendor/agent-skills fetch origin
git -C vendor/agent-skills checkout <reviewed-commit>
node vendor/agent-skills/scripts/validate-skills.js
node vendor/agent-skills/scripts/validate-reference-links.js
git add vendor/agent-skills
```

Record the new commit and verification date in this file. Never commit a dirty
submodule or point `.gitmodules` at a workstation-only absolute path.
