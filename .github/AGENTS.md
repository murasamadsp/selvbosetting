<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-03-27 | Updated: 2026-03-27 -->

# .github

## Purpose
CI/CD configuration for documentation publishing.

## Key Files

| File | Description |
|------|-------------|
| `workflows/deploy.yml` | GitHub Actions workflow that builds and deploys MkDocs site. |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `workflows/` | Workflow definitions for build and deployment pipelines. |

## For AI Agents

### Working In This Directory
- Keep workflow triggers and permissions explicit when adjusting branches or environments.
- Limit edits to YAML structure and pinned action versions.
- Verify secret and permission assumptions (for example, pages deployment) before changing
  environment settings.

### Testing Requirements
- Validate syntax and intent of workflow YAML after any change.
- Prefer a temporary branch/pull request check run for any workflow modification.

### Common Patterns
- Uses standard `push` triggers with `main`/`master` branches.
- Two-stage job pattern: build then deploy.
- Uses Material tooling in build phase.

### Dependencies

### External
- `actions/checkout`, `actions/setup-python`, `actions/upload-pages-artifact`, `actions/deploy-pages`.

<!-- MANUAL: -->
