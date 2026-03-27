<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-03-27 | Updated: 2026-03-27 -->

# workflows

## Purpose
Workflow definitions for building, verifying, and deploying docs.

## Key Files

| File | Description |
|------|-------------|
| `deploy.yml` | Deploys MkDocs site to GitHub Pages on push to main/master. |

## Subdirectories

No nested subdirectories currently.

## For AI Agents

### Working In This Directory
- Keep action versions pinned and explicit.
- Preserve job structure and stage order when editing existing workflows.
- Recheck concurrency, groups, and environment settings on deployment changes.

### Testing Requirements
- Validate workflow changes via repository CI checks.

### Common Patterns
- Jobs split into `build` and `deploy`.
- Build job installs Python and MkDocs Material, then runs `mkdocs build`.
- Deployment job depends on successful build.

### Dependencies

### External
- GitHub Actions marketplace actions used by this workflow.

<!-- MANUAL: -->
