<!-- Generated: 2026-03-27 | Updated: 2026-03-27 -->

# Selvbosetting Repository

## Purpose
Documentation source for the Avtalt Selvbosetting knowledge base, built with MkDocs Material.
Focus areas: practical guides, legal references, and housing-relocation checklists for Norway.

## Key Files

| File | Description |
|------|-------------|
| `README.md` | High-level project description and structure notes. |
| `mkdocs.yml` | MkDocs Material site configuration and navigation. |
| `Makefile` | Local maintenance commands (format, lint, build, serve, link checks). |
| `.pre-commit-config.yaml` | Pre-commit checks configuration. |
| `.markdown-link-check.json` | Link-checker runtime settings. |
| `.mdformat.toml` | MkDocs-aware markdown formatter options. |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `.github/` | CI configuration and deployment workflow (see `.github/AGENTS.md`). |
| `docs/` | All documentation content and markdown structure (see `docs/AGENTS.md`). |

## For AI Agents

### Working in this Directory
- Keep content authoritative and practical.
- Keep `mkdocs.yml` in sync with docs structure.
- Avoid generated folders (for example, `site/`) unless explicitly requested.
- Use local checks (`make lint`, `make links`, `make build`) after doc updates.

### Testing Requirements
- For documentation updates, run at minimum `make lint` and `make links`.
- Run `make build` to confirm the docs render.

### Common Patterns
- Use Markdown sections with short, actionable headings.
- Keep cross-links relative and valid for MkDocs.
- Reuse consistent naming style for files with hyphens where needed (for example, `index.md`, `checklists/move-in.md`).

### Dependencies

- MkDocs Material (`mkdocs`, `mkdocs-material`) is used for publishing the site.
- `mdformat`, `markdown-link-check` are used for linting and quality checks.

<!-- MANUAL: -->
