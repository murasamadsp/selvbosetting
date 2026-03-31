<!-- Parent: ../AGENTS.md -->

<!-- Generated: 2026-03-27 | Updated: 2026-03-27 -->

# docs/stylesheets

## Purpose

Compact CSS overrides for MkDocs Material styling.

## Key Files

| File        | Description                                                      |
| ----------- | ---------------------------------------------------------------- |
| `extra.css` | Global typography weight adjustments for headings and body text. |

## Subdirectories

No nested subdirectories.

## For AI Agents

### Working In This Directory

- Keep selectors scoped and avoid broad global overrides.
- Prefer small additive CSS changes aligned with Material classes.

### Testing Requirements

- Validate rendering with `make build` after style updates.

### Common Patterns

- Simple, compact CSS overrides rather than full theme rewrites.

### Dependencies

### Internal

- Loaded by `mkdocs.yml` via `extra_css`.

### External

- Relies on MkDocs Material base theme class names.

<!-- MANUAL: -->
