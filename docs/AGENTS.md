<!-- Parent: ../AGENTS.md -->

<!-- Generated: 2026-03-27 | Updated: 2026-03-27 -->

# docs

## Purpose

Primary documentation source for MkDocs Material, including guide, legal, forms,
checklists, commune, and finance pages.

## Key Files

| File          | Description                                                   |
| ------------- | ------------------------------------------------------------- |
| `index.md`    | Main landing page content.                                    |
| `start.md`    | Initial onboarding guidance.                                  |
| `daily.md`    | Practical everyday-life instructions for users after move-in. |
| `glossary.md` | Term definitions used across the guide.                       |

## Subdirectories

| Directory      | Purpose                                                   |
| -------------- | --------------------------------------------------------- |
| `faq/`         | Frequently asked questions.                               |
| `guide/`       | Step-by-step guidance and structured process description. |
| `forms/`       | Editable templates and correspondence examples.           |
| `checklists/`  | Process checklists for key milestones.                    |
| `communes/`    | Commune-specific details for different municipalities.    |
| `finances/`    | NAV payments, guarantees, and banking topics.             |
| `laws/`        | Legal references and rights sections.                     |
| `stylesheets/` | Custom markdown/site styling overrides.                   |

## For AI Agents

### Working In This Directory

- Keep language style aligned with existing Ukrainian/Russian documentation.
- Use direct instructions and avoid legal statements without sources.
- Update `mkdocs.yml` when adding, removing, or reordering pages.
- Keep file names descriptive and structure-aware (`section/page.md`).

### Testing Requirements

- Run `make lint` after markdown changes.
- Run `make links` after link edits or legal updates.
- Optionally run `make build` to verify rendering.

### Common Patterns

- Use section indexes (`index.md`) for top-level and major subsection roots.
- Keep page hierarchy mirrored in `mkdocs.yml`.
- Keep emphasis on clear steps and checklists.

### Dependencies

### Internal

- `mkdocs.yml` references every page.

### External

- MkDocs Material theme and optional markdown extensions (`admonition`, `pymdownx`).

<!-- MANUAL: -->
