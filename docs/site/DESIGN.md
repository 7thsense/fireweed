# Fireweed — voice and design system

**Applies to:** the microsite under `docs/site/` and any external-facing
Fireweed page.
**Visual implementation:** [`assets/site.css`](assets/site.css)
**Site metadata:** [`_meta/site-meta.json`](_meta/site-meta.json)

This document sets how the microsite sounds and looks. Copy and UI that
conflict with it are bugs.

What the site may say about the product comes from two documents:
[ADR-027](../helix/02-design/adr/ADR-027-data-plane-interface-is-the-implementation-boundary.md)
(the decision) and
[API-008](../helix/02-design/contracts/API-008-data-plane-dsl.md) (the DSL
reference), and from the code in `crates/fireweed-dsl` and
`crates/fireweed-dsl-postgres`. When this file and those disagree, they win.

---

## 1. Identity

- **Name:** Fireweed. The header wordmark and first reference use it.
- **Descriptor (home and meta description):**

  > Queues and tables for durable workflow data, changed by typed programs
  > that run as plain SQL.

- **What it is:** a data-plane language. A schema document declares a space's
  queues, tables and reads; a program document is a list of typed statements
  that run as one transaction. The PostgreSQL 18 executor creates real tables
  from the schema and runs each statement as one SQL statement.
- **Who it serves:** Snorri and Cayce, which keep their durable workflow
  data in it.
- **What it is not:** a SQL interface for consumers, a control plane, a log
  or analytics store, a holder of tenant-wide counters, or a blob store.

### Metaphor

*Fireweed* (a pioneer plant after disturbance) is a restraint, not a theme. At
most one metaphor line in long-form copy. Never flower photography, mascots,
flame gradients or "blazing" puns.

---

## 2. Voice

| Trait | In practice |
|-------|-------------|
| **Precise** | Use the DSL's words: space, queue, table, program, statement, claim, settle, lease, token, `expect`, request id. |
| **Contract-first** | Say what a program does and what comes back: committed, rejected with a reason, or an error that changes nothing. |
| **Honest about status** | PostgreSQL 18 is the first and currently only executor. The crates are not on crates.io. Say so early. |
| **Engineer-first** | Explain why a rule exists (for example, every `where` needs an index). No motivational copy. |
| **Short** | Tables and lists over paragraphs. One idea per sentence. |

### Point of view

- Second person for guides ("Declare a queue, then claim from it").
- Product subject for specs ("The executor keeps nothing in the process").
- "We" only for maintainer policy ("We do not accept code pull requests").

---

## 3. Vocabulary

| Prefer | Avoid | Why |
|--------|-------|-----|
| space | database, namespace | A space is the atomic domain a program runs in |
| program | request, batch (except `execute_batch`) | A program is one transaction |
| statement | operation, verb, method | Programs are lists of statements |
| claim / settle / renew | dequeue / ack / heartbeat | The DSL's statement names |
| lease, token | lock | A lease is fenced by its token |
| rejected (with a reason) | failed | A rejection is recorded and replays; an error is not |
| executor | backend, engine, driver | The crate that runs documents |
| declared read | view, query endpoint | Named, versioned `select` |

### Claims discipline

**May claim** (each is in ADR-027, API-008 or the crates):

- Schema and program documents in YAML or JSON, checked against the schema.
- One program runs as one transaction; rejections roll back and replay.
- Every selection uses an index; every statement has a limit.
- Time-driven state is computed from stored times and the database clock.
- PostgreSQL 18 is the first and currently only executor.

**Must not claim:**

- Executors, features or statements that API-008 doesn't define.
- Throughput or latency numbers.
- crates.io packages or any install path other than the repository.
- Joins, cohorts, group ordering or anything API-008 lists as not in v1.
- A community code contribution program.

---

## 4. Writing patterns

- **Hero:** badges (data-plane DSL · PostgreSQL 18 · source only), the
  descriptor, a lede naming schema, program and executor, then CTAs (Get
  started · Why Fireweed · Examples).
- **Examples:** real documents from `docs/site/examples/src/`, shown unchanged,
  each followed by what comes back. No idealized snippets.
- **Footers:** status, issues welcome, code contributions not accepted,
  `MIT OR Apache-2.0`, a link to API-008 and to this file.
- **Inclusive language:** no gendered defaults; status never by color alone.

---

## 5. Visual design system

### Mood

**Field notebook after the burn:** warm paper, sharp ink, measured grid—pioneer
establishment, not startup purple. The product is infrastructure; the UI should
feel **legible under stress**, not decorative.

Fireweed-the-plant cues are **structural**, not illustrative:

| Cue | Visual translation |
|-----|--------------------|
| Ash / disturbed ground | Warm paper `#f7f6f1`, ink `#161616`, grid underlay |
| New growth holding ground | Teal accent `#0f766e` for action and links (calm, not neon) |
| Spike of blossom (rare emphasis) | Rose `#9f1239` (`--accent-2`) for sparse strong emphasis—not default CTAs |
| Hardiness / fail-loud | Clear ok / warn / blocked badge triad; no pastel ambiguity |

**Still forbidden:** flower photography, leaf logos as mascot, flame gradients,
“blazing” red-orange marketing themes that fight anti-hype voice.

### Color tokens

Names match `assets/site.css`. Change this table and CSS together.

| Token | Hex | Role |
|-------|-----|------|
| `--paper` | `#f7f6f1` | Page background |
| `--ink` | `#161616` | Primary text, borders, primary buttons |
| `--muted` | `#595852` | Secondary text |
| `--line` | `#c9c4b7` | Card and table borders |
| `--panel` | `#fffffb` | Elevated surface |
| `--field` | `#22211e` | Code background |
| `--field-2` | `#2e312c` | Code header |
| `--accent` | `#0f766e` | Links, focus, emphasis |
| `--accent-2` | `#9f1239` | Rare strong emphasis |
| `--ok` | `#146c43` | Supported / success |
| `--warn` | `#b45309` | Verify-first |
| `--blocked` | `#b91c1c` | Deferred / danger |
| `--shadow` | `0 16px 40px rgba(31, 28, 22, 0.14)` | Elevation |

**Badge surfaces:** `.ok` `#dcefe4` · `.warn` `#fff0d8` · `.blocked` `#fde2e2` ·
`.neutral` `#ebe7dc`

**Rules:** the light theme is canonical; never status-by-color-alone; no second
accent family without revising this document and the naming mood section.

### Typography

| Role | Stack | Why |
|------|-------|-----|
| UI / body | Aptos, Segoe UI, Liberation Sans, system-ui, sans-serif | Legibility |
| Display / brand | Georgia, Times New Roman, serif | Editorial seriousness; durable “notebook” authority |
| Code | SFMono, Consolas, Liberation Mono, Menlo, monospace | Specs and excerpts |

The header wordmark is the short name (**Fireweed**), not a crafted phrase.

### Layout & components

| Pattern | Spec |
|---------|------|
| Shell | max-width `1180px`, padding `24px` |
| Grid underlay | 44×44px faint ink on paper |
| Cards | panel, 1px line, 6px radius, soft shadow |
| Header | 2px solid ink rule; nav chips with `aria-current` inverse |
| Primary button | ink fill |
| Secondary button | panel fill, ink text |
| Breakpoints | ~960px stack; ~560px compact brand |

Canonical classes: `.badge`, `.card`, `.callout`, `table.data`, `.code-block`,
`.command-block`, `.section-title`, `.example-meta`, `.tag`, `.diagram`.

### Imagery

- Tables and SVG diagrams over icon packs
- **Branch Signal** is the canonical Fireweed mark: a central durable spine with
  branching paths and sparse state nodes. Use the full SVG in the home
  hero and a simplified version in the header; never substitute a literal flower,
  leaf, flame, or mascot.
- No stock photos, no generative mascots
- Diagrams reuse ink/panel/ok/warn fills

### Motion

- None required; copy-button feedback only
- No carousels, parallax, autoplay  

### Accessibility

- Ink-on-paper and panel-on-ink contrast
- Semantic landmarks; `aria-current`
- Horizontal scroll for code, not clipped content

---

## 6. Microsite application

| Route | Must include | Must not |
|-------|--------------|----------|
| Home | Descriptor, the two documents, executor status | Unsupported claims |
| Why | What Fireweed holds and what stays outside it | Vendor comparisons |
| Concepts | Shapes, leases, index rule, time, transactions | A copy of API-008 |
| Get started | Clone, test against PostgreSQL 18, run from Rust | A crates.io install |
| Examples | Documents from `examples/src/`, shown unchanged | Hand-edited excerpts |
| Reference | Summary tables and a link to API-008 | Statements API-008 lacks |
| Support | Status, test coverage, not-in-v1 list | Silent overclaim |
| Contribute | Issues only; private security reports | "Open a PR" |

### Pipeline

1. ADR-027, API-008 and the crates (governing).
2. This file (voice and visuals).
3. `scripts/site/render_site.py`, the example manifest and
   `docs/site/examples/src/`.
4. `assets/site.css`.
5. `scripts/ci/microsite-gate.sh`: links, example documents, rendered pages,
   staging.

---

## 7. Quick copy checks

1. Name is Fireweed; descriptor on the home page.
2. Every claim traces to ADR-027, API-008 or the crates.
3. PostgreSQL 18 named as the first and currently only executor.
4. No crates.io install path.
5. Examples come from `docs/site/examples/src/` and pass the gate.
6. Issues, not code pull requests.
7. System tokens; labeled badges.
