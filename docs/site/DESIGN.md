# Fireweed Queue — Brand voice & design system

**Status:** active · **Applies to:** product microsite, README, release notes,
announcements, and any external-facing Fireweed surface  
**Visual implementation:** [`assets/site.css`](assets/site.css)  
**Version badge source:** [`_meta/site-meta.json`](_meta/site-meta.json)

This document is the source of truth for how Fireweed *sounds* and *looks*.
The microsite must follow it. Copy and UI that conflict with this guide are
bugs—not stylistic preference.

### Research lineage (do not invent a second brand story)

Voice and identity are **downstream of naming and positioning research**, not a
fresh marketing exercise:

| Artifact | What it contributes |
|----------|---------------------|
| [name-and-positioning-analysis](../helix/00-discover/name-and-positioning-analysis.md) | Selected name, descriptor, identifier map, anti-positions, reject lessons, trademark caveat |
| [product-vision](../helix/00-discover/product-vision.md) | Mission, north star, audience, value props |
| [public-preview-boundary](../helix/00-discover/public-preview-boundary.md) | What we may claim in public preview |
| [choosing-fireweed](../helix/01-frame/guides/choosing-fireweed.md) | Fireweed vs stream decision language |

When this file and those artifacts disagree on **identity or claims**, update
this file to match the governing discovery/design docs—do not “win” in DESIGN.

---

## 1. Brand essence (from the naming decision)

### Decision snapshot

From the 2026-07-23 naming decision (`pqueue-1dd9158f`):

- **Working public name:** Fireweed Queue (legal clearance may still be pending—
  do not imply trademark finality).
- **Public descriptor (required first reference):**

  > A durable work-state engine for ordered, recoverable execution.

- **Reserve fallback only if clearance fails:** Taskcairn  
- **Historical only (never current identity):** Queueyard, `pqueue`  
- **Rejected for public use:** Workspool, Queuewright, Ledgerail, Taskweir  

### Product semantics the name must carry

The naming analysis is explicit: the product is **not merely a priority queue**.
It owns the durable work lifecycle:

acceptance → configurable ordering → eligibility → grouping → claims & leases →
retries → idempotency → terminal state → recovery across log × projection combinations.

It is **not** a workflow engine and does **not** define dependency graphs.

The name and all public copy must support that broader contract **without**
promising:

- strict FIFO behavior,
- a generic message broker,
- a worker pool, or
- a workflow-DAG engine.

### Identifier map (authoritative)

Use these forms exactly. The map is naming input for packaging and docs; it is
not a license to reintroduce historical stems.

| Surface | Value | Public use |
|---------|-------|------------|
| Display name | **Fireweed Queue** | First reference, titles, legal, packaging display |
| Short name | **Fireweed** | Subsequent prose, short labels |
| Repository stem | `fireweed` | Clone URLs, monorepo identity |
| Package / chart stem | `fireweed-queue` | Helm chart and package coordinates |
| CLI | `fireweed` | Commands |
| Rust crate stem | `fireweed` | Embed surface (`fireweed` facade crate) |
| Environment prefix | `FIREWEED_` | Config |
| Informal abbreviation | **FWQ** | Chat/internal only—not hero, legal, or first reference |

### Why “Fireweed” (selection rationale → brand personality)

The scorecard optimized for **semantic fit**, **distinctiveness**, **identifier
ergonomics**, and **collision risk**. Fireweed was selected as the working
public identity because it keeps durable-work semantics while staying
**short and packagable** for announcement—unlike longer or noisier candidates.

Translate those selection criteria into brand personality:

| Naming criterion | Brand implication |
|------------------|-------------------|
| **Semantic fit** | Copy leads with work-state lifecycle, not “yet another queue” |
| **Distinctiveness** | Prefer a clear, slightly natural-world name over generic infra mashups |
| **Identifier ergonomics** | Short stems; easy to say and type (`fireweed`, `FIREWEED_`); no clever misspellings |
| **Collision risk** | Avoid names that parse as adjacent products, industrial jargon, or physical-queue businesses |

### Lessons from rejected and historical names (anti-patterns)

The reject list is free brand research. Do not recreate these problems in voice
or metaphor:

| Candidate | Why it failed / limited | Do not import into Fireweed voice |
|-----------|-------------------------|-----------------------------------|
| **Workspool** | Collision with WorkPool; “WorksPool” spoken parse; oil-field “work spool”; generic work-pool pattern; Spooling.ai noise | No “spool”, “pool”, or job-pool metaphors; no near-competitor sound-alikes |
| **Ledgerail** | Active SaaS already using the name | No “ledger product for documents” framing; durability ≠ accounting SaaS |
| **Queuewright** | Longer, easier to misspell; domain already taken at check time | No ornate “wright/craftsman” persona; keep identifiers short |
| **Taskcairn** | Distinctive but reads as durable *markers*, not ordering/dispatch | Do not brand as monuments, markers, or memorials of work |
| **Queueyard** | Strong fit historically; generic searches surface physical yard / vehicle-queue management | Do not lean parking-lot, freight-yard, or “yard management” imagery |
| **pqueue** | Pre-cutover technical identity | Lineage only; never as current public name |

### North star (from product vision)

> Every accepted item is durably executed according to its queue’s priority and
> progress guarantees, with no lost work, no concurrent execution of the same
> claim, and an explicit final state.

Public copy should orbit this sentence. Throughput and scale claims are
secondary and evidence-bound.

### Audience (from product vision)

**Who:** engineers building durable, high-volume async work systems  
**Pain:** FIFO queues and ad hoc scheduler tables do not model priority,
eligibility, leases, batching, and retries as one contract  
**Switch reason:** those concerns belong in the queue primitive, on infrastructure
teams already operate—not in every worker’s bespoke glue

Write for that reader. Do not write for “anyone with a backlog.”

### Metaphor: fireweed as pioneer, not decoration

*Fireweed* (pioneer plant after disturbance) is a useful **restraint rail**, not
a campaign theme:

| Lean into | Stay away from |
|-----------|----------------|
| Establishes after disruption | Wildflower lifestyle branding |
| Holds ground under harsh conditions | “Blazing fast” / fire puns (conflicts with anti-hype) |
| Durable recovery of work state | Mascots, petals, garden growth metaphors |
| Quiet toughness | Apocalypse / burn-it-down swagger |

**Rule:** at most **one** optional metaphor line in long-form marketing. Prefer
lifecycle verbs. Never: stock flower photography, “grow your queue,” or
emoji flora.

---

## 2. Brand voice

### Character (named after the research, not a mood board)

| Trait | Grounding | In practice |
|-------|-----------|-------------|
| **Lifecycle-precise** | Product semantics list in naming analysis | Use claim, lease, finalize, eligibility, request identity—not soft synonyms |
| **Contract-first** | Vision external transaction contract | Success durable+visible; rejection no effect; ambiguity via `request_id` |
| **Operator-honest** | Preview boundary | Supported / deferred / experimental / dev-only said early |
| **Engineer-first** | Target market table | Explain why a primitive exists; no motivational fluff |
| **Ergonomic** | Identifier ergonomics score | Short names, scannable tables, no ornamental prose |
| **Collision-aware** | Naming diligence | No claims that invite confusion with brokers, pools, or streams |
| **Fail-loud** | Storage axes philosophy | Celebrate explicit failure of unsupported pairings |

### Voice spectrum

```
❌  Hype / generic infra             ✅  Fireweed (research-aligned)
    "Unlock next-gen throughput"         "Claim eligible work under a lease"
    "Seamless job pool at scale"         "Group-aware claims for downstream batches"
    "Blazing priority queue"             "Priority and eligibility are separate"
    "Your workflow engine"               "Not a workflow DAG—work-state lifecycle"
    "Production-ready everything"        "Public preview; matrix support is evidence-bound"
    "Just works™"                        "Unsupported pairings fail at startup"
```

### What we always sound like (pillars from vision value props)

Use these as **value pillars** on home and get-started—worded as contracts, not
slogans:

1. **Configurable priority ordering** — without rewriting workers  
2. **Bounded progress guarantees** — relaxation without starving eligible work  
3. **Durable execution lifecycle** — recoverable across worker/process failure  
4. **Batch and group-aware claims** — downstream-compatible batches  
5. **Backend-independent transaction integrity** — same external contract across log × projection cells  
6. **Tunable durability economics** — commit-latency bound vs object-log cost (ops tone)  

Pillars 1–5 are default marketing. Pillar 6 is for deploy/operator surfaces.

### Point of view

- **Second person** for guides (“Create a queue, then claim a batch”).
- **Product subject** for specs (“Fireweed retains request identity for replay”).
- **We** only for maintainer policy (“We do not accept code PRs”).
- Never cheerful-bot, corporate-family, or growth-hacker register.

### Register by surface

| Surface | Register | Research cue |
|---------|----------|--------------|
| Microsite home | Descriptor + pillars + status badges | Naming descriptor + vision props |
| Why | Decision tables vs streams | choosing-fireweed |
| Concepts | Definitions, out-of-scope | Product semantics list |
| Examples | What it proves + provenance | Engineer-first honesty |
| API | Normative families; public vs internal | Identifier map + facade boundary |
| Deploy | Verify-first; axes; deferred registries | Preview + operator honesty |
| Contribute | Issues-only; security private | ADR-021 (policy), not naming |
| Release notes | Past-tense deltas + deferred | Preview boundary |

---

## 3. Vocabulary

### Preferred terms

| Prefer | Avoid in public copy | Why (research-linked) |
|--------|----------------------|------------------------|
| work item / item | message (except broker comparison) | Not a generic broker |
| claim | consume / dequeue (except RESP mapping) | Lease-based model |
| lease | lock | Different semantics |
| finalize / complete / retry / release / fail | “process” as whole lifecycle | Explicit terminal outcomes (north star) |
| eligible / eligibility | “ready” alone | Distinct from priority |
| projection | “cache” as the model | Durable composition axis |
| command log / durable log | “the stream” for internal log | Workers do not consume the log as an app stream |
| work-state engine | workflow engine / DAG | Explicit anti-position |
| public preview | beta (unless a different program) | Preview boundary language |
| preview-supported | production-ready (unless certified) | Honesty criterion |
| construction / `open_*` | “backend type parameter” as public model | Concrete `Fireweed` facade |

### Public surface vocabulary

- Rust: concrete **`Fireweed`** handle; crate **`fireweed`** only for embedders  
- Service: **`fireweed-service`**, RESP, Streams-shaped worker path  
- Say **two public faces**, not “SDK ecosystem”  
- Historical: **pqueue / Queueyard** only in lineage footnotes  

### Claims discipline

**May claim** (with preview framing where relevant)

- Descriptor and lifecycle ownership from the naming analysis  
- Priority/eligibility selection; leased claim; complete/retry/release/fail  
- External transaction contract on **supported durable** Class A cells  

- Log × projection axes; fail-loud unsupported pairings  
- Source release; dual license; issues-only contributions  
- Value props above, without universal capacity numbers  

**Must not claim**

- FIFO-as-product, generic broker, worker pool, workflow DAG (naming decision)  
- Production readiness for every compiled pairing  
- Universal latency/throughput leadership  
- crates.io / GHCR as default install while deferred  
- Internal log as application event stream  
- Community code PR program  
- Trademark-cleared finality of the name while diligence says clearance pending  

When unsure → [public-preview-boundary](../helix/00-discover/public-preview-boundary.md).

---

## 4. Writing patterns

### Hero formula

1. Status badges (preview · version · source release)  
2. **Exact descriptor** from naming analysis  
3. Lede: audience + lifecycle capabilities + “priority or eligibility, not append order”  
4. CTAs: Get started · Why not a stream? · Examples  

### Anti-hero (never)

- “The priority queue for everyone”  
- “Kafka alternative” as identity  
- Botanical taglines without the descriptor  

### Comparison copy

Use **when / when not** tables (choosing-fireweed). Compare **primitives**, not
vendors. The naming research forbids worker-pool and DAG framing; the choosing
guide forbids stream-as-consumption-model confusion.

### Code and examples

- Real tests/examples only, with provenance  
- “What this proves” in lifecycle language from §1 semantics  
- No idealized snippets that hide construction or leases  

### Footers (always-on honesty)

- version + public preview  
- crates.io / GHCR deferred (while true)  
- issues welcome · code contributions not accepted  
- MIT OR Apache-2.0  
- link to this DESIGN.md  

### Inclusive language

- No gendered engineer defaults  
- Status not color-only  
- Clearance caveat: do not assert exclusive trademark rights in marketing  

---

## 5. Visual design system

### Mood (derived from name + product, not a generic SaaS kit)

**Field notebook after the burn:** warm paper, sharp ink, measured grid—pioneer
establishment, not startup purple. The product is recovery-minded infrastructure;
the UI should feel **legible under stress**, not decorative.

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
| `--warn` | `#b45309` | Preview / verify-first |
| `--blocked` | `#b91c1c` | Deferred / danger |
| `--shadow` | `0 16px 40px rgba(31, 28, 22, 0.14)` | Elevation |

**Badge surfaces:** `.ok` `#dcefe4` · `.warn` `#fff0d8` · `.blocked` `#fde2e2` ·
`.neutral` `#ebe7dc`

**Rules:** light theme canonical for v1; never status-by-color-alone; no second
accent family without revising this document and the naming mood section.

### Typography

| Role | Stack | Why |
|------|-------|-----|
| UI / body | Aptos, Segoe UI, Liberation Sans, system-ui, sans-serif | Operator legibility |
| Display / brand | Georgia, Times New Roman, serif | Editorial seriousness; durable “notebook” authority |
| Code | SFMono, Consolas, Liberation Mono, Menlo, monospace | Specs and excerpts |

Ergonomics from the naming scorecard: brand wordmark short (**Fireweed Queue**),
not a long crafted phrase in the header.

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

- Topology SVG and tables over icon packs  
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

### IA voice map

| Route | Must include | Must not |
|-------|--------------|----------|
| Home | Descriptor, pillars, two faces, preview honesty | Broker/DAG identity |
| Why | Stream decision table | Vendor dunking |
| Concepts | Lifecycle list from naming semantics | Full helix dump |
| Get started | Source-clone path; deferred registries | Fake crates.io install |
| Examples | Provenance to real tests | Idealized rewrite of product semantics |
| API | Public `fireweed` + RESP only | Internal crates as SDKs |
| Deploy | Verify-first; chart ≠ preview support | “All values production” |
| Preview | Support matrix | Silent overclaim |
| Contribute | Issues-only | PR welcome |

### Pipeline

1. Naming / vision / preview boundary (governing)  
2. **This DESIGN.md** (voice + visual)  
3. `render_site.py` + example manifest  
4. `site.css` tokens  
5. Link + provenance gates  

### Anti-patterns (compiled)

- Omitting preview status on entry pages  
- Using Queueyard/pqueue as current identity  
- Spool/pool/yard/cairn metaphors  
- Deploy badges that contradict preview boundary  
- Examples without provenance  
- Glassmorphism / purple SaaS skin  
- “Open a PR” / Discord-first community  

---

## 7. Quick copy checks

1. **Name:** Fireweed Queue first; short Fireweed after; no historical-as-current  
2. **Descriptor:** exact line on major entry surfaces  
3. **Anti-positions:** not FIFO product, broker, pool, or DAG  
4. **Status:** public preview + version visible  
5. **Deferred:** crates.io / GHCR while deferred  
6. **North star:** no lost work, no dual active claims, explicit final state  
7. **Clearance:** no “trademark secured” language while caveat holds  
8. **CTA:** issues, not code PRs  
9. **Visual:** system tokens; labeled badges  
10. **Examples:** provenance intact  

---

## 8. Change control

| Change | Requires |
|--------|----------|
| Tone tweak inside this voice | Ordinary review |
| New public name or descriptor | Update naming analysis **first**, then this file |
| New claim class / install path | Preview boundary + this file |
| Color / type / component family | §5 + `site.css` together |
| Fallback rename to Taskcairn | Naming decision + full identity pass (out of band) |

**Owner:** project maintainers  

**Governing inputs:**  
[name-and-positioning-analysis](../helix/00-discover/name-and-positioning-analysis.md) ·
[product-vision](../helix/00-discover/product-vision.md) ·
[public-preview-boundary](../helix/00-discover/public-preview-boundary.md) ·
[choosing-fireweed](../helix/01-frame/guides/choosing-fireweed.md) ·
[ADR-021](../helix/02-design/adr/ADR-021-open-source-license-and-contribution-policy.md)
