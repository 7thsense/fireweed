# Fireweed microsite

Static pages describing Fireweed's data-plane DSL and its PostgreSQL executor:
overview, concepts, getting started, examples and a reference summary. The
full DSL reference is
[API-008](../helix/02-design/contracts/API-008-data-plane-dsl.md).

**Voice and visual system:** [DESIGN.md](DESIGN.md).

## Open locally

```sh
# from repository root
xdg-open docs/site/index.html   # or open / start as appropriate
```

## Layout

| Path | Role |
|------|------|
| `DESIGN.md` | Voice and design system |
| `index.html` | Home |
| `why.html` | What Fireweed holds, and what stays outside it |
| `concepts.html` | Shapes, leases, index rule, time, transactions |
| `get-started.html` | Clone, test, run a program from Rust |
| `examples/` | Example index and one page per example |
| `examples/src/` | The example documents: `schema.yaml`, one folder of programs per example, and `embed.rs` |
| `api/index.html` | DSL reference summary |
| `api/rust.html` | Executor functions and DSL types |
| `support.html` | Status, test coverage, not in v1 |
| `contribute.html` | Issues-only policy |
| `_meta/site-meta.json` | Name, descriptor, status, repository |
| `_meta/example-manifest.yaml` | Examples: titles, documents, notes |
| `assets/` | Shared CSS and JS |

## Regenerate and check

The HTML is generated; edit `scripts/site/render_site.py`, the manifest or the
documents under `examples/src/`, then:

```sh
python3 scripts/site/render_site.py
bash scripts/ci/microsite-gate.sh
```

The scripts need Python 3 with PyYAML. The gate runs:

1. `check_links.py`: every local `href` in the HTML resolves.
2. `check_example_provenance.py`: every file under `examples/src/` is listed in
   the manifest and every listed file exists; each document parses and follows
   API-008's grammar (known keys and statements, objects and reads declared in
   `schema.yaml`, `request_id` and `issued_at` on programs that write); and
   every committed page equals what `render_site.py` renders, so each page
   shows its documents unchanged.
3. `stage_pages.py`: stages the Pages tree and checks that every relative link
   stays inside it.

The gate doesn't run documents against PostgreSQL. When you change an example,
run it against a PostgreSQL 18 server before committing, for example with a
program like `examples/src/embed.rs`.

## GitHub Pages deployment

Workflow: [`.github/workflows/pages.yml`](../../.github/workflows/pages.yml)

On push to `main` (site paths) or `workflow_dispatch`:

1. Run `scripts/ci/microsite-gate.sh`, which stages the tree at
   `target/site-pages` (`site/` plus a root redirect).
2. Run Playwright screenshot and layout checks against the staged tree.
3. Deploy with `actions/deploy-pages`.
4. Run Playwright again against the live site: screenshots at common
   viewports and a link crawl.

Published URL: **https://7thsense.github.io/fireweed/**, which redirects to
`/site/`.

If organization policy blocks repository Actions, publish with:

```sh
bash scripts/site/publish_gh_pages.sh
```

That runs the gate and force-updates the `gh-pages` branch.

### Local stage + Playwright

```sh
python3 scripts/site/stage_pages.py target/site-pages
python3 -m http.server 4173 --bind 127.0.0.1 --directory target/site-pages

# In another shell:
cd scripts/site
npm ci
npx playwright install chromium
BASE_URL=http://127.0.0.1:4173 SITE_PREFIX=/site npm run verify
```

Screenshots land in `target/site-verify/screenshots/`; the report in
`target/site-verify/report.json`.
