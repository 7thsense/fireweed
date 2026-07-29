# Fireweed product microsite

Openable static site for marketing, technical concepts, curated examples, API
guides, and the operator deploy console.

**Brand voice and visual system:** [DESIGN.md](DESIGN.md) — source of truth for
copy tone, claims discipline, color tokens, typography, and components.
`assets/site.css` and site copy must follow it.

## Open locally

```sh
# from repository root
xdg-open docs/site/index.html   # or open / start as appropriate
```

## Layout

| Path | Role |
|------|------|
| `DESIGN.md` | Brand voice and design system (source of truth) |
| `index.html` | Product home |
| `why.html` | Fireweed vs stream |
| `concepts.html` | Technical specification |
| `get-started.html` | Embed + RESP paths |
| `examples/` | Gallery + per-example pages |
| `examples/src/*.rs` | Provenance-tracked excerpts |
| `api/` | Rust / RESP / types |
| `deploy/` | Operator console |
| `support.html` | Support matrix |
| `contribute.html` | Issues-only policy |
| `_meta/` | site version + example manifest |
| `assets/` | Shared CSS/JS |

## Regenerate

```sh
python3 scripts/site/extract_examples.py
python3 scripts/site/render_site.py
python3 scripts/site/check_example_provenance.py
python3 scripts/site/check_links.py
```

Committed HTML is the source of truth for browsing offline. Re-run the
scripts after editing content in `scripts/site/render_site.py` or the example
manifest.

## CI

`scripts/ci/deployment-release-gate.sh` invokes the link and provenance checks
as part of `validate_docs_microsite`.

## GitHub Pages deployment

Workflow: [`.github/workflows/pages.yml`](../../.github/workflows/pages.yml)

On push to `main` (site-related paths) or `workflow_dispatch`:

1. Validate source links + example provenance  
2. Stage a Pages tree with `scripts/site/stage_pages.py`  
   (`site/`, `helix/`, `deployment/`, `operator/`, root policy files)  
3. Deploy via `actions/deploy-pages`  
4. Playwright post-deploy: screenshots at common viewports + link crawl  

Published URL (project Pages):  
**https://7thsense.github.io/fireweed/** → redirects to `/site/`.

If organization policy blocks repository Actions, publish with the fallback:

```sh
bash scripts/site/publish_gh_pages.sh
```

That force-updates the `gh-pages` branch (legacy Pages source). When Actions is
enabled, switch the Pages build type to **GitHub Actions** so `pages.yml` owns
deploy + Playwright verification.

### Local stage + Playwright

```sh
# Stage the same artifact CI deploys
python3 scripts/site/stage_pages.py target/site-pages

# Serve (mimics Pages layout)
python3 -m http.server 4173 --bind 127.0.0.1 --directory target/site-pages

# In another shell:
cd scripts/site
npm ci
npx playwright install chromium
BASE_URL=http://127.0.0.1:4173 SITE_PREFIX=/site npm run verify
```

Screenshots land in `target/site-verify/screenshots/`; report in
`target/site-verify/report.json`.
