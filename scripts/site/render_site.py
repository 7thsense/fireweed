#!/usr/bin/env python3
"""Render the Fireweed product microsite under docs/site/."""

from __future__ import annotations

import html
import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "docs/site"
META_PATH = SITE / "_meta/site-meta.json"
MANIFEST = SITE / "_meta/example-manifest.yaml"
EX_SRC = SITE / "examples/src"

NAV = [
    ("index.html", "Home", "home"),
    ("why.html", "Why", "why"),
    ("concepts.html", "Concepts", "concepts"),
    ("get-started.html", "Get started", "get-started"),
    ("examples/index.html", "Examples", "examples"),
    ("api/index.html", "API", "api"),
    ("support.html", "Support", "support"),
]


def load_meta() -> dict:
    return json.loads(META_PATH.read_text(encoding="utf-8"))


def load_examples() -> list[dict]:
    text = MANIFEST.read_text(encoding="utf-8")
    if yaml is None:
        # Import lightweight loader from extractor
        sys.path.insert(0, str(ROOT / "scripts/site"))
        from extract_examples import load_manifest  # type: ignore

        return load_manifest(MANIFEST)
    return list(yaml.safe_load(text)["examples"])


def rel_to(page: str, target: str) -> str:
    """Relative path from page to target, both under docs/site/."""
    import os

    page_path = Path(page)
    target_path = Path(target)
    from_dir = page_path.parent
    return os.path.relpath(
        str(Path("docs/site") / target_path),
        str(Path("docs/site") / from_dir),
    ).replace("\\", "/")


def asset(page: str, name: str) -> str:
    return rel_to(page, f"assets/{name}")


def nav_html(page: str, active: str) -> str:
    items = []
    for target, label, key in NAV:
        href = rel_to(page, target)
        current = ' aria-current="page"' if key == active else ""
        items.append(f'<a href="{href}"{current}>{html.escape(label)}</a>')
    return "\n        ".join(items)


def wrap_tables(body: str) -> str:
    """Ensure data tables scroll inside a constrained wrapper on narrow viewports."""
    import re
    return re.sub(
        r"(<table class=\"data\">.*?</table>)",
        r'<div class="table-wrap">\1</div>',
        body,
        flags=re.S,
    )


def layout(
    *,
    page: str,
    active: str,
    title: str,
    body: str,
    meta: dict,
    description: str | None = None,
) -> str:
    desc = description or meta["descriptor"]
    version = meta["version"]
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="description" content="{html.escape(desc)}" />
    <title>{html.escape(title)}</title>
    <link rel="stylesheet" href="{asset(page, "site.css")}" />
  </head>
  <body>
    <div class="shell">
      <header class="site-header">
        <a class="brand" href="{rel_to(page, "index.html")}">
          <svg class="brand-mark" viewBox="0 0 80 80" role="img" aria-label="Fireweed branch signal mark">
            <path d="M40 70V10M40 25 18 38m22 4 22-13M40 51 13 66m27-6 27-16" fill="none" stroke="currentColor" stroke-width="5" stroke-linecap="square"/>
            <circle cx="40" cy="10" r="8" fill="#c13d74"/>
            <circle cx="18" cy="38" r="5" fill="#0f766e"/>
            <circle cx="62" cy="29" r="5" fill="#92ad61"/>
          </svg>
          <h1>Fireweed Queue</h1>
          <span>v{html.escape(version)}</span>
        </a>
        <nav class="site-nav" aria-label="Primary">
        {nav_html(page, active)}
        </nav>
      </header>
      <main>
{wrap_tables(body)}
      </main>
      <footer class="site-footer">
        <p>
          Fireweed Queue v{html.escape(version)} · {html.escape(meta["status"])} ·
          crates.io deferred ·
          issues welcome · code contributions not accepted ·
          {html.escape(meta["license"])}.
        </p>
        <p>
          Static docs in the repository.
          Open <code>docs/site/index.html</code> from a clone, or browse
          <a href="{html.escape(meta["repository"])}">{html.escape(meta["repository"].removeprefix("https://"))}</a>.
          Brand voice and style:
          <a href="{rel_to(page, "DESIGN.md")}">DESIGN.md</a>.
          See <a href="{rel_to(page, "contribute.html")}">contribute</a> and
          <a href="{rel_to(page, "support.html")}">support boundary</a>.
        </p>
      </footer>
    </div>
    <script src="{asset(page, "site.js")}"></script>
  </body>
</html>
"""


def table_wrap(inner: str) -> str:
    return f'<div class="table-wrap">\n{inner}\n</div>'


def code_block(label: str, code: str, lang: str = "rust") -> str:
    return f"""<div class="code-block">
  <div class="label"><span>{html.escape(label)}</span><span>{html.escape(lang)}</span></div>
  <pre><code>{html.escape(code.rstrip())}</code></pre>
</div>"""


def write(page: str, content: str) -> None:
    path = SITE / page
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"wrote {path.relative_to(ROOT)}")


def page_home(meta: dict) -> str:
    body = f"""
        <section class="hero hero-home">
          <div class="hero-copy">
          <div class="badge-row">
            <span class="badge warn">priority queue</span>
            <span class="badge neutral">v{html.escape(meta["version"])}</span>
            <span class="badge ok">source release</span>
          </div>
          <h2>{html.escape(meta["descriptor"])}</h2>
          <p class="lede">
            A durable priority queue for engineers building async execution systems.
            Put priority first, then combine it with eligibility, leases, retries,
            and final state in one queue contract. Embed the Rust library when FIFO
            append order is not the scheduling policy your work needs.
          </p>
          <div class="hero-actions">
            <a class="btn" href="get-started.html">Get started</a>
            <a class="btn secondary" href="why.html">Why not a stream?</a>
            <a class="btn secondary" href="examples/index.html">Browse examples</a>
          </div>
          </div>
          <aside class="signal-panel" aria-label="Priority queue lifecycle">
            <span class="signal-label">Priority queue signal</span>
            <svg class="signal-mark" viewBox="0 0 360 360" role="img" aria-label="Branch Signal mark">
              <path d="M180 322V38M180 108 74 170m106 8 106-62M180 228 49 303m131-35 131-76" fill="none" stroke="#f8f5eb" stroke-width="5" stroke-linecap="square"/>
              <circle cx="180" cy="38" r="28" fill="#c13d74" stroke="#17211d" stroke-width="3"/>
              <circle cx="74" cy="170" r="17" fill="#0f766e" stroke="#17211d" stroke-width="3"/>
              <circle cx="286" cy="116" r="17" fill="#92ad61" stroke="#17211d" stroke-width="3"/>
              <circle cx="49" cy="303" r="12" fill="#d87950" stroke="#17211d" stroke-width="3"/>
              <circle cx="311" cy="192" r="12" fill="#f5c85b" stroke="#17211d" stroke-width="3"/>
            </svg>
            <div class="signal-states"><span>accept</span><span>eligible</span><span>claim</span><span>finalize</span></div>
          </aside>
        </section>

        <div class="section-title"><h2>Priority is the primitive</h2></div>
        <div class="grid-3">
          <article class="card card-body">
            <h3>Configurable priority ordering</h3>
            <p>
              Model timestamp, numeric, or score-ordered work without rewriting
              workers—priority is part of the queue contract, not glue around a FIFO.
            </p>
          </article>
          <article class="card card-body">
            <h3>Durable execution lifecycle</h3>
            <p>
              Accept, claim under a lease, then complete, retry, release, or fail.
              Recoverable across worker and process failure; explicit final state.
            </p>
          </article>
          <article class="card card-body">
            <h3>One transaction contract</h3>
            <p>
              Success is durable and visible; rejection has no durable effect;
              ambiguous retries resolve by request identity—across storage profiles.
            </p>
          </article>
        </div>

        <div class="section-title"><h2>Rust library</h2></div>
        <div class="grid-2">
          <article class="card card-body">
            <h3>Embed in your process</h3>
            <p>
              Open one <code>Queue</code> handle, provision a queue, insert work,
              claim batches under a lease, and commit the outcome. The storage
              backend is a construction input, not a type parameter.
            </p>
            <p><a href="api/rust.html">Rust API →</a> · <a href="examples/basic-lifecycle.html">Basic lifecycle →</a></p>
          </article>
        </div>

        <div class="callout">
          <p>
            v{html.escape(meta["version"])} is a <strong>GitHub source tree</strong>
            (workspace package identity; annotated tag cut is a separate release step).
            Publication to crates.io is deferred. The public product is the
            <code>Queue</code> API over S3 + Turso + PostgreSQL leases, with a
            PostgreSQL reference store—see the
            <a href="support.html">support boundary</a>.
          </p>
        </div>

        <div class="section-title"><h2>Featured examples</h2></div>
        <div class="grid-2">
          <article class="card card-body">
            <h3>Provision, insert, claim, commit</h3>
            <p>Happy-path embed over a throwaway on-disk queue.</p>
            <p><a href="examples/basic-lifecycle.html">Read the example →</a></p>
          </article>
          <article class="card card-body">
            <h3>Scheduled delivery and retry</h3>
            <p>Hold work until it is due and retry a claimed item later.</p>
            <p><a href="examples/scheduled-delivery.html">Read the example →</a></p>
          </article>
        </div>
"""
    return layout(
        page="index.html",
        active="home",
        title="Fireweed Queue — durable priority queue",
        body=body,
        meta=meta,
    )


def page_why(meta: dict) -> str:
    body = """
        <div class="page-intro">
          <h2>Why Fireweed instead of a stream</h2>
          <p>
            Use Fireweed when the application owns durable work state whose
            eligibility, priority, lease, retry, and completion state can change
            per item. Use an immutable sequential stream when consumers only need
            ordered append records, offset progress, replay, and fan-out.
          </p>
        </div>
        <table class="data">
          <thead>
            <tr><th>Need</th><th>Choose Fireweed when</th><th>Choose a stream when</th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Mutable priority</td>
              <td>Workers claim the highest-priority eligible item; priority may update before terminal completion.</td>
              <td>Record order is append/partition order; changing priority means writing another event.</td>
            </tr>
            <tr>
              <td><code>not_before</code> scheduling</td>
              <td>Items become eligible at a future timestamp; workers skip ineligible work without advancing past it forever.</td>
              <td>Consumers read every record and defer locally.</td>
            </tr>
            <tr>
              <td>Leases</td>
              <td>One active worker at a time; recover after lease expiry; finalize with complete, retry, fail, or release.</td>
              <td>Consumers keep their own processing state; storage exposes records and offsets.</td>
            </tr>
            <tr>
              <td>Item-level retry</td>
              <td>Failed items need their own delay, attempt state, and re-entry into claim order.</td>
              <td>Retry is a consumer concern (new event, seek, or side channel).</td>
            </tr>
            <tr>
              <td>Groups / cohorts</td>
              <td>Claims batch compatible work by account, connector, job, or campaign while preserving progress.</td>
              <td>Partition key or topic layout already provides cohort ordering.</td>
            </tr>
            <tr>
              <td>Progress model</td>
              <td>Worker progress is lease/finalize state on items.</td>
              <td>Progress is a durable consumer offset through a record sequence.</td>
            </tr>
            <tr>
              <td>Broadcast</td>
              <td>One work item completed once by one worker (unless intentionally enqueued per recipient).</td>
              <td>Multiple independent consumer groups each observe the same stream.</td>
            </tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Use Fireweed</h2>
          <ul>
            <li>Scheduled delivery where <code>not_before</code>, priority updates, leases, and delayed retry are work state.</li>
            <li>Connector or API work claimed in account/job/campaign cohorts.</li>
            <li>Recovery-sensitive work where a crash leaves a lease that must expire back into claim order.</li>
            <li>A mutable backlog operators may reschedule, retry, release, fail, or complete per item.</li>
          </ul>
          <h2>Do not use Fireweed</h2>
          <ul>
            <li>Event distribution where every subscriber observes every event.</li>
            <li>Audit logs, analytics ingestion, or CDC where immutable append order is the contract.</li>
            <li>Work that is fine as sequential batches without arbitrary priority or per-item leases.</li>
            <li>Downstream rate tokens, quotas, or worker placement—those stay in the caller or scheduler layer.</li>
          </ul>
          <div class="callout">
            <p>
              Fireweed has a durable change log internally so supported profiles can
              rebuild queue state. That log is <strong>not</strong> the worker consumption model.
              Workers claim and finalize; they do not advance offsets through the internal log.
            </p>
          </div>
          <p>
            Governing guide:
            <a href="../helix/01-frame/guides/choosing-fireweed.md">choosing-fireweed.md</a>.
          </p>
        </div>
"""
    return layout(
        page="why.html",
        active="why",
        title="Why Fireweed — Fireweed Queue",
        body=body,
        meta=meta,
        description="When to choose Fireweed Queue instead of an immutable stream.",
    )


def page_concepts(meta: dict) -> str:
    body = """
        <div class="page-intro">
          <h2>Technical concepts</h2>
          <p>
            The product contract in scannable form: lifecycle, eligibility,
            leases, transaction integrity, and storage composition.
          </p>
        </div>
        <div class="prose">
          <h2>Work item lifecycle</h2>
          <p>
            Items are inserted, ordered by priority, claimed under a lease by a worker, then
            settled in one atomic <code>commit</code> with a <code>Disposition</code>:
            <strong>Complete</strong>, <strong>Fail</strong>, <strong>Release</strong>,
            <strong>RetryAt</strong>, <strong>Retain</strong> or <strong>Delete</strong>.
            A commit can also change state records and insert new work in the same step.
          </p>
          <p><a href="examples/basic-lifecycle.html">Example: basic lifecycle</a> ·
             <a href="api/rust.html">Rust API</a></p>

          <h2>Priority vs eligibility</h2>
          <p>
            <strong>Priority</strong> ranks eligible work. <strong>Eligibility</strong>
            decides whether an item may be claimed at all: <code>available_at</code>,
            active leases, and retry times.
          </p>
          <p><a href="examples/scheduled-delivery.html">Example: scheduled delivery and retry</a></p>

          <h2>Leases and recovery</h2>
          <p>
            A claim grants temporary exclusive execution authority. A lease that expires
            returns the item to pending so another worker can continue; a commit with a
            stale lease is rejected.
          </p>

          <h2>Filters, indexes and state</h2>
          <p>
            A queue schema declares typed fields and indexes. Claims, queries and stats
            select by filter over those fields; state collections hold per-key records that
            commit atomically with work.
          </p>

          <h2>External transaction contract</h2>
          <p>
            On supported durable profiles: a successful mutation is durable and visible;
            a rejected mutation has no durable effect; unknown outcomes are resolved by
            retained <code>request_id</code> replay (same body → same result;
            different body → <code>RequestIdConflict</code>).
          </p>

          <h2>Architecture</h2>
          <p>
            <code>Queue</code> sits on a shared engine. The production backend composes an
            S3 <strong>command log</strong>, a disposable local Turso
            <strong>projection</strong> and PostgreSQL queue leases.
          </p>

          <h2>Out of scope</h2>
          <ul>
            <li>Workflow DAGs and multi-step graph engines</li>
            <li>Broadcast event buses and multi consumer-group fan-out of one item</li>
            <li>Using the internal change log as an application event stream</li>
            <li>Downstream API rate admission as a core Fireweed feature</li>
          </ul>
          <p>
            Normative contracts:
            <a href="../helix/02-design/contracts/API-001-native-client-interface.md">API-001</a>,
            <a href="../helix/02-design/contracts/API-004-hot-projection-query-surface.md">API-004</a>,
            <a href="../helix/02-design/contracts/API-007-queue-claim-commit-contract.md">API-007</a>.
          </p>
        </div>
"""
    return layout(
        page="concepts.html",
        active="concepts",
        title="Concepts — Fireweed Queue",
        body=body,
        meta=meta,
    )


def page_get_started(meta: dict) -> str:
    embed = """git clone https://github.com/7thsense/fireweed.git
cd fireweed
# Requires rustup; toolchain from rust-toolchain.toml
cargo run -p fireweed --example queue_lifecycle"""
    body = f"""
        <div class="page-intro">
          <h2>Get started</h2>
          <p>
            Fireweed v{html.escape(meta["version"])} is the current workspace package
            identity. Clone the repository; crates.io publication is deferred.
          </p>
        </div>
        <div class="callout warn">
          <p>
            Prerequisites: Git, rustup, a C toolchain and CMake for native crypto
            dependencies. <code>Queue::ephemeral_disk</code> needs no services;
            production uses <code>Queue::s3_turso</code> with S3 and PostgreSQL.
          </p>
        </div>
        <div class="section-title"><h2>Embed Rust</h2></div>
        {code_block("clone and run the lifecycle example", embed, "sh")}
        <div class="prose">
          <p>
            The public entry point is <code>fireweed::queue::Queue</code>:
            <code>Queue::s3_turso</code> (production),
            <code>Queue::ephemeral_disk</code> (development and tests) and
            <code>Queue::postgres</code> (feature <code>postgres</code>, the reference store).
            Walk <a href="examples/basic-lifecycle.html">basic lifecycle</a> then
            <a href="examples/scheduled-delivery.html">scheduled delivery</a>.
          </p>
        </div>
        <div class="prose">
          <h2>Next</h2>
          <ul>
            <li><a href="concepts.html">Concepts</a> for the product contract</li>
            <li><a href="support.html">Support boundary</a> for supported profiles</li>
            <li><a href="api/index.html">API</a> for the public Rust surface</li>
          </ul>
        </div>
"""
    return layout(
        page="get-started.html",
        active="get-started",
        title="Get started — Fireweed Queue",
        body=body,
        meta=meta,
    )


def page_support(meta: dict) -> str:
    body = f"""
        <div class="page-intro">
          <h2>Support boundary</h2>
          <p>
            Supported means maintainers accept correctness reports against
            the documented contract and intend to preserve configuration compatibility
            within each supported 0.x minor line. It is not a 1.0 SemVer, SLA, capacity,
            or production-readiness claim.
          </p>
          <p>
            The public API is <code>fireweed::queue::Queue</code> (API-007). There is one
            production storage composition and one reference store.
          </p>
        </div>
        <table class="data">
          <thead><tr><th>Constructor</th><th>Status</th><th>Notes</th></tr></thead>
          <tbody>
            <tr><td><code>Queue::s3_turso</code></td><td><span class="badge ok">supported</span></td><td>Production: S3 object log (native conditional write), disposable local Turso projection per owned queue, PostgreSQL queue leases. Provider brands are not product SKUs.</td></tr>
            <tr><td><code>Queue::postgres</code></td><td><span class="badge ok">supported</span></td><td>Native PostgreSQL reference store; opt-in cargo feature <code>postgres</code>.</td></tr>
            <tr><td><code>Queue::ephemeral_disk</code></td><td><span class="badge neutral">development</span></td><td>Throwaway on-disk store for development and tests; nothing survives shutdown.</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>What this source tree ships</h2>
          <ul>
            <li>Workspace package identity v{html.escape(meta["version"])} (source tree; tag cut is a separate release step)</li>
            <li>The <code>Queue</code> Rust API (<code>fireweed::queue</code>)</li>
            <li>Issues-only contribution policy; MIT OR Apache-2.0</li>
          </ul>
          <h2>Explicitly deferred</h2>
          <ul>
            <li>crates.io package publication</li>
            <li>Universal performance bounds, multi-region failover, provider certification</li>
            <li>Remote / sync / MVCC Turso modes</li>
          </ul>
          <h2>Public examples</h2>
          <p>
            Microsite examples are excerpts of real tests for illustration. They are
            non-governing and never satisfy a required product or CI route. Required
            work is proven only by governing routes and gates.
          </p>
          <p>
            Full boundary:
            <a href="../helix/00-discover/public-preview-boundary.md">support policy</a>.
            Checklist:
            <a href="../helix/05-deploy/public-preview-checklist.md">release checklist</a>.
          </p>
        </div>
"""
    return layout(
        page="support.html",
        active="support",
        title="Support boundary — Fireweed Queue",
        body=body,
        meta=meta,
    )


def page_contribute(meta: dict) -> str:
    body = f"""
        <div class="page-intro">
          <h2>Contribute and support</h2>
          <p>
            Fireweed Queue is a maintainer-developed open-source project.
            Collaboration happens through issues—not code pull requests.
            Public copy and UI follow
            <a href="DESIGN.md">DESIGN.md</a> (brand voice and visual system).
          </p>
        </div>
        <div class="grid-2">
          <article class="card card-body">
            <h3>Issues are welcome</h3>
            <p>
              Bugs, feature requests, documentation problems, usage questions, and
              interoperability reports. Include version, storage configuration,
              expected vs actual result, and a minimal reproduction.
            </p>
            <p><a href="{html.escape(meta["repository"])}/issues">Open an issue →</a></p>
          </article>
          <article class="card card-body">
            <h3>Code contributions are not accepted</h3>
            <p>
              Pull requests, patches, and other code contributions are not accepted.
              No CLA or DCO applies while contributions are closed. Small reproduction
              snippets in issues are offered under {html.escape(meta["license"])}.
            </p>
          </article>
        </div>
        <div class="prose">
          <h2>Security</h2>
          <p>
            Do not disclose vulnerabilities in public issues. Use GitHub private
            vulnerability reporting on the repository Security tab. See
            <a href="../../SECURITY.md">SECURITY.md</a>.
          </p>
          <h2>Support</h2>
          <p>
            Support is best-effort through public issues with no SLA. See
            <a href="../../SUPPORT.md">SUPPORT.md</a> and
            <a href="../../CONTRIBUTING.md">CONTRIBUTING.md</a>.
          </p>
          <h2>License</h2>
          <p>
            Dual-licensed {html.escape(meta["license"])}. See
            <a href="../../LICENSE-MIT">LICENSE-MIT</a> and
            <a href="../../LICENSE-APACHE">LICENSE-APACHE</a>. Policy:
            <a href="../helix/02-design/adr/ADR-021-open-source-license-and-contribution-policy.md">ADR-021</a>.
          </p>
        </div>
"""
    return layout(
        page="contribute.html",
        active="home",
        title="Contribute — Fireweed Queue",
        body=body,
        meta=meta,
    )


def page_examples_index(meta: dict, examples: list[dict]) -> str:
    cards = []
    for ex in examples:
        tags = "".join(f'<span class="tag">{html.escape(t)}</span>' for t in ex.get("tags", []))
        cards.append(
            f"""
          <article class="card card-body">
            <h3><a href="{html.escape(ex["slug"])}.html">{html.escape(ex["title"])}</a></h3>
            <p>{html.escape(ex.get("summary", "").strip())}</p>
            <div class="tag-list">{tags}</div>
          </article>"""
        )
    body = f"""
        <div class="page-intro">
          <h2>Examples</h2>
          <p>
            Excerpts of the compiled <code>queue_lifecycle</code> example. Every code block
            is regenerated from source symbols; if the example moves, provenance checks fail.
          </p>
        </div>
        <div class="grid-2">
          {"".join(cards)}
        </div>
"""
    return layout(
        page="examples/index.html",
        active="examples",
        title="Examples — Fireweed Queue",
        body=body,
        meta=meta,
    )


def page_example(meta: dict, example: dict, examples: list[dict]) -> str:
    slug = example["slug"]
    src_path = EX_SRC / f"{slug}.rs"
    code = src_path.read_text(encoding="utf-8") if src_path.is_file() else "// missing excerpt\n"
    provenance = []
    for s in example["sources"]:
        provenance.append(f"{s['path']}::{s['symbol']}")
    prov = " · ".join(provenance)
    # next example
    idx = next(i for i, e in enumerate(examples) if e["slug"] == slug)
    nxt = examples[(idx + 1) % len(examples)]
    tags = "".join(f'<span class="tag">{html.escape(t)}</span>' for t in example.get("tags", []))
    body = f"""
        <div class="page-intro">
          <h2>{html.escape(example["title"])}</h2>
          <p>{html.escape(example.get("summary", "").strip())}</p>
        </div>
        <div class="example-meta">
          <span class="badge neutral">{html.escape(example.get("category", "example"))}</span>
          <span>Provenance: <code>{html.escape(prov)}</code></span>
        </div>
        <div class="tag-list" style="margin-bottom:16px">{tags}</div>
        {code_block(f"docs/site/examples/src/{slug}.rs", code)}
        <div class="prose">
          <p>
            Full sources live in the repository paths above. Regenerate excerpts with
            <code>python3 scripts/site/extract_examples.py</code>.
          </p>
          <p>
            <a href="index.html">All examples</a> ·
            Next: <a href="{html.escape(nxt["slug"])}.html">{html.escape(nxt["title"])}</a>
          </p>
        </div>
"""
    return layout(
        page=f"examples/{slug}.html",
        active="examples",
        title=f"{example['title']} — Fireweed Queue",
        body=body,
        meta=meta,
        description=example.get("summary", meta["descriptor"]).strip(),
    )


def page_api_index(meta: dict) -> str:
    body = """
        <div class="page-intro">
          <h2>API documentation</h2>
          <p>
            Fireweed exposes one public API: <code>Queue</code> in the Rust
            <code>fireweed</code> crate.
          </p>
        </div>
        <div class="grid-2">
          <article class="card card-body">
            <h3>Rust API</h3>
            <p>
              The <code>Queue</code> handle: constructors and the ten operations.
            </p>
            <p><a href="rust.html">Rust API guide →</a></p>
          </article>
          <article class="card card-body">
            <h3>Types</h3>
            <p>
              Request and result types re-exported through <code>fireweed::queue</code>.
            </p>
            <p><a href="types.html">Type catalog →</a></p>
          </article>
          <article class="card card-body">
            <h3>Generated rustdoc</h3>
            <p>
              For field-level signatures, generate local docs (crates.io deferred):
            </p>
            <p><code>cargo doc -p fireweed --no-deps --open</code></p>
          </article>
        </div>
        <div class="callout">
          <p>
            Adapter crates (<code>fireweed-turso</code>, <code>fireweed-objectlog</code>, …)
            implement storage; they are not standalone public APIs. Depend on
            <code>fireweed</code> only for embedding.
          </p>
        </div>
"""
    return layout(
        page="api/index.html",
        active="api",
        title="API — Fireweed Queue",
        body=body,
        meta=meta,
    )


def page_api_rust(meta: dict) -> str:
    body = """
        <div class="page-intro">
          <h2>Rust API</h2>
          <p>
            Normative shape: <a href="../../helix/02-design/contracts/API-007-queue-claim-commit-contract.md">API-007</a>.
          </p>
        </div>
        <div class="prose">
          <h2>Root type</h2>
          <p>
            <code>fireweed::queue::Queue</code> is a cloneable handle. Clones share storage and
            admission; <code>shutdown</code> closes admission and drains operations for all clones.
            The backend is chosen only at construction.
          </p>
          <h2>Constructors</h2>
        </div>
        <table class="data">
          <thead><tr><th>Constructor</th><th>Typical use</th><th>Support note</th></tr></thead>
          <tbody>
            <tr><td><code>Queue::s3_turso(S3TursoOptions, clock)</code></td><td>Production</td><td>Feature <code>s3-turso</code> (default)</td></tr>
            <tr><td><code>Queue::ephemeral_disk(clock)</code></td><td>Development and tests</td><td>Throwaway; same feature</td></tr>
            <tr><td><code>Queue::postgres(PostgresOptions, clock)</code></td><td>Reference store</td><td>Feature <code>postgres</code></td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Operations</h2>
        </div>
        <table class="data">
          <thead><tr><th>Method</th><th>Purpose</th><th>Example</th></tr></thead>
          <tbody>
            <tr><td><code>provision</code></td><td>Create a queue from a <code>QueueSchema</code></td><td><a href="../examples/basic-lifecycle.html">basic</a></td></tr>
            <tr><td><code>insert</code></td><td>Insert or upsert work, by request id</td><td><a href="../examples/basic-lifecycle.html">basic</a></td></tr>
            <tr><td><code>claim</code></td><td>Lease the next eligible items by filter and order</td><td><a href="../examples/scheduled-delivery.html">scheduled</a></td></tr>
            <tr><td><code>commit</code></td><td>Atomically settle claimed work, update state, insert work</td><td><a href="../examples/basic-lifecycle.html">basic</a></td></tr>
            <tr><td><code>preview</code></td><td>Evaluate a commit without applying it</td><td>API-007</td></tr>
            <tr><td><code>stats</code></td><td>Counts by lifecycle, group and bucket</td><td><a href="../examples/basic-lifecycle.html">basic</a></td></tr>
            <tr><td><code>read_state</code> / <code>query</code></td><td>Read state records; page through an index</td><td>API-007</td></tr>
            <tr><td><code>read_commit</code></td><td>Resolve an unknown outcome by request id</td><td>API-007</td></tr>
            <tr><td><code>shutdown</code></td><td>Close admission and drain</td><td><a href="../examples/basic-lifecycle.html">basic</a></td></tr>
          </tbody>
        </table>
        <div class="prose">
          <p>
            Generate signatures: <code>cargo doc -p fireweed --no-deps --open</code>.
          </p>
        </div>
"""
    return layout(
        page="api/rust.html",
        active="api",
        title="Rust API — Fireweed Queue",
        body=body,
        meta=meta,
    )


def page_api_types(meta: dict) -> str:
    body = """
        <div class="page-intro">
          <h2>Type catalog</h2>
          <p>
            Embedders import types from <code>fireweed::queue</code>, so downstream crates
            compile against <code>fireweed</code> alone.
          </p>
        </div>
        <table class="data">
          <thead><tr><th>Area</th><th>Types (representative)</th></tr></thead>
          <tbody>
            <tr><td>Handle</td><td><code>Queue</code>, <code>QueueFuture</code>, <code>QueueError</code>, <code>QueueResult</code></td></tr>
            <tr><td>Construction</td><td><code>S3TursoOptions</code>, <code>S3LogOptions</code>, <code>TursoCacheOptions</code>, <code>PostgresControlPlaneOptions</code>, <code>PostgresOptions</code>, <code>QueueLimits</code>, <code>QueueRuntimeOptions</code>, <code>ConfigSecret</code>, <code>SystemClock</code>, <code>Clock</code>, <code>OwnerId</code></td></tr>
            <tr><td>Schema</td><td><code>QueueAddress</code>, <code>QueueSchema</code>, <code>CollectionDefinition</code>, <code>QueueRetention</code>, <code>FieldType</code>, <code>FieldValue</code></td></tr>
            <tr><td>Work</td><td><code>NewWork</code>, <code>WorkRecord</code>, <code>ClaimedWork</code>, <code>LeaseCredential</code>, <code>Lifecycle</code></td></tr>
            <tr><td>Requests</td><td><code>ProvisionRequest</code>, <code>InsertRequest</code>, <code>ClaimRequest</code>, <code>CommitBatch</code>, <code>PreviewRequest</code>, <code>StatsRequest</code>, <code>StateReadRequest</code>, <code>QueryRequest</code>, <code>ReceiptReadRequest</code></td></tr>
            <tr><td>Commit</td><td><code>CommitGroup</code>, <code>ClaimedUpdate</code>, <code>Disposition</code>, <code>StateUpdate</code>, <code>CommitReceipt</code>, <code>GroupOutcome</code>, <code>Rejection</code></td></tr>
            <tr><td>Selection</td><td><code>Filter</code>, <code>Predicate</code>, <code>ClaimOrder</code></td></tr>
          </tbody>
        </table>
        <div class="prose">
          <p>
            For field layouts and trait bounds, run
            <code>cargo doc -p fireweed --no-deps --open</code>.
            Full contract: API-007.
          </p>
        </div>
"""
    return layout(
        page="api/types.html",
        active="api",
        title="Types — Fireweed Queue",
        body=body,
        meta=meta,
    )


def main() -> int:
    meta = load_meta()
    examples = load_examples()

    write("index.html", page_home(meta))
    write("why.html", page_why(meta))
    write("concepts.html", page_concepts(meta))
    write("get-started.html", page_get_started(meta))
    write("support.html", page_support(meta))
    write("contribute.html", page_contribute(meta))
    write("examples/index.html", page_examples_index(meta, examples))
    for ex in examples:
        write(f"examples/{ex['slug']}.html", page_example(meta, ex, examples))
    write("api/index.html", page_api_index(meta))
    write("api/rust.html", page_api_rust(meta))
    write("api/types.html", page_api_types(meta))
    print("render complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
