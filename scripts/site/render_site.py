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
    ("deploy/index.html", "Deploy", "deploy"),
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
          crates.io and GHCR deferred ·
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
            and final state in one queue contract. Embed the Rust facade or run a
            Redis Streams-shaped RESP worker path when FIFO append order is not the
            scheduling policy your work needs.
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

        <div class="section-title"><h2>Two entry points</h2></div>
        <div class="grid-2">
          <article class="card card-body">
            <h3>Rust library</h3>
            <p>
              Construct a backend-erased <code>Fireweed</code> handle, create queues,
              push work, claim batches, and finalize. Storage composition is a
              construction input—not a post-open type parameter.
            </p>
            <p><a href="api/rust.html">Rust API →</a> · <a href="examples/basic-lifecycle.html">Basic lifecycle →</a></p>
          </article>
          <article class="card card-body">
            <h3>RESP / Redis clients</h3>
            <p>
              Speak stock Streams commands (<code>XADD</code>, <code>XREADGROUP</code>,
              <code>XACK</code>, and related) against priority-ordered leased delivery.
            </p>
            <p><a href="api/resp.html">RESP API →</a> · <a href="examples/resp-drain.html">Redis drain example →</a></p>
          </article>
        </div>

        <div class="callout">
          <p>
            v{html.escape(meta["version"])} is a <strong>GitHub source tree</strong>
            (workspace package identity; annotated tag cut is a separate release step).
            Publication to crates.io and GHCR is deferred. The public product is the
            full 5×4 log×projection matrix with Turso as the default projection—see the
            <a href="support.html">support boundary</a>.
          </p>
        </div>

        <div class="section-title"><h2>Featured examples</h2></div>
        <div class="grid-2">
          <article class="card card-body">
            <h3>Open, push, claim, complete</h3>
            <p>Happy-path embed from the concrete facade product tests.</p>
            <p><a href="examples/basic-lifecycle.html">Read the example →</a></p>
          </article>
          <article class="card card-body">
            <h3>Scheduler boundary workflow</h3>
            <p>Discover active scopes, disperse workers, claim across queues.</p>
            <p><a href="examples/scheduler-boundary.html">Read the example →</a></p>
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
            Items are accepted (push/upsert), ordered by the queue’s priority model,
            claimed under a lease by a worker, then finalized:
            <strong>complete</strong>, <strong>retry</strong> / <strong>retry_after</strong>,
            <strong>release</strong>, or <strong>fail</strong>. Convenience verbs are
            batch-shaped aliases over the native finalize surface.
          </p>
          <p><a href="examples/basic-lifecycle.html">Example: basic lifecycle</a> ·
             <a href="api/rust.html">Rust lifecycle methods</a></p>

          <h2>Priority vs eligibility</h2>
          <p>
            <strong>Priority</strong> ranks eligible work. <strong>Eligibility</strong>
            decides whether an item may be claimed at all—gates, <code>not_before</code>,
            active leases, and retry backoff. Ineligible items must not starve forever
            under the queue’s progress bound.
          </p>
          <p><a href="examples/scheduled-delivery.html">Example: scheduled delivery</a> ·
             <a href="examples/priority-retry.html">Example: priority + retry</a></p>

          <h2>Leases and recovery</h2>
          <p>
            A claim grants temporary exclusive execution authority. Workers may renew
            leases without charging a new delivery. Expired leases reclaim to pending
            so another worker can continue. Fail marks a terminal dead-letter state;
            rearm can requeue with reset attempts when the product path allows it.
          </p>
          <p><a href="examples/lease-ops.html">Example: lease operations</a></p>

          <h2>Groups, cohorts, and discovery</h2>
          <p>
            Queues may group items so claims form downstream-compatible batches
            (account, connector, job, campaign). Schedulers discover active scopes,
            optionally disperse workers with a stateless selector, then claim—sometimes
            across queues with explicit non-atomic fan-in.
          </p>
          <p><a href="examples/scheduler-boundary.html">Example: scheduler boundary</a> ·
             <a href="examples/dispersion.html">Example: dispersion</a></p>

          <h2>External transaction contract</h2>
          <p>
            On supported durable profiles: a successful mutation is durable and visible;
            a rejected mutation has no durable effect; unknown outcomes are resolved by
            retained <code>request_id</code> replay (same body → same result;
            different body → <code>RequestIdConflict</code>).
          </p>

          <h2>Architecture</h2>
          <p>
            Two driving faces (Rust facade and RESP) sit on a shared engine.
            Durable profiles compose a <strong>command log</strong> with a
            <strong>projection</strong>. Log and projection are independent axes;
            unsupported pairings fail at startup instead of silently substituting.
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
            <a href="../helix/02-design/contracts/API-005-fireweed-rust-facade.md">API-005</a>.
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
# Requires rustup; toolchain from rust-toolchain.toml (Rust 1.97.1)
cargo test -p fireweed --test concrete_fireweed
cargo run -p fireweed --example scheduler_boundary"""
    resp = """# From repository root after building fireweed-service
# See README Quickstart for the in-memory RESP path with redis-cli.
cargo build -p fireweed-server
# Follow README for XADD / XREADGROUP / XACK against the local service."""
    body = f"""
        <div class="page-intro">
          <h2>Get started</h2>
          <p>
            Fireweed v{html.escape(meta["version"])} is the current workspace package
            identity. Clone the repository; crates.io and GHCR publication are deferred.
          </p>
        </div>
        <div class="callout warn">
          <p>
            Prerequisites: Git, rustup, a C toolchain and CMake for native crypto
            dependencies. Prefer <code>open(StorageConfig)</code> for the full matrix;
            convenience open helpers remain sugar over that model.
          </p>
        </div>
        <div class="section-title"><h2>Path A — embed Rust</h2></div>
        {code_block("clone and exercise the facade", embed, "sh")}
        <div class="prose">
          <p>
            The public root type is concrete <code>Fireweed</code>. Compose with
            <code>open</code> / <code>open_async(StorageConfig)</code> (Turso is the
            default projection), or use convenience helpers such as
            <code>open_memory</code>, <code>open_sqlite</code>, and
            <code>open_objectlog</code>. Walk
            <a href="examples/basic-lifecycle.html">basic lifecycle</a> then
            <a href="examples/scheduler-boundary.html">scheduler boundary</a>.
          </p>
        </div>
        <div class="section-title"><h2>Path B — Redis-shaped workers</h2></div>
        {code_block("build the RESP service", resp, "sh")}
        <div class="prose">
          <p>
            Stock Streams worker commands map onto Fireweed claim/finalize semantics.
            See the README quickstart for a full <code>redis-cli</code> session, and
            <a href="examples/resp-drain.html">the RESP drain example</a> extracted from e2e tests.
          </p>
          <p>
            Worked <strong>Python</strong> queue-management scenarios (docs + e2e + optional
            performance) live in the repository at
            <a href="{html.escape(meta['repository'])}/tree/main/examples/python-resp"><code>examples/python-resp/</code></a>
            (<code>run_e2e.py</code>, <code>run_perf.py</code>).
          </p>
          <h2>Next</h2>
          <ul>
            <li><a href="concepts.html">Concepts</a> for the product contract</li>
            <li><a href="support.html">Support boundary</a> for supported profiles</li>
            <li><a href="deploy/index.html">Deploy</a> for Helm and runtime axes</li>
            <li><a href="api/index.html">API</a> for the two public faces</li>
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
            Storage is the public 5×4 matrix: log
            (<code>memory</code> | <code>sqlite</code> | <code>postgres</code> |
            <code>filesystem</code> | <code>s3</code>) × projection
            (<code>memory</code> | <code>sqlite</code> | <code>turso</code> (default) |
            <code>postgres</code>). Hybrid and legacy profile names are not product values.
          </p>
        </div>
        <table class="data">
          <thead><tr><th>Log × projection</th><th>Status</th><th>Notes</th></tr></thead>
          <tbody>
            <tr><td><code>memory</code> × <code>memory</code> | <code>sqlite</code> | <code>turso</code> | <code>postgres</code></td><td><span class="badge ok">supported</span></td><td>Class B: durability limited to the projection after process death; no Class A log replay.</td></tr>
            <tr><td><code>sqlite</code> × all four projections</td><td><span class="badge ok">supported</span></td><td>Class A local durable log; projection as selected.</td></tr>
            <tr><td><code>postgres</code> × all four projections</td><td><span class="badge ok">supported</span></td><td>Class A; optional postgres cargo feature / image packaging must fail closed when omitted.</td></tr>
            <tr><td><code>filesystem</code> × all four projections</td><td><span class="badge ok">supported</span></td><td>Class A local/NAS object log; default deploy log axis with Turso projection.</td></tr>
            <tr><td><code>s3</code> × all four projections</td><td><span class="badge ok">supported</span></td><td>Class A object log; NativeConditionalWrite S3 only—provider brands are not product SKUs.</td></tr>
            <tr><td><code>turso</code> projection (any log)</td><td><span class="badge ok">supported default</span></td><td>Embedded/local Turso 0.7 WAL; public default when projection is unset. Remote/sync/MVCC modes are out of scope.</td></tr>
            <tr><td><code>hybrid</code> / <code>hybrid-async</code> / <code>hybrid-strict</code></td><td><span class="badge blocked">retired</span></td><td>Not public selectors; hard-rejected on env/Helm. Historical evidence only.</td></tr>
            <tr><td><code>objectlog</code> / <code>inmemory</code> aliases</td><td><span class="badge blocked">retired</span></td><td>Use <code>filesystem</code>|<code>s3</code> and <code>memory</code>.</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>What this source tree ships</h2>
          <ul>
            <li>Workspace package identity v{html.escape(meta["version"])} (source tree; tag cut is a separate release step)</li>
            <li>Concrete <code>Fireweed</code> Rust facade (<code>open</code> / <code>open_async(StorageConfig)</code>) and RESP service path</li>
            <li>Issues-only contribution policy; MIT OR Apache-2.0</li>
          </ul>
          <h2>Explicitly deferred</h2>
          <ul>
            <li>crates.io package publication</li>
            <li>GHCR container publication</li>
            <li>Universal performance bounds, multi-region failover, provider certification</li>
            <li>Historical query (<code>read_as_of</code>) as a supported facade API</li>
            <li>Remote / sync / MVCC Turso modes</li>
          </ul>
          <h2>Public examples</h2>
          <p>
            Microsite examples are excerpts of real tests for illustration. Example-only
            harness status strings (for example Python RESP <code>SKIP:</code> in
            optional local scenarios) are non-governing and never satisfy a required
            product or CI route. Required work is proven only by governing routes and gates.
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
            Curated from real tests and the public <code>scheduler_boundary</code>
            example. Every code block is regenerated from source symbols—if a test
            moves, provenance checks fail.
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
            Fireweed exposes two public faces over one queue model: the Rust
            <code>fireweed</code> facade and the RESP worker surface.
          </p>
        </div>
        <div class="grid-2">
          <article class="card card-body">
            <h3>Rust facade</h3>
            <p>
              Concrete <code>Fireweed</code> handle, constructors, lifecycle verbs,
              discovery, mutation, and hot projection queries.
            </p>
            <p><a href="rust.html">Rust API guide →</a></p>
          </article>
          <article class="card card-body">
            <h3>RESP / Streams</h3>
            <p>
              Stock Redis Streams worker path plus Fireweed-native live reads.
            </p>
            <p><a href="resp.html">RESP API guide →</a></p>
          </article>
          <article class="card card-body">
            <h3>Types</h3>
            <p>
              Public DTO export closure for embedders that depend only on
              <code>fireweed</code>.
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
            Adapter crates (<code>fireweed-sqlite</code>, <code>fireweed-objectlog</code>, …)
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
          <h2>Rust facade API</h2>
          <p>
            Normative shape: <a href="../../helix/02-design/contracts/API-005-fireweed-rust-facade.md">API-005</a>.
            Semantics: <a href="../../helix/02-design/contracts/API-001-native-client-interface.md">API-001</a>.
          </p>
        </div>
        <div class="prose">
          <h2>Root type</h2>
          <p>
            <code>pub struct Fireweed</code> — no type parameter. <code>Send + Sync</code>.
            Backend, projection, and coordination are construction inputs only.
            Optional <code>projection_control()</code> for disposable projections.
          </p>
          <h2>Constructors</h2>
        </div>
        <table class="data">
          <thead><tr><th>Constructor</th><th>Typical use</th><th>Support note</th></tr></thead>
          <tbody>
            <tr><td><code>open</code> / <code>open_async</code></td><td>Full matrix via <code>StorageConfig</code></td><td>Canonical 5×4 entry; Turso is the default projection</td></tr>
            <tr><td><code>open_memory</code></td><td>Memory log × memory projection</td><td>Class B convenience sugar</td></tr>
            <tr><td><code>open_sqlite</code> / <code>open_sqlite_*</code></td><td>SQLite log convenience helpers</td><td>Supported Class A cells</td></tr>
            <tr><td><code>open_objectlog</code> / <code>open_objectlog_*</code></td><td>Filesystem object-log helpers</td><td>Supported Class A; prefer typed <code>StorageConfig</code> for S3</td></tr>
            <tr><td><code>open_postgres</code> / <code>open_postgres_*</code></td><td>Postgres log convenience helpers</td><td>Supported Class A (feature-gated packaging fails closed)</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Method families</h2>
        </div>
        <table class="data">
          <thead><tr><th>Family</th><th>Methods (representative)</th><th>Example</th></tr></thead>
          <tbody>
            <tr><td>Queue control</td><td><code>create_queue</code>, <code>ensure_queue</code>, <code>queue_definition</code>, <code>ownership</code></td><td><a href="../examples/basic-lifecycle.html">basic</a></td></tr>
            <tr><td>Append</td><td><code>push</code>, <code>push_batch</code>, <code>push_with_request_id</code>, <code>upsert</code></td><td><a href="../examples/basic-lifecycle.html">basic</a></td></tr>
            <tr><td>Claim</td><td><code>claim</code>, <code>claim_with</code>, <code>claim_across_queues</code>, <code>claim_by_query</code></td><td><a href="../examples/multi-queue-claim.html">multi-queue</a></td></tr>
            <tr><td>Finalize</td><td><code>complete</code>, <code>ack</code>, <code>retry</code>, <code>retry_after</code>, <code>release</code>, <code>fail</code>, <code>commit</code></td><td><a href="../examples/lease-ops.html">leases</a></td></tr>
            <tr><td>Lease ops</td><td><code>renew</code>, <code>reassign</code>, <code>reclaim_expired</code></td><td><a href="../examples/lease-ops.html">leases</a></td></tr>
            <tr><td>Discovery</td><td><code>discover_active_scopes</code>, <code>discover_active_scopes_stamped</code></td><td><a href="../examples/scheduler-boundary.html">scheduler</a></td></tr>
            <tr><td>Mutation</td><td><code>update</code>, <code>update_fields</code>, <code>mutate_items</code>, <code>set_gates</code></td><td><a href="../examples/basic-lifecycle.html">basic</a></td></tr>
            <tr><td>Query / metrics</td><td><code>live_item</code>, <code>peek</code>, <code>metrics</code>, <code>range_scan</code>, <code>query_index</code></td><td>API-004</td></tr>
            <tr><td>Projection</td><td><code>projection_control().verify/delete/rebuild</code></td><td>object-log disposable projection</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Stateless helpers</h2>
          <ul>
            <li><code>QueueTemplate</code> — exact ensure / creation policy</li>
            <li><code>select_active_scope_from_prefix</code> / <code>OldestFirstScopePrefix</code> — dispersion</li>
          </ul>
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


def page_api_resp(meta: dict) -> str:
    py_examples = f"{html.escape(meta['repository'])}/tree/main/examples/python-resp"
    body = f"""
        <div class="page-intro">
          <h2>RESP worker API</h2>
          <p>
            Wire adapter design:
            <a href="../../helix/02-design/technical-designs/TD-006-resp-wire-adapter.md">TD-006</a>.
            Two faces rationale:
            <a href="../../helix/02-design/adr/ADR-007-hexagonal-architecture-and-two-interfaces.md">ADR-007</a>.
          </p>
        </div>
        <table class="data">
          <thead><tr><th>Command</th><th>Role</th><th>Notes</th></tr></thead>
          <tbody>
            <tr><td><code>XADD</code></td><td>Enqueue / mutate</td><td>Maps to append semantics under queue priority rules</td></tr>
            <tr><td><code>XREADGROUP &gt;</code></td><td>Claim / deliver</td><td>Leased delivery of eligible work</td></tr>
            <tr><td><code>XACK</code></td><td>Complete</td><td>Finalize claimed items</td></tr>
            <tr><td><code>XCLAIM</code> / <code>XAUTOCLAIM</code></td><td>Redeliver</td><td>Lease expiry recovery paths</td></tr>
            <tr><td><code>XLEN</code>, <code>XINFO</code>, <code>XPENDING</code>, <code>XRANGE</code></td><td>Inspect</td><td>Bounded-stale reads where applicable</td></tr>
            <tr><td><code>FW.MGET</code>, <code>FW.HGETALL</code>, <code>FW.HMGET</code></td><td>Native live read</td><td>Fireweed extensions beyond stock Streams</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Library-only on the wire</h2>
          <p>
            Filtered/cohort claim, gates, rich finalize, queue create, metrics/discovery,
            force purge, and repair remain library-primary; the RESP face is the
            tested Streams worker subset, not a full API-001 mirror.
          </p>
          <p><a href="../examples/resp-drain.html">Example: Redis client drain</a></p>
          <h2>Python examples and e2e</h2>
          <p>
            Runnable, commented scenarios for batch insert, pending update, due-batch
            claim, complete, status, and performance load live in
            <a href="{py_examples}"><code>examples/python-resp/</code></a>.
            They double as an evidence-capturing e2e harness over <code>redis-py</code>.
          </p>
        </div>
"""
    return layout(
        page="api/resp.html",
        active="api",
        title="RESP API — Fireweed Queue",
        body=body,
        meta=meta,
    )


def page_api_types(meta: dict) -> str:
    body = """
        <div class="page-intro">
          <h2>Type catalog</h2>
          <p>
            Embedders should import types from the <code>fireweed</code> crate root.
            The export closure is fixed by API-005 so downstream crates (e.g. Snorri)
            compile against <code>fireweed</code> alone.
          </p>
        </div>
        <table class="data">
          <thead><tr><th>Area</th><th>Types (representative)</th></tr></thead>
          <tbody>
            <tr><td>Handle</td><td><code>Fireweed</code>, <code>ProjectionControl</code>, capability/result types</td></tr>
            <tr><td>Identity</td><td><code>TenantId</code>, <code>QueueId</code>, <code>QueueKey</code>, <code>ItemId</code>, <code>WorkerId</code>, <code>OwnerId</code>, <code>ClientItemKey</code>, <code>RequestId</code></td></tr>
            <tr><td>Definition</td><td><code>QueueDefinition</code>, <code>PriorityModel</code>, <code>OrderingMode</code>, <code>RetryPolicy</code>, <code>EligibilityPolicy</code></td></tr>
            <tr><td>Write</td><td><code>NewItem</code>, <code>PriorityValue</code>, <code>ScheduleUpdate</code></td></tr>
            <tr><td>Claim</td><td><code>ClaimedItem</code>, <code>ClaimAt</code>, <code>MultiQueueClaimTarget</code>, <code>ClaimRef</code></td></tr>
            <tr><td>Commit</td><td><code>CommitRequest</code>, <code>CommitCapabilities</code>, side-record types</td></tr>
            <tr><td>Query</td><td><code>ItemView</code>, <code>QueueMetrics</code>, index query types, hot-projection flags</td></tr>
            <tr><td>Construction</td><td><code>ObjectLogRuntimeConfig</code>, <code>PostgresRuntimeConfig</code>, <code>ConfigSecret</code>, <code>SystemClock</code>, <code>Clock</code></td></tr>
            <tr><td>Helpers</td><td><code>QueueTemplate</code>, <code>ActiveScopeDiscovery</code>, <code>OldestFirstScopePrefix</code></td></tr>
            <tr><td>Errors</td><td><code>EngineError</code>, <code>EngineResult</code>, structured error variants</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <p>
            For field layouts and trait bounds, run
            <code>cargo doc -p fireweed --no-deps --open</code>.
            Full export list: API-005 export closure section.
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


def page_deploy(meta: dict) -> str:
    # Adapted operator console under product chrome
    body = f"""
        <div class="page-intro">
          <h2>Operator deploy console</h2>
          <p>
            v{html.escape(meta["version"])} is a source release. GHCR publication is deferred;
            use only deployment assets explicitly listed on the GitHub Release.
          </p>
        </div>
        <div class="callout warn">
          <p>
            Align production intent with the
            <a href="../support.html">support boundary</a> and
            <a href="../../helix/04-build/DEPLOYMENT-READINESS.md">deployment readiness</a>
            contracts. Public chart values match the 5×4 matrix; legacy
            <code>objectlog</code>/<code>inmemory</code>/<code>hybrid*</code> names fail schema validation.
          </p>
        </div>
        <div class="operator-grid">
          <section class="card" aria-labelledby="install-title">
            <div class="panel-heading">
              <h2 id="install-title">Install path</h2>
              <span class="badge warn">verify first</span>
            </div>
            <div class="commands">
              <div class="command-block">
                <div class="label"><span>release artifacts</span><span>checksums</span></div>
                <pre><code>OWNER=&lt;github-owner&gt;
REPO=fireweed
TAG=v{html.escape(meta["version"])}
VERSION="${{TAG#v}}"
DIST_DIR="release-${{TAG}}"

gh release download "$TAG" --repo "${{OWNER}}/${{REPO}}" \\
  --pattern "fireweed-${{VERSION}}-*.tar.gz" \\
  --pattern "fireweed-queue-${{VERSION}}.tgz" \\
  --pattern "fireweed-service-image.txt" \\
  --pattern "fireweed-queue-helm-chart.txt" \\
  --pattern "SHA256SUMS" \\
  --dir "$DIST_DIR"

(cd "$DIST_DIR" &amp;&amp; shasum -a 256 -c SHA256SUMS)</code></pre>
              </div>
              <div class="command-block">
                <div class="label"><span>helm install</span><span>filesystem × turso</span></div>
                <pre><code>NAMESPACE=fireweed
RELEASE=fireweed
IMAGE="ghcr.io/${{OWNER}}/fireweed-service"

kubectl create namespace "$NAMESPACE"

helm install "$RELEASE" "$DIST_DIR/fireweed-queue-${{VERSION}}.tgz" \\
  --namespace "$NAMESPACE" \\
  --set image.repository="$IMAGE" \\
  --set image.tag="$VERSION" \\
  --set storage.log.backend=filesystem \\
  --set storage.projection.backend=turso</code></pre>
              </div>
              <div class="command-block">
                <div class="label"><span>kind smoke</span><span>local proof</span></div>
                <pre><code>bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend turso</code></pre>
              </div>
            </div>
          </section>
          <div>
            <section class="card" aria-labelledby="storage-axes-title">
              <div class="panel-heading">
                <h2 id="storage-axes-title">Storage axes</h2>
                <span class="badge ok">orthogonal</span>
              </div>
              <div class="storage-axis-list">
                <article class="storage-axis">
                  <h3>log backend</h3>
                  <p>Public values: <code>memory</code>, <code>sqlite</code>, <code>postgres</code>, <code>filesystem</code> (chart default), <code>s3</code>. Object-log roots use <code>storage.log.objectLog.*</code> for filesystem/S3 only.</p>
                </article>
                <article class="storage-axis">
                  <h3>projection backend</h3>
                  <p>Public values: <code>memory</code>, <code>sqlite</code>, <code>turso</code> (default), <code>postgres</code>. Chart defaults render <code>FIREWEED_PROJECTION_BACKEND=turso</code> and <code>FIREWEED_TURSO_PROJECTION_PATH</code>.</p>
                </article>
              </div>
            </section>
            <section class="card" style="margin-top:18px" aria-labelledby="status-title">
              <div class="panel-heading">
                <h2 id="status-title">Support status (deploy)</h2>
                <span class="badge warn">not 1.0</span>
              </div>
              <div class="status-list">
                <article class="status-row">
                  <h3><span class="badge ok">supported</span> full 5×4 matrix</h3>
                  <p>All twenty log×projection cells are preview-supported. Class B memory-log cells carry a durability disclaimer only.</p>
                </article>
                <article class="status-row">
                  <h3><span class="badge ok">default</span> filesystem × turso</h3>
                  <p>Chart and server defaults select local/NAS object log with embedded Turso projection.</p>
                </article>
                <article class="status-row">
                  <h3><span class="badge warn">verify</span> release artifacts</h3>
                  <p>GHCR deferred: only install images/charts listed on the GitHub Release for the tag.</p>
                </article>
              </div>
            </section>
          </div>
        </div>
        <div class="split">
          <section class="card" aria-labelledby="topology-title">
            <div class="panel-heading"><h2 id="topology-title">Runtime topology</h2></div>
            <div class="diagram" aria-hidden="true">
              <svg viewBox="0 0 760 260" role="img">
                <rect x="20" y="30" width="160" height="70" rx="6" fill="#fffffb" stroke="#161616"/>
                <text x="48" y="62">Helm release</text>
                <text x="48" y="84" class="thin">charts/fireweed-queue</text>
                <path d="M180 65 H285" stroke="#161616" stroke-width="3"/>
                <path d="M275 55 L295 65 L275 75" fill="none" stroke="#161616" stroke-width="3"/>
                <rect x="295" y="30" width="170" height="70" rx="6" fill="#dcefe4" stroke="#146c43"/>
                <text x="322" y="62">fireweed-service</text>
                <text x="322" y="84" class="thin">RESP TCP</text>
                <path d="M465 65 H575" stroke="#161616" stroke-width="3"/>
                <path d="M565 55 L585 65 L565 75" fill="none" stroke="#161616" stroke-width="3"/>
                <rect x="585" y="30" width="155" height="70" rx="6" fill="#fff0d8" stroke="#b45309"/>
                <text x="620" y="62">Projection</text>
                <text x="620" y="84" class="thin">turso default</text>
                <path d="M380 100 V158" stroke="#161616" stroke-width="3"/>
                <path d="M370 148 L380 168 L390 148" fill="none" stroke="#161616" stroke-width="3"/>
                <rect x="225" y="168" width="170" height="62" rx="6" fill="#fde2e2" stroke="#b91c1c"/>
                <text x="255" y="194">Durable log</text>
                <text x="255" y="216" class="thin">filesystem / s3 / …</text>
                <rect x="425" y="168" width="185" height="62" rx="6" fill="#fffffb" stroke="#161616"/>
                <text x="455" y="194">Projection store</text>
                <text x="455" y="216" class="thin">storage mount</text>
                <path d="M395 198 H425" stroke="#161616" stroke-width="3"/>
              </svg>
            </div>
          </section>
          <section class="card" aria-labelledby="docs-title">
            <div class="panel-heading"><h2 id="docs-title">Runbooks</h2></div>
            <div class="doc-list">
              <article class="doc-row">
                <h3><a href="../../deployment/operator-guide.md">Operator guide</a></h3>
                <p>Helm install, upgrade, uninstall, values, troubleshooting.</p>
              </article>
              <article class="doc-row">
                <h3><a href="../../deployment/container-runtime-contract.md">Runtime contract</a></h3>
                <p>Environment variables, storage axes, health probes.</p>
              </article>
              <article class="doc-row">
                <h3><a href="../../deployment/operator-release-artifacts.md">Release artifacts</a></h3>
                <p>Images, charts, archives, checksum verification.</p>
              </article>
              <article class="doc-row">
                <h3><a href="../../helix/04-build/DEPLOYMENT-READINESS.md">Readiness contract</a></h3>
                <p>Formal production gate and certification boundary.</p>
              </article>
              <article class="doc-row">
                <h3><a href="../../deployment/helm-static-validation.md">Helm gate</a></h3>
                <p><code>bash scripts/ci/helm-gate.sh</code></p>
              </article>
              <article class="doc-row">
                <h3><a href="../../deployment/kind-helm-integration.md">kind smoke</a></h3>
                <p>kind RESP smoke for public log×projection pairs (Turso default).</p>
              </article>
            </div>
          </section>
        </div>
"""
    return layout(
        page="deploy/index.html",
        active="deploy",
        title="Deploy — Fireweed Queue",
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
    write("api/resp.html", page_api_resp(meta))
    write("api/types.html", page_api_types(meta))
    write("deploy/index.html", page_deploy(meta))
    print("render complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
