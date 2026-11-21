#!/usr/bin/env python3
"""Render the Fireweed microsite under docs/site/.

Example pages are built from the documents listed in
docs/site/_meta/example-manifest.yaml, shown unchanged.
check_example_provenance.py renders every page in memory and compares it
with the committed file.
"""

from __future__ import annotations

import html
import json
import os
import re
from pathlib import Path

import yaml

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
    ("api/index.html", "Reference", "api"),
    ("support.html", "Support", "support"),
]

DOC_PATHS = {
    "api008": "docs/helix/02-design/contracts/API-008-data-plane-dsl.md",
    "adr027": "docs/helix/02-design/adr/ADR-027-data-plane-interface-is-the-implementation-boundary.md",
    "tests": "crates/fireweed-dsl-postgres/tests/executor.rs",
    "security": "SECURITY.md",
    "support": "SUPPORT.md",
    "contributing": "CONTRIBUTING.md",
    "license_mit": "LICENSE-MIT",
    "license_apache": "LICENSE-APACHE",
}


def load_meta() -> dict:
    return json.loads(META_PATH.read_text(encoding="utf-8"))


def load_manifest() -> dict:
    return yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))


def gh(meta: dict, key: str) -> str:
    """A repository file on GitHub's main branch."""
    return f"{meta['repository']}/blob/main/{DOC_PATHS[key]}"


def rel_to(page: str, target: str) -> str:
    """Relative path from page to target, both under docs/site/."""
    return os.path.relpath(
        str(Path("docs/site") / target),
        str(Path("docs/site") / Path(page).parent),
    ).replace("\\", "/")


def asset(page: str, name: str) -> str:
    return rel_to(page, f"assets/{name}")


def inline(text: str) -> str:
    """Escape prose and turn `code` spans into <code>."""
    escaped = html.escape(" ".join(text.split()))
    return re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)


def nav_html(page: str, active: str) -> str:
    items = []
    for target, label, key in NAV:
        current = ' aria-current="page"' if key == active else ""
        items.append(f'<a href="{rel_to(page, target)}"{current}>{html.escape(label)}</a>')
    return "\n        ".join(items)


def wrap_tables(body: str) -> str:
    """Let data tables scroll inside a wrapper on narrow viewports."""
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
    repo = meta["repository"]
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
          <h1>{html.escape(meta["product"])}</h1>
          <span>{html.escape(meta["badge"])}</span>
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
          {html.escape(meta["product"])} · {html.escape(meta["status"])} ·
          issues welcome · code contributions not accepted ·
          {html.escape(meta["license"])}.
        </p>
        <p>
          Source and documents:
          <a href="{html.escape(repo)}">{html.escape(repo.removeprefix("https://"))}</a>.
          Full DSL reference: <a href="{gh(meta, "api008")}">API-008</a>.
          Voice and style: <a href="{rel_to(page, "DESIGN.md")}">DESIGN.md</a>.
          See <a href="{rel_to(page, "contribute.html")}">contribute</a> and
          <a href="{rel_to(page, "support.html")}">support</a>.
        </p>
      </footer>
    </div>
    <script src="{asset(page, "site.js")}"></script>
  </body>
</html>
"""


def code_block(label: str, code: str, lang: str) -> str:
    return f"""<div class="code-block">
  <div class="label"><span>{html.escape(label)}</span><span>{html.escape(lang)}</span></div>
  <pre><code>{html.escape(code.rstrip())}</code></pre>
</div>"""


def source(rel: str) -> str:
    return (EX_SRC / rel).read_text(encoding="utf-8")


def page_home(meta: dict, examples: list[dict]) -> str:
    cards = "".join(
        f"""
          <article class="card card-body">
            <h3>{html.escape(ex["title"])}</h3>
            <p>{inline(ex["summary"])}</p>
            <p><a href="examples/{html.escape(ex["slug"])}.html">Read the example →</a></p>
          </article>"""
        for ex in examples
    )
    body = f"""
        <section class="hero hero-home">
          <div class="hero-copy">
          <div class="badge-row">
            <span class="badge warn">data-plane DSL</span>
            <span class="badge neutral">PostgreSQL 18</span>
            <span class="badge neutral">{html.escape(meta["status"])}</span>
          </div>
          <h2>{html.escape(meta["descriptor"])}</h2>
          <p class="lede">
            Declare a space's queues, tables and reads in a schema document.
            Change them with programs: lists of typed statements, such as claim,
            settle, update and select, that run in order as one transaction.
            The PostgreSQL executor turns each statement into one SQL statement
            and keeps nothing in the process between calls.
          </p>
          <div class="hero-actions">
            <a class="btn" href="get-started.html">Get started</a>
            <a class="btn secondary" href="why.html">Why Fireweed</a>
            <a class="btn secondary" href="examples/index.html">Browse examples</a>
          </div>
          </div>
          <aside class="signal-panel" aria-label="From documents to SQL">
            <span class="signal-label">Documents to SQL</span>
            <svg class="signal-mark" viewBox="0 0 360 360" role="img" aria-label="Branch Signal mark">
              <path d="M180 322V38M180 108 74 170m106 8 106-62M180 228 49 303m131-35 131-76" fill="none" stroke="#f8f5eb" stroke-width="5" stroke-linecap="square"/>
              <circle cx="180" cy="38" r="28" fill="#c13d74" stroke="#17211d" stroke-width="3"/>
              <circle cx="74" cy="170" r="17" fill="#0f766e" stroke="#17211d" stroke-width="3"/>
              <circle cx="286" cy="116" r="17" fill="#92ad61" stroke="#17211d" stroke-width="3"/>
              <circle cx="49" cy="303" r="12" fill="#d87950" stroke="#17211d" stroke-width="3"/>
              <circle cx="311" cy="192" r="12" fill="#f5c85b" stroke="#17211d" stroke-width="3"/>
            </svg>
            <div class="signal-states"><span>schema</span><span>program</span><span>check</span><span>SQL</span></div>
          </aside>
        </section>

        <div class="section-title"><h2>Two documents</h2></div>
        <div class="grid-3">
          <article class="card card-body">
            <h3>Schema</h3>
            <p>
              Queues, tables and sequenced tables with typed fields, indexes,
              merge rules, retention and named, versioned reads. Each object
              becomes a real table. Changes only add.
            </p>
          </article>
          <article class="card card-body">
            <h3>Program</h3>
            <p>
              <code>insert</code>, <code>claim</code>, <code>settle</code>,
              <code>renew</code>, <code>update</code>, <code>delete</code>,
              <code>assert</code>, <code>select</code>, <code>read</code>,
              <code>pause</code> and <code>resume</code>. Predicates and
              assignments are typed data, never SQL text.
            </p>
          </article>
          <article class="card card-body">
            <h3>Checked, then lowered</h3>
            <p>
              A program is checked against the schema: names resolve, types match
              with no implicit casts, and every selection uses an index. Then each
              statement becomes one SQL statement with bound parameters.
            </p>
          </article>
        </div>

        <div class="section-title"><h2>The database does the work</h2></div>
        <div class="grid-3">
          <article class="card card-body">
            <h3>Indexes choose rows</h3>
            <p>
              A <code>where</code> must test an indexed column. A claim reads the
              <code>(due_at, priority, id)</code> claim index, so it reads about
              <code>limit</code> entries however many items are sleeping.
            </p>
          </article>
          <article class="card card-body">
            <h3>Row locks order writers</h3>
            <p>
              Claims skip rows other transactions hold. Guards lock the rows they
              count, so a counted row can't change before the program commits.
            </p>
          </article>
          <article class="card card-body">
            <h3>Its clock decides what is due</h3>
            <p>
              Due items, lapsed leases and retention are conditions on stored
              times, read against the database's clock. No caller drives a tick.
            </p>
          </article>
        </div>

        <div class="callout">
          <p>
            PostgreSQL 18 is the first and currently only executor. The crates
            <code>fireweed-dsl</code> and <code>fireweed-dsl-postgres</code> are not
            published to crates.io; build them from the repository. See
            <a href="support.html">support</a>.
          </p>
        </div>

        <div class="section-title"><h2>Examples</h2></div>
        <div class="grid-3">{cards}
        </div>
"""
    return layout(
        page="index.html",
        active="home",
        title="Fireweed — data-plane DSL for queues and tables",
        body=body,
        meta=meta,
    )


def page_why(meta: dict) -> str:
    body = f"""
        <div class="page-intro">
          <h2>Why Fireweed</h2>
          <p>
            Fireweed is the data plane for Snorri and Cayce. It holds the durable
            workflow data that is high in volume or rate of change: work items,
            workflow state, per-recipient state, admission chunk state, operational
            events and timelines. Each kind gets a declared shape and a real table,
            so a consumer doesn't keep it in process memory or rewrite one large
            JSON document on every change.
          </p>
        </div>
        <table class="data">
          <thead>
            <tr><th>Need</th><th>In Fireweed</th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Work that becomes due, is leased and finishes</td>
              <td>A queue. <code>claim</code> leases due items in <code>(due_at, priority, id)</code> order; <code>settle</code> ends a lease as <code>complete</code>, <code>fail</code>, <code>retry</code> or <code>release</code>.</td>
            </tr>
            <tr>
              <td>Keyed state that must not go backwards</td>
              <td>A table with merge rules: <code>add_only</code> sets and <code>non_decreasing</code> numbers and times.</td>
            </tr>
            <tr>
              <td>History in commit order</td>
              <td>A sequenced table: ids are gapless and in commit order, and rows are never updated.</td>
            </tr>
            <tr>
              <td>A change across several objects</td>
              <td>One program. Its statements run in order in one transaction; a rejection rolls all of them back.</td>
            </tr>
            <tr>
              <td>Compare-and-set and guards</td>
              <td><code>update</code> with <code>expect</code>, and <code>assert</code>, which counts rows and writes nothing.</td>
            </tr>
            <tr>
              <td>Retrying a write whose outcome is unknown</td>
              <td>A request id. A replay returns the recorded response; the same id with a different program fails with <code>request_id_conflict</code>.</td>
            </tr>
            <tr>
              <td>Counts and lists for a UI</td>
              <td><code>select</code> with <code>aggregate</code> over an index, and declared reads.</td>
            </tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>A space is the unit of atomic change</h2>
          <p>
            A space is one atomic domain owned by one tenant, never wider than one
            workflow or one campaign. A program runs in one space. Snorri uses one
            space per workflow, holding all its runs; a run is an indexed
            <code>run</code> field on its items and rows.
          </p>
          <h2>Outside Fireweed</h2>
          <ul>
            <li>The control plane: tenants, campaign definitions and configuration, in its own PostgreSQL.</li>
            <li>Exhaust: logs, traces, full history and analytics across data sets go to Niflheim.</li>
            <li>State a tenant shares across all its spaces, such as send caps and rate windows.</li>
            <li>Large payloads: a field holds a reference.</li>
            <li>SQL from consumers, and joins. A lookup inside a <code>where</code> tests existence only.</li>
          </ul>
          <p>
            The decisions behind this boundary are in
            <a href="{gh(meta, "adr027")}">ADR-027</a>.
          </p>
        </div>
"""
    return layout(
        page="why.html",
        active="why",
        title="Why Fireweed",
        body=body,
        meta=meta,
        description="What Fireweed holds for its consumers, and what stays outside it.",
    )


def page_concepts(meta: dict) -> str:
    body = f"""
        <div class="page-intro">
          <h2>Concepts</h2>
          <p>
            Spaces, shapes, programs, time and request ids. The
            <a href="api/index.html">reference</a> lists every key and statement;
            <a href="{gh(meta, "api008")}">API-008</a> is the full specification.
          </p>
        </div>
        <div class="prose">
          <h2>Shapes</h2>
        </div>
        <table class="data">
          <thead><tr><th>Shape</th><th>Columns</th><th>Holds</th></tr></thead>
          <tbody>
            <tr><td>Queue</td><td><code>id</code>, <code>key</code>, <code>state</code>, <code>priority</code>, <code>due_at</code>, <code>attempts</code>, <code>version</code>, <code>lease_owner</code>, timestamps and a JSON <code>body</code>, then the declared fields</td><td>Work items: sends, workflow instance heads, effects, external events deduplicated by key</td></tr>
            <tr><td>Table</td><td><code>id</code>, <code>version</code>, <code>created_at</code>, <code>updated_at</code>, <code>expires_at</code> with <code>expire_after_write</code>, then the declared fields; the <code>key</code> fields get a unique index</td><td>Keyed state, idempotency records, admission chunks, suppression flags, counters</td></tr>
            <tr><td>Sequenced table</td><td>A table without <code>key</code>, whose <code>id</code> comes from a counter row in the same transaction</td><td>Timelines and transition history</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <p>
            Field types are <code>text</code>, <code>int</code>, <code>float</code>,
            <code>bool</code>, <code>timestamp</code>, <code>json</code> and
            <code>text_set</code>. Every declared field may be null.
            <code>index: true</code> creates an index on a field, a GIN index for a
            <code>text_set</code>. A table field may declare a merge rule:
            <code>add_only</code> for a set, <code>non_decreasing</code> for a number
            or time. An update that breaks it rejects the program unless it sets
            <code>override_merge: true</code>.
          </p>
          <p>
            Schema changes only add: queues and tables with any indexes, unindexed
            fields on existing objects, and reads or read versions. They can also
            raise <code>request_retention</code> and a queue's <code>retention</code>.
            A field added to a live object can't be indexed, because building the
            index would block the object's writes.
          </p>

          <h2>Queue items</h2>
          <p>
            An item is <code>pending</code>, <code>leased</code>, <code>complete</code>
            or <code>failed</code>. <code>due_at</code> is when it next needs
            attention: when a pending item becomes claimable, or when a lease
            expires. A claim leases due items in <code>(due_at, priority, id)</code>
            order, so priority breaks ties among items due at the same instant.
          </p>
          <ul>
            <li>Each claim charges an attempt and returns a random lease token.</li>
            <li><code>settle</code> and <code>renew</code> address an item by id and token. A wrong token or a lapsed lease reports <code>stale</code> and leaves the item alone.</li>
            <li>A lapsed lease makes the item claimable again if it has attempts left; otherwise it counts as <code>failed</code>. <code>max_attempts: unlimited</code> turns the bound off.</li>
            <li><code>release</code> gives the claim's attempt back, so a timer that charges nothing is a <code>release</code> settle with <code>after</code>.</li>
            <li>Every change gives the item a new <code>version</code> from a sequence per space, so a key's versions never repeat, even after a delete and a reinsert.</li>
          </ul>

          <h2>Every selection uses an index</h2>
          <p>
            A <code>where</code> in <code>select</code>, <code>update</code>,
            <code>delete</code> and <code>assert</code> needs an access term: an
            <code>eq</code>, <code>in</code> or <code>contains</code> between an
            indexed column and a value. Ranges narrow an access term but aren't
            one. Selecting by <code>ids</code> or <code>keys</code> needs no
            <code>where</code>, and an aggregate whose columns one btree index holds
            reads that index. Every statement has a limit, and every transaction
            runs under a 5 s statement timeout.
          </p>

          <h2>Time is the database's clock</h2>
          <p>
            Every statement reads the time from <code>fireweed.now()</code>, the
            transaction's start time. Tests set the session setting
            <code>fireweed.now</code> to fix an instant. Time-driven changes are
            conditions on stored times: a pending item is claimable once
            <code>due_at &lt;= now</code>, and every statement that reads
            <code>state</code> sees the effective state. <code>sweep</code> writes
            effective states back and deletes finished items past retention,
            expired table rows and old request records. Results don't depend on
            when it runs.
          </p>

          <h2>One program, one transaction</h2>
          <p>
            Statements run in order, and each sees the effects of those before it.
            A statement can reject the program: <code>key_conflict</code>,
            <code>stale_lease</code>, <code>limit_exceeded</code>,
            <code>expect_unmet</code> or <code>merge_violation</code>. Then every
            statement rolls back and the response names the statement and reason.
            Programs run at <code>READ COMMITTED</code>; guards lock the rows they
            count.
          </p>
          <p>
            A program that writes carries a <code>request_id</code> and the
            <code>issued_at</code> time the caller created it, both resent unchanged
            on every retry. A replay returns the recorded response, rejections
            included. A request issued before <code>now - request_retention</code>
            fails with <code>request_expired</code> and has no effect, so a request
            whose record may be gone is refused, never run twice.
          </p>

          <h2>Durability</h2>
          <p>
            On PostgreSQL, <code>durable</code> (the default) is a normal commit to
            a server assumed fully replicated. <code>fire_and_forget</code> commits
            with <code>synchronous_commit = off</code>. The response reports the
            level.
          </p>
        </div>
"""
    return layout(
        page="concepts.html",
        active="concepts",
        title="Concepts — Fireweed",
        body=body,
        meta=meta,
    )


def page_get_started(meta: dict, manifest: dict) -> str:
    clone = """git clone https://github.com/7thsense/fireweed.git
cd fireweed
# A PostgreSQL 18 server for the executor's tests; any reachable one works.
docker run --rm -d --name fireweed-pg -e POSTGRES_PASSWORD=fireweed \\
  -p 127.0.0.1:5432:5432 postgres:18
FIREWEED_PG_TEST_URL=postgres://postgres:fireweed@127.0.0.1:5432/postgres \\
  cargo test --workspace"""
    deps = """[dependencies]
fireweed-dsl = { git = "https://github.com/7thsense/fireweed" }
fireweed-dsl-postgres = { git = "https://github.com/7thsense/fireweed" }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tokio-postgres = "0.7\""""
    snippet = manifest["snippets"][0]
    schema = manifest["schema"]
    body = f"""
        <div class="page-intro">
          <h2>Get started</h2>
          <p>
            Clone the repository, run the executor's tests against PostgreSQL 18,
            then run your own schema and programs from Rust.
          </p>
        </div>
        <div class="callout warn">
          <p>
            Prerequisites: Git, <a href="https://rustup.rs/">rustup</a> (the
            repository selects Rust 1.99.0 in <code>rust-toolchain.toml</code>) and
            a PostgreSQL 18 server. Without <code>FIREWEED_PG_TEST_URL</code> the
            executor's tests skip.
          </p>
        </div>
        <div class="section-title"><h2>1. Run the tests</h2></div>
        {code_block("clone and test", clone, "sh")}
        <div class="prose">
          <p>
            The tests in
            <a href="{gh(meta, "tests")}"><code>crates/fireweed-dsl-postgres/tests/executor.rs</code></a>
            run schema and program documents against PostgreSQL and check the
            responses and the rows left behind. Each test uses its own space.
          </p>
        </div>
        <div class="section-title"><h2>2. Run a program from Rust</h2></div>
        <div class="prose">
          <p>The crates aren't on crates.io; depend on the repository.</p>
        </div>
        {code_block("Cargo.toml", deps, "toml")}
        {code_block(f"docs/site/examples/src/{snippet}", source(snippet), "rust")}
        <div class="prose">
          <p>
            For <code>schema.yaml</code>, use the examples'
            <a href="examples/src/{html.escape(schema)}">shared schema</a>; for
            <code>program.yaml</code>, any program from the
            <a href="examples/index.html">examples</a>. Set <code>issued_at</code>
            to the current time: a request issued more than
            <code>request_retention</code> (default one day) ago fails with
            <code>request_expired</code>, and one more than 5 minutes ahead of the
            database clock is <code>invalid</code>.
          </p>
          <h2>Next</h2>
          <ul>
            <li><a href="concepts.html">Concepts</a>: shapes, leases, time and request ids</li>
            <li><a href="api/index.html">Reference</a>: every key, statement and limit</li>
            <li><a href="api/rust.html">Rust crates</a>: the executor's functions and types</li>
          </ul>
        </div>
"""
    return layout(
        page="get-started.html",
        active="get-started",
        title="Get started — Fireweed",
        body=body,
        meta=meta,
    )


def page_support(meta: dict) -> str:
    body = f"""
        <div class="page-intro">
          <h2>Status and support</h2>
          <p>
            Fireweed is two crates in one repository. PostgreSQL 18 is the first and
            currently only executor.
          </p>
        </div>
        <table class="data">
          <thead><tr><th>Component</th><th>Status</th><th>Notes</th></tr></thead>
          <tbody>
            <tr><td><code>fireweed-dsl</code></td><td><span class="badge ok">in repository</span></td><td>Schema and program types, YAML and JSON reading, and the checker. No I/O.</td></tr>
            <tr><td><code>fireweed-dsl-postgres</code></td><td><span class="badge ok">in repository</span></td><td>The executor for PostgreSQL 18: DDL from a schema, and one SQL statement per DSL statement. Its tests run on PostgreSQL 18.</td></tr>
            <tr><td>crates.io</td><td><span class="badge blocked">not published</span></td><td>Both crates set <code>publish = false</code>. Depend on the git repository.</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>What the executor's tests cover</h2>
          <ul>
            <li>The DDL and indexes each schema creates, and additive changes.</li>
            <li>Every statement, rejections rolling back while keeping the request record, replay and fingerprint conflicts.</li>
            <li>Concurrent claims from several connections never leasing an item twice.</li>
            <li>Lease expiry, reclaim and attempt exhaustion under a fixed <code>fireweed.now</code>.</li>
            <li>Merge rules, expectations and stepped updates.</li>
            <li>That claims, indexed selects, keyed updates, grouped counts and sweeps over 5,000 items do no sequential scans of the table.</li>
            <li>That requests issued before the space's horizon are refused, also after their records are swept.</li>
            <li>That programs in a batch commit, reject or fail independently.</li>
          </ul>
          <h2>Not in v1</h2>
          <ul>
            <li>Cohorts, group keys and per-group ordering.</li>
            <li>Dynamic gates and metadata blockers.</li>
            <li>Recurrence beyond <code>release</code> with <code>after</code>.</li>
            <li>Joins.</li>
            <li>Maintained counts by state: an aggregate over a whole queue needs an index-backed condition.</li>
          </ul>
          <h2>Support</h2>
          <p>
            Support is best-effort through public issues, with no service-level
            agreement. See <a href="{gh(meta, "support")}">SUPPORT.md</a> and
            <a href="contribute.html">contribute</a>.
          </p>
        </div>
"""
    return layout(
        page="support.html",
        active="support",
        title="Support — Fireweed",
        body=body,
        meta=meta,
    )


def page_contribute(meta: dict) -> str:
    body = f"""
        <div class="page-intro">
          <h2>Contribute</h2>
          <p>
            Fireweed is a maintainer-developed open-source project. Collaboration
            happens through issues, not code pull requests.
          </p>
        </div>
        <div class="grid-2">
          <article class="card card-body">
            <h3>Issues are welcome</h3>
            <p>
              Bugs, feature requests, documentation problems and usage questions.
              Include the schema and program documents, the PostgreSQL version, the
              expected and actual response, and a minimal reproduction.
            </p>
            <p><a href="{html.escape(meta["repository"])}/issues">Open an issue →</a></p>
          </article>
          <article class="card card-body">
            <h3>Code contributions are not accepted</h3>
            <p>
              Pull requests, patches and other code contributions are not accepted.
              Small reproduction snippets in issues are offered under
              {html.escape(meta["license"])}.
            </p>
          </article>
        </div>
        <div class="prose">
          <h2>Security</h2>
          <p>
            Do not disclose vulnerabilities in public issues. Use GitHub private
            vulnerability reporting on the repository's Security tab. See
            <a href="{gh(meta, "security")}">SECURITY.md</a>.
          </p>
          <h2>Policies</h2>
          <p>
            <a href="{gh(meta, "contributing")}">CONTRIBUTING.md</a> ·
            <a href="{gh(meta, "support")}">SUPPORT.md</a> ·
            dual-licensed {html.escape(meta["license"])}:
            <a href="{gh(meta, "license_mit")}">LICENSE-MIT</a>,
            <a href="{gh(meta, "license_apache")}">LICENSE-APACHE</a>.
          </p>
        </div>
"""
    return layout(
        page="contribute.html",
        active="home",
        title="Contribute — Fireweed",
        body=body,
        meta=meta,
    )


def page_examples_index(meta: dict, manifest: dict) -> str:
    cards = []
    for ex in manifest["examples"]:
        tags = "".join(f'<span class="tag">{html.escape(t)}</span>' for t in ex.get("tags", []))
        cards.append(
            f"""
          <article class="card card-body">
            <h3><a href="{html.escape(ex["slug"])}.html">{html.escape(ex["title"])}</a></h3>
            <p>{inline(ex["summary"])}</p>
            <div class="tag-list">{tags}</div>
          </article>"""
        )
    schema = manifest["schema"]
    body = f"""
        <div class="page-intro">
          <h2>Examples</h2>
          <p>
            Every example runs against one space, declared by the shared schema
            below. Each document on these pages is a file under
            <code>docs/site/examples/src/</code>, shown unchanged; the site gate
            checks it against the schema and API-008's grammar.
          </p>
        </div>
        <div class="grid-3">
          {"".join(cards)}
        </div>
        <div class="section-title"><h2>Shared schema</h2></div>
        {code_block(f"docs/site/examples/src/{schema}", source(schema), "yaml")}
"""
    return layout(
        page="examples/index.html",
        active="examples",
        title="Examples — Fireweed",
        body=body,
        meta=meta,
    )


def page_example(meta: dict, example: dict, manifest: dict) -> str:
    examples = manifest["examples"]
    slug = example["slug"]
    idx = next(i for i, e in enumerate(examples) if e["slug"] == slug)
    nxt = examples[(idx + 1) % len(examples)]
    tags = "".join(f'<span class="tag">{html.escape(t)}</span>' for t in example.get("tags", []))
    steps = []
    for doc in example["documents"]:
        path = doc["path"]
        steps.append(
            f"""
        {code_block(f"docs/site/examples/src/{path}", source(path), "yaml")}
        <div class="prose">
          <p>{inline(doc["note"])}</p>
        </div>"""
        )
    schema = manifest["schema"]
    body = f"""
        <div class="page-intro">
          <h2>{html.escape(example["title"])}</h2>
          <p>{inline(example["summary"])}</p>
        </div>
        <div class="example-meta">
          <span class="badge neutral">{html.escape(example.get("category", "example"))}</span>
          <span>Schema: <a href="src/{html.escape(schema)}"><code>{html.escape(schema)}</code></a></span>
        </div>
        <div class="tag-list" style="margin-bottom:16px">{tags}</div>
        {"".join(steps)}
        <div class="prose">
          <p>
            Programs that write carry a fixed <code>issued_at</code> here. Set it
            to the time you create the request id, and resend it unchanged on
            every retry. Ids and tokens come from earlier responses and are passed
            as <code>params</code>.
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
        title=f"{example['title']} — Fireweed",
        body=body,
        meta=meta,
        description=" ".join(example["summary"].split()),
    )


def page_api_index(meta: dict) -> str:
    api008 = gh(meta, "api008")
    body = f"""
        <div class="page-intro">
          <h2>DSL reference</h2>
          <p>
            A summary of the schema and program documents. The full specification,
            with every rule and the SQL each statement becomes, is
            <a href="{api008}">API-008</a>. For the Rust functions, see
            <a href="rust.html">Rust crates</a>.
          </p>
        </div>
        <div class="prose">
          <p>
            Both documents are YAML or JSON; unknown keys are refused. Object, field,
            parameter and read names match <code>[a-z][a-z0-9_]{{0,47}}</code>.
            Durations are a string with a unit (<code>250ms</code>, <code>30s</code>,
            <code>5m</code>, <code>2h</code>, <code>7d</code>) or milliseconds.
          </p>
          <h2>Schema document</h2>
        </div>
        <table class="data">
          <thead><tr><th>Key</th><th>Meaning</th></tr></thead>
          <tbody>
            <tr><td><code>version</code></td><td>Increases with every change. <code>apply_schema</code> takes the version it expects to replace.</td></tr>
            <tr><td><code>request_retention</code></td><td>How long request ids are remembered. Default <code>1d</code>.</td></tr>
            <tr><td><code>queues.&lt;name&gt;</code></td><td><code>max_attempts</code> (default 10, or <code>unlimited</code>), <code>retention</code> of finished items (default <code>7d</code>), <code>priority</code> (<code>asc</code> or <code>desc</code>), <code>fields</code>.</td></tr>
            <tr><td><code>tables.&lt;name&gt;</code></td><td><code>key</code> (fields that identify a row), or <code>sequenced: true</code>; <code>expire_after_write</code>; <code>fields</code>.</td></tr>
            <tr><td><code>fields.&lt;name&gt;</code></td><td><code>type</code>; <code>index: true</code>; <code>merge</code> (<code>add_only</code> or <code>non_decreasing</code>).</td></tr>
            <tr><td><code>reads</code></td><td>Named, versioned <code>select</code>s with typed <code>params</code>. A name and version, once declared, never change.</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Program document</h2>
        </div>
        <table class="data">
          <thead><tr><th>Key</th><th>Meaning</th></tr></thead>
          <tbody>
            <tr><td><code>request_id</code></td><td>Required if any statement writes. A replay returns the recorded response.</td></tr>
            <tr><td><code>issued_at</code></td><td>Required with <code>request_id</code>: when the caller created it. Resent unchanged on every retry.</td></tr>
            <tr><td><code>durability</code></td><td><code>durable</code> (default) or <code>fire_and_forget</code>.</td></tr>
            <tr><td><code>params</code></td><td>Values the statements refer to as <code>{{param: name}}</code>.</td></tr>
            <tr><td><code>statements</code></td><td>Run in order, as one transaction. The response has one result per statement.</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Statements</h2>
        </div>
        <table class="data">
          <thead><tr><th>Statement</th><th>Does</th><th>Result</th></tr></thead>
          <tbody>
            <tr><td><code>insert</code></td><td>Adds queue items or table rows. <code>on_conflict</code>: <code>skip</code> (default), <code>reject</code>, or <code>update</code> on tables.</td><td><code>[{{id, key?, status}}]</code></td></tr>
            <tr><td><code>claim</code></td><td>Leases up to <code>limit</code> due items for <code>lease</code>, in claim order, skipping rows other transactions hold.</td><td>Items with <code>token</code></td></tr>
            <tr><td><code>settle</code></td><td>Ends leases as <code>complete</code>, <code>fail</code>, <code>retry</code> or <code>release</code>, with <code>set</code> and <code>body</code> patches.</td><td><code>[{{id, status: ok | stale}}]</code></td></tr>
            <tr><td><code>renew</code></td><td>Extends unexpired leases.</td><td>As <code>settle</code></td></tr>
            <tr><td><code>update</code></td><td>Changes matched rows. <code>expect</code> bounds the match count; <code>on_limit: step</code> pages large changes; <code>leased</code> picks how leased items are treated.</td><td><code>{{matched, met?, more?, last?}}</code></td></tr>
            <tr><td><code>delete</code></td><td>Deletes matched rows.</td><td><code>{{matched, met?}}</code></td></tr>
            <tr><td><code>assert</code></td><td>Counts matched rows against <code>expect</code>; writes nothing.</td><td><code>{{matched, met}}</code></td></tr>
            <tr><td><code>select</code></td><td>Reads <code>fields</code>, or an <code>aggregate</code> with <code>group_by</code>; <code>order_by</code>; <code>limit</code> required.</td><td>Row objects</td></tr>
            <tr><td><code>read</code></td><td>Runs a declared read with its parameters.</td><td>Row objects</td></tr>
            <tr><td><code>pause</code>, <code>resume</code></td><td>A paused queue's claims return nothing.</td><td><code>{{}}</code></td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Inside a statement</h2>
          <ul>
            <li><strong>Operands:</strong> a column, <code>{{field: name}}</code>, <code>{{param: name}}</code>, <code>{{value: v}}</code>, <code>{{now: "-5m"}}</code>, <code>{{path: /a/b, type: text}}</code> into a queue item's body. No implicit casts.</li>
            <li><strong>Predicates:</strong> <code>and</code>, <code>or</code>, <code>not</code>; <code>eq</code>, <code>ne</code>, <code>lt</code>, <code>le</code>, <code>gt</code>, <code>ge</code>; <code>in</code>; <code>contains</code>; <code>is_null</code>, <code>not_null</code>; <code>exists</code>, a lookup in another object of the space.</li>
            <li><strong>Assignments:</strong> a value or operand, <code>add</code>, <code>sub</code>, <code>coalesce</code>, <code>set_add</code>, <code>set_remove</code>.</li>
            <li><strong>Body patches:</strong> <code>set</code>, <code>remove</code>, <code>append</code> with <code>keep_last</code>, <code>replace</code>.</li>
          </ul>
          <h2>Rejections and errors</h2>
          <p>
            A rejection rolls back every statement, keeps the request record and
            reports the statement and reason: <code>key_conflict</code>,
            <code>stale_lease</code>, <code>limit_exceeded</code>,
            <code>expect_unmet</code>, <code>merge_violation</code>. Errors change
            nothing and are not recorded: <code>invalid</code>,
            <code>space_not_found</code>, <code>schema_conflict</code>,
            <code>request_id_conflict</code>, <code>request_expired</code>,
            <code>contention</code>, <code>unavailable</code>.
          </p>
          <h2>Limits</h2>
        </div>
        <table class="data">
          <thead><tr><th>Limit</th><th>Value</th></tr></thead>
          <tbody>
            <tr><td>Statements per program</td><td>64</td></tr>
            <tr><td>Rows per <code>insert</code>; items per <code>settle</code> or <code>renew</code></td><td>1,000</td></tr>
            <tr><td><code>claim</code> limit</td><td>1,000</td></tr>
            <tr><td><code>update</code>, <code>delete</code> and <code>select</code> limit</td><td>10,000</td></tr>
            <tr><td>Program size</td><td>8 MiB</td></tr>
            <tr><td>Programs per <code>execute_batch</code></td><td>64</td></tr>
            <tr><td>Statement timeout</td><td>5 s</td></tr>
          </tbody>
        </table>
"""
    return layout(
        page="api/index.html",
        active="api",
        title="DSL reference — Fireweed",
        body=body,
        meta=meta,
        description="Schema and program documents: keys, statements, rejections and limits.",
    )


def page_api_rust(meta: dict) -> str:
    body = f"""
        <div class="page-intro">
          <h2>Rust crates</h2>
          <p>
            <code>fireweed-dsl</code> reads and checks documents.
            <code>fireweed-dsl-postgres</code> runs them. Generate the full docs with
            <code>cargo doc -p fireweed-dsl -p fireweed-dsl-postgres --no-deps --open</code>.
          </p>
        </div>
        <div class="prose">
          <h2>Executor functions</h2>
          <p>
            <code>fireweed_dsl_postgres</code> is a set of async functions over a
            <code>tokio_postgres::Client</code>. They hold no state between calls:
            each reads the space's schema from the database, checks the program
            against it, and runs it. Each returns <code>fireweed_dsl::Result</code>.
          </p>
        </div>
        <table class="data">
          <thead><tr><th>Function</th><th>Does</th></tr></thead>
          <tbody>
            <tr><td><code>apply_schema(&amp;mut Client, tenant, space, &amp;Schema, expected: Option&lt;i64&gt;) -&gt; Applied</code></td><td>Creates a space, or applies an additive change. <code>expected</code> is the version being replaced, <code>None</code> for a new space. An identical schema is accepted unchanged.</td></tr>
            <tr><td><code>get_schema(&amp;Client, tenant, space) -&gt; Option&lt;Schema&gt;</code></td><td>The stored schema of a space.</td></tr>
            <tr><td><code>execute(&amp;mut Client, tenant, space, &amp;Program) -&gt; Response</code></td><td>Runs a program as one transaction. Retries a deadlock up to 5 times, then fails with <code>Contention</code>.</td></tr>
            <tr><td><code>execute_batch(&amp;mut Client, tenant, space, &amp;[Program]) -&gt; Vec&lt;Result&lt;Response&gt;&gt;</code></td><td>Runs up to 64 independent programs in one transaction with one commit, each under its own savepoint. Results come back in order.</td></tr>
            <tr><td><code>sweep(&amp;mut Client, tenant, space, batch: u32) -&gt; Swept</code></td><td>Writes back lapsed leases and deletes what retention has expired, at most <code>batch</code> rows per object.</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <h2>Types in <code>fireweed_dsl</code></h2>
        </div>
        <table class="data">
          <thead><tr><th>Item</th><th>What it is</th></tr></thead>
          <tbody>
            <tr><td><code>from_yaml&lt;T&gt;(&amp;str)</code></td><td>Reads a YAML or JSON document into a <code>Schema</code> or <code>Program</code>.</td></tr>
            <tr><td><code>Schema</code>, <code>Program</code>, <code>Statement</code></td><td>The documents, with serde names matching the YAML keys.</td></tr>
            <tr><td><code>Response</code></td><td><code>request_id</code>, <code>status</code> (<code>Committed</code> or <code>Rejected</code>), <code>rejected</code>, <code>results</code>, <code>durability</code>, <code>replayed</code>.</td></tr>
            <tr><td><code>Applied</code></td><td><code>version</code> and whether the schema <code>changed</code>.</td></tr>
            <tr><td><code>Error</code></td><td><code>Invalid</code>, <code>SpaceNotFound</code>, <code>SchemaConflict</code>, <code>RequestIdConflict</code>, <code>RequestExpired</code>, <code>Contention</code>, <code>Unavailable</code>.</td></tr>
            <tr><td><code>check::resolve(&amp;Schema, &amp;Program)</code></td><td>Checks a program against a schema without a database.</td></tr>
          </tbody>
        </table>
        <div class="prose">
          <p>
            <code>Swept</code> (in <code>fireweed_dsl_postgres</code>) reports
            <code>expired_leases</code> and <code>deleted</code>. For a working
            program, see <a href="../get-started.html">get started</a>.
          </p>
        </div>
"""
    return layout(
        page="api/rust.html",
        active="api",
        title="Rust crates — Fireweed",
        body=body,
        meta=meta,
        description="The PostgreSQL executor's functions and the DSL crate's types.",
    )


def render_all() -> dict[str, str]:
    """Every page, keyed by its path under docs/site/."""
    meta = load_meta()
    manifest = load_manifest()
    pages = {
        "index.html": page_home(meta, manifest["examples"]),
        "why.html": page_why(meta),
        "concepts.html": page_concepts(meta),
        "get-started.html": page_get_started(meta, manifest),
        "support.html": page_support(meta),
        "contribute.html": page_contribute(meta),
        "examples/index.html": page_examples_index(meta, manifest),
        "api/index.html": page_api_index(meta),
        "api/rust.html": page_api_rust(meta),
    }
    for ex in manifest["examples"]:
        pages[f"examples/{ex['slug']}.html"] = page_example(meta, ex, manifest)
    return pages


def main() -> int:
    for page, content in render_all().items():
        path = SITE / page
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)}")
    print("render complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
