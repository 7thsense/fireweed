#!/usr/bin/env node
/**
 * Playwright verification for the Fireweed microsite.
 *
 * - Loads key routes at common viewports and writes full-page screenshots
 * - Asserts primary landmarks and no horizontal overflow
 * - Crawls microsite HTML links; validates outbound staged docs as leaf URLs (HTTP 200)
 *
 * Usage:
 *   BASE_URL=http://127.0.0.1:4173 node scripts/site/verify_pages.mjs
 *   BASE_URL=https://7thsense.github.io/fireweed node scripts/site/verify_pages.mjs
 *
 * Env:
 *   BASE_URL       required — origin (no trailing slash required)
 *   SCREENSHOT_DIR optional — default target/site-verify/screenshots
 *   REPORT_PATH    optional — default target/site-verify/report.json
 *   SITE_PREFIX    optional — default /site
 */

import { chromium } from "playwright";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../..");

const BASE_URL = (process.env.BASE_URL || "").replace(/\/$/, "");
if (!BASE_URL) {
  console.error(
    "BASE_URL is required (e.g. http://127.0.0.1:4173 or https://…github.io/fireweed)"
  );
  process.exit(2);
}

const SITE_PREFIX = (process.env.SITE_PREFIX || "/site").replace(/\/$/, "");
/** Full URL path prefix including project Pages base (e.g. /fireweed/site). */
const BASE_PATH = new URL(BASE_URL.endsWith("/") ? BASE_URL : BASE_URL + "/").pathname.replace(
  /\/$/,
  ""
);
const SITE_PATH_PREFIX = `${BASE_PATH}${SITE_PREFIX}` || SITE_PREFIX;
const SCREENSHOT_DIR =
  process.env.SCREENSHOT_DIR || path.join(REPO_ROOT, "target/site-verify/screenshots");
const REPORT_PATH =
  process.env.REPORT_PATH || path.join(REPO_ROOT, "target/site-verify/report.json");

const VIEWPORTS = [
  { name: "iphone-se", width: 375, height: 667 },
  { name: "iphone-12", width: 390, height: 844 },
  { name: "pixel-5", width: 393, height: 851 },
  { name: "ipad-portrait", width: 768, height: 1024 },
  { name: "ipad-landscape", width: 1024, height: 768 },
  { name: "laptop", width: 1280, height: 800 },
  { name: "desktop", width: 1440, height: 900 },
  { name: "full-hd", width: 1920, height: 1080 },
];

const SCREENSHOT_ROUTES = [
  { id: "home", path: "/" },
  { id: "why", path: "/why.html" },
  { id: "concepts", path: "/concepts.html" },
  { id: "get-started", path: "/get-started.html" },
  { id: "examples", path: "/examples/" },
  { id: "example-basic", path: "/examples/basic-lifecycle.html" },
  { id: "api", path: "/api/" },
  { id: "api-rust", path: "/api/rust.html" },
  { id: "deploy", path: "/deploy/" },
  { id: "support", path: "/support.html" },
  { id: "contribute", path: "/contribute.html" },
];

function siteUrl(p) {
  const suffix = p === "/" ? "/" : p;
  return `${BASE_URL}${SITE_PREFIX}${suffix}`;
}

function normalizeUrl(url) {
  const u = new URL(url);
  u.hash = "";
  return u.href;
}

function isUnderSite(url) {
  const u = new URL(url);
  const base = new URL(BASE_URL.endsWith("/") ? BASE_URL : BASE_URL + "/");
  if (u.origin !== base.origin) return false;
  const prefix = `${SITE_PATH_PREFIX}/`;
  return (
    u.pathname === SITE_PATH_PREFIX ||
    u.pathname === `${SITE_PATH_PREFIX}/` ||
    u.pathname.startsWith(prefix)
  );
}

function isSameOrigin(url) {
  try {
    return new URL(url).origin === new URL(BASE_URL).origin;
  } catch {
    return false;
  }
}

async function headOrGetOk(page, url) {
  // Prefer request API (no navigation) for leaf link checks.
  const response = await page.request.get(url, {
    maxRedirects: 5,
    timeout: 30_000,
    failOnStatusCode: false,
  });
  const status = response.status();
  return { ok: status >= 200 && status < 400, status };
}

/**
 * Crawl only microsite HTML. Staged markdown/policy links are checked as leaves
 * (status only) so we do not walk the entire helix tree.
 */
async function crawlSite(page) {
  const start = siteUrl("/");
  const queue = [start];
  const seenHtml = new Set();
  const leafChecked = new Set();
  const results = [];
  const broken = [];

  // Root redirect should resolve
  {
    const root = `${BASE_URL}/`;
    const { ok, status } = await headOrGetOk(page, root);
    results.push({ url: root, ok, status, kind: "root" });
    if (!ok) broken.push({ url: root, status, kind: "root" });
  }

  while (queue.length) {
    const current = queue.shift();
    const key = normalizeUrl(current);
    if (seenHtml.has(key)) continue;
    seenHtml.add(key);

    let response;
    try {
      response = await page.goto(current, {
        waitUntil: "domcontentloaded",
        timeout: 30_000,
      });
    } catch (err) {
      broken.push({ url: current, error: String(err), kind: "html" });
      results.push({ url: current, ok: false, status: null, error: String(err), kind: "html" });
      continue;
    }

    const status = response ? response.status() : null;
    const ok = status !== null && status >= 200 && status < 400;
    results.push({ url: current, ok, status, kind: "html" });
    if (!ok) {
      broken.push({ url: current, status, kind: "html" });
      continue;
    }

    const hrefs = await page.$$eval("a[href]", (as) =>
      as.map((a) => a.href).filter(Boolean)
    );

    for (const href of hrefs) {
      if (!isSameOrigin(href)) continue;
      const bare = href.split("#")[0];
      const n = normalizeUrl(bare);
      const pathname = new URL(bare).pathname;

      if (isUnderSite(bare) && (pathname.endsWith(".html") || pathname.endsWith("/"))) {
        if (!seenHtml.has(n)) queue.push(bare);
        continue;
      }

      // Leaf assets / staged docs / policy files linked from the site
      if (leafChecked.has(n)) continue;
      leafChecked.add(n);
      const leaf = await headOrGetOk(page, bare);
      results.push({ url: bare, ok: leaf.ok, status: leaf.status, kind: "leaf" });
      if (!leaf.ok) broken.push({ url: bare, status: leaf.status, kind: "leaf" });
    }
  }

  return {
    results,
    broken,
    htmlPages: seenHtml.size,
    leaves: leafChecked.size,
  };
}

async function assertLayout(page, routeId) {
  const issues = [];
  const header = page.locator("header.site-header");
  const main = page.locator("main");
  const nav = page.locator("nav.site-nav");
  const footer = page.locator("footer.site-footer");

  for (const [name, loc] of [
    ["header", header],
    ["main", main],
    ["nav", nav],
    ["footer", footer],
  ]) {
    if ((await loc.count()) === 0) {
      issues.push(`${routeId}: missing ${name}`);
      continue;
    }
    if (!(await loc.first().isVisible())) {
      issues.push(`${routeId}: ${name} not visible`);
    }
  }

  const overflow = await page.evaluate(() => {
    const doc = document.documentElement;
    return { scrollWidth: doc.scrollWidth, clientWidth: doc.clientWidth };
  });
  if (overflow.scrollWidth > overflow.clientWidth + 8) {
    issues.push(
      `${routeId}: horizontal overflow (scrollWidth=${overflow.scrollWidth}, clientWidth=${overflow.clientWidth})`
    );
  }

  if (routeId.startsWith("home@")) {
    const panel = page.locator(".signal-panel");
    if ((await panel.count()) !== 1) {
      issues.push(`${routeId}: missing priority queue signal panel`);
    } else {
      const box = await panel.boundingBox();
      const viewport = page.viewportSize();
      if (!box) {
        issues.push(`${routeId}: priority queue signal panel has no bounds`);
      } else if (
        viewport &&
        viewport.width >= 1024 &&
        (box.width > viewport.width * 0.38 ||
          box.height > 360 ||
          Math.abs(box.width / box.height - 4 / 3) > 0.05)
      ) {
        issues.push(
          `${routeId}: priority queue signal panel has an invalid desktop footprint (width=${box.width}, height=${box.height})`
        );
      }
    }
  }

  return issues;
}

async function main() {
  await fs.mkdir(SCREENSHOT_DIR, { recursive: true });
  const browser = await chromium.launch();
  const report = {
    baseUrl: BASE_URL,
    sitePrefix: SITE_PREFIX,
    startedAt: new Date().toISOString(),
    viewports: VIEWPORTS.map((v) => v.name),
    screenshots: [],
    layoutIssues: [],
    linkCrawl: null,
    ok: true,
  };

  try {
    for (const vp of VIEWPORTS) {
      const context = await browser.newContext({
        viewport: { width: vp.width, height: vp.height },
        deviceScaleFactor: 1,
      });
      const page = await context.newPage();

      for (const route of SCREENSHOT_ROUTES) {
        const url = siteUrl(route.path);
        const response = await page.goto(url, {
          waitUntil: "networkidle",
          timeout: 45_000,
        });
        const status = response ? response.status() : null;
        if (status === null || status >= 400) {
          report.ok = false;
          report.layoutIssues.push(
            `${route.id}@${vp.name}: failed to load ${url} status=${status}`
          );
          continue;
        }

        await page.waitForTimeout(100);
        const issues = await assertLayout(page, `${route.id}@${vp.name}`);
        report.layoutIssues.push(...issues);
        if (issues.length) report.ok = false;

        const shotName = `${vp.name}__${route.id}.png`;
        const shotPath = path.join(SCREENSHOT_DIR, shotName);
        await page.screenshot({ path: shotPath, fullPage: true });
        report.screenshots.push({
          viewport: vp.name,
          route: route.id,
          url,
          path: path.relative(REPO_ROOT, shotPath),
        });
      }

      await context.close();
    }

    const crawlContext = await browser.newContext({
      viewport: { width: 1280, height: 800 },
    });
    const crawlPage = await crawlContext.newPage();
    const crawl = await crawlSite(crawlPage);
    report.linkCrawl = crawl;
    if (crawl.broken.length) report.ok = false;
    await crawlContext.close();
  } finally {
    await browser.close();
  }

  report.finishedAt = new Date().toISOString();
  await fs.mkdir(path.dirname(REPORT_PATH), { recursive: true });
  await fs.writeFile(REPORT_PATH, JSON.stringify(report, null, 2));

  console.log(`screenshots: ${report.screenshots.length} → ${SCREENSHOT_DIR}`);
  console.log(`layout issues: ${report.layoutIssues.length}`);
  console.log(
    `html pages: ${report.linkCrawl?.htmlPages ?? 0}, leaves: ${report.linkCrawl?.leaves ?? 0}, broken: ${report.linkCrawl?.broken.length ?? 0}`
  );
  console.log(`report: ${REPORT_PATH}`);

  if (!report.ok) {
    if (report.layoutIssues.length) {
      console.error("Layout issues:");
      for (const i of report.layoutIssues) console.error(" -", i);
    }
    if (report.linkCrawl?.broken?.length) {
      console.error("Broken links:");
      for (const b of report.linkCrawl.broken) console.error(" -", JSON.stringify(b));
    }
    process.exit(1);
  }
  console.log("site verification passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
