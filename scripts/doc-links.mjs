/**
 * Which repository Markdown files are site pages, their URLs, how links between them resolve, and
 * the build checks on them (heading anchors, dashes, llms.txt). Shared by
 * site/.vitepress/config.mts and scripts/docs-gen.mjs.
 *
 * The repository's own Markdown is the single source: VitePress reads docs/*.md and the Grafana
 * datasource README in place (srcDir is the repository root) and `rewrites` gives them clean URLs.
 * Paths listed in .publicignore never become pages; links to them resolve to GitHub.
 */
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join, posix } from 'node:path';

export const repositoryUrl = 'https://github.com/nathabonfim59/parkana';
export const defaultBranch = 'main';
/** Where the site is published; llms.txt links the Markdown copies under it. */
export const siteUrl = 'https://nathabonfim59.github.io/parkana';

/** Markdown outside docs/ that is published, and the page path it gets (under the site root). */
const extraPages = new Map([['grafana/parkana-datasource/README.md', 'grafana-datasource.md']]);

/** docs/ pages that exist only to build the site, never published as Markdown (llms.txt). */
export const siteOnlyPages = new Set(['docs/index.md']);

/** Directory names never walked for Markdown, wherever they appear. */
const skipDirs = new Set(['.git', 'node_modules', '.tty59', '.build', 'bin', 'data', 'dist', 'site', 'public']);

/**
 * The HTTP reference has one page per OpenAPI tag, in this order, at docs/api-reference/http/<slug>.md
 * (scripts/docs-gen.mjs fails on a tag not listed here), plus the schemas page.
 */
export const httpTags = [
  { tag: 'Push', slug: 'push' },
  { tag: 'Query', slug: 'query' },
  { tag: 'Metadata', slug: 'metadata' },
  { tag: 'Tail', slug: 'tail' },
  { tag: 'SQL', slug: 'sql' },
  { tag: 'Pipelines', slug: 'pipelines' },
  { tag: 'Operations', slug: 'operations' },
  { tag: 'Internal', slug: 'internal' },
];

/** The generated API reference: its pages are checked with the VitePress slug only. */
const generatedPrefix = 'docs/api-reference/';

function publicIgnore(root) {
  const file = join(root, '.publicignore');
  if (!existsSync(file)) return [];
  return readFileSync(file, 'utf-8')
    .split('\n')
    .map((l) => l.replace(/#.*/, '').trim())
    .filter(Boolean);
}

function ignored(path, patterns) {
  return patterns.some((p) => {
    const glob = p.replace(/\/$/, '');
    const re = new RegExp('^' + glob.split('*').map((s) => s.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('[^/]*') + '(/|$)');
    return re.test(path);
  });
}

/**
 * Returns { pages: Map<repoPath, pagePath>, excluded: string[] } where pagePath is the path under the
 * site root (docs/query/sql.md -> query/sql.md) and excluded lists every other Markdown file, for srcExclude.
 */
export function collectPages(root) {
  const patterns = publicIgnore(root);
  const pages = new Map();
  const excluded = [];
  const walk = (dir) => {
    for (const entry of readdirSync(join(root, dir), { withFileTypes: true })) {
      const rel = dir ? `${dir}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        if (!skipDirs.has(entry.name)) walk(rel);
        continue;
      }
      if (!entry.name.endsWith('.md')) continue;
      let page = null;
      if (!ignored(rel, patterns)) {
        if (rel.startsWith('docs/')) page = rel.slice('docs/'.length);
        else if (extraPages.has(rel)) page = extraPages.get(rel);
      }
      if (page) pages.set(rel, page);
      else excluded.push(rel);
    }
  };
  walk('');
  return { pages, excluded };
}

/** Site URL of a page path (cleanUrls: no .md, index.md is its directory). */
export function pageURL(page) {
  const p = page.replace(/\.md$/, '');
  if (p === 'index') return '/';
  if (p.endsWith('/index')) return '/' + p.slice(0, -'index'.length);
  return '/' + p;
}

const external = /^(?:[a-z][a-z0-9+.-]*:|\/\/|#|\/)/i;

/**
 * Resolves href, as written in the repository file sourcePath, to a repository path the way GitHub
 * does. Returns { repoPath, suffix } or null for external, absolute and fragment-only links.
 */
function repoTarget(sourcePath, href) {
  if (!href || external.test(href)) return null;
  const at = href.search(/[?#]/);
  const target = at === -1 ? href : href.slice(0, at);
  const suffix = at === -1 ? '' : href.slice(at);
  if (!target) return null;
  const repoPath = posix.normalize(posix.join(posix.dirname(sourcePath), target)).replace(/\/$/, '');
  if (repoPath.startsWith('..')) return null;
  return { repoPath, suffix };
}

/** The page (repository path) a resolved repository path names, if any. */
function pageFor(pages, repoPath) {
  for (const candidate of [repoPath, `${repoPath}.md`, `${repoPath}/index.md`, `${repoPath}/README.md`]) {
    if (pages.has(candidate)) return candidate;
  }
  return null;
}

/**
 * Resolves href as written in the repository file sourcePath, the way GitHub does, and returns the
 * site URL of the target page, the GitHub URL of a repository file, or null to leave it unchanged.
 */
export function resolveRepoLink(root, pages, sourcePath, href) {
  const t = repoTarget(sourcePath, href);
  if (!t) return null;
  const page = pageFor(pages, t.repoPath);
  if (page) return pageURL(pages.get(page)) + t.suffix;
  const full = join(root, t.repoPath);
  if (!existsSync(full)) return null; // left alone: VitePress reports it as a dead link
  const kind = statSync(full).isDirectory() ? 'tree' : 'blob';
  return `${repositoryUrl}/${kind}/${defaultBranch}/${t.repoPath}${t.suffix}`;
}

// ---------------------------------------------------------------------------------------------
// Anchor check

/**
 * GitHub's heading id (github-slugger): lowercase, drop every character that is not a letter, mark,
 * number, connector punctuation, space or hyphen, then each space becomes a hyphen. seen counts
 * earlier ids of the same page: a repeated id gets -1, -2, ...
 */
export function githubSlug(text, seen) {
  const base = text.toLowerCase().replace(/[^\p{L}\p{M}\p{N}\p{Pc} -]/gu, '').replace(/ /g, '-');
  let slug = base;
  while (seen.has(slug)) {
    seen.set(base, seen.get(base) + 1);
    slug = `${base}-${seen.get(base)}`;
  }
  seen.set(slug, 0);
  return slug;
}

/**
 * Token meta key under which the site's link rule (site/.vitepress/config.mts) keeps a link's href
 * as written in the repository, before it rewrites it to a site or GitHub URL.
 */
export const sourceHref = 'parkanaSourceHref';

/** VitePress's own "#" permalink inside each heading; not part of the text, not a link to check. */
const isPermalink = (t) => t.type === 'link_open' && t.attrGet('class') === 'header-anchor';

/** The visible text of a heading's inline token, as GitHub reads it: text and code, no markup. */
function inlineText(inline) {
  const children = inline.children ?? [];
  const end = children.findIndex(isPermalink);
  return (end === -1 ? children : children.slice(0, end))
    .filter((t) => t.type === 'text' || t.type === 'code_inline')
    .map((t) => t.content)
    .join('')
    .trim();
}

const explicitId = /\s*(\{[^{}]*#[^{}]*\})\s*$/;
const htmlAnchor = /<a\s[^>]*\b(?:name|id)\s*=\s*"([^"]+)"/g;

/**
 * Parses one Markdown file with the site's own renderer. Returns the heading ids VitePress gives it
 * (site), the ids GitHub gives it (github), the ids GitHub cannot resolve because they are explicit
 * {#id} attributes (explicit), and every link with its source line.
 */
function anchorsOf(md, root, repoPath) {
  const src = readFileSync(join(root, repoPath), 'utf-8');
  const lines = src.split('\n');
  const env = { path: join(root, repoPath), realPath: join(root, repoPath), relativePath: repoPath };
  const tokens = md.parse(src, env);
  const site = new Set();
  const github = new Set();
  const explicit = new Set();
  const seen = new Map();
  const links = [];
  // Front matter is stripped before parsing, so token lines are offset by its length.
  const offset = src.startsWith('---\n') ? src.slice(0, src.indexOf('\n---', 3) + 4).split('\n').length : 0;
  // Inline tokens in table cells carry no line map: they get the line of the last token that has one.
  let line = 0;
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token.map) line = token.map[0] + offset + 1;
    if (token.type === 'heading_open') {
      const inline = tokens[i + 1];
      const id = token.attrGet('id');
      if (id) site.add(id);
      const sourceLine = token.map ? lines[token.map[0] + offset] ?? '' : '';
      const attrs = sourceLine.match(explicitId);
      // GitHub shows a {#id} attribute as text, so it is part of GitHub's slug.
      const text = attrs ? `${inlineText(inline)} ${attrs[1]}` : inlineText(inline);
      github.add(githubSlug(text, seen));
      if (attrs && id) explicit.add(id);
    }
    const scan = token.type === 'inline' ? token.children ?? [] : [token];
    let at = line;
    for (const t of scan) {
      if (t.type === 'softbreak' || t.type === 'hardbreak') at++;
      if (t.type === 'html_block' || t.type === 'html_inline') {
        for (const m of t.content.matchAll(htmlAnchor)) site.add(m[1]), github.add(m[1]);
      }
      if (t.type === 'link_open' && !isPermalink(t)) links.push({ href: t.meta?.[sourceHref] ?? t.attrGet('href'), line: at });
    }
  }
  for (const id of explicit) if (github.has(id)) explicit.delete(id);
  return { site, github, explicit, links };
}

/**
 * Checks every #fragment link of every page against the headings of its target. Hand-written pages
 * must link anchors that exist under both the VitePress and the GitHub heading ids, so a link works
 * on the site and on GitHub; generated pages (docs/api-reference/) are site-only and are checked
 * against the VitePress ids. Links into repository Markdown that is not a page go to GitHub and are
 * checked against GitHub's ids. Returns the failures as "file:line: link -> reason" lines.
 */
export function checkAnchors(md, root, pages) {
  const cache = new Map();
  const info = (repoPath) => {
    if (!cache.has(repoPath)) cache.set(repoPath, anchorsOf(md, root, repoPath));
    return cache.get(repoPath);
  };
  const byURL = new Map([...pages].map(([src, page]) => [pageURL(page), src]));
  const failures = [];
  for (const source of pages.keys()) {
    const generated = source.startsWith(generatedPrefix);
    for (const { href, line } of info(source).links) {
      if (!href || !href.includes('#')) continue;
      const hash = href.indexOf('#');
      let fragment = href.slice(hash + 1);
      try {
        fragment = decodeURIComponent(fragment);
      } catch {
        // Not percent-encoded: compared as written.
      }
      const path = href.slice(0, hash).replace(/\?.*/, '');
      let target;
      let siteOnly = generated;
      let githubOnly = false;
      if (!path) target = source;
      else if (path.startsWith('/') && !path.startsWith('//')) {
        // A site URL: valid on the site only.
        target = byURL.get(path.replace(/\.md$/, '').replace(/\.html$/, '')) ?? byURL.get(path.replace(/\/?$/, '/'));
        siteOnly = true;
        if (!target) continue; // not a page: VitePress reports it as a dead link
      } else {
        const t = repoTarget(source, href);
        if (!t) continue; // external
        target = pageFor(pages, t.repoPath);
        if (!target) {
          if (!t.repoPath.endsWith('.md') || !existsSync(join(root, t.repoPath))) continue;
          target = t.repoPath;
          githubOnly = true;
        }
      }
      if (!fragment) continue;
      const where = `${source}:${line}: ${href} ->`;
      const ids = info(target);
      const onSite = ids.site.has(fragment);
      const onGitHub = ids.github.has(fragment);
      if (githubOnly) {
        if (!onGitHub) failures.push(`${where} no heading "${fragment}" in ${target} (GitHub)`);
      } else if (siteOnly) {
        if (!onSite) failures.push(`${where} no heading "${fragment}" in ${target} (site)`);
      } else if (!onSite && !onGitHub) {
        failures.push(`${where} no heading "${fragment}" in ${target}`);
      } else if (!onGitHub) {
        const why = ids.explicit.has(fragment) ? 'is an explicit {#id}, which GitHub does not resolve' : 'is the site id only';
        failures.push(`${where} "${fragment}" ${why} in ${target}; GitHub ids differ`);
      } else if (!onSite) {
        failures.push(`${where} "${fragment}" is the GitHub id only in ${target}; the site id differs`);
      }
    }
  }
  return failures;
}

// ---------------------------------------------------------------------------------------------
// Dash check

const dash = /[–—]/;

/**
 * Checks the hand-written pages for em dashes (U+2014) and en dashes (U+2013) in prose: text,
 * headings, table cells, inline and block HTML, and front matter. Code spans and code blocks are
 * exempt, and so are the generated pages (docs/api-reference/). Returns the failures as
 * "file:line: em dash|en dash: text" lines.
 */
export function checkDashes(md, root, pages) {
  const failures = [];
  for (const repoPath of pages.keys()) {
    if (repoPath.startsWith(generatedPrefix)) continue;
    const src = readFileSync(join(root, repoPath), 'utf-8');
    const report = (line, text) => {
      const m = text.match(dash);
      if (!m) return;
      const kind = m[0] === '—' ? 'em dash' : 'en dash';
      failures.push(`${repoPath}:${line}: ${kind}: ${text.trim().slice(0, 80)}`);
    };
    // Front matter is stripped before parsing: check its lines as written, then offset token lines.
    let offset = 0;
    if (src.startsWith('---\n')) {
      const front = src.slice(0, src.indexOf('\n---', 3) + 4).split('\n');
      offset = front.length;
      front.forEach((l, i) => report(i + 1, l));
    }
    const env = { path: join(root, repoPath), realPath: join(root, repoPath), relativePath: repoPath };
    let line = 0;
    for (const token of md.parse(src, env)) {
      if (token.map) line = token.map[0] + offset + 1;
      if (token.type === 'html_block') {
        token.content.split('\n').forEach((l, i) => report(line + i, l));
        continue;
      }
      if (token.type !== 'inline') continue;
      let at = line;
      for (const t of token.children ?? []) {
        if (t.type === 'softbreak' || t.type === 'hardbreak') at++;
        if (t.type === 'text' || t.type === 'html_inline') report(at, t.content);
      }
    }
  }
  return failures;
}

// ---------------------------------------------------------------------------------------------
// llms.txt check

/**
 * Checks docs/public/llms.txt against the published hand-written pages: every page must be linked
 * as `${siteUrl}/<page>.md`, and every such link must name a page. Returns the failures.
 */
export function checkLlms(root, pages) {
  const file = 'docs/public/llms.txt';
  const text = readFileSync(join(root, file), 'utf-8');
  const want = new Map();
  for (const [src, page] of pages) {
    if (siteOnlyPages.has(src) || src.startsWith(generatedPrefix)) continue;
    want.set(`${siteUrl}${markdownURL(page)}`, src);
  }
  const failures = [];
  const linked = new Set();
  const prefix = siteUrl.replace(/[.+?^${}()|[\]\\/]/g, '\\$&');
  for (const [i, line] of text.split('\n').entries()) {
    for (const m of line.matchAll(new RegExp(`\\]\\((${prefix}/[^)\\s]+)\\)`, 'g'))) {
      const url = m[1];
      if (url.includes('/api-reference/')) continue; // the generated reference, linked as HTML
      linked.add(url);
      if (!want.has(url)) failures.push(`${file}:${i + 1}: ${url} -> no such published page`);
    }
  }
  for (const [url, src] of want) if (!linked.has(url)) failures.push(`${file}: ${src} is published but not linked (${url})`);
  return failures;
}

/** URL path of the Markdown copy of a page (the build writes one next to each HTML page). */
export function markdownURL(page) {
  const url = pageURL(page);
  return url.endsWith('/') ? `${url}index.md` : `${url}.md`;
}
