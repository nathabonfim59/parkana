import { copyFileSync, existsSync, mkdirSync, readdirSync } from 'node:fs'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createMarkdownRenderer, defineConfigWithTheme, type DefaultTheme, type SiteConfig } from 'vitepress'
import {
  checkAnchors,
  checkDashes,
  checkLlms,
  collectPages,
  httpTags,
  markdownURL,
  repositoryUrl,
  resolveRepoLink,
  siteOnlyPages,
  siteUrl,
  sourceHref,
} from '../../scripts/doc-links.mjs'
import { logql } from './logql.mts'

// The repository's Markdown is the single source of the site: VitePress reads it in place
// (srcDir is the repository root) and the rewrites give each file a clean URL. See
// scripts/doc-links.mjs for which files are pages; .publicignore paths never are.
const root = fileURLToPath(new URL('../..', import.meta.url))
const { pages, excluded } = collectPages(root)
const rewrites = Object.fromEntries([...pages].filter(([src, page]) => src !== page))
const toPosix = (p: string) => p.split('\\').join('/')
const generated = join(root, 'docs', 'api-reference')

// siteUrl (scripts/doc-links.mjs, also what llms.txt links to) is the one place the published
// address is written: the site base is its path, and the theme gets its origin so the home page
// can show the absolute URL of the Markdown copies (theme/AgentPrompt.vue).
const published = new URL(siteUrl)
const base = `${published.pathname.replace(/\/$/, '')}/`

/** Theme settings: the default theme's, plus the origin the site is published at. */
export type ThemeConfig = DefaultTheme.Config & { siteOrigin: string }

// One sidebar entry per generated HTTP page (scripts/docs-gen.mjs), in the fixed tag order.
function httpItems(): DefaultTheme.SidebarItem[] {
  const page = (slug: string) => existsSync(join(generated, 'http', `${slug}.md`))
  return [
    { text: 'Overview', link: '/api-reference/http/' },
    ...httpTags.filter((t) => page(t.slug)).map((t) => ({ text: t.tag, link: `/api-reference/http/${t.slug}` })),
    ...(page('schemas') ? [{ text: 'Schemas', link: '/api-reference/http/schemas' }] : []),
  ]
}

// One sidebar entry per generated Go package page (scripts/docs-gen.mjs).
function goPackageItems(): DefaultTheme.SidebarItem[] {
  const goDir = join(generated, 'go')
  const items: DefaultTheme.SidebarItem[] = []
  const walk = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name)
      if (entry.isDirectory()) walk(path)
      else if (entry.name === 'index.md' && dir !== goDir) {
        const rel = toPosix(relative(goDir, dir))
        items.push({ text: rel, link: `/api-reference/go/${rel}/` })
      }
    }
  }
  if (existsSync(goDir)) walk(goDir)
  return items.sort((a, b) => a.text!.localeCompare(b.text!))
}

// Copy the hand-written pages into the output as .md files at their URLs, so llms.txt can link
// to Markdown. The generated API reference is left out: it is large and has its own HTML pages.
function copyMarkdown(outDir: string) {
  for (const [src, page] of pages) {
    if (siteOnlyPages.has(src) || page.startsWith('api-reference/')) continue
    const out = join(outDir, markdownURL(page))
    mkdirSync(dirname(out), { recursive: true })
    copyFileSync(join(root, src), out)
  }
}

// Build checks VitePress does not do: #anchor links, em and en dashes in prose, and llms.txt. Every failure is listed, then the
// build fails once. The anchor check parses each page with the site's own Markdown renderer, so
// heading ids are exactly the ones the build gives them.
async function checkDocs(siteConfig: SiteConfig) {
  const md = await createMarkdownRenderer(siteConfig.srcDir, siteConfig.markdown, siteConfig.site.base, siteConfig.logger)
  const failures = [...checkAnchors(md, root, pages), ...checkDashes(md, root, pages), ...checkLlms(root, pages)]
  if (failures.length) {
    throw new Error(`docs check: ${failures.length} problem(s):\n${failures.join('\n')}`)
  }
}

export default defineConfigWithTheme<ThemeConfig>({
  title: 'Parkana',
  description: 'Parkana is a log store with a Loki-compatible HTTP API.',
  srcDir: '..',
  srcExclude: excluded,
  rewrites,
  base,
  cleanUrls: true,
  lastUpdated: true,
  head: [['link', { rel: 'icon', type: 'image/svg+xml', href: `${base}favicon.svg` }]],
  buildEnd: async (siteConfig) => {
    await checkDocs(siteConfig)
    copyMarkdown(siteConfig.outDir)
  },
  vite: { publicDir: join(root, 'docs', 'public') },
  markdown: {
    // LogQL fences are written for GitHub; Shiki has no LogQL grammar, so the site brings one.
    languages: [logql],
    // Links are written for GitHub, relative to the file. Resolve each one from the file's real
    // location: to the page's URL when the target is a page, otherwise to the file on GitHub.
    config(md) {
      md.core.ruler.push('parkana-repo-links', (state) => {
        // realPath is the file's location in the repository; path is its rewritten page path.
        const file = (state.env?.realPath ?? state.env?.path) as string | undefined
        if (!file) return
        const source = toPosix(relative(root, file))
        for (const block of state.tokens) {
          for (const token of block.children ?? []) {
            if (token.type !== 'link_open') continue
            const href = token.attrGet('href')
            const resolved = href && resolveRepoLink(root, pages, source, href)
            if (!resolved) continue
            // The anchor check (checkDocs) needs the link as written, to judge it the way GitHub does.
            token.meta = { ...token.meta, [sourceHref]: href }
            token.attrSet('href', resolved)
          }
        }
      })
      // VitePress compiles pages as Vue templates and protects code blocks from interpolation, but
      // not code spans: `{{ .src }}` would be evaluated (or fail the build). v-pre keeps them literal.
      const codeInline = md.renderer.rules.code_inline!
      md.renderer.rules.code_inline = (tokens, idx, options, env, self) => {
        tokens[idx].attrSet('v-pre', '')
        return codeInline(tokens, idx, options, env, self)
      }
    },
  },
  themeConfig: {
    siteOrigin: published.origin,
    nav: [
      {
        text: 'Docs',
        link: '/getting-started',
        activeMatch: '^/(?!api-reference/)(?!$).+',
      },
      { text: 'API reference', link: '/api-reference/http/', activeMatch: '^/api-reference/' },
      { text: 'Releases', link: `${repositoryUrl}/releases` },
    ],
    // Content runners uncomment their page's entry when the page lands; a group whose pages
    // all are still missing stays commented as a whole.
    sidebar: [
      {
        text: 'Get started',
        items: [
          { text: 'Getting started', link: '/getting-started' },
          { text: 'Install with an agent', link: '/agent-install' },
        ],
      },
      {
        text: 'Run it',
        items: [
          { text: 'Deploy', link: '/deploy' },
          { text: 'Configuration', link: '/configuration' },
          { text: 'Catalog', link: '/catalog' },
          { text: 'Storage', link: '/storage' },
        ],
      },
      {
        text: 'Send logs',
        items: [{ text: 'Ingest', link: '/ingest' }],
      },
      {
        text: 'Query',
        collapsed: false,
        items: [
          { text: 'Overview', link: '/query/' },
          { text: 'LogQL', link: '/query/logql' },
          { text: 'Best practices', link: '/query/best-practices' },
          { text: 'SQL', link: '/query/sql' },
          { text: 'Live tail', link: '/query/tail' },
          { text: 'Grafana', link: '/grafana' },
          { text: 'Grafana SQL datasource', link: '/grafana-datasource' },
        ],
      },
      {
        text: 'Pipelines',
        collapsed: false,
        items: [
          { text: 'Overview', link: '/pipelines/' },
          { text: 'Steps', link: '/pipelines/steps' },
          { text: 'Read semantics', link: '/pipelines/read' },
          { text: 'HTTP API', link: '/pipelines/api' },
          { text: 'Redaction', link: '/pipelines/redaction' },
        ],
      },
      {
        text: 'AI skills',
        collapsed: false,
        items: [
          { text: 'Catalog', link: '/skills/' },
          { text: 'Parkana query skill', link: '/skills/parkana' },
          { text: 'Redaction skill', link: '/skills/parkana-redaction' },
        ],
      },
      {
        text: 'Operate',
        items: [
          { text: 'Operations', link: '/operations' },
          { text: 'Security', link: '/security' },
          { text: 'Compactor', link: '/compactor' },
          { text: 'Benchmarks', link: '/benchmarks' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { text: 'HTTP API behavior', link: '/http-api' },
          { text: 'HTTP reference', collapsed: false, items: httpItems() },
          {
            text: 'Go reference',
            collapsed: true,
            items: [{ text: 'Overview', link: '/api-reference/go/' }, ...goPackageItems()],
          },
        ],
      },
      {
        text: 'Develop',
        items: [
          { text: 'Architecture', link: '/architecture' },
          { text: 'Development', link: '/development/' },
          { text: 'Loki comparison', link: '/development/loki-comparison' },
          { text: 'OTLP comparison stack', link: '/development/otlp-comparison' },
        ],
      },
    ],
    socialLinks: [{ icon: 'github', link: repositoryUrl }],
    // Every hand-written page edits its own source file, wherever it lives in the repository.
    editLink: {
      // This function runs in the browser, so it cannot use the constants above.
      pattern: ({ filePath }) => `https://github.com/nathabonfim59/parkana/edit/main/${filePath}`,
    },
    search: { provider: 'local' },
    outline: { label: 'Contents', level: [2, 3] },
    footer: { message: 'Released under the MIT License.' },
  },
})
