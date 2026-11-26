<script setup lang="ts">
/**
 * Home page, under the hero actions: a one-line prompt that tells a coding agent to follow the
 * published Markdown of docs/agent-install.md, a button that copies it, and links to that page
 * and to the AI skills catalog. The prompt's URL is the configured origin (themeConfig.siteOrigin,
 * from siteUrl in scripts/doc-links.mjs) plus the site base, so it is written in one place.
 */
import { computed, onBeforeUnmount, ref } from 'vue'
import { useData, withBase } from 'vitepress'
import type { ThemeConfig } from '../config.mts'

const { theme } = useData<ThemeConfig>()

const url = computed(() => `${theme.value.siteOrigin}${withBase('/agent-install.md')}`)
const before = 'Install Parkana by following '
const after = ' step by step.'
const prompt = computed(() => `${before}${url.value}${after}`)
// On a narrow screen the URL wraps only after a slash (a <wbr> after each part), never inside a
// word or between the two slashes of "https://".
const urlParts = computed(() => url.value.split(/(?<=\/)(?!\/)/))

const copied = ref(false)
const status = ref('')
let timer: ReturnType<typeof setTimeout> | undefined

async function copy() {
  clearTimeout(timer)
  try {
    await navigator.clipboard.writeText(prompt.value)
    copied.value = true
    status.value = 'Prompt copied. Paste it into your agent.'
  } catch {
    // No clipboard (insecure context, permission denied): select the text so the user can copy it.
    const el = document.getElementById('pk-agent-prompt')
    if (el) window.getSelection()?.selectAllChildren(el)
    status.value = 'Copy failed. The prompt is selected: press Ctrl+C or Cmd+C.'
  }
  timer = setTimeout(() => {
    copied.value = false
    status.value = ''
  }, 2500)
}

onBeforeUnmount(() => clearTimeout(timer))
</script>

<template>
  <section class="pk-agent" aria-labelledby="pk-agent-label">
    <h2 id="pk-agent-label" class="pk-agent-label">Install with an agent</h2>
    <div class="pk-agent-box">
      <!-- One line of markup: whitespace between these nodes would show in the prompt. -->
      <code id="pk-agent-prompt" class="pk-agent-prompt">{{ before }}<template v-for="(part, i) in urlParts" :key="i"><span class="pk-agent-seg">{{ part }}</span><wbr /></template>{{ after }}</code>
      <button
        type="button"
        class="pk-agent-copy"
        :class="{ copied }"
        aria-describedby="pk-agent-label"
        @click="copy"
      >
        <svg v-if="!copied" aria-hidden="true" viewBox="0 0 16 16" width="14" height="14">
          <rect x="5.5" y="5.5" width="8" height="8" fill="none" stroke="currentColor" stroke-width="1.5" />
          <path d="M10.5 2.5h-8v8" fill="none" stroke="currentColor" stroke-width="1.5" />
        </svg>
        <svg v-else aria-hidden="true" viewBox="0 0 16 16" width="14" height="14">
          <path d="M2.5 8.5l3.5 3.5 7.5-8" fill="none" stroke="currentColor" stroke-width="1.75" />
        </svg>
        <span>{{ copied ? 'Copied' : 'Copy' }}</span>
      </button>
    </div>
    <p class="pk-agent-links">
      <a :href="withBase('/agent-install')">What the agent does</a>
      <a :href="withBase('/skills/')">AI skills</a>
      <code class="pk-agent-skill">npx skills add nathabonfim59/parkana</code>
    </p>
    <span class="pk-sr-only" role="status" aria-live="polite">{{ status }}</span>
  </section>
</template>

<style scoped>
.pk-agent {
  width: 100%;
  max-width: 920px;
  margin-top: 28px;
}
.pk-agent-label {
  margin: 0 0 8px;
  font-family: var(--pk-mono);
  font-size: 11px;
  font-weight: 600;
  line-height: 1.4;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--vp-c-text-2);
}
.pk-agent-box {
  display: flex;
  align-items: stretch;
  border: 1px solid var(--vp-button-alt-border);
  background: var(--vp-c-bg-alt);
}
.pk-agent-prompt {
  flex: 1;
  min-width: 0;
  padding: 10px 14px;
  font-family: var(--pk-mono);
  font-size: 13px;
  line-height: 1.55;
  color: var(--vp-c-text-1);
  background: transparent;
  overflow-wrap: break-word;
}
/* A URL segment never breaks inside (not at its hyphen either); the <wbr> after it may. */
.pk-agent-seg { white-space: nowrap; }
.pk-agent-prompt::before {
  content: '> ';
  color: var(--pk-accent);
  font-weight: 700;
}
.pk-agent-copy {
  display: inline-flex;
  flex: none;
  align-items: center;
  gap: 8px;
  padding: 0 14px;
  min-height: 40px;
  border: 0;
  border-left: 1px solid var(--vp-button-alt-border);
  font-family: var(--pk-mono);
  font-size: 13px;
  font-weight: 600;
  color: var(--vp-c-text-1);
  background: transparent;
  cursor: pointer;
  transition: color 0.15s, background-color 0.15s;
}
.pk-agent-copy svg { color: var(--pk-accent); }
.pk-agent-copy:hover { color: var(--pk-accent); background: var(--vp-c-bg-soft); }
.pk-agent-copy.copied { color: var(--pk-accent); }
/* The site-wide ring (style.css) sits outside the box border, so it shows on every side. */
.pk-agent-copy:focus-visible { outline: 2px solid var(--pk-accent); outline-offset: 2px; }
/* Narrow screens: the button goes under the prompt, full width, so the prompt keeps every column. */
@media (max-width: 479px) {
  .pk-agent-box { flex-direction: column; }
  .pk-agent-copy {
    justify-content: center;
    border-left: 0;
    border-top: 1px solid var(--vp-button-alt-border);
    min-height: 44px;
  }
}
.pk-agent-links {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 4px 18px;
  margin: 10px 0 0;
  font-size: 13px;
  line-height: 1.6;
}
.pk-agent-links a {
  font-family: var(--pk-mono);
  color: var(--vp-c-text-2);
  text-decoration: underline;
  text-decoration-color: var(--vp-c-border);
  text-underline-offset: 3px;
  transition: color 0.15s, text-decoration-color 0.15s;
}
.pk-agent-links a:hover { color: var(--pk-accent); text-decoration-color: var(--pk-accent); }
.pk-agent-skill {
  font-family: var(--pk-mono);
  font-size: 12px;
  color: var(--vp-c-text-2);
}
.pk-agent-skill::before { content: '$ '; color: var(--vp-c-text-2); }
.pk-sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  margin: -1px;
  padding: 0;
  overflow: hidden;
  clip: rect(0 0 0 0);
  white-space: nowrap;
  border: 0;
}
</style>
