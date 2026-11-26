import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import { h } from 'vue'
import '@fontsource-variable/inter'
import '@fontsource-variable/jetbrains-mono'
import './style.css'
import AgentPrompt from './AgentPrompt.vue'

export default {
  extends: DefaultTheme,
  // The "Install with an agent" prompt sits under the hero buttons on the home page.
  Layout: () => h(DefaultTheme.Layout, null, { 'home-hero-actions-after': () => h(AgentPrompt) }),
} satisfies Theme
