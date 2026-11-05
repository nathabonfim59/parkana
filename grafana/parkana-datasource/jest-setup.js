// Jest setup provided by Grafana scaffolding
import './.config/jest-setup';

// The scaffold setup above installs Node worker_threads MessageChannel and
// MessagePort globals because jsdom lacks them and React 19's scheduler wants
// them when `setImmediate` is unavailable. Those ports keep a libuv handle
// open for the whole test process, so Jest cannot exit cleanly. jsdom tests
// here do not need them: with the globals removed the scheduler uses its
// plain timer fallback and nothing is left hanging after the run.
delete globalThis.MessageChannel;
delete globalThis.MessagePort;
