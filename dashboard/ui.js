import { serverNow } from './api.js';

const { reactive, ref, onMounted, onUnmounted } = Vue;

export const STATES = ['QUEUED', 'SCHEDULED', 'LEASED', 'SUCCEEDED', 'DEAD', 'CANCELLED', 'EXPIRED'];

// one clock for every countdown on the page, set to the server's time because the timestamps compared with it are
// the server's; components read it so relative times tick without a request
export const now = ref(serverNow());
setInterval(() => {
  now.value = serverNow();
}, 1000);

function pad(n) {
  return n < 10 ? '0' + n : String(n);
}

export function relative(iso, at = now.value) {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '';
  const diff = Math.round((t - at) / 1000);
  const abs = Math.abs(diff);
  let text;
  if (abs < 2) return 'now';
  if (abs < 60) text = abs + ' s';
  else if (abs < 3600) text = Math.floor(abs / 60) + ':' + pad(abs % 60);
  else if (abs < 86400) text = Math.floor(abs / 3600) + ' h ' + pad(Math.floor((abs % 3600) / 60)) + ' min';
  else text = Math.floor(abs / 86400) + ' d ' + Math.floor((abs % 86400) / 3600) + ' h';
  return diff > 0 ? 'in ' + text : text + ' ago';
}

export function localTime(iso) {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return String(iso);
  const d = new Date(t);
  const today = new Date();
  const sameDay = d.toDateString() === today.toDateString();
  const time = pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  return sameDay ? time : d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' }) + ' ' + time;
}

export function fullTime(iso) {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return String(iso);
  return new Date(t).toLocaleString(undefined, { hour12: false });
}

export function duration(iso) {
  if (!iso) return '';
  const m = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/i.exec(iso);
  if (!m) return iso;
  const parts = [];
  if (m[1]) parts.push(m[1] + 'd');
  if (m[2]) parts.push(m[2] + 'h');
  if (m[3]) parts.push(m[3] + 'm');
  if (m[4]) parts.push(m[4] + 's');
  return parts.length ? parts.join(' ') : '0s';
}

export function sum(obj) {
  return Object.values(obj || {}).reduce((a, b) => a + Number(b || 0), 0);
}

// Lays out JSON text over lines without parsing it: JSON.parse would round a number above 2^53 and drop the
// trailing zero of 1.10, and a payload is shown as it is stored.
export function indentJson(text) {
  let out = '';
  let depth = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === '"') {
      let end = i + 1;
      while (end < text.length && text[end] !== '"') end += text[end] === '\\' ? 2 : 1;
      out += text.slice(i, end + 1);
      i = end;
    } else if (c === '{' || c === '[') {
      let next = i + 1;
      while (/\s/.test(text[next] || '')) next++;
      if (text[next] === (c === '{' ? '}' : ']')) {
        out += c + text[next];
        i = next;
      } else {
        depth++;
        out += c + '\n' + '  '.repeat(depth);
      }
    } else if (c === '}' || c === ']') {
      depth = Math.max(0, depth - 1);
      out += '\n' + '  '.repeat(depth) + c;
    } else if (c === ',') out += ',\n' + '  '.repeat(depth);
    else if (c === ':') out += ': ';
    else if (!/\s/.test(c)) out += c;
  }
  return out;
}

// Orders two instants as the server writes them: Date.parse stops at milliseconds, and the fraction has
// zero to nine digits, so the text alone does not sort either.
export function compareInstants(a, b) {
  const byMillis = Date.parse(a) - Date.parse(b);
  if (byMillis || Number.isNaN(byMillis)) return byMillis || 0;
  const fraction = (iso) => ((/\.(\d+)/.exec(iso) || [])[1] || '').padEnd(9, '0');
  return fraction(a).localeCompare(fraction(b));
}

export const toasts = reactive([]);
let toastSeq = 0;

export function toast(message, kind = 'error') {
  const id = ++toastSeq;
  if (toasts.some((t) => t.message === message)) return;
  toasts.push({ id, message, kind });
  setTimeout(() => dismissToast(id), kind === 'error' ? 10000 : 4000);
}

export function dismissToast(id) {
  const i = toasts.findIndex((t) => t.id === id);
  if (i >= 0) toasts.splice(i, 1);
}

export function toastError(e) {
  if (e && e.status === 401) return;
  toast(e && e.message ? e.message : String(e));
}

export const dialog = reactive({
  open: false,
  title: '',
  message: '',
  askReason: false,
  danger: false,
  confirmLabel: 'Confirm',
  reason: '',
  resolve: null,
});

export function confirmDialog({ title, message, askReason = false, danger = false, confirmLabel = 'Confirm' }) {
  return new Promise((resolve) => {
    Object.assign(dialog, { open: true, title, message, askReason, danger, confirmLabel, reason: '', resolve });
  });
}

export function closeDialog(ok) {
  const resolve = dialog.resolve;
  const reason = dialog.reason.trim();
  dialog.open = false;
  dialog.resolve = null;
  if (resolve) resolve(ok ? { ok: true, reason } : null);
}

export function usePoll(fetcher, intervalMs = 0) {
  const data = ref(null);
  const loading = ref(true);
  const error = ref('');
  let timer = null;
  let inFlight = false;
  let rerun = null;
  let alive = true;

  async function load() {
    if (inFlight || !alive) return;
    inFlight = true;
    try {
      const result = await fetcher();
      if (!alive) return;
      data.value = result;
      error.value = '';
    } catch (e) {
      if (!alive) return;
      error.value = e.message || String(e);
      toastError(e);
    } finally {
      inFlight = false;
      loading.value = false;
      const next = rerun;
      rerun = null;
      if (next) (alive ? load() : Promise.resolve()).then(next.resolve);
    }
  }

  // A timer tick that finds a request in flight is skipped. A reload asked for after an action must show the
  // state after it, so it waits for the request in flight and makes one more, whatever number of reloads asked.
  function reload() {
    if (!inFlight) return load();
    if (!rerun) {
      rerun = {};
      rerun.promise = new Promise((resolve) => {
        rerun.resolve = resolve;
      });
    }
    return rerun.promise;
  }

  onMounted(() => {
    load();
    if (intervalMs > 0) timer = setInterval(load, intervalMs);
  });
  onUnmounted(() => {
    alive = false;
    if (timer) clearInterval(timer);
  });
  return { data, loading, error, reload };
}

const HOLD_MS = 3000;

// cells that changed between two polls keep a tint for a few seconds, so a change is seen rather than guessed
export function useChanges() {
  const marks = reactive(new Map());
  let previous = null;
  function diff(current) {
    if (previous) {
      for (const [k, v] of Object.entries(current)) {
        if (previous[k] !== undefined && previous[k] !== v) marks.set(k, serverNow() + HOLD_MS);
      }
    }
    previous = current;
  }
  function changed(key) {
    const until = marks.get(key);
    return until !== undefined && until > now.value;
  }
  return { diff, changed };
}

export const StateBadge = {
  props: { state: String, lg: Boolean },
  template:
    '<span class="badge" :class="[\'st-\' + (state || \'none\').toLowerCase(), { lg: lg }]">{{ state || "?" }}</span>',
};

export const Clock = {
  template: '<span class="clock" aria-label="Current time">{{ text }}</span>',
  computed: {
    text() {
      const d = new Date(now.value);
      return pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
    },
  },
};

export const TimeAgo = {
  props: { at: String, late: Boolean },
  template:
    '<span v-if="at" class="time" :class="{ wait: late && !overdue, late: late && overdue }" :title="full"><span class="rel">{{ rel }}</span><span class="abs">{{ local }}</span></span>' +
    '<span v-else class="muted">-</span>',
  computed: {
    local() {
      return localTime(this.at);
    },
    full() {
      return fullTime(this.at);
    },
    rel() {
      return relative(this.at, now.value);
    },
    overdue() {
      return Date.parse(this.at) < now.value;
    },
  },
};

export const Loading = {
  props: { loading: Boolean, empty: Boolean, error: String, emptyText: { type: String, default: 'Nothing to show.' }, emptyHint: String },
  template:
    '<div v-if="loading" class="skeleton" aria-label="Loading"><i></i><i></i><i></i></div>' +
    '<p v-else-if="error && empty" class="state-msg err">Could not load: {{ error }}</p>' +
    '<div v-else-if="empty" class="empty"><b>{{ emptyText }}</b><span v-if="emptyHint">{{ emptyHint }}</span></div>',
};

export function registerComponents(app) {
  app.component('state-badge', StateBadge);
  app.component('time-ago', TimeAgo);
  app.component('station-clock', Clock);
  app.component('status-msg', Loading);
}
