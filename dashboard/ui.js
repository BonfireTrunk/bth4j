const { reactive, ref, onMounted, onUnmounted } = Vue;

export const STATES = ['QUEUED', 'SCHEDULED', 'LEASED', 'SUCCEEDED', 'DEAD', 'CANCELLED', 'EXPIRED'];

export function relative(iso, now = Date.now()) {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '';
  const diff = Math.round((t - now) / 1000);
  const abs = Math.abs(diff);
  let text;
  if (abs < 5) return 'just now';
  if (abs < 60) text = abs + ' s';
  else if (abs < 3600) text = Math.round(abs / 60) + ' min';
  else if (abs < 86400) text = Math.round(abs / 3600) + ' h';
  else text = Math.round(abs / 86400) + ' d';
  return diff > 0 ? 'in ' + text : text + ' ago';
}

export function localTime(iso) {
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
  let alive = true;

  async function load() {
    if (inFlight) return;
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
    }
  }

  onMounted(() => {
    load();
    if (intervalMs > 0) timer = setInterval(load, intervalMs);
  });
  onUnmounted(() => {
    alive = false;
    if (timer) clearInterval(timer);
  });
  return { data, loading, error, reload: load };
}

export const StateBadge = {
  props: { state: String },
  template: '<span class="badge" :class="\'st-\' + (state || \'none\').toLowerCase()">{{ state || "?" }}</span>',
};

export const TimeAgo = {
  props: { at: String },
  template:
    '<span v-if="at" class="time" :title="at">{{ local }} <span class="rel">({{ rel }})</span></span><span v-else class="muted">-</span>',
  computed: {
    local() {
      return localTime(this.at);
    },
    rel() {
      return relative(this.at);
    },
  },
};

export const Loading = {
  props: { loading: Boolean, empty: Boolean, error: String, emptyText: { type: String, default: 'Nothing to show.' } },
  template:
    '<p v-if="loading" class="state-msg">Loading...</p>' +
    '<p v-else-if="error && empty" class="state-msg err">Could not load: {{ error }}</p>' +
    '<p v-else-if="empty" class="state-msg">{{ emptyText }}</p>',
};

export function registerComponents(app) {
  app.component('state-badge', StateBadge);
  app.component('time-ago', TimeAgo);
  app.component('status-msg', Loading);
}
