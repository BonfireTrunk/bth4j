import { api, session, clearSession, setSession, isAdmin } from './api.js';
import { registerComponents, toasts, dismissToast, dialog, closeDialog, toastError } from './ui.js';
import Login from './pages/login.js';
import Overview from './pages/overview.js';
import Queues from './pages/queues.js';
import Tasks from './pages/tasks.js';
import Task from './pages/task.js';
import Dead from './pages/dead.js';
import Schedules from './pages/schedules.js';
import Workers from './pages/workers.js';
import Issues from './pages/issues.js';

const { createApp, ref, computed, watch, onMounted, onUnmounted } = Vue;

const PAGES = {
  overview: Overview,
  queues: Queues,
  tasks: Tasks,
  task: Task,
  dead: Dead,
  schedules: Schedules,
  workers: Workers,
  issues: Issues,
};

const NAV = [
  ['overview', 'Overview'],
  ['queues', 'Queues'],
  ['tasks', 'Tasks'],
  ['dead', 'Dead'],
  ['schedules', 'Schedules'],
  ['workers', 'Workers'],
  ['issues', 'Issues'],
];

function parseHash() {
  const raw = location.hash.replace(/^#\/?/, '');
  const [path, qs] = raw.split('?');
  const segs = path.split('/').filter(Boolean).map(decodeURIComponent);
  const query = Object.fromEntries(new URLSearchParams(qs || ''));
  return { name: segs[0] || 'overview', param: segs[1] || null, query, key: raw };
}

const HEALTH_CLASS = { RUNNING: 'st-succeeded', QUIET: 'st-leased', NEW: 'st-queued', STOPPED: 'st-dead' };

const App = {
  components: { Login },
  setup() {
    const route = ref(parseHash());
    const checking = ref(!!session.csrf);
    const navOpen = ref(false);

    const onHash = () => {
      route.value = parseHash();
      navOpen.value = false;
    };
    onMounted(async () => {
      window.addEventListener('hashchange', onHash);
      if (session.csrf) {
        try {
          const me = await api.me();
          setSession({ user: me.user, role: me.role });
        } catch (e) {
          clearSession();
        }
      }
      checking.value = false;
      guard();
    });
    onUnmounted(() => window.removeEventListener('hashchange', onHash));

    const authed = computed(() => !!session.csrf);
    function guard() {
      if (!authed.value && route.value.name !== 'login') location.hash = '#/login';
      else if (authed.value && route.value.name === 'login') location.hash = '#/overview';
    }
    watch([authed, () => route.value.name], () => {
      if (!checking.value) guard();
    });

    const page = computed(() => PAGES[route.value.name] || null);
    const activeNav = computed(() => (route.value.name === 'task' ? 'tasks' : route.value.name));

    const health = ref(undefined);
    let healthTimer = null;
    async function loadHealth() {
      if (!authed.value) return;
      try {
        health.value = await api.health();
      } catch (e) {
        health.value = undefined;
      }
    }
    onMounted(() => {
      loadHealth();
      healthTimer = setInterval(loadHealth, 10000);
    });
    onUnmounted(() => clearInterval(healthTimer));
    watch(authed, (v) => v && loadHealth());

    const healthLabel = computed(() => {
      if (health.value === undefined) return 'health unknown';
      if (health.value === null) return 'no runner';
      return health.value.status;
    });
    const healthClass = computed(() => (health.value ? HEALTH_CLASS[health.value.status] || 'st-cancelled' : 'st-cancelled'));

    async function logout() {
      try {
        await api.logout();
      } catch (e) {
        toastError(e);
      }
      clearSession();
      location.hash = '#/login';
    }

    function onDialogKey(e) {
      if (e.key === 'Escape') closeDialog(false);
    }

    return {
      route, checking, authed, page, session, NAV, activeNav, navOpen, healthLabel, healthClass, health,
      logout, toasts, dismissToast, dialog, closeDialog, onDialogKey, isAdmin,
    };
  },
  template: `
  <p v-if="checking" class="boot">Checking session...</p>
  <login v-else-if="!authed || route.name === 'login'"></login>
  <div v-else class="shell">
    <header class="top">
      <button class="menu" type="button" @click="navOpen = !navOpen" aria-label="Toggle navigation">
        <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true"><path d="M2 4h14M2 9h14M2 14h14" stroke="currentColor" stroke-width="2" fill="none"/></svg>
      </button>
      <span class="brand">bth4j</span>
      <span class="spacer"></span>
      <span class="badge" :class="healthClass" :title="health ? 'worker ' + health.workerId + ', running ' + health.running : ''">{{ healthLabel }}</span>
      <span class="who">{{ session.user }} <span class="role">{{ session.role }}</span></span>
      <button type="button" @click="logout">Log out</button>
    </header>
    <nav class="side" :class="{ open: navOpen }">
      <a v-for="n in NAV" :key="n[0]" :href="'#/' + n[0]" :class="{ active: activeNav === n[0] }">{{ n[1] }}</a>
    </nav>
    <main class="content">
      <component v-if="page" :is="page" :route="route" :key="route.key"></component>
      <p v-else class="state-msg">Unknown page. <a href="#/overview">Go to overview</a></p>
    </main>
  </div>
  <div class="toasts" aria-live="polite">
    <div v-for="t in toasts" :key="t.id" class="toast" :class="t.kind">
      <span>{{ t.message }}</span>
      <button type="button" class="x" @click="dismissToast(t.id)" aria-label="Dismiss">x</button>
    </div>
  </div>
  <div v-if="dialog.open" class="overlay" @keydown="onDialogKey" @click.self="closeDialog(false)">
    <div class="dialog" role="dialog" aria-modal="true">
      <h2>{{ dialog.title }}</h2>
      <p class="pre">{{ dialog.message }}</p>
      <label v-if="dialog.askReason">Reason
        <input v-model="dialog.reason" type="text" autofocus placeholder="Why is this being cancelled?" @keydown.enter="dialog.reason.trim() && closeDialog(true)">
      </label>
      <div class="row end">
        <button type="button" @click="closeDialog(false)">Back</button>
        <button type="button" :class="dialog.danger ? 'danger' : 'primary'" :disabled="dialog.askReason && !dialog.reason.trim()" @click="closeDialog(true)">{{ dialog.confirmLabel }}</button>
      </div>
    </div>
  </div>`,
};

const app = createApp(App);
registerComponents(app);
app.mount('#app');
