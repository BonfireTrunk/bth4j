import { api, isAdmin } from '../api.js';
import { STATES, confirmDialog, toast, toastError } from '../ui.js';

const { ref, onMounted } = Vue;

const LIMIT = 50;

export function makeTasksPage({ fixedState = null, title = 'Tasks', basePath = 'tasks', bulkRetry = false } = {}) {
  return {
    props: { route: Object },
    setup(props) {
      const state = ref(fixedState || props.route.query.state || '');
      const queue = ref(props.route.query.queue || '');
      const rows = ref([]);
      const queues = ref([]);
      const loading = ref(true);
      const loadingMore = ref(false);
      const error = ref('');
      const hasMore = ref(false);
      const bulkBusy = ref(false);
      const admin = isAdmin;

      async function fetchPage(after) {
        return api.tasks({ state: state.value, queue: queue.value, after, limit: LIMIT });
      }

      async function load() {
        loading.value = true;
        error.value = '';
        try {
          const page = await fetchPage(undefined);
          rows.value = page;
          hasMore.value = page.length >= LIMIT;
        } catch (e) {
          error.value = e.message;
          toastError(e);
        } finally {
          loading.value = false;
        }
      }

      async function more() {
        if (!rows.value.length) return;
        loadingMore.value = true;
        try {
          const page = await fetchPage(rows.value[rows.value.length - 1].id);
          rows.value = rows.value.concat(page);
          hasMore.value = page.length >= LIMIT;
        } catch (e) {
          toastError(e);
        } finally {
          loadingMore.value = false;
        }
      }

      function apply() {
        const q = new URLSearchParams();
        if (!fixedState && state.value) q.set('state', state.value);
        if (queue.value) q.set('queue', queue.value);
        const qs = q.toString();
        const next = '#/' + basePath + (qs ? '?' + qs : '');
        if (location.hash === next) load();
        else location.hash = next;
      }

      async function retryAll() {
        const targets = rows.value.slice();
        const res = await confirmDialog({
          title: 'Retry ' + targets.length + ' dead task' + (targets.length === 1 ? '' : 's'),
          message:
            'Each visible task is retried with the revision shown here. Tasks that changed meanwhile are skipped.',
          confirmLabel: 'Retry all visible',
          danger: true,
        });
        if (!res) return;
        bulkBusy.value = true;
        let ok = 0;
        let stale = 0;
        let failed = 0;
        for (const t of targets) {
          try {
            await api.retryTask(t.id, t.revision);
            ok++;
          } catch (e) {
            if (e.status === 401) break;
            if (e.conflict) stale++;
            else failed++;
          }
        }
        bulkBusy.value = false;
        let msg = ok + ' retried';
        if (stale) msg += ', ' + stale + ' changed meanwhile (skipped)';
        if (failed) msg += ', ' + failed + ' failed';
        toast(msg, failed || stale ? 'error' : 'ok');
        await load();
      }

      onMounted(async () => {
        load();
        try {
          queues.value = (await api.queues()).map((q) => q.name);
        } catch (e) {
          /* the queue filter then just stays a free text box */
        }
      });

      function open(id) {
        location.hash = '#/task/' + encodeURIComponent(id);
      }
      return {
        state, queue, rows, queues, loading, loadingMore, error, hasMore, bulkBusy, admin,
        load, more, apply, retryAll, open, STATES, fixedState, title, bulkRetry, LIMIT,
      };
    },
    template: `
    <section>
      <div class="page-head">
        <h1>{{ title }}</h1>
        <button type="button" @click="load">Refresh</button>
        <button v-if="bulkRetry" type="button" class="danger" :disabled="!admin() || bulkBusy || !rows.length" :title="admin() ? '' : 'Admin role required'" @click="retryAll">
          {{ bulkBusy ? 'Retrying...' : 'Retry all visible (' + rows.length + ')' }}
        </button>
      </div>
      <form class="filters" @submit.prevent="apply">
        <label v-if="!fixedState">State
          <select v-model="state"><option value="">any</option><option v-for="s in STATES" :key="s" :value="s">{{ s }}</option></select>
        </label>
        <label>Queue
          <select v-if="queues.length" v-model="queue"><option value="">any</option><option v-for="q in queues" :key="q" :value="q">{{ q }}</option></select>
          <input v-else v-model="queue" type="text" placeholder="any">
        </label>
        <button type="submit" class="primary">Apply</button>
      </form>
      <status-msg :loading="loading" :error="error" :empty="!rows.length" empty-text="No tasks match these filters."></status-msg>
      <div class="table-wrap" v-if="!loading && rows.length">
        <table class="clickable">
          <thead><tr><th>Id</th><th>State</th><th>Queue</th><th>Event</th><th class="num">Attempts</th><th>Due</th><th>Changed</th><th>Last failure</th></tr></thead>
          <tbody>
            <tr v-for="t in rows" :key="t.id" @click="open(t.id)">
              <td class="mono"><a :href="'#/task/' + encodeURIComponent(t.id)" @click.stop>{{ t.id }}</a></td>
              <td><state-badge :state="t.state"></state-badge></td>
              <td>{{ t.queue }}</td>
              <td>{{ t.event }}</td>
              <td class="num">{{ t.attempts }}/{{ t.maxAttempts }}</td>
              <td><time-ago :at="t.due"></time-ago></td>
              <td><time-ago :at="t.stateChanged"></time-ago></td>
              <td class="trunc">{{ t.lastFailure ? t.lastFailure.type + ': ' + (t.lastFailure.message || '') : '' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="row" v-if="!loading && rows.length">
        <span class="muted">{{ rows.length }} shown</span>
        <button v-if="hasMore" type="button" :disabled="loadingMore" @click="more">{{ loadingMore ? 'Loading...' : 'Load next ' + LIMIT }}</button>
        <span v-else class="muted">end of list</span>
      </div>
    </section>`,
  };
}

export default makeTasksPage();
