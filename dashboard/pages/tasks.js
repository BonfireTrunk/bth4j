import { api, isAdmin } from '../api.js';
import { STATES, confirmDialog, toast, toastError } from '../ui.js';

const { ref, onMounted } = Vue;

const LIMIT = 50;

export function makeTasksPage({ fixedState = null, title = 'Tasks', basePath = 'tasks', bulkRetry = false } = {}) {
  return {
    props: { route: Object },
    setup(props) {
      const state = ref(fixedState || props.route.query.state || 'QUEUED');
      const queue = ref(props.route.query.queue || '');
      const rows = ref([]);
      const queues = ref([]);
      const loading = ref(true);
      const loadingMore = ref(false);
      const error = ref('');
      const hasMore = ref(false);
      const bulkBusy = ref(false);
      const keyText = ref('');
      const keyMissing = ref(false);
      const finding = ref(false);
      const admin = isAdmin;

      let nextCursor = null;
      let requestId = 0;

      async function fetchPage(cursor, id) {
        const page = await api.tasks({ state: state.value, queue: queue.value, cursor: cursor ?? '', limit: LIMIT });
        if (id !== requestId) return null;
        nextCursor = page.next;
        return page.items;
      }

      async function load() {
        const id = ++requestId;
        nextCursor = null;
        hasMore.value = false;
        loading.value = true;
        error.value = '';
        try {
          const page = await fetchPage(undefined, id);
          if (page === null) return;
          rows.value = page;
          hasMore.value = nextCursor !== null;
        } catch (e) {
          if (id !== requestId) return;
          error.value = e.message;
          toastError(e);
        } finally {
          if (id === requestId) loading.value = false;
        }
      }

      async function more() {
        if (nextCursor === null) return;
        const id = requestId;
        loadingMore.value = true;
        try {
          const page = await fetchPage(nextCursor, id);
          if (page === null) return;
          rows.value = rows.value.concat(page);
          hasMore.value = nextCursor !== null;
        } catch (e) {
          if (id === requestId) toastError(e);
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
            'Each visible task is retried with the revision shown here. Tasks that changed meanwhile are skipped. ' +
            'Keys still inside their keep windows are retained; retries with a key held by another task are refused. ' +
            'Released keys are not restored, so a same-key enqueue can start a second task.',
          confirmLabel: 'Retry all visible',
          danger: true,
        });
        if (!res) return;
        bulkBusy.value = true;
        let ok = 0;
        let stale = 0;
        let duplicate = 0;
        let failed = 0;
        for (const t of targets) {
          try {
            await api.retryTask(t.id, t.revision);
            ok++;
          } catch (e) {
            if (e.status === 401) break;
            if (e.conflict) stale++;
            else if (e.duplicate) duplicate++;
            else failed++;
          }
        }
        bulkBusy.value = false;
        let msg = ok + ' retried';
        if (stale) msg += ', ' + stale + ' changed meanwhile (skipped)';
        if (duplicate) msg += ', ' + duplicate + ' refused: the unique key is held by another task';
        if (failed) msg += ', ' + failed + ' failed';
        toast(msg, failed || stale || duplicate ? 'error' : 'ok');
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

      async function findByKey() {
        const key = keyText.value.trim();
        keyMissing.value = false;
        if (!key || finding.value) return;
        finding.value = true;
        try {
          const task = await api.taskByKey(key);
          open(task.id);
        } catch (e) {
          if (e.status === 404) keyMissing.value = true;
          else toastError(e);
        } finally {
          finding.value = false;
        }
      }

      function open(id) {
        location.hash = '#/task/' + encodeURIComponent(id);
      }
      function failureText(t, full) {
        const f = t.failures && t.failures[t.failures.length - 1];
        if (!f) return '';
        const type = full ? f.type : f.type.replace(/^.*\./, '');
        return f.message ? type + ': ' + f.message : type;
      }
      return {
        state, queue, rows, queues, loading, loadingMore, error, hasMore, bulkBusy, admin, keyText, keyMissing, finding, findByKey,
        load, more, apply, retryAll, open, STATES, fixedState, title, bulkRetry, LIMIT, failureText,
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
          <select v-model="state"><option v-for="s in STATES" :key="s" :value="s">{{ s }}</option></select>
        </label>
        <label>Queue
          <select v-if="queues.length" v-model="queue"><option value="">any</option><option v-for="q in queues" :key="q" :value="q">{{ q }}</option></select>
          <input v-else v-model="queue" type="text" placeholder="any">
        </label>
        <button type="submit" class="primary">Show</button>
        <label>Find by key
          <input v-model="keyText" type="text" placeholder="unique key" @input="keyMissing = false" @keydown.enter.prevent="findByKey">
        </label>
        <button type="button" :disabled="finding" @click="findByKey">Find</button>
        <span v-if="keyMissing" class="muted" role="status">No task holds this key</span>
      </form>
      <status-msg :loading="loading" :error="error" :empty="!rows.length" empty-text="No tasks match these filters." :empty-hint="fixedState ? 'Nothing is dead. Good.' : 'Pick another state, or clear the queue filter.'"></status-msg>
      <div class="table-wrap" v-if="!loading && rows.length">
        <table class="clickable">
          <thead><tr><th>Id</th><th>State</th><th>Queue</th><th>Event</th><th class="num">Attempts</th><th>Due</th><th>Changed</th><th>Last failure</th></tr></thead>
          <tbody>
            <tr v-for="t in rows" :key="t.id" @click="open(t.id)">
              <td class="mono"><a class="quiet" :href="'#/task/' + encodeURIComponent(t.id)" @click.stop>{{ t.id }}</a></td>
              <td><state-badge :state="t.state"></state-badge></td>
              <td>{{ t.queue }}</td>
              <td>{{ t.event }}</td>
              <td class="num">{{ t.attempts }} / {{ t.maxAttempts }}</td>
              <td><time-ago :at="t.due" :late="t.state === 'QUEUED' || t.state === 'SCHEDULED'"></time-ago></td>
              <td><time-ago :at="t.stateChanged"></time-ago></td>
              <td class="trunc mono small" :title="failureText(t, true)">{{ failureText(t, false) }}</td>
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
