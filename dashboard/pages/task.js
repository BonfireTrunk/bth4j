import { api, isAdmin } from '../api.js';
import { duration, confirmDialog, toast, toastError } from '../ui.js';

const { ref, computed, onMounted } = Vue;

const RETRYABLE = ['DEAD', 'CANCELLED', 'EXPIRED'];
const CANCELLABLE = ['QUEUED', 'SCHEDULED', 'LEASED'];
const TERMINAL = ['SUCCEEDED', 'DEAD', 'CANCELLED', 'EXPIRED'];

export default {
  props: { route: Object },
  setup(props) {
    const id = props.route.param;
    const task = ref(null);
    const loading = ref(true);
    const error = ref('');
    const stale = ref(false);
    const busy = ref(false);
    const payload = ref(null);
    const payloadLoading = ref(false);
    const admin = isAdmin;

    async function load() {
      loading.value = true;
      error.value = '';
      try {
        task.value = await api.task(id);
        stale.value = false;
        loadPayload();
      } catch (e) {
        error.value = e.status === 404 ? 'Task ' + id + ' does not exist (it may have been deleted or expired).' : e.message;
        task.value = null;
        toastError(e);
      } finally {
        loading.value = false;
      }
    }

    async function loadPayload() {
      payloadLoading.value = true;
      try {
        const res = await api.payload(id);
        payload.value = res && res.payload !== undefined ? res.payload : null;
        if (payload.value === null) payload.value = '';
      } catch (e) {
        toastError(e);
      } finally {
        payloadLoading.value = false;
      }
    }

    const prettyPayload = computed(() => {
      if (payload.value === null) return null;
      if (payload.value === '') return '(no payload)';
      if (typeof payload.value !== 'string') return JSON.stringify(payload.value, null, 2);
      try {
        return JSON.stringify(JSON.parse(payload.value), null, 2);
      } catch (e) {
        return payload.value;
      }
    });

    async function act(fn, okMessage, after) {
      busy.value = true;
      try {
        await fn();
        toast(okMessage, 'ok');
        if (after) after();
        else await load();
      } catch (e) {
        if (e.stale) stale.value = true;
        toastError(e);
        if (e.gone) await load();
      } finally {
        busy.value = false;
      }
    }

    async function retry() {
      const t = task.value;
      const res = await confirmDialog({
        title: 'Retry task ' + t.id,
        message: 'Re-queue this ' + t.state + ' task at revision ' + t.revision +
          '. A key still inside its keep window is retained, with a fresh window when the task finishes. ' +
          'A released key is not restored; an enqueue with that key can start a second task.',
        confirmLabel: 'Retry',
      });
      if (res) await act(() => api.retryTask(t.id, t.revision), 'Task ' + t.id + ' retried');
    }

    async function cancel() {
      const t = task.value;
      const res = await confirmDialog({
        title: 'Cancel task ' + t.id,
        message: 'The task stops at its next safe point. A reason is required and kept with the task.',
        askReason: true,
        confirmLabel: 'Cancel task',
        danger: true,
      });
      if (res) await act(() => api.cancelTask(t.id, t.revision, res.reason), 'Cancel requested for task ' + t.id);
    }

    async function remove() {
      const t = task.value;
      const res = await confirmDialog({
        title: 'Delete task ' + t.id,
        message: 'This permanently removes the task and its history. It cannot be undone.',
        confirmLabel: 'Delete',
        danger: true,
      });
      if (res) {
        await act(() => api.deleteTask(t.id, t.revision), 'Task ' + t.id + ' deleted', () => {
          location.hash = '#/tasks';
        });
      }
    }

    const FINISHED = { SUCCEEDED: 'Succeeded', DEAD: 'Dead', CANCELLED: 'Cancelled', EXPIRED: 'Expired' };
    const route = computed(() => {
      const t = task.value;
      if (!t) return [];
      const stops = [
        { label: 'Created', at: t.createdAt, reached: true },
        { label: 'Due', at: t.due, reached: Date.parse(t.due) <= Date.now() || t.state !== 'SCHEDULED' },
        { label: 'Claimed', at: t.claimedAt, reached: !!t.claimedAt },
      ];
      if (FINISHED[t.state]) stops.push({ label: FINISHED[t.state], at: t.stateChangedAt, reached: true });
      else if (t.state === 'LEASED') stops.push({ label: 'Running', at: t.stateChangedAt, reached: true });
      else stops.push({ label: 'Finished', at: null, reached: false });
      return stops;
    });
    const cancelled = computed(() => task.value && (task.value.cancelRequestedAt || task.value.cancelReason));

    const steps = computed(() =>
      Object.entries((task.value && task.value.steps) || {}).map(([name, s]) => ({ name, at: s && s.at, result: s && s.result })),
    );
    const failures = computed(() => {
      const f = (task.value && task.value.failures) || [];
      return f.slice(-20).reverse();
    });
    const headers = computed(() => Object.entries((task.value && task.value.headers) || {}));
    const canRetry = computed(() => task.value && RETRYABLE.includes(task.value.state));
    const canCancel = computed(() => task.value && CANCELLABLE.includes(task.value.state));
    const canDelete = computed(() => task.value && TERMINAL.includes(task.value.state));

    onMounted(load);
    return {
      id, task, loading, error, stale, busy, payload, prettyPayload, payloadLoading, admin,
      load, loadPayload, retry, cancel, remove, route, cancelled, steps, failures, headers,
      canRetry, canCancel, canDelete, duration,
    };
  },
  template: `
  <section>
    <p class="crumbs"><a href="#/tasks">Tasks</a> / task {{ id }}</p>
    <div class="page-head"><h1>Task <span class="id">{{ id }}</span></h1><button type="button" @click="load">Reload</button></div>
    <p v-if="stale" class="banner" role="alert"><span>This task changed meanwhile.</span><button type="button" class="small" @click="load">Reload</button><span class="muted">before trying again.</span></p>
    <status-msg :loading="loading" :error="error" :empty="!task"></status-msg>
    <template v-if="task">
      <div class="panel head-panel">
        <state-badge :state="task.state" lg></state-badge>
        <span class="rev">revision {{ task.revision }}</span>
        <span v-if="task.state === 'LEASED' && task.leaseUntil" class="muted">lease <time-ago :at="task.leaseUntil"></time-ago></span>
        <span v-else-if="task.state === 'SCHEDULED' || task.state === 'QUEUED'" class="muted">due <time-ago :at="task.due" late></time-ago></span>
        <span class="spacer"></span>
        <button type="button" :disabled="!admin() || busy || !canRetry" :title="admin() ? (canRetry ? '' : 'Only DEAD, CANCELLED or EXPIRED tasks can be retried') : 'Admin role required'" @click="retry">Retry</button>
        <button type="button" class="danger" :disabled="!admin() || busy || !canCancel" :title="admin() ? (canCancel ? '' : 'Only QUEUED, SCHEDULED or LEASED tasks can be cancelled') : 'Admin role required'" @click="cancel">Cancel...</button>
        <button type="button" class="danger" :disabled="!admin() || busy || !canDelete" :title="admin() ? (canDelete ? '' : 'Only finished tasks can be deleted') : 'Admin role required'" @click="remove">Delete</button>
      </div>
      <p v-if="!admin()" class="muted small">Read-only: your role is viewer.</p>

      <ol class="route" aria-label="Route">
        <li v-for="s in route" :key="s.label" class="stop" :class="{ reached: s.reached, pending: !s.reached }">
          <span class="l">{{ s.label }}</span>
          <time-ago v-if="s.at" :at="s.at"></time-ago><span v-else class="muted">not yet</span>
        </li>
      </ol>

      <div class="cols">
        <div class="panel">
          <h2>Routing</h2>
          <dl class="kv">
            <dt>Queue</dt><dd><a :href="'#/tasks?queue=' + encodeURIComponent(task.queue) + '&state=' + task.state">{{ task.queue }}</a></dd>
            <dt>Type</dt><dd>{{ task.type }}</dd>
            <dt>Payload version</dt><dd>{{ task.payloadVersion }}</dd>
            <dt>Account</dt><dd>{{ task.account || '-' }}</dd>
            <dt>Unique key</dt><dd class="mono">{{ task.uniqueKey || '-' }}</dd>
            <dt>Schedule</dt><dd><a v-if="task.scheduleId" href="#/schedules">{{ task.scheduleId }}</a><span v-else>-</span></dd>
            <dt>Retry of</dt><dd><a v-if="task.retryOf" class="mono" :href="'#/task/' + encodeURIComponent(task.retryOf)">{{ task.retryOf }}</a><span v-else>-</span></dd>
          </dl>
          <template v-if="headers.length">
            <h3>Headers</h3>
            <dl class="kv"><template v-for="h in headers" :key="h[0]"><dt class="mono">{{ h[0] }}</dt><dd class="mono">{{ h[1] }}</dd></template></dl>
          </template>
          <h3>Execution</h3>
          <dl class="kv">
            <dt>Attempts</dt><dd>{{ task.attempts }} of {{ task.maxAttempts }}</dd>
            <dt>Runner</dt><dd>{{ task.runnerId ?? '-' }}</dd>
            <dt>Lease until</dt><dd><time-ago :at="task.leaseUntil"></time-ago></dd>
            <dt>Start by</dt><dd><time-ago :at="task.startBy" late></time-ago></dd>
            <dt>Max run time</dt><dd>{{ task.maxRunTime ? duration(task.maxRunTime) : '-' }}</dd>
            <dt>Progress</dt><dd>{{ task.progress || '-' }}</dd>
          </dl>
          <template v-if="cancelled">
            <h3>Cancellation</h3>
            <dl class="kv">
              <dt>Requested</dt><dd><time-ago :at="task.cancelRequestedAt"></time-ago></dd>
              <dt>Reason</dt><dd>{{ task.cancelReason || '-' }}</dd>
            </dl>
          </template>
        </div>

        <div class="panel">
          <h2>Payload</h2>
          <div v-if="payloadLoading && prettyPayload === null" class="skeleton" aria-label="Loading payload"><i></i><i></i></div>
          <pre v-else-if="prettyPayload !== null" class="code">{{ prettyPayload }}</pre>
          <p v-else class="muted">Payload could not be loaded. <button type="button" class="small" @click="loadPayload">Try again</button></p>
        </div>
      </div>

      <div class="panel">
        <h2>Steps</h2>
        <status-msg :empty="!steps.length" empty-text="No steps completed." empty-hint="Steps appear when the processor records them with ctx.step(...)."></status-msg>
        <div class="table-wrap" v-if="steps.length">
          <table><thead><tr><th>Step</th><th>Finished</th><th>Result</th></tr></thead>
            <tbody><tr v-for="s in steps" :key="s.name"><td class="mono">{{ s.name }}</td><td><time-ago :at="s.at"></time-ago></td><td class="mono wrap">{{ s.result ?? '' }}</td></tr></tbody>
          </table>
        </div>
      </div>

      <div class="panel">
        <h2>Failures <span class="muted" v-if="task.failures && task.failures.length > 20">(last 20 of {{ task.failures.length }})</span></h2>
        <status-msg :empty="!failures.length" empty-text="No failures recorded."></status-msg>
        <ul class="failures" v-if="failures.length">
          <li v-for="f in failures" :key="f.attempt + '-' + f.at">
            <div><strong>Attempt {{ f.attempt }}</strong> <time-ago :at="f.at"></time-ago></div>
            <div class="mono wrap">{{ f.type }}<template v-if="f.message">: {{ f.message }}</template></div>
            <details v-if="f.stack"><summary>Stack trace</summary><pre class="code">{{ f.stack }}</pre></details>
          </li>
        </ul>
      </div>
    </template>
  </section>`,
};
