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
        if (e.conflict) stale.value = true;
        toastError(e);
      } finally {
        busy.value = false;
      }
    }

    async function retry() {
      const t = task.value;
      const res = await confirmDialog({
        title: 'Retry task ' + t.id,
        message: 'Re-queue this ' + t.state + ' task at revision ' + t.revision + '.',
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

    const timeline = computed(() => {
      const t = task.value;
      if (!t) return [];
      return [
        ['Created', t.created],
        ['Due', t.due],
        ['Claimed', t.claimedAt],
        ['State changed (' + t.state + ')', t.stateChanged],
      ].filter((e) => e[1]);
    });

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
      load, loadPayload, retry, cancel, remove, timeline, steps, failures, headers,
      canRetry, canCancel, canDelete, duration,
    };
  },
  template: `
  <section>
    <p><a href="#/tasks">&larr; Tasks</a></p>
    <div class="page-head"><h1>Task <span class="mono">{{ id }}</span></h1><button type="button" @click="load">Reload</button></div>
    <p v-if="stale" class="banner" role="alert">This task changed meanwhile. <button type="button" @click="load">Reload</button> before trying again.</p>
    <status-msg :loading="loading" :error="error" :empty="!task"></status-msg>
    <template v-if="task">
      <div class="card">
        <div class="row">
          <state-badge :state="task.state"></state-badge>
          <span class="muted">revision {{ task.revision }}</span>
          <span class="spacer"></span>
          <button type="button" :disabled="!admin() || busy || !canRetry" :title="admin() ? (canRetry ? '' : 'Only DEAD, CANCELLED or EXPIRED tasks can be retried') : 'Admin role required'" @click="retry">Retry</button>
          <button type="button" class="danger" :disabled="!admin() || busy || !canCancel" :title="admin() ? (canCancel ? '' : 'Only QUEUED, SCHEDULED or LEASED tasks can be cancelled') : 'Admin role required'" @click="cancel">Cancel...</button>
          <button type="button" class="danger" :disabled="!admin() || busy || !canDelete" :title="admin() ? (canDelete ? '' : 'Only finished tasks can be deleted') : 'Admin role required'" @click="remove">Delete</button>
        </div>
        <p v-if="!admin()" class="muted">Read-only: your role is viewer.</p>
      </div>

      <div class="cols">
        <div class="card">
          <h2>Fields</h2>
          <dl class="kv">
            <dt>Queue</dt><dd>{{ task.queue }}</dd>
            <dt>Event</dt><dd>{{ task.event }}</dd>
            <dt>Attempts</dt><dd>{{ task.attempts }} of {{ task.maxAttempts }}</dd>
            <dt>Payload version</dt><dd>{{ task.payloadVersion }}</dd>
            <dt>Account</dt><dd>{{ task.account || '-' }}</dd>
            <dt>Unique key</dt><dd class="mono">{{ task.uniqueKey || '-' }}</dd>
            <dt>Schedule</dt><dd><a v-if="task.scheduleId" href="#/schedules">{{ task.scheduleId }}</a><span v-else>-</span></dd>
            <dt>Retry of</dt><dd><a v-if="task.retryOf" class="mono" :href="'#/task/' + encodeURIComponent(task.retryOf)">{{ task.retryOf }}</a><span v-else>-</span></dd>
            <dt>Worker</dt><dd>{{ task.workerId ?? '-' }}</dd>
            <dt>Lease until</dt><dd><time-ago :at="task.leaseUntil"></time-ago></dd>
            <dt>Start by</dt><dd><time-ago :at="task.startBy"></time-ago></dd>
            <dt>Max run</dt><dd>{{ task.maxRun ? duration(task.maxRun) : '-' }}</dd>
            <dt>Cancel requested</dt><dd><time-ago :at="task.cancelRequestedAt"></time-ago></dd>
            <dt>Cancel reason</dt><dd>{{ task.cancelReason || '-' }}</dd>
            <dt>Progress</dt><dd>{{ task.progress || '-' }}</dd>
          </dl>
          <template v-if="headers.length">
            <h3>Headers</h3>
            <dl class="kv"><template v-for="h in headers" :key="h[0]"><dt class="mono">{{ h[0] }}</dt><dd class="mono">{{ h[1] }}</dd></template></dl>
          </template>
        </div>

        <div class="card">
          <h2>Timeline</h2>
          <ol class="timeline">
            <li v-for="e in timeline" :key="e[0]"><span class="tl-label">{{ e[0] }}</span><time-ago :at="e[1]"></time-ago></li>
          </ol>
          <h2>Payload</h2>
          <button v-if="prettyPayload === null" type="button" :disabled="payloadLoading" @click="loadPayload">{{ payloadLoading ? 'Loading...' : 'Load payload' }}</button>
          <pre v-else class="code">{{ prettyPayload }}</pre>
        </div>
      </div>

      <div class="card">
        <h2>Steps</h2>
        <status-msg :empty="!steps.length" empty-text="No steps completed."></status-msg>
        <div class="table-wrap" v-if="steps.length">
          <table><thead><tr><th>Step</th><th>Finished</th><th>Result</th></tr></thead>
            <tbody><tr v-for="s in steps" :key="s.name"><td class="mono">{{ s.name }}</td><td><time-ago :at="s.at"></time-ago></td><td class="mono wrap">{{ s.result ?? '' }}</td></tr></tbody>
          </table>
        </div>
      </div>

      <div class="card">
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
