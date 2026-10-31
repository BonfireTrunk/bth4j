import { api, isAdmin } from '../api.js';
import { STATES, usePoll, confirmDialog, toast, toastError } from '../ui.js';

const { ref } = Vue;

export default {
  setup() {
    const { data, loading, error, reload } = usePoll(() => api.queues(), 5000);
    const busy = ref('');
    const admin = isAdmin;

    async function toggle(q) {
      const pausing = !q.paused;
      const res = await confirmDialog({
        title: (pausing ? 'Pause' : 'Resume') + ' queue ' + q.name,
        message: pausing
          ? 'Workers stop claiming tasks from this queue. Running tasks finish.'
          : 'Workers resume claiming tasks from this queue.',
        confirmLabel: pausing ? 'Pause' : 'Resume',
        danger: pausing,
      });
      if (!res) return;
      busy.value = q.name;
      try {
        if (pausing) await api.pauseQueue(q.name);
        else await api.resumeQueue(q.name);
        toast('Queue ' + q.name + (pausing ? ' paused' : ' resumed'), 'ok');
        await reload();
      } catch (e) {
        toastError(e);
      } finally {
        busy.value = '';
      }
    }
    return { data, loading, error, reload, busy, admin, toggle, STATES };
  },
  template: `
  <section>
    <div class="page-head"><h1>Queues</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data || data.length === 0" empty-text="No queues defined."></status-msg>
    <div class="table-wrap" v-if="data && data.length">
      <table>
        <thead><tr><th>Queue</th><th>Paused</th><th v-for="s in STATES" :key="s" class="num"><state-badge :state="s"></state-badge></th><th></th></tr></thead>
        <tbody>
          <tr v-for="q in data" :key="q.name">
            <td><a :href="'#/tasks?queue=' + encodeURIComponent(q.name)">{{ q.name }}</a></td>
            <td><span v-if="q.paused" class="badge st-leased">paused</span><span v-else class="muted">no</span></td>
            <td v-for="s in STATES" :key="s" class="num" :class="{ zero: !(q.counts && q.counts[s]) }">{{ (q.counts && q.counts[s]) || 0 }}</td>
            <td class="actions">
              <button type="button" :disabled="!admin() || busy === q.name" :title="admin() ? '' : 'Admin role required'" @click="toggle(q)">{{ q.paused ? 'Resume' : 'Pause' }}</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>`,
};
