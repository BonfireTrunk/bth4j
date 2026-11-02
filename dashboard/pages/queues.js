import { api, isAdmin } from '../api.js';
import { STATES, usePoll, confirmDialog, toast, toastError, useChanges } from '../ui.js';

const { ref, watch } = Vue;

export default {
  setup() {
    const { data, loading, error, reload } = usePoll(() => api.queues(), 5000);
    const { diff, changed } = useChanges();
    const busy = ref('');
    const admin = isAdmin;

    watch(data, (d) => {
      if (!d) return;
      const flat = {};
      for (const q of d) for (const s of STATES) flat[q.name + '|' + s] = Number((q.counts && q.counts[s]) || 0);
      diff(flat);
    });

    async function toggle(q) {
      const pausing = !q.paused;
      const res = await confirmDialog({
        title: (pausing ? 'Pause' : 'Resume') + ' queue ' + q.name,
        message: pausing
          ? 'Workers stop claiming tasks from this queue. Tasks already running finish; scheduled tasks still become due.'
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
    const link = (q, s) => '#/tasks?queue=' + encodeURIComponent(q) + (s ? '&state=' + s : '');
    return { data, loading, error, reload, busy, admin, toggle, STATES, changed, link };
  },
  template: `
  <section>
    <div class="page-head"><h1>Queues</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data || data.length === 0" empty-text="No queues defined."></status-msg>
    <div class="table-wrap" v-if="data && data.length">
      <table>
        <thead><tr><th>Queue</th><th>Claims</th><th v-for="s in STATES" :key="s" class="num">{{ s }}</th><th></th></tr></thead>
        <tbody>
          <tr v-for="q in data" :key="q.name">
            <td class="platform"><a :href="link(q.name)">{{ q.name }}</a></td>
            <td><span v-if="q.paused" class="tag paused">PAUSED</span><span v-else class="muted">open</span></td>
            <td v-for="s in STATES" :key="s" class="num count" :class="{ zero: !(q.counts && q.counts[s]), wait: s === 'SCHEDULED', changed: changed(q.name + '|' + s) }">
              <a v-if="q.counts && q.counts[s]" class="quiet" :href="link(q.name, s)">{{ q.counts[s] }}</a><span v-else>0</span>
            </td>
            <td class="actions">
              <button type="button" class="small" :disabled="!admin() || busy === q.name" :title="admin() ? '' : 'Admin role required'" @click="toggle(q)">{{ q.paused ? 'Resume' : 'Pause' }}</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <p v-if="data && data.length && !admin()" class="muted small">Read-only: your role is viewer.</p>
  </section>`,
};
