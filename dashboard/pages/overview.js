import { api } from '../api.js';
import { STATES, usePoll, sum } from '../ui.js';

const { computed } = Vue;

export default {
  setup() {
    const { data, loading, error, reload } = usePoll(() => api.overview(), 5000);
    const queues = computed(() => Object.keys((data.value && data.value.counts) || {}).sort());
    const totals = computed(() => {
      const t = {};
      for (const q of queues.value) {
        for (const s of STATES) t[s] = (t[s] || 0) + Number(data.value.counts[q][s] || 0);
      }
      return t;
    });
    const grand = computed(() => sum(totals.value));
    const health = computed(() => (data.value ? data.value.health : null));
    return { data, loading, error, reload, queues, totals, grand, health, STATES };
  },
  template: `
  <section>
    <div class="page-head"><h1>Overview</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data"></status-msg>
    <template v-if="data">
      <div class="cards">
        <div class="card stat"><span class="num">{{ grand }}</span><span class="lbl">tasks stored</span></div>
        <div class="card stat"><span class="num">{{ data.workers }}</span><span class="lbl">workers</span></div>
        <div class="card stat"><a :href="'#/issues'" class="num" :class="{ warn: data.issues > 0 }">{{ data.issues }}</a><span class="lbl">issues</span></div>
        <div class="card">
          <h2>Runner health</h2>
          <p v-if="!health" class="muted">No runner in this process.</p>
          <dl v-else class="kv">
            <dt>Status</dt><dd>{{ health.status }}</dd>
            <dt>Worker</dt><dd>{{ health.workerId }} (running {{ health.running }})</dd>
            <dt>Last claim</dt><dd><time-ago :at="health.lastClaimAt"></time-ago></dd>
            <dt>Last maintenance</dt><dd><time-ago :at="health.lastMaintenanceAt"></time-ago></dd>
            <dt>Last store error</dt>
            <dd><template v-if="health.lastStoreErrorAt"><time-ago :at="health.lastStoreErrorAt"></time-ago><br><span class="err">{{ health.lastStoreError }}</span></template><span v-else class="muted">none</span></dd>
          </dl>
        </div>
      </div>
      <h2>Tasks per queue and state</h2>
      <status-msg :empty="queues.length === 0" empty-text="No queues have tasks yet."></status-msg>
      <div class="table-wrap" v-if="queues.length">
        <table>
          <thead><tr><th>Queue</th><th v-for="s in STATES" :key="s" class="num"><state-badge :state="s"></state-badge></th><th class="num">Total</th></tr></thead>
          <tbody>
            <tr v-for="q in queues" :key="q">
              <td><a :href="'#/tasks?queue=' + encodeURIComponent(q)">{{ q }}</a></td>
              <td v-for="s in STATES" :key="s" class="num" :class="{ zero: !data.counts[q][s] }">
                <a v-if="data.counts[q][s]" :href="'#/tasks?queue=' + encodeURIComponent(q) + '&state=' + s">{{ data.counts[q][s] }}</a><span v-else>0</span>
              </td>
              <td class="num">{{ Object.values(data.counts[q]).reduce((a, b) => a + Number(b), 0) }}</td>
            </tr>
          </tbody>
          <tfoot><tr><th>Total</th><th v-for="s in STATES" :key="s" class="num">{{ totals[s] || 0 }}</th><th class="num">{{ grand }}</th></tr></tfoot>
        </table>
      </div>
    </template>
  </section>`,
};
