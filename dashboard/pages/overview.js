import { api } from '../api.js';
import { STATES, usePoll, sum, useChanges } from '../ui.js';

const { computed, watch } = Vue;

export default {
  setup() {
    const { data, loading, error, reload } = usePoll(() => api.overview(), 5000);
    const board = usePoll(loadDepartures, 5000);
    const unserved = usePoll(() => api.unserved(), 5000);
    const { diff, changed } = useChanges();
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
    const rowTotal = (q) => sum(data.value.counts[q]);
    const link = (q, s) => '#/tasks?queue=' + encodeURIComponent(q) + (s ? '&state=' + s : '');

    // A fixed-delay definition is parked a century ahead while its occurrence runs (see schedules.js).
    const PARKED_AFTER_MS = 50 * 365 * 86400 * 1000;
    async function loadDepartures() {
      // The listing is ordered by state change, not by due, so the soonest departures are picked client-side
      // from the server's largest page; beyond 200 waiting tasks the board is approximate.
      const [waiting, schedules] = await Promise.all([api.tasks({ state: 'SCHEDULED', limit: 200 }), api.schedules()]);
      const rows = [];
      for (const t of waiting.items) rows.push({ key: 't' + t.id, at: t.due, what: t.type, queue: t.queue, kind: 'task', href: '#/task/' + t.id, who: '#' + t.id });
      for (const s of schedules) {
        if (s.paused || !s.nextFire || Date.parse(s.nextFire) - Date.now() > PARKED_AFTER_MS) continue;
        rows.push({ key: 's' + s.id, at: s.nextFire, what: s.type, queue: s.queue, kind: 'fire', href: '#/schedules', who: s.id });
      }
      rows.sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
      return rows.slice(0, 8);
    }

    watch(data, (d) => {
      if (!d) return;
      const flat = {};
      for (const [q, counts] of Object.entries(d.counts || {})) {
        for (const s of STATES) flat[q + '|' + s] = Number(counts[s] || 0);
      }
      for (const s of STATES) flat['total|' + s] = Number(totals.value[s] || 0);
      diff(flat);
    });

    const reloadAll = () => Promise.all([reload(), board.reload(), unserved.reload()]);

    return { data, loading, error, reload: reloadAll, board, unserved, queues, totals, grand, health, STATES, changed, rowTotal, link };
  },
  template: `
  <section>
    <div class="page-head"><h1>Overview</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data"></status-msg>
    <template v-if="data">
      <div class="strip">
        <span><span class="n">{{ grand }}</span><span class="l">tasks stored</span></span>
        <a class="strip-item" href="#/tasks?state=LEASED"><span class="n" :class="{ zero: !totals.LEASED }">{{ totals.LEASED || 0 }}</span><span class="l">running now</span></a>
        <a class="strip-item" href="#/tasks?state=SCHEDULED"><span class="n" :class="{ wait: totals.SCHEDULED > 0, zero: !totals.SCHEDULED }">{{ totals.SCHEDULED || 0 }}</span><span class="l">waiting for time</span></a>
        <a class="strip-item" href="#/dead"><span class="n" :class="{ alert: totals.DEAD > 0, zero: !totals.DEAD }">{{ totals.DEAD || 0 }}</span><span class="l">dead</span></a>
        <a class="strip-item" href="#/issues"><span class="n" :class="{ alert: data.issues > 0, zero: !data.issues }">{{ data.issues }}</span><span class="l">issues</span></a>
        <a class="strip-item" href="#/runners"><span class="n" :class="{ zero: !data.runners }">{{ data.runners }}</span><span class="l">runners</span></a>
      </div>

      <div class="cols board">
        <div>
          <h2>Tasks per queue and state</h2>
          <status-msg :empty="queues.length === 0" empty-text="No queues have tasks yet." empty-hint="Counts appear once the first task is enqueued."></status-msg>
          <div class="table-wrap" v-if="queues.length">
            <table>
              <thead><tr><th>Queue</th><th v-for="s in STATES" :key="s" class="num">{{ s }}</th><th class="num">Total</th></tr></thead>
              <tbody>
                <tr v-for="q in queues" :key="q">
                  <td class="platform"><a :href="link(q)">{{ q }}</a></td>
                  <td v-for="s in STATES" :key="s" class="num count" :class="{ zero: !data.counts[q][s], wait: s === 'SCHEDULED', changed: changed(q + '|' + s) }">
                    <a v-if="data.counts[q][s]" class="quiet" :href="link(q, s)">{{ data.counts[q][s] }}</a><span v-else>0</span>
                  </td>
                  <td class="num">{{ rowTotal(q) }}</td>
                </tr>
              </tbody>
              <tfoot><tr><th>Total</th><th v-for="s in STATES" :key="s" class="num" :class="{ changed: changed('total|' + s) }">{{ totals[s] || 0 }}</th><th class="num">{{ grand }}</th></tr></tfoot>
            </table>
          </div>

          <h2>Next to leave</h2>
          <status-msg :loading="board.loading.value && !board.data.value" :error="board.error.value" :empty="board.data.value && board.data.value.length === 0" empty-text="Nothing is waiting for a time." empty-hint="Delayed tasks and the next schedule fires line up here, soonest first."></status-msg>
          <div class="table-wrap departures" v-if="board.data.value && board.data.value.length">
            <table>
              <thead><tr><th>Leaves</th><th>Type</th><th>Queue</th><th class="kindcol">What</th></tr></thead>
              <tbody>
                <tr v-for="r in board.data.value" :key="r.key">
                  <td class="when"><time-ago :at="r.at" late></time-ago></td>
                  <td class="what"><a class="quiet" :href="r.href">{{ r.what }}</a> <span class="kind sm" :class="r.kind">{{ r.kind === 'fire' ? 'schedule' : 'delayed' }}</span></td>
                  <td class="platform">{{ r.queue }}</td>
                  <td class="kindcol"><span class="kind" :class="r.kind">{{ r.kind === 'fire' ? 'schedule' : 'delayed' }}</span> <span class="muted mono">{{ r.who }}</span></td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <div>
          <h2>Unserved work</h2>
          <status-msg :loading="unserved.loading.value && !unserved.data.value" :error="unserved.error.value"></status-msg>
          <div class="panel" v-if="unserved.data.value">
            <p v-if="!unserved.data.value.length" class="muted">No unserved work detected.</p>
            <p v-for="work in unserved.data.value" :key="JSON.stringify([work.queue, work.type])">
              <span class="tag issue">UNSERVED</span>
              <template v-if="work.type === null">No live runner processes queue {{ work.queue }} ({{ work.waiting }} waiting)</template>
              <template v-else>No live runner on queue {{ work.queue }} handles type {{ work.type }} ({{ work.waiting }} waiting)</template>
            </p>
          </div>
          <h2>Runner in this process</h2>
          <div class="panel">
            <p v-if="!health" class="muted">No runner in this process; the server only reads the store.</p>
            <dl v-else class="kv">
              <dt>Status</dt><dd>{{ health.status }}</dd>
              <dt>Runner</dt><dd>{{ health.runnerId }}, {{ health.runningTasks }} running</dd>
              <dt>Last claim</dt><dd><time-ago :at="health.lastClaimAt"></time-ago></dd>
              <dt>Last maintenance</dt><dd><time-ago :at="health.lastMaintenanceAt"></time-ago></dd>
              <dt>Last store error</dt>
              <dd><template v-if="health.lastStoreErrorAt"><time-ago :at="health.lastStoreErrorAt"></time-ago><br><span class="err">{{ health.lastStoreError }}</span></template><span v-else class="muted">none</span></dd>
            </dl>
          </div>
        </div>
      </div>
    </template>
  </section>`,
};
