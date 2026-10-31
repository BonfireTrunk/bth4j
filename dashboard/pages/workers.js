import { api } from '../api.js';
import { usePoll } from '../ui.js';

const STALE_MS = 60000;

export default {
  setup() {
    const { data, loading, error, reload } = usePoll(() => api.workers(), 5000);
    const isStale = (w) => !w.lastSeen || Date.now() - Date.parse(w.lastSeen) > STALE_MS;
    return { data, loading, error, reload, isStale };
  },
  template: `
  <section>
    <div class="page-head"><h1>Workers</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data || data.length === 0" empty-text="No workers registered."></status-msg>
    <div class="table-wrap" v-if="data && data.length">
      <table>
        <thead><tr><th>Id</th><th>Label</th><th>Started</th><th>Last seen</th><th class="num">Running</th></tr></thead>
        <tbody>
          <tr v-for="w in data" :key="w.id" :class="{ stale: isStale(w) }">
            <td class="mono">{{ w.id }}</td>
            <td>{{ w.label }}</td>
            <td><time-ago :at="w.startedAt"></time-ago></td>
            <td><time-ago :at="w.lastSeen"></time-ago><span v-if="isStale(w)" class="badge st-expired stale-tag">silent over 1 min</span></td>
            <td class="num">{{ w.running }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>`,
};
