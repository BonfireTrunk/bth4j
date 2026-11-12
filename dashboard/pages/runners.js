import { api } from '../api.js';
import { usePoll, now } from '../ui.js';

const STALE_MS = 60000;

export default {
  setup() {
    const { data, loading, error, reload } = usePoll(() => api.runners(), 5000);
    const isStale = (w) => !w.lastSeen || now.value - Date.parse(w.lastSeen) > STALE_MS;
    return { data, loading, error, reload, isStale };
  },
  template: `
  <section>
    <div class="page-head"><h1>Runners</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data || data.length === 0" empty-text="No runners registered." empty-hint="A runner appears here when it starts against this store."></status-msg>
    <div class="table-wrap" v-if="data && data.length">
      <table>
        <thead><tr><th>Id</th><th>Label</th><th>Queues</th><th>Types</th><th>Started</th><th>Last heartbeat</th><th class="num">Running</th><th>Status</th></tr></thead>
        <tbody>
          <tr v-for="w in data" :key="w.id" :class="{ stale: isStale(w) }">
            <td class="mono">{{ w.id }}</td>
            <td>{{ w.label }}</td>
            <td>{{ w.queues.join(', ') || 'none' }}</td>
            <td>{{ w.types.join(', ') || 'none' }}</td>
            <td><time-ago :at="w.startedAt"></time-ago></td>
            <td><time-ago :at="w.lastSeen"></time-ago></td>
            <td class="num">{{ w.running }}</td>
            <td><span v-if="isStale(w)" class="tag issue">SILENT OVER 1 MIN</span><span v-else-if="!w.accepting" class="tag">quiet</span><span v-else class="muted">alive</span></td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>`,
};
