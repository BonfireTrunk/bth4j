import { api } from '../api.js';
import { usePoll } from '../ui.js';

const { computed } = Vue;

export default {
  setup() {
    const { data, loading, error, reload } = usePoll(() => api.issues(), 5000);
    const sorted = computed(() => (data.value || []).slice().sort((a, b) => Date.parse(b.at) - Date.parse(a.at)));
    return { sorted, data, loading, error, reload };
  },
  template: `
  <section>
    <div class="page-head"><h1>Issues</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data || data.length === 0" empty-text="No issues. Nothing needs attention."></status-msg>
    <div class="table-wrap" v-if="sorted.length">
      <table>
        <thead><tr><th>When</th><th>Kind</th><th>Detail</th><th>Task</th></tr></thead>
        <tbody>
          <tr v-for="(i, n) in sorted" :key="n">
            <td><time-ago :at="i.at"></time-ago></td>
            <td><span class="badge st-expired">{{ i.kind }}</span></td>
            <td class="wrap">{{ i.detail }}</td>
            <td class="mono"><a v-if="i.taskId" :href="'#/task/' + encodeURIComponent(i.taskId)">{{ i.taskId }}</a><span v-else class="muted">-</span></td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>`,
};
