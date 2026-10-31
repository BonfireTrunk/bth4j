import { api, isAdmin } from '../api.js';
import { usePoll, duration, confirmDialog, toast, toastError } from '../ui.js';

const { ref } = Vue;

export default {
  setup() {
    const { data, loading, error, reload } = usePoll(() => api.schedules(), 5000);
    const busy = ref('');
    const admin = isAdmin;

    async function run(s, spec) {
      const res = await confirmDialog({
        title: spec.label + ' schedule ' + s.id,
        message: spec.message,
        confirmLabel: spec.label,
        danger: !!spec.danger,
      });
      if (!res) return;
      busy.value = s.id;
      try {
        await spec.call(s.id);
        toast('Schedule ' + s.id + ': ' + spec.done, 'ok');
        await reload();
      } catch (e) {
        toastError(e);
      } finally {
        busy.value = '';
      }
    }

    const pause = (s) => run(s, { label: 'Pause', done: 'paused', message: 'No new tasks are created until it is resumed.', call: api.pauseSchedule });
    const resume = (s) => run(s, { label: 'Resume', done: 'resumed', message: 'The schedule fires again from its next fire time.', call: api.resumeSchedule });
    const trigger = (s) => run(s, { label: 'Trigger', done: 'triggered', message: 'Creates one task now, in addition to the regular schedule.', call: api.triggerSchedule });
    const remove = (s) => run(s, { label: 'Delete', done: 'deleted', danger: true, message: 'The schedule definition is removed permanently.', call: api.deleteSchedule });

    return { data, loading, error, reload, busy, admin, pause, resume, trigger, remove, duration };
  },
  template: `
  <section>
    <div class="page-head"><h1>Schedules</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data || data.length === 0" empty-text="No schedules defined."></status-msg>
    <div class="table-wrap" v-if="data && data.length">
      <table>
        <thead><tr><th>Id</th><th>Event</th><th>Queue</th><th>Next fire</th><th>Trigger</th><th>Zone</th><th>Misfire</th><th>Overlap</th><th>Paused</th><th></th></tr></thead>
        <tbody>
          <tr v-for="s in data" :key="s.id">
            <td class="mono">{{ s.id }}</td>
            <td>{{ s.event }}</td>
            <td>{{ s.queue }}</td>
            <td><time-ago :at="s.nextFire"></time-ago></td>
            <td class="mono"><template v-if="s.cron">cron {{ s.cron }}</template><template v-else-if="s.fixedDelay">every {{ duration(s.fixedDelay) }}</template><template v-else>-</template></td>
            <td>{{ s.zone || '-' }}</td>
            <td>{{ s.misfire || '-' }}</td>
            <td>{{ s.overlapSkip ? 'skip' : 'allow' }}</td>
            <td><span v-if="s.paused" class="badge st-leased">paused</span><span v-else class="muted">no</span></td>
            <td class="actions">
              <button type="button" :disabled="!admin() || busy === s.id" @click="s.paused ? resume(s) : pause(s)">{{ s.paused ? 'Resume' : 'Pause' }}</button>
              <button type="button" :disabled="!admin() || busy === s.id" @click="trigger(s)">Trigger</button>
              <button type="button" class="danger" :disabled="!admin() || busy === s.id" @click="remove(s)">Delete</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <p v-if="!admin()" class="muted">Read-only: your role is viewer.</p>
  </section>`,
};
