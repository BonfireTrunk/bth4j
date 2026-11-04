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

    const pause = (s) => run(s, { label: 'Pause', done: 'paused', message: 'No new tasks are created until it is resumed. Tasks already created still run.', call: api.pauseSchedule });
    const resume = (s) => run(s, { label: 'Resume', done: 'resumed', message: 'The schedule fires again from its next fire time.', call: api.resumeSchedule });
    const trigger = (s) => run(s, { label: 'Trigger', done: 'triggered', message: 'Creates one task now, in addition to the regular schedule.', call: api.triggerSchedule });
    const remove = (s) => run(s, { label: 'Delete', done: 'deleted', danger: true, message: 'The schedule definition is removed permanently. Tasks already created are kept.', call: api.deleteSchedule });

    // The runner parks a fixed-delay definition a century ahead while its occurrence runs; the real
    // next fire is only known once that occurrence ends.
    const PARKED_AFTER_MS = 50 * 365 * 86400 * 1000;
    const parked = (s) => !!s.fixedDelay && Date.parse(s.nextFire) - Date.now() > PARKED_AFTER_MS;

    return { data, loading, error, reload, busy, admin, pause, resume, trigger, remove, duration, parked };
  },
  template: `
  <section>
    <div class="page-head"><h1>Schedules</h1><button type="button" @click="reload">Refresh</button></div>
    <status-msg :loading="loading" :error="error" :empty="!data || data.length === 0" empty-text="No schedules defined." empty-hint="Schedules are registered by the application with client.putSchedule(...)."></status-msg>
    <div class="table-wrap" v-if="data && data.length">
      <table>
        <thead><tr><th>Next fire</th><th>Id</th><th>Type</th><th>Queue</th><th>Trigger</th><th>Zone</th><th>Misfire</th><th>Overlap</th><th>Status</th><th></th></tr></thead>
        <tbody>
          <tr v-for="s in data" :key="s.id">
            <td><span v-if="parked(s)" class="muted">held until the running occurrence ends</span><time-ago v-else :at="s.nextFire" :late="!s.paused"></time-ago></td>
            <td class="mono">{{ s.id }}</td>
            <td>{{ s.type }}</td>
            <td>{{ s.queue }}</td>
            <td class="mono"><template v-if="s.cron">{{ s.cron }}</template><template v-else-if="s.fixedDelay">every {{ duration(s.fixedDelay) }}</template><template v-else>-</template></td>
            <td>{{ s.zone || 'UTC' }}</td>
            <td>{{ s.misfire === 'CATCH_UP' ? 'CATCH_UP, up to ' + s.catchUpMaxFires : (s.misfire || '-') }}</td>
            <td>{{ s.overlap.toLowerCase() }}</td>
            <td><span v-if="s.paused" class="tag paused">PAUSED</span><span v-else class="muted">armed</span></td>
            <td class="actions">
              <button type="button" class="small" :disabled="!admin() || busy === s.id" :title="admin() ? '' : 'Admin role required'" @click="s.paused ? resume(s) : pause(s)">{{ s.paused ? 'Resume' : 'Pause' }}</button>
              <button type="button" class="small" :disabled="!admin() || busy === s.id" :title="admin() ? '' : 'Admin role required'" @click="trigger(s)">Trigger</button>
              <button type="button" class="small danger" :disabled="!admin() || busy === s.id" :title="admin() ? '' : 'Admin role required'" @click="remove(s)">Delete</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <p v-if="data && data.length && !admin()" class="muted small">Read-only: your role is viewer.</p>
  </section>`,
};
