import { api, isAdmin, serverNow } from '../api.js';
import { usePoll, duration, confirmDialog, toast, toastError } from '../ui.js';

const { ref, computed } = Vue;

const LIMIT = 100;

export default {
  setup() {
    // The poll asks again for every page the operator has loaded, one after the other, each from the cursor the
    // one before answered, so that the pages neither overlap nor leave a gap when definitions come and go. "Load
    // more" and the poll take turns, so that neither overwrites what the other read.
    let pages = 1;
    let turn = Promise.resolve();
    let shown = { items: [], next: null };
    const inTurn = (work) => {
      const result = turn.then(work);
      turn = result.catch(() => {});
      return result;
    };
    const { data, loading, error, reload } = usePoll(
      () =>
        inTurn(async () => {
          let items = [];
          let cursor = null;
          for (let i = 0; i < pages; i++) {
            const page = await api.schedules({ cursor: cursor ?? '', limit: LIMIT });
            items = items.concat(page.items);
            cursor = page.next;
            if (cursor === null) break;
          }
          shown = { items, next: cursor };
          return shown;
        }),
      5000,
    );
    const rows = computed(() => (data.value ? data.value.items : []));
    const hasMore = computed(() => !!data.value && data.value.next !== null);
    const loadingMore = ref(false);
    async function more() {
      loadingMore.value = true;
      try {
        await inTurn(async () => {
          if (shown.next === null) return;
          const page = await api.schedules({ cursor: shown.next, limit: LIMIT });
          pages++;
          shown = { items: shown.items.concat(page.items), next: page.next };
          data.value = shown;
        });
      } catch (e) {
        toastError(e);
      } finally {
        loadingMore.value = false;
      }
    }
    const busy = ref('');
    const stale = ref(false);
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
        await spec.call(s.id, s.revision);
        stale.value = false;
        toast('Schedule ' + s.id + ': ' + spec.done, 'ok');
        await reload();
      } catch (e) {
        if (e.stale) stale.value = true;
        toastError(e);
        if (e.stale || e.gone) await reload();
      } finally {
        busy.value = '';
      }
    }

    const pause = (s) => run(s, { label: 'Pause', done: 'paused', message: 'No new tasks are created until it is resumed. Tasks already created still run.', call: api.pauseSchedule });
    const resume = (s) => run(s, { label: 'Resume', done: 'resumed', message: 'The schedule fires again from its next fire time.', call: api.resumeSchedule });
    const trigger = (s) => run(s, { label: 'Trigger', done: 'triggered', message: 'Moves the next fire time to now; the runner creates one task on its next scheduling round, in addition to the regular schedule.', call: api.triggerSchedule });
    const remove = (s) => run(s, { label: 'Delete', done: 'deleted', danger: true, message: 'The schedule definition is removed permanently. Tasks already created are kept.', call: api.deleteSchedule });

    // The runner parks a fixed-delay definition a century ahead while its occurrence runs; the real
    // next fire is only known once that occurrence ends.
    const PARKED_AFTER_MS = 50 * 365 * 86400 * 1000;
    const parked = (s) => !!s.fixedDelay && Date.parse(s.nextFireAt) - serverNow() > PARKED_AFTER_MS;

    function refresh() {
      stale.value = false;
      return reload();
    }

    return { rows, loading, error, reload, refresh, stale, busy, admin, pause, resume, trigger, remove, duration, parked, hasMore, loadingMore, more };
  },
  template: `
  <section>
    <div class="page-head"><h1>Schedules</h1><button type="button" @click="refresh">Refresh</button></div>
    <p v-if="stale" class="banner" role="alert"><span>This schedule changed meanwhile.</span><button type="button" class="small" @click="refresh">Reload</button><span class="muted">before trying again.</span></p>
    <status-msg :loading="loading" :error="error" :empty="rows.length === 0" empty-text="No schedules defined." empty-hint="Schedules are registered by the application with client.putSchedule(...)."></status-msg>
    <div class="table-wrap" v-if="rows.length">
      <table>
        <thead><tr><th>Next fire</th><th>Id</th><th>Type</th><th>Queue</th><th>Trigger</th><th>Zone</th><th>Misfire</th><th>Overlap</th><th>Status</th><th></th></tr></thead>
        <tbody>
          <tr v-for="s in rows" :key="s.id">
            <td><span v-if="parked(s)" class="muted">held until the running occurrence ends</span><time-ago v-else :at="s.nextFireAt" :late="!s.paused"></time-ago></td>
            <td class="mono">{{ s.id }}</td>
            <td>{{ s.type }}</td>
            <td>{{ s.queue }}</td>
            <td class="mono"><template v-if="s.cron">{{ s.cron }}</template><template v-else-if="s.fixedDelay">every {{ duration(s.fixedDelay) }}</template><template v-else>-</template></td>
            <td>{{ s.cron ? (s.zone || 'UTC') : '-' }}</td>
            <td>{{ s.misfire === 'CATCH_UP' ? 'CATCH_UP, up to ' + s.catchUpMaxFires : (s.misfire || '-') }}</td>
            <td>{{ s.overlap.toLowerCase() }}</td>
            <td><span v-if="s.paused" class="tag paused">PAUSED</span><span v-else class="muted">armed</span></td>
            <td class="actions">
              <button type="button" class="small" :disabled="!admin() || busy === s.id" :title="admin() ? '' : 'Admin role required'" @click="s.paused ? resume(s) : pause(s)">{{ s.paused ? 'Resume' : 'Pause' }}</button>
              <button type="button" class="small" :disabled="!admin() || busy === s.id || s.paused" :title="!admin() ? 'Admin role required' : s.paused ? 'Resume the schedule first; a paused schedule does not fire' : ''" @click="trigger(s)">Trigger</button>
              <button type="button" class="small danger" :disabled="!admin() || busy === s.id" :title="admin() ? '' : 'Admin role required'" @click="remove(s)">Delete</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div class="row" v-if="rows.length">
      <span class="muted">{{ rows.length }} shown</span>
      <button v-if="hasMore" type="button" :disabled="loadingMore" @click="more">{{ loadingMore ? 'Loading...' : 'Load more' }}</button>
      <span v-else class="muted">end of list</span>
    </div>
    <p v-if="rows.length && !admin()" class="muted small">Read-only: your role is viewer.</p>
  </section>`,
};
