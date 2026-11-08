package today.bonfire.oss.bth4j;

import java.time.Instant;

/**
 * Something an operator should look at, kept in memory by the runner that saw it (bounded, newest last).
 *
 * @param kind   one of
 *               <ul>
 *               <li>{@code unknown-type}: a claimed task has no processor on this runner and was deferred</li>
 *               <li>{@code store-error}: a store call failed; repeats of the same error text are one issue whose
 *               detail carries the count and whose time is the latest</li>
 *               <li>{@code lease-lost}: a lease was not renewed or an outcome was refused because the task was
 *               taken over</li>
 *               <li>{@code retry-policy}: a task names a retry policy this runner does not have, or a policy threw</li>
 *               <li>{@code executor-rejected}: the executor refused a claimed task; it went back to its queue</li>
 *               <li>{@code attempt-abandoned}: an attempt ran past its maximum run time and did not stop; its lease
 *               is no longer renewed and its slots stay held until it ends</li>
 *               <li>{@code corrupt-task}: a task with a stored value that cannot be read ended {@code DEAD}</li>
 *               <li>{@code resource-signal-unavailable}: the adaptive executor could not read the CPU or memory
 *               signal</li>
 *               <li>{@code schedule-misfire}: a recurring fire was late and its misfire policy was applied</li>
 *               <li>{@code schedule-overlap}: a recurring fire was skipped because the previous occurrence is still
 *               running</li>
 *               <li>{@code schedule-paused}: a fixed-delay definition was paused because its occurrence did not
 *               succeed</li>
 *               <li>{@code schedule-rearm}: a fixed-delay definition was not re-armed because its delay header
 *               cannot be read</li>
 *               <li>{@code schedule-broken}: a recurring definition cannot be evaluated (for example an invalid
 *               cron expression or a removed time zone); its next fire was moved ahead so that it does not block
 *               the others</li>
 *               </ul>
 * @param taskId the task concerned, or null
 */
public record Issue(Instant at, String kind, String detail, Long taskId) {}
