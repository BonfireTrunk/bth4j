package today.bonfire.oss.bth4j.redis;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The Lua scripts that implement every store operation. Each script is atomic and checks the task's
 * revision and state before it moves a task, so a call from a worker that lost its lease does nothing.
 * <p>
 * ARGV[1] of every script is the namespace prefix {@code {ns}:}; the scripts build all other key names from it
 * and the single declared key only routes the call. This is valid because every key shares the hash tag, so
 * all of them are in one slot on Redis Cluster. Times come from {@code redis.call('TIME')} in milliseconds;
 * reading the clock before writing needs effect replication, the default since Redis 7.0.
 * <p>
 * Every state change goes through {@code move}, {@code enter} and {@code leave}, which keep the structure that
 * represents the state (queue zset, {@code sched}, {@code leased} plus {@code idx}, or {@code idx}) in step
 * with the hash. Zset writes are collected per key and flushed as one command per key at the end of the
 * script, so a batch of tasks costs a few commands per zset instead of a few per task. The hash is read with
 * one HMGET and written with one HSET per task; the revision and the attempt count are computed in Lua from
 * the values read. Task ids never go through Lua numbers (a double keeps 53 bits), only revisions, attempts
 * and times do.
 */
final class RedisScripts {

    private RedisScripts() {}

    /** Failures kept per task in {@code f:<id>}; FAIL trims the list after every push. */
    static final int MAX_FAILURES = 20;

    private static final String PRELUDE = """
            local P = ARGV[1]
            local adds, rems = {}, {}

            local function now()
              local t = redis.call('TIME')
              return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
            end

            local function zadd(key, score, id)
              local a = adds[key]
              if not a then a = {}; adds[key] = a end
              a[#a + 1] = score
              a[#a + 1] = id
            end

            local function zrem(key, id)
              local r = rems[key]
              if not r then r = {}; rems[key] = r end
              r[#r + 1] = id
            end

            -- Removals go first, so an id removed from and added to the same zset in one call ends up in it.
            local function flush()
              for key, m in pairs(rems) do
                for i = 1, #m, 1000 do redis.call('ZREM', key, unpack(m, i, math.min(i + 999, #m))) end
              end
              for key, m in pairs(adds) do
                for i = 1, #m, 2000 do redis.call('ZADD', key, unpack(m, i, math.min(i + 1999, #m))) end
              end
              adds, rems = {}, {}
            end

            local function enter(id, qid, state, score, at)
              if state == 0 then
                zadd(P .. 'q:' .. qid, score, id)
              elseif state == 1 then
                zadd(P .. 'sched:' .. qid, score, id)
              else
                if state == 2 then zadd(P .. 'leased', score, id) end
                zadd(P .. 'idx:' .. state .. ':' .. qid, at, id)
              end
            end

            local function leave(id, qid, state)
              if state == 0 then
                zrem(P .. 'q:' .. qid, id)
              elseif state == 1 then
                zrem(P .. 'sched:' .. qid, id)
              else
                if state == 2 then zrem(P .. 'leased', id) end
                zrem(P .. 'idx:' .. state .. ':' .. qid, id)
              end
            end

            -- The zset side of a state change; the caller writes state, sc and the new rev with its own HSET.
            local function move(id, qid, from, to, at, score)
              leave(id, qid, from)
              enter(id, qid, to, score, at)
            end

            -- Reads rev, state, q and the given fields; nil unless the task is LEASED at that revision.
            local function fence(tkey, rev, ...)
              local v = redis.call('HMGET', tkey, 'rev', 'state', 'q', ...)
              if v[1] ~= rev or v[2] ~= '2' then return nil end
              return v
            end

            -- Settles the unique key of a task that ended. A kept window keeps the k: string alive through
            -- PEXPIREAT and, when the hash is kept, its uk, kk and ku fields through HPEXPIREAT (the caller
            -- stored ku = at + kk already); without a window both go now. The k: string is only touched while
            -- it still names this task.
            local function finishKey(tkey, id, uk, kk, at, hashKept)
              if not uk then return end
              local kkey = P .. 'k:' .. uk
              local owned = redis.call('GET', kkey) == id
              if kk then
                local until_ = at + tonumber(kk)
                if owned then redis.call('PEXPIREAT', kkey, until_) end
                if hashKept then redis.call('HPEXPIREAT', tkey, until_, 'FIELDS', 3, 'uk', 'kk', 'ku') end
              else
                if owned then redis.call('DEL', kkey) end
                if hashKept then redis.call('HDEL', tkey, 'uk', 'kk') end
              end
            end

            -- Ends a task in the terminal state to. Kept: one HSET of state, sc, rev, ku and the extra
            -- field/value pairs; the lease field stays and readers ignore it outside LEASED. Not kept: every
            -- key of the task is removed. Either way the unique key is settled.
            local function finish(tkey, id, qid, from, to, at, rev, uk, kk, extra, kept)
              if kept then
                move(id, qid, from, to, at, at)
                local f = {'state', to, 'sc', at, 'rev', rev + 1}
                if kk then f[#f + 1] = 'ku'; f[#f + 1] = at + tonumber(kk) end
                for _, x in ipairs(extra) do f[#f + 1] = x end
                redis.call('HSET', tkey, unpack(f))
              else
                leave(id, qid, from)
                redis.call('UNLINK', tkey, P .. 'd:' .. id, P .. 'st:' .. id, P .. 'f:' .. id)
              end
              finishKey(tkey, id, uk, kk, at, kept)
            end

            -- The schedule change that rides on a task transition, in the same script run so a crash cannot split
            -- them. It does nothing unless the definition's occ field names this task (the definition was
            -- replaced, or a newer occurrence took over, or it was deleted); the field is cleared when it applies.
            -- A paused definition keeps the next fire time out of the cron zset until it is resumed. An empty
            -- next pauses. Returns 1 when it changed the definition, else 0.
            local function effect(sid, occ, nextMs)
              if sid == '' then return 0 end
              local ckey = P .. 'cron:' .. sid
              if redis.call('HGET', ckey, 'occ') ~= occ then return 0 end
              redis.call('HDEL', ckey, 'occ')
              if nextMs == '' then
                redis.call('HSET', ckey, 'paused', 1)
                redis.call('ZREM', P .. 'cron', sid)
              else
                redis.call('HSET', ckey, 'next', nextMs)
                if redis.call('HGET', ckey, 'paused') ~= '1' then
                  redis.call('ZADD', P .. 'cron', nextMs, sid)
                end
              end
              return 1
            end

            -- startby only tracks tasks that wait (QUEUED or SCHEDULED). Callers put a task back in with track
            -- whenever it returns to a waiting state, and take it out when it leaves one.
            local function track(id, sb)
              if sb then zadd(P .. 'startby', sb, id) end
            end

            -- fields come from ARGV[from..]. Returns {0} when the id exists, {2, id} when the unique key is
            -- held, {1} when stored. A k: string without TTL whose task is gone is stale and is overwritten.
            local function store(id, dueArg, uk, qid, sb, data, from, forceScheduled)
              local tkey = P .. 't:' .. id
              if redis.call('EXISTS', tkey) == 1 then return {0, ''} end
              if uk then
                local kkey = P .. 'k:' .. uk
                local holder = redis.call('SET', kkey, id, 'NX', 'GET')
                if holder then
                  if redis.call('PTTL', kkey) ~= -1 or redis.call('EXISTS', P .. 't:' .. holder) == 1 then
                    return {2, holder}
                  end
                  redis.call('SET', kkey, id)
                end
              end
              local at = now()
              local due = dueArg and tonumber(dueArg) or at
              local state = (forceScheduled or due > at) and 1 or 0
              local fields = {}
              for i = from, #ARGV do fields[#fields + 1] = ARGV[i] end
              fields[#fields + 1] = 'state'
              fields[#fields + 1] = state
              fields[#fields + 1] = 'due'
              fields[#fields + 1] = due
              fields[#fields + 1] = 'sc'
              fields[#fields + 1] = at
              redis.call('HSET', tkey, unpack(fields))
              if data then redis.call('SET', P .. 'd:' .. id, data) end
              enter(id, qid, state, due, at)
              track(id, sb)
              flush()
              return {1, ''}
            end
            """;

    /**
     * ARGV: id, due ms or '', unique key or '', queue id, start-by ms or '', has payload (1 or ''), payload,
     * then field/value pairs. Returns {code, existing id}.
     */
    static final String ENQUEUE = PRELUDE + """
            local uk = ARGV[4] ~= '' and ARGV[4] or nil
            local due = ARGV[3] ~= '' and ARGV[3] or nil
            local sb = ARGV[6] ~= '' and ARGV[6] or nil
            local data = ARGV[7] == '1' and ARGV[8] or nil
            return store(ARGV[2], due, uk, ARGV[5], sb, data, 9, false)
            """;

    /**
     * The hash fields CLAIM reads, in reply order. The first seven drive the script; the store fills in state,
     * sc, rev, att, ca, lease and wid from what the script wrote.
     */
    static final String[] CLAIM_FIELDS = {
        "ev", "state", "sb", "rev", "att", "uk", "kk", "q", "pv", "max", "created", "due", "acc", "hdr", "ku", "rp",
        "sid", "rof", "mr", "cra", "crs", "prg"
    };

    /**
     * ARGV: count, lease ms, worker id, known event ids (comma separated, '' for any), queue ids in order.
     * A queue is scanned for at most count*4 ids (cap 1000). Paused queues are skipped. A popped task whose
     * start-by time has passed is expired on the spot and never returned. Tasks of unknown events are deferred
     * by 5 s in the queue zset only, without an attempt. Zset writes are deferred to the end, so the scan pages
     * by offset: every id examined is still in place. Returns {at, entry...}, each entry {id, payload, steps,
     * {failure...}, then the CLAIM_FIELDS values as read before the claim}.
     */
    static final String CLAIM = PRELUDE
            + """
            local F = {@FIELDS@}
            local at = now()
            local count = tonumber(ARGV[2])
            local lease = at + tonumber(ARGV[3])
            local known = nil
            if ARGV[5] ~= '' then
              known = {}
              for e in string.gmatch(ARGV[5], '[^,]+') do known[e] = true end
            end
            local remaining = count
            local result = {at}
            for i = 6, #ARGV do
              if remaining <= 0 then break end
              local qid = ARGV[i]
              local qkey = P .. 'q:' .. qid
              local budget = math.min(count * 4, 1000)
              if redis.call('SISMEMBER', P .. 'paused', qid) == 1 then budget = 0 end
              local offset = 0
              while remaining > 0 and budget > 0 do
                local ids = redis.call('ZRANGE', qkey, '-inf', at, 'BYSCORE', 'LIMIT', offset, math.min(budget, remaining))
                if #ids == 0 then break end
                budget = budget - #ids
                offset = offset + #ids
                for _, id in ipairs(ids) do
                  local tkey = P .. 't:' .. id
                  local v = redis.call('HMGET', tkey, unpack(F))
                  if v[2] ~= '0' then
                    zrem(qkey, id)
                  elseif v[3] and tonumber(v[3]) <= at then
                    finish(tkey, id, qid, 0, 6, at, tonumber(v[4]), v[6], v[7], {}, true)
                    zrem(P .. 'startby', id)
                  elseif known and not known[v[1]] then
                    zadd(qkey, at + 5000, id)
                  else
                    move(id, qid, 0, 2, at, lease)
                    if v[3] then zrem(P .. 'startby', id) end
                    redis.call('HSET', tkey, 'state', 2, 'sc', at, 'rev', tonumber(v[4]) + 1,
                      'att', tonumber(v[5]) + 1, 'ca', at, 'lease', lease, 'wid', ARGV[4])
                    local entry = {id, redis.call('GET', P .. 'd:' .. id), redis.call('GET', P .. 'st:' .. id),
                      redis.call('LRANGE', P .. 'f:' .. id, 0, -1)}
                    for j = 1, #F do entry[#entry + 1] = v[j] end
                    result[#result + 1] = entry
                    remaining = remaining - 1
                  end
                end
              end
            end
            flush()
            return result
            """.replace(
                            "@FIELDS@",
                            Arrays.stream(CLAIM_FIELDS).map(f -> "'" + f + "'").collect(Collectors.joining(", ")));

    /** ARGV: lease ms, then id/revision pairs. Returns per pair 0 (not owned), 1 (owned) or 2 (owned, cancel requested). */
    static final String RENEW = PRELUDE + """
            local lease = now() + tonumber(ARGV[2])
            local out = {}
            for i = 3, #ARGV, 2 do
              local id = ARGV[i]
              local tkey = P .. 't:' .. id
              local v = fence(tkey, ARGV[i + 1], 'cra')
              if v then
                zadd(P .. 'leased', lease, id)
                redis.call('HSET', tkey, 'lease', lease)
                out[#out + 1] = v[4] and 2 or 1
              else
                out[#out + 1] = 0
              end
            end
            flush()
            return out
            """;

    /**
     * ARGV: id, revision, keep (1/0), keep payload (1/0), then the schedule effect: schedule id or '', the
     * occurrence id, next fire ms or '' to pause. Returns 2 when the effect changed the definition. Returns 1, or 0 when the fence fails. A task with a
     * key window is kept as SUCCEEDED even when keep is 0 (its payload is dropped), so purge removes it after
     * the window; only a task without a window is removed here.
     */
    static final String ACK = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = fence(tkey, ARGV[3], 'uk', 'kk')
            if not v then return 0 end
            local qid, rev, uk, kk = tonumber(v[3]), tonumber(v[1]), v[4], v[5]
            local at = now()
            if ARGV[4] == '1' or kk then
              finish(tkey, id, qid, 2, 3, at, rev, uk, kk, {}, true)
              if ARGV[4] ~= '1' or ARGV[5] ~= '1' then redis.call('UNLINK', P .. 'd:' .. id) end
            else
              finish(tkey, id, qid, 2, 3, at, rev, uk, kk, {}, false)
            end
            local fx = effect(ARGV[6], ARGV[7], ARGV[8])
            flush()
            return 1 + fx
            """;

    /**
     * ARGV: id, revision, retry at ms or '', keep dead (1/0), expired only (1/0), the new failure as JSON, then
     * the schedule effect as for ACK, applied only when the task dies. Returns RETRY, DEAD or LOST; DEAD+ when the
     * effect changed the definition. With expired only the stored lease must be at or before now. The failure
     * is appended to {@code f:<id>} (trimmed to the last MAX_FAILURES) when the task stays; a task with a key
     * window is kept as DEAD even when keep dead is 0.
     */
    static final String FAIL = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = fence(tkey, ARGV[3], 'lease', 'att', 'max', 'uk', 'kk', 'sb')
            if not v then return 'LOST' end
            local at = now()
            if ARGV[6] == '1' and tonumber(v[4]) > at then return 'LOST' end
            local qid, rev, uk, kk = tonumber(v[3]), tonumber(v[1]), v[7], v[8]
            local fkey = P .. 'f:' .. id
            local function record()
              redis.call('RPUSH', fkey, ARGV[7])
              redis.call('LTRIM', fkey, -@MAX@, -1)
            end
            if ARGV[4] ~= '' and tonumber(v[5]) < tonumber(v[6]) then
              local retryAt = tonumber(ARGV[4])
              record()
              move(id, qid, 2, 1, at, retryAt)
              redis.call('HSET', tkey, 'state', 1, 'sc', at, 'rev', rev + 1, 'due', retryAt)
              track(id, v[9])
              flush()
              return 'RETRY'
            end
            if ARGV[5] == '1' or kk then
              record()
              finish(tkey, id, qid, 2, 4, at, rev, uk, kk, {}, true)
            else
              finish(tkey, id, qid, 2, 4, at, rev, uk, kk, {}, false)
            end
            local fx = effect(ARGV[8], ARGV[9], ARGV[10])
            flush()
            return fx == 1 and 'DEAD+' or 'DEAD'
            """.replace("@MAX@", String.valueOf(MAX_FAILURES));

    /** ARGV: id, revision, due ms. Returns 1, or 0 when the fence fails. */
    static final String RESCHEDULE = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = fence(tkey, ARGV[3], 'att', 'sb')
            if not v then return 0 end
            local at = now()
            local due = tonumber(ARGV[4])
            move(id, tonumber(v[3]), 2, 1, at, due)
            redis.call('HSET', tkey, 'state', 1, 'sc', at, 'rev', tonumber(v[1]) + 1, 'att', tonumber(v[4]) - 1, 'due', due)
            track(id, v[5])
            flush()
            return 1
            """;

    /** ARGV: id, revision. Returns 1, or 0 when the fence fails. */
    static final String UNCLAIM = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = fence(tkey, ARGV[3], 'att', 'sb', 'due')
            if not v then return 0 end
            local at = now()
            move(id, tonumber(v[3]), 2, 0, at, tonumber(v[6]))
            redis.call('HSET', tkey, 'state', 0, 'sc', at, 'rev', tonumber(v[1]) + 1, 'att', tonumber(v[4]) - 1)
            track(id, v[5])
            flush()
            return 1
            """;

    /**
     * ARGV: id, revision, target, value. Writes without touching the revision: target {@code steps} replaces
     * the {@code st:<id>} string, any other target is a field of the task hash. An empty value deletes.
     */
    static final String WRITE = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            if not fence(tkey, ARGV[3]) then return 0 end
            if ARGV[4] == 'steps' then
              if ARGV[5] == '' then
                redis.call('UNLINK', P .. 'st:' .. id)
              else
                redis.call('SET', P .. 'st:' .. id, ARGV[5])
              end
            elseif ARGV[5] == '' then
              redis.call('HDEL', tkey, ARGV[4])
            else
              redis.call('HSET', tkey, ARGV[4], ARGV[5])
            end
            return 1
            """;

    /**
     * ARGV: limit. Walks the {@code sched} zset of every queue. Returns {moved, next due ms over all queues or
     * -1}. Ids whose task is gone or no longer SCHEDULED are dropped.
     */
    static final String PROMOTE = PRELUDE + """
            local at = now()
            local remaining = tonumber(ARGV[2])
            local qids = redis.call('HVALS', P .. 'reg:queue')
            local moved = 0
            for _, qid in ipairs(qids) do
              if remaining <= 0 then break end
              local skey = P .. 'sched:' .. qid
              local ids = redis.call('ZRANGE', skey, '-inf', at, 'BYSCORE', 'LIMIT', 0, remaining)
              for _, id in ipairs(ids) do
                local tkey = P .. 't:' .. id
                local v = redis.call('HMGET', tkey, 'state', 'due', 'rev')
                if v[1] == '1' then
                  move(id, qid, 1, 0, at, tonumber(v[2]))
                  redis.call('HSET', tkey, 'state', 0, 'sc', at, 'rev', tonumber(v[3]) + 1)
                  moved = moved + 1
                else
                  zrem(skey, id)
                end
              end
              remaining = remaining - #ids
            end
            flush()
            local nextDue = -1
            for _, qid in ipairs(qids) do
              local head = redis.call('ZRANGE', P .. 'sched:' .. qid, 0, 0, 'WITHSCORES')
              if head[2] and (nextDue < 0 or tonumber(head[2]) < nextDue) then nextDue = tonumber(head[2]) end
            end
            return {moved, nextDue}
            """;

    /**
     * ARGV: state, retention ms, limit. Walks the idx zsets of every queue oldest first. A task whose key window
     * is still open is skipped; zset writes are deferred, so the walk pages by offset. One call examines at most
     * max(4*limit, 1000) entries, so held tasks cannot make a call unbounded.
     */
    static final String PURGE = PRELUDE + """
            local state = tonumber(ARGV[2])
            local at = now()
            local cutoff = at - tonumber(ARGV[3])
            local remaining = tonumber(ARGV[4])
            local budget = math.max(remaining * 4, 1000)
            local removed = 0
            for _, qid in ipairs(redis.call('HVALS', P .. 'reg:queue')) do
              local ikey = P .. 'idx:' .. state .. ':' .. qid
              local offset = 0
              while remaining > 0 and budget > 0 do
                local ids = redis.call('ZRANGE', ikey, '-inf', cutoff, 'BYSCORE', 'LIMIT', offset, math.min(remaining, budget, 500))
                if #ids == 0 then break end
                for _, id in ipairs(ids) do
                  local tkey = P .. 't:' .. id
                  local ku = redis.call('HGET', tkey, 'ku')
                  if not (ku and tonumber(ku) > at) then
                    zrem(ikey, id)
                    redis.call('UNLINK', tkey, P .. 'd:' .. id, P .. 'st:' .. id, P .. 'f:' .. id)
                    removed = removed + 1
                    remaining = remaining - 1
                  end
                end
                offset = offset + #ids
                budget = budget - #ids
              end
              if remaining <= 0 or budget <= 0 then break end
            end
            flush()
            return removed
            """;

    /** ARGV: kind (queue or event), name. Returns the id; fails when the 32767 ids are used up. */
    static final String REGISTER = PRELUDE + """
            local reg = P .. 'reg:' .. ARGV[2]
            local id = redis.call('HGET', reg, ARGV[3])
            if id then return tonumber(id) end
            local seq = P .. 'seq:' .. ARGV[2]
            if tonumber(redis.call('GET', seq) or '0') >= 32767 then
              return redis.error_reply('too many ' .. ARGV[2] .. ' names registered (limit 32767)')
            end
            id = redis.call('INCR', seq)
            redis.call('HSET', reg, ARGV[3], id)
            return id
            """;

    /**
     * ARGV: schedule id, next fire ms, keep-until ms, then field/value pairs. The fire time is kept when the
     * schedule shape (cron expression, zone and fixed delay) is unchanged and either the task named by the occ
     * field is not finished or the stored time is not later than keep-until; otherwise occ is dropped. The revision comes from the counter, so no two
     * definitions under the same id share one. The fire time is also stored in the hash as {@code next}; a
     * paused definition is left out of the {@code cron} zset and keeps it there. Replacing an existing definition
     * keeps its paused flag and ignores the given one; a new definition stores the flag as given.
     */
    static final String CRON_PUT = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            local function shape()
              return (redis.call('HGET', ckey, 'cron') or '') .. '|' .. (redis.call('HGET', ckey, 'zone') or '')
                  .. '|' .. (redis.call('HGET', ckey, 'fd') or '')
            end
            local existed = redis.call('EXISTS', ckey) == 1
            local before = existed and shape() or nil
            local kept = redis.call('HGET', ckey, 'next')
            local occ = redis.call('HGET', ckey, 'occ')
            local wasPaused = redis.call('HGET', ckey, 'paused')
            local fields = {}
            for i = 5, #ARGV do fields[#fields + 1] = ARGV[i] end
            -- counter first, so a counter failure leaves the old definition intact; read back as a string
            -- because the INCR reply would pass through a Lua double
            redis.call('INCR', P .. 'cronrev')
            local rev = redis.call('GET', P .. 'cronrev')
            redis.call('DEL', ckey)
            redis.call('HSET', ckey, unpack(fields))
            local nextFire = ARGV[3]
            if kept and before == shape() then
              local st = occ and redis.call('HGET', P .. 't:' .. occ, 'state')
              if st and tonumber(st) < 3 then
                nextFire = kept
                redis.call('HSET', ckey, 'occ', occ)
              elseif tonumber(kept) <= tonumber(ARGV[4]) then
                nextFire = kept
              end
            end
            redis.call('HSET', ckey, 'rev', rev, 'next', nextFire)
            if existed then
              if wasPaused then redis.call('HSET', ckey, 'paused', wasPaused) else redis.call('HDEL', ckey, 'paused') end
            end
            redis.call('SADD', P .. 'crondefs', ARGV[2])
            if redis.call('HGET', ckey, 'paused') == '1' then
              redis.call('ZREM', P .. 'cron', ARGV[2])
            else
              redis.call('ZADD', P .. 'cron', nextFire, ARGV[2])
            end
            return rev
            """;

    /** ARGV: schedule id. Returns 1 when a definition was removed. */
    static final String CRON_DELETE = PRELUDE + """
            redis.call('ZREM', P .. 'cron', ARGV[2])
            redis.call('SREM', P .. 'crondefs', ARGV[2])
            return redis.call('DEL', P .. 'cron:' .. ARGV[2])
            """;

    /**
     * ARGV: schedule id, definition revision, fire at ms, next fire ms, occurrence id, unique key or '', queue
     * id, start-by ms or '', has payload (1 or ''), payload, then the occurrence's field/value pairs. Stores the occurrence as
     * SCHEDULED for the fire time and advances the definition, unless the revision or the stored fire time
     * changed or the definition is paused. An occurrence that exists already or whose unique key is held is not
     * stored again; the definition still advances. A stored occurrence of a fixed-delay definition becomes its
     * occ field; when the unique key is held and occ is empty, the holder becomes it. Returns 1 when committed.
     */
    static final String CRON_COMMIT = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            if redis.call('HGET', ckey, 'rev') ~= ARGV[3] then return 0 end
            if redis.call('HGET', ckey, 'paused') == '1' then return 0 end
            local score = redis.call('ZSCORE', P .. 'cron', ARGV[2])
            if not score or tonumber(score) ~= tonumber(ARGV[4]) then return 0 end
            local uk = ARGV[7] ~= '' and ARGV[7] or nil
            local sb = ARGV[9] ~= '' and ARGV[9] or nil
            local data = ARGV[10] == '1' and ARGV[11] or nil
            local stored = store(ARGV[6], ARGV[4], uk, ARGV[8], sb, data, 12, true)
            if redis.call('HEXISTS', ckey, 'fd') == 1 then
              if stored[1] == 1 then
                redis.call('HSET', ckey, 'occ', ARGV[6])
              elseif stored[1] == 2 and redis.call('HEXISTS', ckey, 'occ') == 0 then
                redis.call('HSET', ckey, 'occ', stored[2])
              end
            end
            redis.call('ZADD', P .. 'cron', ARGV[5], ARGV[2])
            redis.call('HSET', ckey, 'next', ARGV[5])
            return 1
            """;

    /** ARGV: limit. Expires QUEUED and SCHEDULED tasks whose start-by time passed, oldest first; returns their ids. */
    static final String EXPIRE = PRELUDE + """
            local at = now()
            local ids = redis.call('ZRANGE', P .. 'startby', '-inf', at, 'BYSCORE', 'LIMIT', 0, ARGV[2])
            local out = {}
            for _, id in ipairs(ids) do
              local tkey = P .. 't:' .. id
              local v = redis.call('HMGET', tkey, 'state', 'q', 'rev', 'uk', 'kk')
              local state = tonumber(v[1])
              if state == 0 or state == 1 then
                finish(tkey, id, v[2], state, 6, at, tonumber(v[3]), v[4], v[5], {}, true)
                out[#out + 1] = id
              end
              zrem(P .. 'startby', id)
            end
            flush()
            return out
            """;

    /**
     * ARGV: id, revision, reason or '', keep payload (1/0), then the schedule effect as for ACK. LEASED to CANCELLED; the worker id stays, the
     * cancel request time is kept when there is one. Returns 1, or 0 when the fence fails.
     */
    static final String FINISH_CANCELLED = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = fence(tkey, ARGV[3], 'uk', 'kk', 'cra')
            if not v then return 0 end
            local at = now()
            local extra = {}
            if not v[6] then extra = {'cra', at} end
            if ARGV[4] ~= '' then extra[#extra + 1] = 'crs'; extra[#extra + 1] = ARGV[4] end
            finish(tkey, id, tonumber(v[3]), 2, 5, at, tonumber(v[1]), v[4], v[5], extra, true)
            if ARGV[5] ~= '1' then redis.call('UNLINK', P .. 'd:' .. id) end
            local fx = effect(ARGV[6], ARGV[7], ARGV[8])
            flush()
            return 1 + fx
            """;

    /**
     * ARGV: id, revision, reason or '', then the schedule effect as for ACK. Returns CANCELLED (QUEUED or
     * SCHEDULED, moved; CANCELLED+ when the effect changed the definition), REQUESTED (LEASED, only the request is
     * stored and the revision stays) or STALE.
     */
    static final String CANCEL = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = redis.call('HMGET', tkey, 'rev', 'state', 'q', 'uk', 'kk')
            if v[1] ~= ARGV[3] then return 'STALE' end
            local state = tonumber(v[2])
            local at = now()
            local extra = {'cra', at}
            if ARGV[4] ~= '' then extra[3] = 'crs'; extra[4] = ARGV[4] end
            if state == 0 or state == 1 then
              finish(tkey, id, v[3], state, 5, at, tonumber(v[1]), v[4], v[5], extra, true)
              zrem(P .. 'startby', id)
              local fx = effect(ARGV[5], ARGV[6], ARGV[7])
              flush()
              return fx == 1 and 'CANCELLED+' or 'CANCELLED'
            elseif state == 2 then
              redis.call('HSET', tkey, unpack(extra))
              return 'REQUESTED'
            end
            return 'STALE'
            """;

    /**
     * ARGV: id, revision. DEAD, CANCELLED or EXPIRED back to QUEUED. The start-by time is cleared as well, so a
     * retried EXPIRED task is not expired again. Steps and failures stay. Returns 1, or 0 on a mismatch.
     */
    static final String RETRY = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = redis.call('HMGET', tkey, 'rev', 'state', 'q', 'uk')
            if v[1] ~= ARGV[3] then return 0 end
            local state = tonumber(v[2])
            if state < 4 or state > 6 then return 0 end
            local at = now()
            move(id, v[3], state, 0, at, at)
            zrem(P .. 'startby', id)
            if v[4] then
              local kkey = P .. 'k:' .. v[4]
              if redis.call('GET', kkey) == id then redis.call('DEL', kkey) end
            end
            redis.call('HSET', tkey, 'state', 0, 'sc', at, 'rev', tonumber(v[1]) + 1, 'due', at, 'att', 0)
            redis.call('HDEL', tkey, 'wid', 'lease', 'cra', 'crs', 'kk', 'ku', 'uk', 'sb')
            flush()
            return 1
            """;

    /**
     * ARGV: id, revision, then the schedule effect as for ACK. Removes a task in any state but LEASED. Returns 1
     * (2 when the effect changed the definition), or 0 on a mismatch.
     */
    static final String DELETE = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = redis.call('HMGET', tkey, 'rev', 'state', 'q', 'uk')
            if v[1] ~= ARGV[3] then return 0 end
            local state = tonumber(v[2])
            if state == 2 then return 0 end
            leave(id, v[3], state)
            zrem(P .. 'startby', id)
            if v[4] then
              local kkey = P .. 'k:' .. v[4]
              if redis.call('GET', kkey) == id then redis.call('DEL', kkey) end
            end
            redis.call('UNLINK', tkey, P .. 'd:' .. id, P .. 'st:' .. id, P .. 'f:' .. id)
            local fx = effect(ARGV[4], ARGV[5], ARGV[6])
            flush()
            return 1 + fx
            """;

    /** ARGV: label. Allocates the worker id and records it as started and seen now. Returns the id. */
    static final String WORKER_REGISTER = PRELUDE + """
            local id = redis.call('INCR', P .. 'seq:worker')
            local at = now()
            redis.call('HSET', P .. 'w:' .. id, 'label', ARGV[2], 'started', at, 'seen', at, 'running', 0)
            redis.call('ZADD', P .. 'workers', at, id)
            return id
            """;

    /** ARGV: stale after ms. Removes workers last seen strictly before now - stale. Returns how many worker hashes were removed. */
    static final String PURGE_WORKERS = PRELUDE + """
            local ids = redis.call('ZRANGE', P .. 'workers', '-inf', '(' .. (now() - tonumber(ARGV[2])), 'BYSCORE')
            for _, id in ipairs(ids) do
              redis.call('UNLINK', P .. 'w:' .. id)
              redis.call('ZREM', P .. 'workers', id)
            end
            return #ids
            """;

    /**
     * ARGV: schedule id, paused (1/0). A paused definition leaves the {@code cron} zset; its next fire stays in
     * the hash and returns to the zset on resume. Returns 1, or 0 when there is no such definition.
     */
    static final String CRON_PAUSE = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            if redis.call('EXISTS', ckey) == 0 then return 0 end
            if ARGV[3] == '1' then
              redis.call('HSET', ckey, 'paused', 1)
              redis.call('ZREM', P .. 'cron', ARGV[2])
            else
              redis.call('HDEL', ckey, 'paused')
              redis.call('ZADD', P .. 'cron', redis.call('HGET', ckey, 'next'), ARGV[2])
            end
            return 1
            """;

    /**
     * ARGV: schedule id, next fire ms. Operator write: assigns a new revision, so that a runner write based on
     * the old one does nothing. Returns 1, or 0 when there is no such definition.
     */
    static final String CRON_SET_NEXT = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            if redis.call('EXISTS', ckey) == 0 then return 0 end
            redis.call('INCR', P .. 'cronrev')
            redis.call('HSET', ckey, 'next', ARGV[3], 'rev', redis.call('GET', P .. 'cronrev'))
            if redis.call('HGET', ckey, 'paused') ~= '1' then
              redis.call('ZADD', P .. 'cron', ARGV[3], ARGV[2])
            end
            return 1
            """;

    /**
     * ARGV: schedule id, definition revision, expected next fire ms, next fire ms. Runner write: keeps the
     * revision and does nothing unless the definition still has the given one and the expected next fire time
     * and is not paused. Returns 1 when written.
     */
    static final String CRON_ADVANCE = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            if redis.call('HGET', ckey, 'rev') ~= ARGV[3] then return 0 end
            if redis.call('HGET', ckey, 'paused') == '1' then return 0 end
            if tonumber(redis.call('HGET', ckey, 'next')) ~= tonumber(ARGV[4]) then return 0 end
            redis.call('HSET', ckey, 'next', ARGV[5])
            redis.call('ZADD', P .. 'cron', ARGV[5], ARGV[2])
            return 1
            """;
}
