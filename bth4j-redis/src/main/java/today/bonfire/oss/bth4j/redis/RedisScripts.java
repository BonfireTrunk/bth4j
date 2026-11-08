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
 * script, so a batch of tasks costs a few commands per zset instead of a few per task. State transitions read
 * the hash with HMGET and use HGET of the type id when entering or leaving QUEUED. The revision and the attempt
 * count are computed in Lua from
 * the values read. Task ids never go through Lua numbers (a double keeps 53 bits), only revisions, attempts
 * and times do.
 */
final class RedisScripts {

    private RedisScripts() {}

    /** Failures kept per task in {@code f:<id>}; FAIL trims the list when a push takes it past this. */
    static final int MAX_FAILURES = 20;

    /**
     * Life of a {@code ts:<digest>} type set after the last claim that named it: a stopped runner's set goes after
     * a day, and a runner that finds its set gone writes it again.
     */
    static final long TYPE_SET_TTL_MS = 86_400_000;

    /** Workers removed per PURGE_WORKERS call; below the Lua unpack limit and short enough to block Redis for about a millisecond. */
    static final int PURGE_WORKERS_PAGE = 500;

    private static final String PRELUDE = """
            local P = ARGV[1]
            local adds, rems, touched = {}, {}, {}

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
            -- Then active:<qid> follows the type zsets the call touched: a type is in it exactly while its
            -- zset in that queue is not empty.
            local function flush()
              for key, m in pairs(rems) do
                for i = 1, #m, 1000 do redis.call('ZREM', key, unpack(m, i, math.min(i + 999, #m))) end
              end
              for key, m in pairs(adds) do
                for i = 1, #m, 2000 do redis.call('ZADD', key, unpack(m, i, math.min(i + 1999, #m))) end
              end
              for key, qt in pairs(touched) do
                if adds[key] then
                  redis.call('SADD', P .. 'active:' .. qt[1], qt[2])
                elseif redis.call('EXISTS', key) == 0 then
                  redis.call('SREM', P .. 'active:' .. qt[1], qt[2])
                end
              end
              adds, rems, touched = {}, {}, {}
            end

            -- Every write to a type zset goes through here, so that flush can keep active:<qid> in step.
            local function queueKey(id, qid, tid)
              tid = tid or redis.call('HGET', P .. 't:' .. id, 'ty')
              local key = P .. 'q:' .. qid .. ':' .. tid
              if not touched[key] then touched[key] = {qid, tid} end
              return key
            end

            local function queuedCounts(qid)
              local counts = {}
              for _, tid in ipairs(redis.call('SMEMBERS', P .. 'active:' .. qid)) do
                local count = redis.call('ZCARD', P .. 'q:' .. qid .. ':' .. tid)
                if count > 0 then counts[#counts + 1] = {tid, count} end
              end
              return counts
            end

            -- A terminal task whose key window is open is listed in hidx (scored by stateChanged) and waits in
            -- hold (scored by ku); any other one is in idx, which PURGE reads, so it never walks past held tasks.
            -- tid is optional: callers that already know the type pass it and save an HGET.
            local function enter(id, qid, state, score, at, ku, tid)
              if state == 0 then
                zadd(queueKey(id, qid, tid), score, id)
              elseif state == 1 then
                zadd(P .. 'sched:' .. qid, score, id)
              elseif state >= 3 and ku then
                zadd(P .. 'hidx:' .. state .. ':' .. qid, at, id)
                zadd(P .. 'hold:' .. state .. ':' .. qid, ku, id)
              else
                if state == 2 then zadd(P .. 'leased', score, id) end
                zadd(P .. 'idx:' .. state .. ':' .. qid, at, id)
              end
            end

            local function leave(id, qid, state, tid)
              if state == 0 then
                zrem(queueKey(id, qid, tid), id)
              elseif state == 1 then
                zrem(P .. 'sched:' .. qid, id)
              else
                if state == 2 then zrem(P .. 'leased', id) end
                zrem(P .. 'idx:' .. state .. ':' .. qid, id)
                if state >= 3 then
                  zrem(P .. 'hidx:' .. state .. ':' .. qid, id)
                  zrem(P .. 'hold:' .. state .. ':' .. qid, id)
                end
              end
            end

            -- The zset side of a state change; the caller writes state, sc and the new rev with its own HSET.
            local function move(id, qid, from, to, at, score, ku, tid)
              leave(id, qid, from, tid)
              enter(id, qid, to, score, at, ku, tid)
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
                move(id, qid, from, to, at, at, kk and at + tonumber(kk))
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

            -- fields come from ARGV[from..to]. Returns {0} when the id exists, {2, id} when the unique key is
            -- held, {1} when stored. A k: string without TTL whose task is gone is stale and is overwritten.
            -- The hash and the key are written at once, so a later store in the same call sees them; the zset
            -- writes wait for the caller's flush.
            local function store(id, dueArg, uk, qid, sb, data, from, to, forceScheduled, token, at)
              local tkey = P .. 't:' .. id
              if redis.call('EXISTS', tkey) == 1 then
                if token and redis.call('HGET', tkey, 'wt') == token then return {1, ''} end
                return {0, ''}
              end
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
              at = at or now()
              local due = dueArg and tonumber(dueArg) or at
              local state = (forceScheduled or due > at) and 1 or 0
              local fields, tid = {}, nil
              for i = from, to, 2 do
                fields[#fields + 1] = ARGV[i]
                fields[#fields + 1] = ARGV[i + 1]
                if ARGV[i] == 'ty' then tid = ARGV[i + 1] end
              end
              fields[#fields + 1] = 'state'
              fields[#fields + 1] = state
              fields[#fields + 1] = 'due'
              fields[#fields + 1] = due
              fields[#fields + 1] = 'sc'
              fields[#fields + 1] = at
              redis.call('HSET', tkey, unpack(fields))
              if data then redis.call('SET', P .. 'd:' .. id, data) end
              enter(id, qid, state, due, at, nil, tid)
              track(id, sb)
              return {1, ''}
            end
            """;

    /**
     * ARGV: id, due ms or '', unique key or '', queue id, start-by ms or '', has payload (1 or ''), payload,
     * then field/value pairs ending with wt and the write token. Returns {code, existing id}.
     */
    static final String ENQUEUE = PRELUDE + """
            local uk = ARGV[4] ~= '' and ARGV[4] or nil
            local due = ARGV[3] ~= '' and ARGV[3] or nil
            local sb = ARGV[6] ~= '' and ARGV[6] or nil
            local data = ARGV[7] == '1' and ARGV[8] or nil
            local r = store(ARGV[2], due, uk, ARGV[5], sb, data, 9, #ARGV, false, ARGV[#ARGV])
            flush()
            return r
            """;

    /**
     * ARGV: per task, the number of its ENQUEUE arguments, then those arguments. One clock read for the call, so its tasks
     * share created and stateChanged times. Returns one {code, existing id} per task, in order.
     */
    static final String ENQUEUE_ALL = PRELUDE + """
            local at = now()
            local out = {}
            local i = 2
            while i <= #ARGV do
              local n = tonumber(ARGV[i])
              local uk = ARGV[i + 3] ~= '' and ARGV[i + 3] or nil
              local due = ARGV[i + 2] ~= '' and ARGV[i + 2] or nil
              local sb = ARGV[i + 5] ~= '' and ARGV[i + 5] or nil
              local data = ARGV[i + 6] == '1' and ARGV[i + 7] or nil
              out[#out + 1] = store(ARGV[i + 1], due, uk, ARGV[i + 4], sb, data, i + 8, i + n, false, ARGV[i + n], at)
              i = i + n + 1
            end
            flush()
            return out
            """;

    /**
     * The hash fields CLAIM reads, in reply order. The first seven drive the script; the store fills in state,
     * sc, rev, att, ca, lease and wid from what the script wrote.
     */
    static final String[] CLAIM_FIELDS = {
        "ty", "state", "sb", "rev", "att", "uk", "kk", "q", "pv", "max", "created", "due", "acc", "hdr", "ku", "rp",
        "sid", "rof", "mr", "cra", "crs", "prg", "hs", "fc"
    };

    /**
     * ARGV: count, lease ms, worker id, the digest naming the {@code ts:<digest>} set of known type ids ('' for
     * every type), then queue/share/cap triples. Returns {-1} when that set is missing, so that the caller writes
     * it and calls again; each call extends its life by {@link #TYPE_SET_TTL_MS}. A queue's eligible types are
     * the intersection of {@code active:<qid>} and that set, which Redis computes from the smaller one, so types
     * without queued tasks cost nothing. Both passes share each queue's scan budget of min(count*4, 1000).
     * Eligible type heads are merged by (due, lexical id). Deferred zset writes let each type page by offset
     * across both passes. Returns {at, entry...}, each entry {id, payload, steps, {failure...}, then
     * CLAIM_FIELDS as read before claim}. The hs/fc markers avoid reading absent history keys.
     */
    static final String CLAIM = PRELUDE
            + """
            local F = {@FIELDS@}
            local at = now()
            local count = tonumber(ARGV[2])
            local lease = at + tonumber(ARGV[3])
            local tset = ARGV[5] ~= '' and P .. 'ts:' .. ARGV[5] or nil
            if tset and redis.call('PEXPIRE', tset, @TTL@) == 0 then return {-1} end
            local remaining, result, grants = count, {at}, {}
            local function before(a, b)
              return a.score < b.score or (a.score == b.score and a.id < b.id)
            end
            local function push(heap, source)
              local i = #heap + 1
              while i > 1 do
                local parent = math.floor(i / 2)
                if not before(source, heap[parent]) then break end
                heap[i] = heap[parent]
                i = parent
              end
              heap[i] = source
            end
            local function pop(heap)
              local first, last = heap[1], table.remove(heap)
              if #heap > 0 then
                local i = 1
                while i * 2 <= #heap do
                  local child = i * 2
                  if child < #heap and before(heap[child + 1], heap[child]) then child = child + 1 end
                  if not before(heap[child], last) then break end
                  heap[i] = heap[child]
                  i = child
                end
                heap[i] = last
              end
              return first
            end
            local function advance(source, size)
              if source.pos > #source.page then
                source.page = redis.call('ZRANGE', source.key, '-inf', at, 'BYSCORE',
                  'LIMIT', source.offset, size, 'WITHSCORES')
                source.offset = source.offset + #source.page / 2
                source.pos = 1
              end
              if #source.page == 0 then return false end
              source.id = source.page[source.pos]
              source.score = tonumber(source.page[source.pos + 1])
              source.pos = source.pos + 2
              return true
            end
            local function claimQueue(g, take)
              if take <= 0 then return 0 end
              if not g.heap then
                g.heap = {}
                local active = P .. 'active:' .. g.qid
                local eligible = tset and redis.call('SINTER', active, tset) or redis.call('SMEMBERS', active)
                -- the first page splits the take between the types, so one type needs one ZRANGE
                local first = math.max(1, math.min(64, g.budget, math.ceil(take / math.max(1, #eligible))))
                for _, tid in ipairs(eligible) do
                  local source = {key = P .. 'q:' .. g.qid .. ':' .. tid, tid = tid,
                    offset = 0, pos = 1, page = {}}
                  if advance(source, first) then push(g.heap, source) end
                end
              end
              local size = math.min(count, g.budget, 64)
              if g.pending then
                if g.budget > 0 and advance(g.pending, size) then push(g.heap, g.pending) end
                g.pending = nil
              end
              local got = 0
              while got < take and g.budget > 0 and #g.heap > 0 do
                local source = pop(g.heap)
                local id = source.id
                g.budget = g.budget - 1
                local tkey = P .. 't:' .. id
                local v = redis.call('HMGET', tkey, unpack(F))
                if v[@state@] ~= '0' or v[@ty@] ~= source.tid or v[@q@] ~= g.qid then
                  zrem(queueKey(id, g.qid, source.tid), id)
                elseif v[@sb@] and tonumber(v[@sb@]) <= at then
                  finish(tkey, id, g.qid, 0, 6, at, tonumber(v[@rev@]), v[@uk@], v[@kk@], {}, true)
                  zrem(P .. 'startby', id)
                else
                  move(id, g.qid, 0, 2, at, lease, nil, source.tid)
                  if v[@sb@] then zrem(P .. 'startby', id) end
                  redis.call('HSET', tkey, 'state', 2, 'sc', at, 'rev', tonumber(v[@rev@]) + 1,
                    'att', tonumber(v[@att@]) + 1, 'ca', at, 'lease', lease, 'wid', ARGV[4])
                  local entry = {id, redis.call('GET', P .. 'd:' .. id),
                    v[@hs@] == '1' and redis.call('GET', P .. 'st:' .. id) or false,
                    v[@fc@] == '1' and redis.call('LRANGE', P .. 'f:' .. id, 0, -1) or {}}
                  for j = 1, #F do entry[#entry + 1] = v[j] end
                  result[#result + 1] = entry
                  got = got + 1
                  remaining = remaining - 1
                end
                -- the source's next head is only needed if the refill pass returns to this queue
                if got >= take then
                  g.pending = source
                elseif g.budget > 0 and advance(source, math.min(count, g.budget, 64)) then
                  push(g.heap, source)
                end
              end
              return got
            end
            for i = 6, #ARGV, 3 do
              local g = {qid = ARGV[i], share = tonumber(ARGV[i + 1]), cap = tonumber(ARGV[i + 2]),
                budget = math.min(count * 4, 1000), got = 0}
              grants[#grants + 1] = g
              if remaining > 0 and g.share > 0 and redis.call('SISMEMBER', P .. 'paused', g.qid) == 0 then
                local take = math.min(g.share, remaining)
                g.got = claimQueue(g, take)
                g.filled = g.got == take
              end
            end
            for _, g in ipairs(grants) do
              if remaining <= 0 then break end
              if g.filled then claimQueue(g, math.min(g.cap - g.got, remaining)) end
            end
            flush()
            return result
            """.replace(
                            "@FIELDS@",
                            Arrays.stream(CLAIM_FIELDS).map(f -> "'" + f + "'").collect(Collectors.joining(", ")))
                    .replace("@TTL@", String.valueOf(TYPE_SET_TTL_MS))
                    .transform(RedisScripts::withClaimFieldPositions);

    /** Replaces each {@code @field@} with that field's 1-based position in {@code F}, so reordering stays safe. */
    private static String withClaimFieldPositions(String script) {
        for (var i = 0; i < CLAIM_FIELDS.length; i++) {
            script = script.replace("@" + CLAIM_FIELDS[i] + "@", String.valueOf(i + 1));
        }
        return script;
    }

    /** Returns {queue id, {{type id, count}...}} rows for non-empty registered queues. */
    static final String QUEUED_BY_TYPE = PRELUDE + """
            local out = {}
            for _, qid in ipairs(redis.call('HVALS', P .. 'reg:queue')) do
              local counts = queuedCounts(qid)
              if #counts > 0 then out[#out + 1] = {qid, counts} end
            end
            return out
            """;

    /**
     * Returns {queue id, queued, scheduled, leased, succeeded, dead, cancelled, expired} rows. Only the types in
     * {@code active:<qid>} are read for queued.
     */
    static final String COUNTS = PRELUDE + """
            local out = {}
            for _, qid in ipairs(redis.call('HVALS', P .. 'reg:queue')) do
              local queued = 0
              for _, pair in ipairs(queuedCounts(qid)) do queued = queued + pair[2] end
              local row = {qid, queued, redis.call('ZCARD', P .. 'sched:' .. qid),
                redis.call('ZCARD', P .. 'idx:2:' .. qid)}
              for state = 3, 6 do
                row[#row + 1] = redis.call('ZCARD', P .. 'idx:' .. state .. ':' .. qid)
                  + redis.call('ZCARD', P .. 'hidx:' .. state .. ':' .. qid)
              end
              out[#out + 1] = row
            end
            return out
            """;

    /**
     * ARGV: claim time ms, worker id, then task ids. Ends the lease of each task that is still LEASED by the claim
     * of that worker at that time, so reclaim returns it at once.
     */
    static final String END_LEASES = PRELUDE + """
            local at = ARGV[2]
            for i = 4, #ARGV do
              local id = ARGV[i]
              local tkey = P .. 't:' .. id
              local v = redis.call('HMGET', tkey, 'state', 'wid', 'ca')
              if v[1] == '2' and v[2] == ARGV[3] and v[3] == at then
                redis.call('HSET', tkey, 'lease', at)
                zadd(P .. 'leased', tonumber(at), id)
              end
            end
            flush()
            return 0
            """;

    /**
     * ARGV: lease ms, then id/revision pairs. Returns per pair 0 (not owned), 1 (owned) or 2 (owned, cancel
     * requested). A lease at or before now is not owned: the sweeper may take it over and a late extension
     * must not undo that.
     */
    static final String RENEW = PRELUDE + """
            local at = now()
            local lease = at + tonumber(ARGV[2])
            local out = {}
            for i = 3, #ARGV, 2 do
              local id = ARGV[i]
              local tkey = P .. 't:' .. id
              local v = fence(tkey, ARGV[i + 1], 'cra', 'lease')
              if v and tonumber(v[5]) > at then
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
     * The task side of ACK, shared with ACK_ALL. Reads the clock once per script, after the first fence that
     * passes. Returns false when the fence fails.
     */
    private static final String SUCCEED = """
            local at
            local function succeed(id, rev, keep, keepPayload)
              local tkey = P .. 't:' .. id
              local v = fence(tkey, rev, 'uk', 'kk')
              if not v then return false end
              local qid, uk, kk = tonumber(v[3]), v[4], v[5]
              at = at or now()
              if keep == '1' or kk then
                finish(tkey, id, qid, 2, 3, at, tonumber(v[1]), uk, kk, {}, true)
                if keep ~= '1' or keepPayload ~= '1' then redis.call('UNLINK', P .. 'd:' .. id) end
              else
                finish(tkey, id, qid, 2, 3, at, tonumber(v[1]), uk, kk, {}, false)
              end
              return true
            end
            """;

    /**
     * ARGV: id, revision, keep (1/0), keep payload (1/0), then the schedule effect: schedule id or '', the
     * occurrence id, next fire ms or '' to pause. Returns 2 when the effect changed the definition. Returns 1, or 0 when the fence fails. A task with a
     * key window is kept as SUCCEEDED even when keep is 0 (its payload is dropped), so purge removes it after
     * the window; only a task without a window is removed here.
     */
    static final String ACK = PRELUDE + SUCCEED + """
            if not succeed(ARGV[2], ARGV[3], ARGV[4], ARGV[5]) then return 0 end
            local fx = effect(ARGV[6], ARGV[7], ARGV[8])
            flush()
            return 1 + fx
            """;

    /**
     * ARGV: keep (1/0), keep payload (1/0), then id/revision pairs. ACK without a schedule effect for each pair,
     * in one clock read. Returns per pair 1, or 0 when its fence fails.
     */
    static final String ACK_ALL = PRELUDE + SUCCEED + """
            local out = {}
            for i = 4, #ARGV, 2 do
              out[#out + 1] = succeed(ARGV[i], ARGV[i + 1], ARGV[2], ARGV[3]) and 1 or 0
            end
            flush()
            return out
            """;

    /**
     * ARGV: id, revision, retry at ms or '', keep dead (1/0), expired only (1/0), the new failure as JSON, then
     * the schedule effect as for ACK, applied only when the task dies. Returns RETRY, DEAD or LOST; DEAD+ when the
     * effect changed the definition. With expired only the stored lease must be at or before now. The failure
     * is appended to {@code f:<id>} (trimmed to the last MAX_FAILURES, only when the push exceeded it) when the task stays, and the hash
     * field fc marks that the list exists; a task with a key
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
              if redis.call('RPUSH', fkey, ARGV[7]) > @MAX@ then redis.call('LTRIM', fkey, -@MAX@, -1) end
            end
            if ARGV[4] ~= '' and tonumber(v[5]) < tonumber(v[6]) then
              local retryAt = tonumber(ARGV[4])
              record()
              move(id, qid, 2, 1, at, retryAt)
              redis.call('HSET', tkey, 'state', 1, 'sc', at, 'rev', rev + 1, 'due', retryAt, 'fc', 1)
              track(id, v[9])
              flush()
              return 'RETRY'
            end
            if ARGV[5] == '1' or kk then
              record()
              finish(tkey, id, qid, 2, 4, at, rev, uk, kk, {'fc', 1}, true)
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
     * the {@code st:<id>} string and keeps the hs marker field in step with it, any other target is a field of
     * the task hash. An empty value deletes.
     */
    static final String WRITE = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = fence(tkey, ARGV[3], 'hs')
            if not v then return 0 end
            if ARGV[4] == 'steps' then
              if ARGV[5] == '' then
                redis.call('UNLINK', P .. 'st:' .. id)
                if v[4] then redis.call('HDEL', tkey, 'hs') end
              else
                redis.call('SET', P .. 'st:' .. id, ARGV[5])
                if not v[4] then redis.call('HSET', tkey, 'hs', 1) end
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
     * -1}. Ids whose task is gone or no longer SCHEDULED are dropped. Each queue costs one command when nothing
     * in it is due: the head read that decides whether to read a batch is also the next due time. Only queues
     * that were read for a batch have their head read again after the flush.
     */
    static final String PROMOTE = PRELUDE + """
            local at = now()
            local remaining = tonumber(ARGV[2])
            local qids = redis.call('HVALS', P .. 'reg:queue')
            local moved = 0
            local nextDue = -1
            local worked = {}
            for _, qid in ipairs(qids) do
              local skey = P .. 'sched:' .. qid
              local head = redis.call('ZRANGE', skey, 0, 0, 'WITHSCORES')[2]
              if head then
                head = tonumber(head)
                if remaining > 0 and head <= at then
                  worked[#worked + 1] = qid
                  local ids = redis.call('ZRANGE', skey, '-inf', at, 'BYSCORE', 'LIMIT', 0, remaining)
                  for _, id in ipairs(ids) do
                    local tkey = P .. 't:' .. id
                    local v = redis.call('HMGET', tkey, 'state', 'due', 'rev', 'ty')
                    if v[1] == '1' then
                      move(id, qid, 1, 0, at, tonumber(v[2]), nil, v[4])
                      redis.call('HSET', tkey, 'state', 0, 'sc', at, 'rev', tonumber(v[3]) + 1)
                      moved = moved + 1
                    else
                      zrem(skey, id)
                    end
                  end
                  remaining = remaining - #ids
                elseif nextDue < 0 or head < nextDue then
                  nextDue = head
                end
              end
            end
            flush()
            for _, qid in ipairs(worked) do
              local head = redis.call('ZRANGE', P .. 'sched:' .. qid, 0, 0, 'WITHSCORES')[2]
              if head and (nextDue < 0 or tonumber(head) < nextDue) then nextDue = tonumber(head) end
            end
            return {moved, nextDue}
            """;

    /**
     * ARGV: state, retention ms, limit. Per queue: first the {@code hold} zset, whose window has passed, then the
     * {@code idx} zset up to the cutoff. A held task whose retention has not elapsed yet moves from {@code hidx}
     * to {@code idx}, so every entry read leaves the zset it was read from and the next read starts at offset 0;
     * held tasks that are still open are never read. One call reads at most max(4*limit, 1000) hold entries.
     */
    static final String PURGE = PRELUDE + """
            local state = tonumber(ARGV[2])
            local at = now()
            local cutoff = at - tonumber(ARGV[3])
            local remaining = tonumber(ARGV[4])
            local budget = math.max(remaining * 4, 1000)
            local removed = 0
            local function purge(id)
              redis.call('UNLINK', P .. 't:' .. id, P .. 'd:' .. id, P .. 'st:' .. id, P .. 'f:' .. id)
              removed = removed + 1
              remaining = remaining - 1
            end
            for _, qid in ipairs(redis.call('HVALS', P .. 'reg:queue')) do
              local ikey = P .. 'idx:' .. state .. ':' .. qid
              local hikey = P .. 'hidx:' .. state .. ':' .. qid
              local hkey = P .. 'hold:' .. state .. ':' .. qid
              while remaining > 0 and budget > 0 do
                local ids = redis.call('ZRANGE', hkey, '-inf', at, 'BYSCORE', 'LIMIT', 0, math.min(remaining, budget, 500))
                if #ids == 0 then break end
                budget = budget - #ids
                for _, id in ipairs(ids) do
                  zrem(hkey, id)
                  zrem(hikey, id)
                  local v = redis.call('HMGET', P .. 't:' .. id, 'state', 'sc')
                  if v[1] == ARGV[2] then
                    if tonumber(v[2]) <= cutoff then purge(id) else zadd(ikey, tonumber(v[2]), id) end
                  end
                end
                flush()
              end
              while remaining > 0 do
                local ids = redis.call('ZRANGE', ikey, '-inf', cutoff, 'BYSCORE', 'LIMIT', 0, math.min(remaining, 500))
                if #ids == 0 then break end
                for _, id in ipairs(ids) do
                  zrem(ikey, id)
                  purge(id)
                end
                flush()
              end
              if remaining <= 0 then break end
            end
            return removed
            """;

    /** ARGV: token, ms. Takes the migration lock {@code migrating} for that long; returns 1 when taken. */
    static final String MIGRATION_LOCK = PRELUDE + """
            return redis.call('SET', P .. 'migrating', ARGV[2], 'NX', 'PX', ARGV[3]) and 1 or 0
            """;

    /** ARGV: token. Releases the migration lock if that token still holds it. */
    static final String MIGRATION_UNLOCK = PRELUDE + """
            if redis.call('GET', P .. 'migrating') == ARGV[2] then redis.call('DEL', P .. 'migrating') end
            return 1
            """;

    /** Layout 4 to 5, ARGV: queue id. Fills {@code active:<qid>} from the type zsets of the queue that exist. */
    static final String MIGRATE_ACTIVE = PRELUDE + """
            local qid = ARGV[2]
            for _, tid in ipairs(redis.call('HVALS', P .. 'reg:type')) do
              if redis.call('EXISTS', P .. 'q:' .. qid .. ':' .. tid) == 1 then
                redis.call('SADD', P .. 'active:' .. qid, tid)
              end
            end
            return 1
            """;

    /**
     * Layout 4 to 5, ARGV: state, queue id, offset, limit. Moves the held tasks of one page of
     * {@code hold:<state>:<qid>} from {@code idx} to {@code hidx} with their score; a task moved before is left
     * alone. Returns how many hold entries were read.
     */
    static final String MIGRATE_HELD = PRELUDE + """
            local suffix = ARGV[2] .. ':' .. ARGV[3]
            local ids = redis.call('ZRANGE', P .. 'hold:' .. suffix, ARGV[4], tonumber(ARGV[4]) + tonumber(ARGV[5]) - 1)
            for _, id in ipairs(ids) do
              local score = redis.call('ZSCORE', P .. 'idx:' .. suffix, id)
              if score then
                redis.call('ZREM', P .. 'idx:' .. suffix, id)
                redis.call('ZADD', P .. 'hidx:' .. suffix, score, id)
              end
            end
            return #ids
            """;

    /** ARGV: kind (queue or type), name. Returns the id; fails when the 32767 ids are used up. */
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
     * field is not finished or the stored time is not later than keep-until; otherwise occ is dropped. A put that
     * finds every definition field stored already, keeps the fire time and keeps occ writes nothing and returns
     * UNCHANGED. Otherwise the revision comes from the counter, so no two definitions under the same id share one,
     * and it returns CREATED or CHANGED. The fire time is also stored in the hash as {@code next}; a paused
     * definition is left out of the {@code cron} zset and keeps it there. Replacing an existing definition keeps
     * its paused flag and ignores the given one; a new definition stores the flag as given.
     */
    static final String CRON_PUT = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            local raw = redis.call('HGETALL', ckey)
            local existed = #raw > 0
            local old, new, fields = {}, {}, {}
            for i = 1, #raw, 2 do old[raw[i]] = raw[i + 1] end
            for i = 5, #ARGV, 2 do
              new[ARGV[i]] = ARGV[i + 1]
              fields[#fields + 1] = ARGV[i]
              fields[#fields + 1] = ARGV[i + 1]
            end
            local own = {rev = true, next = true, occ = true, paused = true}
            local same = existed
            for k, v in pairs(new) do
              if same and not own[k] and old[k] ~= v then same = false end
            end
            for k in pairs(old) do
              if same and not own[k] and new[k] == nil then same = false end
            end
            local kept, occ = old.next, old.occ
            local running = false
            local keep = false
            if kept and old.cron == new.cron and old.zone == new.zone and old.fd == new.fd then
              local st = occ and redis.call('HGET', P .. 't:' .. occ, 'state')
              running = st and tonumber(st) < 3 or false
              keep = running or tonumber(kept) <= tonumber(ARGV[4])
            end
            if same and keep and (running or not occ) then return 'UNCHANGED' end
            -- counter first, so a counter failure leaves the old definition intact; read back as a string
            -- because the INCR reply would pass through a Lua double
            redis.call('INCR', P .. 'cronrev')
            local rev = redis.call('GET', P .. 'cronrev')
            redis.call('DEL', ckey)
            redis.call('HSET', ckey, unpack(fields))
            local nextFire = keep and kept or ARGV[3]
            if running then redis.call('HSET', ckey, 'occ', occ) end
            redis.call('HSET', ckey, 'rev', rev, 'next', nextFire)
            if existed then
              if old.paused then redis.call('HSET', ckey, 'paused', old.paused) else redis.call('HDEL', ckey, 'paused') end
            end
            redis.call('SADD', P .. 'crondefs', ARGV[2])
            if redis.call('HGET', ckey, 'paused') == '1' then
              redis.call('ZREM', P .. 'cron', ARGV[2])
            else
              redis.call('ZADD', P .. 'cron', nextFire, ARGV[2])
            end
            return existed and 'CHANGED' or 'CREATED'
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
     * occ field; when the unique key is held, occ is empty and the holder belongs to the schedule, the holder becomes it. Returns 1 when committed.
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
            local stored = store(ARGV[6], ARGV[4], uk, ARGV[8], sb, data, 12, #ARGV, true)
            flush()
            if redis.call('HEXISTS', ckey, 'fd') == 1 then
              if stored[1] == 1 then
                redis.call('HSET', ckey, 'occ', ARGV[6])
              elseif stored[1] == 2 and redis.call('HEXISTS', ckey, 'occ') == 0
                  and redis.call('HGET', P .. 't:' .. stored[2], 'sid') == ARGV[2] then
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
     * ARGV: id, revision, reason or '', keep payload (1/0), expired only (1/0), then the schedule effect as for
     * ACK. LEASED to CANCELLED; the worker id stays, the cancel request time is kept when there is one. Returns 1
     * (2 when the effect changed the definition), or 0 when the fence fails or, with expired only, when the
     * stored lease is after now.
     */
    static final String FINISH_CANCELLED = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = fence(tkey, ARGV[3], 'uk', 'kk', 'cra', 'lease')
            if not v then return 0 end
            local at = now()
            if ARGV[6] == '1' and tonumber(v[7]) > at then return 0 end
            local extra = {}
            if not v[6] then extra = {'cra', at} end
            if ARGV[4] ~= '' then extra[#extra + 1] = 'crs'; extra[#extra + 1] = ARGV[4] end
            finish(tkey, id, tonumber(v[3]), 2, 5, at, tonumber(v[1]), v[4], v[5], extra, true)
            if ARGV[5] ~= '1' then redis.call('UNLINK', P .. 'd:' .. id) end
            local fx = effect(ARGV[7], ARGV[8], ARGV[9])
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
     * retried EXPIRED task is not expired again. Steps and failures stay. A task that still has its uk field
     * (its keep window is open, so the field has not expired) becomes the open holder of the key again: k: is
     * taken with the SET NX GET that STORE uses and loses its expiry, and uk and kk are rewritten so they lose
     * theirs, while ku goes. Returns 1, 0 on a mismatch, or 2 when another task holds the key; nothing changes
     * then.
     */
    static final String RETRY = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = redis.call('HMGET', tkey, 'rev', 'state', 'q', 'uk', 'kk')
            if v[1] ~= ARGV[3] then return 0 end
            local state = tonumber(v[2])
            if state < 4 or state > 6 then return 0 end
            local at = now()
            local f = {'state', 0, 'sc', at, 'rev', tonumber(v[1]) + 1, 'due', at, 'att', 0}
            if v[4] then
              local kkey = P .. 'k:' .. v[4]
              local holder = redis.call('SET', kkey, id, 'NX', 'GET')
              if holder then
                if holder ~= id and (redis.call('PTTL', kkey) ~= -1 or redis.call('EXISTS', P .. 't:' .. holder) == 1) then
                  return 2
                end
                redis.call('SET', kkey, id)
              end
              f[#f + 1] = 'uk'; f[#f + 1] = v[4]
              if v[5] then f[#f + 1] = 'kk'; f[#f + 1] = v[5] end
            else
              redis.call('HDEL', tkey, 'kk')
            end
            move(id, v[3], state, 0, at, at)
            zrem(P .. 'startby', id)
            redis.call('HSET', tkey, unpack(f))
            redis.call('HDEL', tkey, 'wid', 'lease', 'cra', 'crs', 'ku', 'sb')
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

    /** ARGV: label, queue ids JSON, type ids JSON. Records coverage and start time; returns the worker id. */
    static final String WORKER_REGISTER = PRELUDE + """
            local id = redis.call('INCR', P .. 'seq:worker')
            local at = now()
            redis.call('HSET', P .. 'w:' .. id, 'label', ARGV[2], 'started', at, 'seen', at, 'running', 0, 'accepting', 1,
              'queues', ARGV[3], 'types', ARGV[4])
            redis.call('ZADD', P .. 'workers', at, id)
            return id
            """;

    /**
     * ARGV: worker id, running, accepting (1/0), label, queue ids JSON, type ids JSON. A removed worker's hash is
     * created again with the given label, queues and types; an existing one keeps its own. Returns 1.
     */
    static final String HEARTBEAT = PRELUDE + """
            local id = ARGV[2]
            local key = P .. 'w:' .. id
            local at = now()
            if redis.call('EXISTS', key) == 0 then
              redis.call('HSET', key, 'label', ARGV[5], 'started', at, 'queues', ARGV[6], 'types', ARGV[7])
            end
            redis.call('HSET', key, 'seen', at, 'running', ARGV[3], 'accepting', ARGV[4])
            redis.call('ZADD', P .. 'workers', at, id)
            return 1
            """;

    /**
     * ARGV: stale after ms, page limit. Removes up to limit workers last seen strictly before now - stale, oldest
     * first, with one UNLINK and one ZREM. Returns how many worker hashes were removed; a full page means more
     * may be left.
     */
    static final String PURGE_WORKERS = PRELUDE + """
            local ids = redis.call('ZRANGE', P .. 'workers', '-inf', '(' .. (now() - tonumber(ARGV[2])), 'BYSCORE',
              'LIMIT', 0, tonumber(ARGV[3]))
            if #ids > 0 then
              local keys = {}
              for i, id in ipairs(ids) do keys[i] = P .. 'w:' .. id end
              redis.call('UNLINK', unpack(keys))
              redis.call('ZREM', P .. 'workers', unpack(ids))
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
