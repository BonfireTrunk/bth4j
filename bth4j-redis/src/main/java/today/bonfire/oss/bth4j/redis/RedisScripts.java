package today.bonfire.oss.bth4j.redis;

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
 * represents the state (queue zset, {@code scheduled}, {@code leased} plus {@code idx}, or {@code idx}) and the
 * {@code c:<qid>} counter in step with the hash.
 */
final class RedisScripts {

    private RedisScripts() {}

    private static final String PRELUDE = """
            local P = ARGV[1]

            local function now()
              local t = redis.call('TIME')
              return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
            end

            local function enter(id, qid, state, score, at)
              if state == 0 then
                redis.call('ZADD', P .. 'q:' .. qid, score, id)
              elseif state == 1 then
                redis.call('ZADD', P .. 'scheduled', score, id)
              else
                if state == 2 then redis.call('ZADD', P .. 'leased', score, id) end
                redis.call('ZADD', P .. 'idx:' .. state .. ':' .. qid, at, id)
              end
              redis.call('HINCRBY', P .. 'c:' .. qid, state, 1)
            end

            local function leave(id, qid, state)
              if state == 0 then
                redis.call('ZREM', P .. 'q:' .. qid, id)
              elseif state == 1 then
                redis.call('ZREM', P .. 'scheduled', id)
              else
                if state == 2 then redis.call('ZREM', P .. 'leased', id) end
                redis.call('ZREM', P .. 'idx:' .. state .. ':' .. qid, id)
              end
              redis.call('HINCRBY', P .. 'c:' .. qid, state, -1)
            end

            local function move(tkey, id, qid, from, to, at, score)
              leave(id, qid, from)
              redis.call('HSET', tkey, 'state', to, 'sc', at)
              redis.call('HINCRBY', tkey, 'rev', 1)
              enter(id, qid, to, score, at)
            end

            local function fence(tkey, rev)
              local v = redis.call('HMGET', tkey, 'rev', 'state', 'q')
              if v[1] ~= rev or v[2] ~= '2' then return nil end
              return tonumber(v[3])
            end

            -- A kept window keeps the k: string alive through PEXPIRE; the keys zset lets releaseKeys find the
            -- hash later. The k: string is only touched while it still names this task.
            local function finishKey(tkey, id, uk, kk, at, hashKept)
              if not uk then return end
              local kkey = P .. 'k:' .. uk
              local owned = redis.call('GET', kkey) == id
              if kk then
                local ms = tonumber(kk)
                if owned then redis.call('PEXPIRE', kkey, ms) end
                if hashKept then
                  redis.call('HSET', tkey, 'ku', at + ms)
                  redis.call('ZADD', P .. 'keys', at + ms, id)
                end
              else
                if owned then redis.call('DEL', kkey) end
                if hashKept then redis.call('HDEL', tkey, 'uk', 'kk') end
              end
            end

            -- startby only tracks tasks that wait (QUEUED or SCHEDULED). Callers put a task back in with track
            -- whenever it returns to a waiting state, and take it out when it leaves one.
            local function track(tkey, id)
              local sb = redis.call('HGET', tkey, 'sb')
              if sb then redis.call('ZADD', P .. 'startby', sb, id) end
            end

            -- QUEUED or SCHEDULED to a terminal state without a worker (CANCELLED or EXPIRED); the hash is kept
            -- for inspection, so the key handling is the same as for a kept SUCCEEDED task.
            local function endWaiting(tkey, id, qid, from, to, at)
              local v = redis.call('HMGET', tkey, 'uk', 'kk')
              move(tkey, id, qid, from, to, at, at)
              redis.call('ZREM', P .. 'startby', id)
              finishKey(tkey, id, v[1] or nil, v[2] or nil, at, true)
            end

            -- fields come from ARGV[from..]. Returns {0} when the id exists, {2, id} when the unique key is
            -- held, {1} when stored. A k: string without TTL whose task is gone is stale and is overwritten.
            local function store(id, dueArg, uk, from, forceScheduled)
              local tkey = P .. 't:' .. id
              if redis.call('EXISTS', tkey) == 1 then return {0, 0} end
              if uk then
                local kkey = P .. 'k:' .. uk
                local holder = redis.call('GET', kkey)
                if holder and (redis.call('PTTL', kkey) ~= -1 or redis.call('EXISTS', P .. 't:' .. holder) == 1) then
                  return {2, tonumber(holder)}
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
              enter(id, redis.call('HGET', tkey, 'q'), state, due, at)
              track(tkey, id)
              if uk then redis.call('SET', P .. 'k:' .. uk, id) end
              return {1, 0}
            end
            """;

    /** ARGV: id, due ms or '', unique key or '', then field/value pairs. Returns {code, existing id}. */
    static final String ENQUEUE = PRELUDE + """
            local uk = ARGV[4] ~= '' and ARGV[4] or nil
            local due = ARGV[3] ~= '' and ARGV[3] or nil
            return store(ARGV[2], due, uk, 5, false)
            """;

    /**
     * ARGV: count, lease ms, worker id, known event ids (comma separated, '' for any), queue ids in order.
     * A queue is scanned for at most count*4 ids (cap 1000). Paused queues are skipped. A popped task whose
     * start-by time has passed is expired on the spot and never returned. Tasks of unknown events are deferred
     * by 5 s in the queue zset only, without an attempt. Returns {id, field, value, ...} per claimed task.
     */
    static final String CLAIM = PRELUDE + """
            local at = now()
            local count = tonumber(ARGV[2])
            local lease = at + tonumber(ARGV[3])
            local known = nil
            if ARGV[5] ~= '' then
              known = {}
              for e in string.gmatch(ARGV[5], '[^,]+') do known[e] = true end
            end
            local remaining = count
            local result = {}
            for i = 6, #ARGV do
              if remaining <= 0 then break end
              local qid = ARGV[i]
              local qkey = P .. 'q:' .. qid
              local budget = math.min(count * 4, 1000)
              if redis.call('SISMEMBER', P .. 'paused', qid) == 1 then budget = 0 end
              while remaining > 0 and budget > 0 do
                local ids = redis.call('ZRANGE', qkey, '-inf', at, 'BYSCORE', 'LIMIT', 0, math.min(budget, remaining))
                if #ids == 0 then break end
                budget = budget - #ids
                for _, id in ipairs(ids) do
                  local tkey = P .. 't:' .. id
                  local v = redis.call('HMGET', tkey, 'ev', 'state', 'sb')
                  if v[2] ~= '0' then
                    redis.call('ZREM', qkey, id)
                  elseif v[3] and tonumber(v[3]) <= at then
                    endWaiting(tkey, id, qid, 0, 6, at)
                  elseif known and not known[v[1]] then
                    redis.call('ZADD', qkey, at + 5000, id)
                  else
                    move(tkey, id, qid, 0, 2, at, lease)
                    if v[3] then redis.call('ZREM', P .. 'startby', id) end
                    redis.call('HINCRBY', tkey, 'att', 1)
                    redis.call('HSET', tkey, 'ca', at, 'lease', lease, 'wid', ARGV[4])
                    local entry = {id}
                    for _, x in ipairs(redis.call('HGETALL', tkey)) do entry[#entry + 1] = x end
                    result[#result + 1] = entry
                    remaining = remaining - 1
                  end
                end
              end
            end
            return result
            """;

    /** ARGV: lease ms, then id/revision pairs. Returns per pair 0 (not owned), 1 (owned) or 2 (owned, cancel requested). */
    static final String RENEW = PRELUDE + """
            local lease = now() + tonumber(ARGV[2])
            local out = {}
            for i = 3, #ARGV, 2 do
              local tkey = P .. 't:' .. ARGV[i]
              if fence(tkey, ARGV[i + 1]) then
                redis.call('ZADD', P .. 'leased', lease, ARGV[i])
                redis.call('HSET', tkey, 'lease', lease)
                out[#out + 1] = redis.call('HEXISTS', tkey, 'cra') == 1 and 2 or 1
              else
                out[#out + 1] = 0
              end
            end
            return out
            """;

    /**
     * ARGV: id, revision, keep (1/0), keep payload (1/0). Returns 1, or 0 when the fence fails. A task with a
     * key window is kept as SUCCEEDED even when keep is 0 (its data is dropped), so purge removes it after the
     * window; only a task without a window is removed here.
     */
    static final String ACK = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local qid = fence(tkey, ARGV[3])
            if not qid then return 0 end
            local at = now()
            local v = redis.call('HMGET', tkey, 'uk', 'kk')
            if ARGV[4] == '1' or v[2] then
              move(tkey, id, qid, 2, 3, at, at)
              redis.call('HDEL', tkey, 'lease')
              if ARGV[4] ~= '1' or ARGV[5] ~= '1' then redis.call('HDEL', tkey, 'data') end
              finishKey(tkey, id, v[1] or nil, v[2] or nil, at, true)
            else
              leave(id, qid, 2)
              redis.call('DEL', tkey)
              finishKey(tkey, id, v[1] or nil, v[2] or nil, at, false)
            end
            return 1
            """;

    /**
     * ARGV: id, revision, retry at ms or '', keep dead (1/0), expired only (1/0), failures JSON. Returns
     * RETRY, DEAD or LOST. With expired only the stored lease must be at or before now. A task with a key
     * window is kept as DEAD even when keep dead is 0.
     */
    static final String FAIL = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local qid = fence(tkey, ARGV[3])
            if not qid then return 'LOST' end
            local at = now()
            local v = redis.call('HMGET', tkey, 'lease', 'att', 'max', 'uk', 'kk')
            if ARGV[6] == '1' and tonumber(v[1]) > at then return 'LOST' end
            if ARGV[4] ~= '' and tonumber(v[2]) < tonumber(v[3]) then
              local retryAt = tonumber(ARGV[4])
              move(tkey, id, qid, 2, 1, at, retryAt)
              redis.call('HSET', tkey, 'due', retryAt, 'fl', ARGV[7])
              redis.call('HDEL', tkey, 'lease', 'wid')
              track(tkey, id)
              return 'RETRY'
            end
            if ARGV[5] == '1' or v[5] then
              move(tkey, id, qid, 2, 4, at, at)
              redis.call('HSET', tkey, 'fl', ARGV[7])
              redis.call('HDEL', tkey, 'lease')
              finishKey(tkey, id, v[4] or nil, v[5] or nil, at, true)
            else
              leave(id, qid, 2)
              redis.call('DEL', tkey)
              finishKey(tkey, id, v[4] or nil, v[5] or nil, at, false)
            end
            return 'DEAD'
            """;

    /** ARGV: id, revision, due ms. Returns 1, or 0 when the fence fails. */
    static final String RESCHEDULE = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local qid = fence(tkey, ARGV[3])
            if not qid then return 0 end
            local due = tonumber(ARGV[4])
            move(tkey, id, qid, 2, 1, now(), due)
            redis.call('HINCRBY', tkey, 'att', -1)
            redis.call('HSET', tkey, 'due', due)
            redis.call('HDEL', tkey, 'lease', 'wid')
            track(tkey, id)
            return 1
            """;

    /** ARGV: id, revision. Returns 1, or 0 when the fence fails. */
    static final String UNCLAIM = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local qid = fence(tkey, ARGV[3])
            if not qid then return 0 end
            move(tkey, id, qid, 2, 0, now(), tonumber(redis.call('HGET', tkey, 'due')))
            redis.call('HINCRBY', tkey, 'att', -1)
            redis.call('HDEL', tkey, 'lease', 'wid')
            track(tkey, id)
            return 1
            """;

    /** ARGV: id, revision, field, value, delete (1/0). Writes the whole field without touching the revision. */
    static final String SET_FIELD = PRELUDE + """
            local tkey = P .. 't:' .. ARGV[2]
            if not fence(tkey, ARGV[3]) then return 0 end
            if ARGV[6] == '1' then
              redis.call('HDEL', tkey, ARGV[4])
            else
              redis.call('HSET', tkey, ARGV[4], ARGV[5])
            end
            return 1
            """;

    /** ARGV: limit. Returns {moved, next due ms or -1}. Ids whose task is gone or no longer SCHEDULED are dropped. */
    static final String PROMOTE = PRELUDE + """
            local at = now()
            local ids = redis.call('ZRANGE', P .. 'scheduled', '-inf', at, 'BYSCORE', 'LIMIT', 0, ARGV[2])
            local moved = 0
            for _, id in ipairs(ids) do
              local tkey = P .. 't:' .. id
              local v = redis.call('HMGET', tkey, 'state', 'q', 'due')
              if v[1] == '1' then
                move(tkey, id, v[2], 1, 0, at, tonumber(v[3]))
                moved = moved + 1
              else
                redis.call('ZREM', P .. 'scheduled', id)
              end
            end
            local head = redis.call('ZRANGE', P .. 'scheduled', 0, 0, 'WITHSCORES')
            if head[2] then return {moved, tonumber(head[2])} end
            return {moved, -1}
            """;

    /** ARGV: limit. Returns {id, revision, attempts, max, event, cancel requested (1/0)} per expired lease. */
    static final String RECLAIM = PRELUDE + """
            local ids = redis.call('ZRANGE', P .. 'leased', '-inf', now(), 'BYSCORE', 'LIMIT', 0, ARGV[2])
            local out = {}
            for _, id in ipairs(ids) do
              local v = redis.call('HMGET', P .. 't:' .. id, 'rev', 'att', 'max', 'ev', 'cra')
              if v[1] then
                out[#out + 1] = {id, v[1], v[2], v[3], v[4], v[5] and 1 or 0}
              else
                redis.call('ZREM', P .. 'leased', id)
              end
            end
            return out
            """;

    /**
     * ARGV: state, retention ms, limit. Walks the idx zsets of every queue oldest first. A task whose key window
     * is still open is skipped and the next page starts behind it, so held tasks cannot starve the purge.
     */
    static final String PURGE = PRELUDE + """
            local state = tonumber(ARGV[2])
            local at = now()
            local cutoff = at - tonumber(ARGV[3])
            local remaining = tonumber(ARGV[4])
            local removed = 0
            for _, qid in ipairs(redis.call('SMEMBERS', P .. 'queues')) do
              local ikey = P .. 'idx:' .. state .. ':' .. qid
              local skipped = 0
              while remaining > 0 do
                local ids = redis.call('ZRANGE', ikey, '-inf', cutoff, 'BYSCORE', 'LIMIT', skipped, math.min(remaining, 500))
                if #ids == 0 then break end
                for _, id in ipairs(ids) do
                  local tkey = P .. 't:' .. id
                  local ku = redis.call('HGET', tkey, 'ku')
                  if ku and tonumber(ku) > at then
                    skipped = skipped + 1
                  else
                    redis.call('ZREM', ikey, id)
                    redis.call('ZREM', P .. 'keys', id)
                    redis.call('DEL', tkey)
                    redis.call('HINCRBY', P .. 'c:' .. qid, state, -1)
                    removed = removed + 1
                    remaining = remaining - 1
                  end
                end
              end
              if remaining <= 0 then break end
            end
            return removed
            """;

    /** ARGV: limit. Clears the key fields of finished tasks whose window passed; returns how many. */
    static final String RELEASE_KEYS = PRELUDE + """
            local ids = redis.call('ZRANGE', P .. 'keys', '-inf', now(), 'BYSCORE', 'LIMIT', 0, ARGV[2])
            for _, id in ipairs(ids) do
              redis.call('HDEL', P .. 't:' .. id, 'uk', 'kk', 'ku')
              redis.call('ZREM', P .. 'keys', id)
            end
            return #ids
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
            redis.call('HSET', reg .. ':names', id, ARGV[3])
            if ARGV[2] == 'queue' then redis.call('SADD', P .. 'queues', id) end
            return id
            """;

    /**
     * ARGV: schedule id, next fire ms, then field/value pairs. The fire time is kept when the schedule shape
     * (the cron expression, empty for fixed delay) is unchanged. The revision comes from the counter, so no two
     * definitions under the same id share one. The fire time is also stored in the hash as {@code next}; a
     * paused definition is left out of the {@code cron} zset and keeps it there.
     */
    static final String CRON_PUT = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            local function shape()
              return redis.call('HGET', ckey, 'cron') or ''
            end
            local before = redis.call('EXISTS', ckey) == 1 and shape() or nil
            local kept = redis.call('HGET', ckey, 'next')
            local fields = {}
            for i = 4, #ARGV do fields[#fields + 1] = ARGV[i] end
            redis.call('DEL', ckey)
            redis.call('HSET', ckey, unpack(fields))
            local rev = redis.call('INCR', P .. 'cronrev')
            local nextFire = ARGV[3]
            if kept and before == shape() then nextFire = kept end
            redis.call('HSET', ckey, 'rev', rev, 'next', nextFire)
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

    /** ARGV: up to ms, limit. Returns {schedule id, fire at, field, value, ...} per due definition. */
    static final String CRON_DUE = PRELUDE + """
            local due = redis.call('ZRANGE', P .. 'cron', '-inf', ARGV[2], 'BYSCORE', 'WITHSCORES', 'LIMIT', 0, ARGV[3])
            local out = {}
            for i = 1, #due, 2 do
              local entry = {due[i], due[i + 1]}
              for _, x in ipairs(redis.call('HGETALL', P .. 'cron:' .. due[i])) do entry[#entry + 1] = x end
              out[#out + 1] = entry
            end
            return out
            """;

    /**
     * ARGV: schedule id, definition revision, fire at ms, next fire ms, occurrence id, unique key or '', then the
     * occurrence's field/value pairs. Stores the occurrence as SCHEDULED for the fire time and advances the
     * definition, unless the revision or the stored fire time changed. An occurrence that exists already or whose
     * unique key is held is not stored again; the definition still advances. Returns 1 when committed.
     */
    static final String CRON_COMMIT = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            if redis.call('HGET', ckey, 'rev') ~= ARGV[3] then return 0 end
            local score = redis.call('ZSCORE', P .. 'cron', ARGV[2])
            if not score or tonumber(score) ~= tonumber(ARGV[4]) then return 0 end
            local uk = ARGV[7] ~= '' and ARGV[7] or nil
            store(ARGV[6], ARGV[4], uk, 8, true)
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
              local v = redis.call('HMGET', P .. 't:' .. id, 'state', 'q')
              local state = tonumber(v[1])
              if state == 0 or state == 1 then
                endWaiting(P .. 't:' .. id, id, v[2], state, 6, at)
                out[#out + 1] = id
              else
                redis.call('ZREM', P .. 'startby', id)
              end
            end
            return out
            """;

    /**
     * ARGV: id, revision, reason or '', keep payload (1/0). LEASED to CANCELLED; the worker id stays, the
     * cancel request time is kept when there is one. Returns 1, or 0 when the fence fails.
     */
    static final String FINISH_CANCELLED = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local qid = fence(tkey, ARGV[3])
            if not qid then return 0 end
            local at = now()
            local v = redis.call('HMGET', tkey, 'uk', 'kk', 'cra')
            move(tkey, id, qid, 2, 5, at, at)
            redis.call('HDEL', tkey, 'lease')
            if not v[3] then redis.call('HSET', tkey, 'cra', at) end
            if ARGV[4] ~= '' then redis.call('HSET', tkey, 'crs', ARGV[4]) end
            if ARGV[5] ~= '1' then redis.call('HDEL', tkey, 'data') end
            finishKey(tkey, id, v[1] or nil, v[2] or nil, at, true)
            return 1
            """;

    /**
     * ARGV: id, revision, reason or ''. Returns CANCELLED (QUEUED or SCHEDULED, moved), REQUESTED (LEASED, only
     * the request is stored and the revision stays) or STALE.
     */
    static final String CANCEL = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = redis.call('HMGET', tkey, 'rev', 'state', 'q')
            if v[1] ~= ARGV[3] then return 'STALE' end
            local state = tonumber(v[2])
            local at = now()
            if state == 0 or state == 1 then
              endWaiting(tkey, id, v[3], state, 5, at)
              redis.call('HSET', tkey, 'cra', at)
              if ARGV[4] ~= '' then redis.call('HSET', tkey, 'crs', ARGV[4]) end
              return 'CANCELLED'
            elseif state == 2 then
              redis.call('HSET', tkey, 'cra', at)
              if ARGV[4] ~= '' then redis.call('HSET', tkey, 'crs', ARGV[4]) end
              return 'REQUESTED'
            end
            return 'STALE'
            """;

    /** ARGV: id, revision. DEAD, CANCELLED or EXPIRED back to QUEUED. Returns 1, or 0 on a mismatch. */
    static final String RETRY = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = redis.call('HMGET', tkey, 'rev', 'state', 'q', 'uk')
            if v[1] ~= ARGV[3] then return 0 end
            local state = tonumber(v[2])
            if state < 4 or state > 6 then return 0 end
            local at = now()
            move(tkey, id, v[3], state, 0, at, at)
            redis.call('ZREM', P .. 'keys', id)
            if v[4] then
              local kkey = P .. 'k:' .. v[4]
              if redis.call('GET', kkey) == id then redis.call('DEL', kkey) end
            end
            redis.call('HSET', tkey, 'due', at, 'att', 0)
            redis.call('HDEL', tkey, 'wid', 'lease', 'cra', 'crs', 'kk', 'ku', 'uk')
            track(tkey, id)
            return 1
            """;

    /** ARGV: id, revision. Removes a task in any state but LEASED. Returns 1, or 0 on a mismatch. */
    static final String DELETE = PRELUDE + """
            local id = ARGV[2]
            local tkey = P .. 't:' .. id
            local v = redis.call('HMGET', tkey, 'rev', 'state', 'q', 'uk')
            if v[1] ~= ARGV[3] then return 0 end
            local state = tonumber(v[2])
            if state == 2 then return 0 end
            leave(id, v[3], state)
            redis.call('ZREM', P .. 'keys', id)
            redis.call('ZREM', P .. 'startby', id)
            if v[4] then
              local kkey = P .. 'k:' .. v[4]
              if redis.call('GET', kkey) == id then redis.call('DEL', kkey) end
            end
            redis.call('UNLINK', tkey)
            return 1
            """;

    /** ARGV: label. Allocates the worker id and records it as started and seen now. Returns the id. */
    static final String WORKER_REGISTER = PRELUDE + """
            local id = redis.call('INCR', P .. 'seq:worker')
            local at = now()
            redis.call('HSET', P .. 'w:' .. id, 'label', ARGV[2], 'started', at, 'seen', at, 'running', 0)
            redis.call('ZADD', P .. 'workers', at, id)
            return id
            """;

    /** ARGV: worker id, running. A worker that was purged is not brought back. */
    static final String HEARTBEAT = PRELUDE + """
            local wkey = P .. 'w:' .. ARGV[2]
            if redis.call('EXISTS', wkey) == 0 then return 0 end
            local at = now()
            redis.call('HSET', wkey, 'seen', at, 'running', ARGV[3])
            redis.call('ZADD', P .. 'workers', at, ARGV[2])
            return 1
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

    /** ARGV: schedule id, next fire ms. Returns 1, or 0 when there is no such definition. */
    static final String CRON_SET_NEXT = PRELUDE + """
            local ckey = P .. 'cron:' .. ARGV[2]
            if redis.call('EXISTS', ckey) == 0 then return 0 end
            redis.call('HSET', ckey, 'next', ARGV[3])
            if redis.call('HGET', ckey, 'paused') ~= '1' then
              redis.call('ZADD', P .. 'cron', ARGV[3], ARGV[2])
            end
            return 1
            """;
}
