package today.bonfire.oss.bth4j.service;

/**
 * The Lua scripts that implement every task state transition. Each script is atomic and checks the
 * lease token before it moves a task, so a call from a worker that lost its lease does nothing.
 * <p>
 * Times are epoch milliseconds passed in by the caller. Hash field names are those of
 * {@code docs/design-3.0.md}.
 */
final class RedisScripts {

    private RedisScripts() {}

    /**
     * KEYS: task hash, scheduled zset, queue list. ARGV: id, due ms or "", wake channel, queue name,
     * then field/value pairs of the hash. Returns 1 when stored, 0 when the id already exists.
     */
    static final String ENQUEUE =
            """
            if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
            local fields = {}
            for i = 5, #ARGV do fields[#fields + 1] = ARGV[i] end
            redis.call('HSET', KEYS[1], unpack(fields))
            if ARGV[2] ~= '' then
              redis.call('HSET', KEYS[1], 'due', ARGV[2])
              redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
            else
              redis.call('LPUSH', KEYS[3], ARGV[1])
              redis.call('PUBLISH', ARGV[3], ARGV[4])
            end
            return 1
            """;

    /**
     * KEYS: leased zset, then the queue lists in the order to try. ARGV: now ms, lease ms, token base,
     * count, task prefix. Pops up to count ids from the queues in order, leases them and returns a list of
     * flattened hashes, each starting with the id. Ids without a hash are dropped.
     */
    static final String CLAIM =
            """
            local now = tonumber(ARGV[1])
            local expiry = now + tonumber(ARGV[2])
            local remaining = tonumber(ARGV[4])
            local prefix = ARGV[5]
            local result = {}
            local n = 0
            for q = 2, #KEYS do
              while remaining > 0 do
                local popped = redis.call('LMPOP', 1, KEYS[q], 'RIGHT', 'COUNT', remaining)
                if not popped then break end
                for _, id in ipairs(popped[2]) do
                  local tkey = prefix .. id
                  if redis.call('EXISTS', tkey) == 1 then
                    n = n + 1
                    local token = ARGV[3] .. ':' .. n
                    redis.call('HINCRBY', tkey, 'attempts', 1)
                    redis.call('HSET', tkey, 'token', token, 'lease', expiry)
                    redis.call('HDEL', tkey, 'due')
                    redis.call('ZADD', KEYS[1], expiry, id)
                    local entry = {id}
                    local hash = redis.call('HGETALL', tkey)
                    for _, v in ipairs(hash) do entry[#entry + 1] = v end
                    result[#result + 1] = entry
                    remaining = remaining - 1
                  end
                end
              end
              if remaining <= 0 then break end
            end
            return result
            """;

    /**
     * KEYS: leased zset. ARGV: new expiry ms, task prefix, then id/token pairs. Returns the number of
     * leases renewed; a task whose token changed is skipped.
     */
    static final String RENEW =
            """
            local renewed = 0
            for i = 3, #ARGV, 2 do
              local tkey = ARGV[2] .. ARGV[i]
              if redis.call('HGET', tkey, 'token') == ARGV[i + 1] then
                redis.call('HSET', tkey, 'lease', ARGV[1])
                redis.call('ZADD', KEYS[1], ARGV[1], ARGV[i])
                renewed = renewed + 1
              end
            end
            return renewed
            """;

    /** KEYS: leased zset. ARGV: task prefix, id, token. Returns 1 when the task was completed, 0 otherwise. */
    static final String ACK =
            """
            local tkey = ARGV[1] .. ARGV[2]
            if redis.call('HGET', tkey, 'token') ~= ARGV[3] then return 0 end
            redis.call('ZREM', KEYS[1], ARGV[2])
            redis.call('UNLINK', tkey)
            return 1
            """;

    /**
     * KEYS: leased zset, scheduled zset, dead zset. ARGV: task prefix, id, token, now ms, retry delay ms,
     * error text, dead hash ttl seconds. Returns "retry", "dead" or "lost" (token mismatch).
     */
    static final String FAIL =
            """
            local tkey = ARGV[1] .. ARGV[2]
            if redis.call('HGET', tkey, 'token') ~= ARGV[3] then return 'lost' end
            redis.call('ZREM', KEYS[1], ARGV[2])
            redis.call('HDEL', tkey, 'token', 'lease')
            redis.call('HSET', tkey, 'error', ARGV[6])
            local attempts = tonumber(redis.call('HGET', tkey, 'attempts'))
            local max = tonumber(redis.call('HGET', tkey, 'max'))
            if attempts >= max then
              redis.call('ZADD', KEYS[3], ARGV[4], ARGV[2])
              redis.call('HSET', tkey, 'died', ARGV[4])
              redis.call('EXPIRE', tkey, ARGV[7])
              return 'dead'
            end
            local due = tonumber(ARGV[4]) + tonumber(ARGV[5])
            redis.call('HSET', tkey, 'due', due)
            redis.call('ZADD', KEYS[2], due, ARGV[2])
            return 'retry'
            """;

    /**
     * KEYS: leased zset, scheduled zset. ARGV: task prefix, id, token, due ms. Moves the task to the
     * scheduled set and gives the attempt back. Returns 1, or 0 on token mismatch.
     */
    static final String RESCHEDULE =
            """
            local tkey = ARGV[1] .. ARGV[2]
            if redis.call('HGET', tkey, 'token') ~= ARGV[3] then return 0 end
            redis.call('ZREM', KEYS[1], ARGV[2])
            redis.call('HDEL', tkey, 'token', 'lease')
            redis.call('HINCRBY', tkey, 'attempts', -1)
            redis.call('HSET', tkey, 'due', ARGV[4])
            redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2])
            return 1
            """;

    /**
     * KEYS: leased zset. ARGV: task prefix, queue prefix, id, token. Puts the task back at the front of its
     * queue and gives the attempt back. Returns 1, or 0 on token mismatch.
     */
    static final String UNCLAIM =
            """
            local tkey = ARGV[1] .. ARGV[3]
            if redis.call('HGET', tkey, 'token') ~= ARGV[4] then return 0 end
            redis.call('ZREM', KEYS[1], ARGV[3])
            redis.call('HDEL', tkey, 'token', 'lease')
            redis.call('HINCRBY', tkey, 'attempts', -1)
            redis.call('RPUSH', ARGV[2] .. redis.call('HGET', tkey, 'queue'), ARGV[3])
            return 1
            """;

    /**
     * KEYS: scheduled zset. ARGV: now ms, limit, task prefix, queue prefix, wake channel. Moves due ids into
     * their queues. Returns {moved count, next due ms or -1}.
     */
    static final String PROMOTE =
            """
            local due = redis.call('ZRANGE', KEYS[1], 0, ARGV[1], 'BYSCORE', 'LIMIT', 0, ARGV[2])
            local moved = 0
            for _, id in ipairs(due) do
              local tkey = ARGV[3] .. id
              local queue = redis.call('HGET', tkey, 'queue')
              if queue then
                redis.call('LPUSH', ARGV[4] .. queue, id)
                redis.call('HDEL', tkey, 'due')
                redis.call('PUBLISH', ARGV[5], queue)
                moved = moved + 1
              end
              redis.call('ZREM', KEYS[1], id)
            end
            local head = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
            local next = -1
            if head[2] then next = tonumber(head[2]) end
            return {moved, next}
            """;

    /**
     * KEYS: leased zset. ARGV: now ms, limit, task prefix. Returns expired leases as {id, token, attempts,
     * max, event} entries. Entries without a hash are removed.
     */
    static final String RECLAIM =
            """
            local expired = redis.call('ZRANGE', KEYS[1], 0, ARGV[1], 'BYSCORE', 'LIMIT', 0, ARGV[2])
            local result = {}
            for _, id in ipairs(expired) do
              local tkey = ARGV[3] .. id
              local v = redis.call('HMGET', tkey, 'token', 'attempts', 'max', 'event')
              if v[1] then
                result[#result + 1] = {id, v[1], v[2], v[3], v[4]}
              else
                redis.call('ZREM', KEYS[1], id)
              end
            end
            return result
            """;

    /**
     * KEYS: cron zset. ARGV: schedule id, cron prefix, next fire ms, then field/value pairs including the
     * 'cron' expression. Keeps the current fire time when the expression is unchanged. Returns the revision.
     */
    static final String CRON_PUT =
            """
            local ckey = ARGV[2] .. ARGV[1]
            local current = redis.call('HGET', ckey, 'cron')
            local fields = {}
            for i = 4, #ARGV do fields[#fields + 1] = ARGV[i] end
            redis.call('HSET', ckey, unpack(fields))
            local rev = redis.call('HINCRBY', ckey, 'rev', 1)
            local newCron = redis.call('HGET', ckey, 'cron')
            if current == newCron then
              redis.call('ZADD', KEYS[1], 'NX', ARGV[3], ARGV[1])
            else
              redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
            end
            return rev
            """;

    /** KEYS: cron zset. ARGV: schedule id, cron prefix. Returns 1 when a definition was removed. */
    static final String CRON_DELETE =
            """
            redis.call('ZREM', KEYS[1], ARGV[1])
            return redis.call('UNLINK', ARGV[2] .. ARGV[1])
            """;

    /**
     * KEYS: cron zset. ARGV: up to ms, limit, cron prefix. Returns definitions due up to the given time as
     * {schedule id, fire at, flattened hash} entries.
     */
    static final String CRON_DUE =
            """
            local due = redis.call('ZRANGE', KEYS[1], 0, ARGV[1], 'BYSCORE', 'WITHSCORES', 'LIMIT', 0, ARGV[2])
            local result = {}
            for i = 1, #due, 2 do
              local entry = {due[i], due[i + 1]}
              local hash = redis.call('HGETALL', ARGV[3] .. due[i])
              for _, v in ipairs(hash) do entry[#entry + 1] = v end
              result[#result + 1] = entry
            end
            return result
            """;

    /**
     * KEYS: cron zset, scheduled zset. ARGV: schedule id, revision, fire at ms, next fire ms, occurrence id,
     * cron prefix, task prefix, then field/value pairs of the occurrence. Creates the occurrence in the
     * scheduled set and advances the schedule, unless the definition changed or was deleted meanwhile.
     * Returns 1 when committed, 0 otherwise.
     */
    static final String CRON_COMMIT =
            """
            local ckey = ARGV[6] .. ARGV[1]
            if redis.call('HGET', ckey, 'rev') ~= ARGV[2] then return 0 end
            local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
            if not score or tonumber(score) ~= tonumber(ARGV[3]) then return 0 end
            local tkey = ARGV[7] .. ARGV[5]
            if redis.call('EXISTS', tkey) == 0 then
              local fields = {}
              for i = 8, #ARGV do fields[#fields + 1] = ARGV[i] end
              redis.call('HSET', tkey, unpack(fields))
              redis.call('HSET', tkey, 'due', ARGV[3])
              redis.call('ZADD', KEYS[2], ARGV[3], ARGV[5])
            end
            redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
            return 1
            """;

    /** KEYS: dead zset. ARGV: before ms, limit, task prefix. Removes dead entries older than the time. */
    static final String DEAD_PURGE =
            """
            local old = redis.call('ZRANGE', KEYS[1], 0, ARGV[1], 'BYSCORE', 'LIMIT', 0, ARGV[2])
            for _, id in ipairs(old) do
              redis.call('UNLINK', ARGV[3] .. id)
              redis.call('ZREM', KEYS[1], id)
            end
            return #old
            """;

    /**
     * KEYS: dead zset. ARGV: task prefix, queue prefix, wake channel, id. Puts a dead task back into its queue
     * with a fresh attempt count. Returns 1, or 0 when the task is not dead or its hash expired.
     */
    static final String RETRY_DEAD =
            """
            local tkey = ARGV[1] .. ARGV[4]
            if redis.call('ZREM', KEYS[1], ARGV[4]) == 0 then return 0 end
            local queue = redis.call('HGET', tkey, 'queue')
            if not queue then return 0 end
            redis.call('PERSIST', tkey)
            redis.call('HSET', tkey, 'attempts', 0)
            redis.call('HDEL', tkey, 'died', 'error')
            redis.call('LPUSH', ARGV[2] .. queue, ARGV[4])
            redis.call('PUBLISH', ARGV[3], queue)
            return 1
            """;
}
