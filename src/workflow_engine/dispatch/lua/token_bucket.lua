-- Token bucket, refilled continuously. Atomic because Redis runs a script start to finish.
-- KEYS[1]  bucket hash (fields: tokens, ts)
-- ARGV[1]  refill rate, tokens per second
-- ARGV[2]  burst (bucket capacity)
-- ARGV[3]  cost of this request
-- Returns {allowed (1|0), retry_after_seconds (string), tokens_left (string)}.
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

-- Server time, so every client shares one clock.
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end

tokens = math.min(burst, tokens + math.max(0, now - ts) * rate)

local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = (cost - tokens) / rate
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
-- A full refill takes burst / rate seconds; after that the key carries no information.
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, tostring(retry_after), tostring(tokens)}
