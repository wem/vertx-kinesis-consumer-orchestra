local setKey = KEYS[1]
local setValue = ARGV[1]
local keyPattern = ARGV[2]
redis.call("SET", setKey, setValue)

local readyParents = redis.call("KEYS", keyPattern)

return table.getn(readyParents)
