-- Small piece of RED lock
if redis.call("GET", KEYS[1]) == "1" then
    return "NOK"
else
    return redis.call("SET", KEYS[1], "1", "PX", ARGV[1])
end
