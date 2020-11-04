return redis.call('DEL', unpack(redis.call('KEYS', ARGV[1])))
