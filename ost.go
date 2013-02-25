package ost

import "github.com/garyburd/redigo/redis"

// Ost iterators have to be of type action, and the return value has
// to express whether destination queue should be discarded.
type action func(item string) (discard bool)

// Apart from the Redis connection and the action, two queues must be
// supplied: the source and the destination. This allows the caller to
// implement two very common usage patterns: circular lists (where
// source and destination are the same) and backups (where each worker
// has its own backup queue). Wheter or not the item is discarded
// depends on which usage pattern is implemented: for circular lists,
// the destination queue is never discarded, whereas for backup queues
// it is mandatory to cleanup the destination queue.
func Each(c redis.Conn, src, dst string, process action) {
	for {
		item, err := redis.String(c.Do("BRPOPLPUSH", src, dst, "5"))

		if err != nil {
			print("ost: ")
			println(err.Error())
			continue
		}

		if discard := process(item); discard {
			print("ost: discarding item ")
			println(item)
			c.Do("DEL", dst)
		}
	}

	println("ost: stopping")
}
