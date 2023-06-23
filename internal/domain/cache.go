package domain

import "sync"

type Cache interface {
	Get(key string) bool
	LoadOrStore(key string, value bool) bool
}

type cache struct {
	dataMap sync.Map
}

func NewCache() Cache {
	return &cache{}
}

func (c *cache) Get(key string) bool {
	// Perform a read operation on the map
	if val, ok := c.dataMap.Load(key); ok {
		return val.(bool)
	}
	return false
}

func (c *cache) LoadOrStore(key string, value bool) bool {
	_, loaded := c.dataMap.LoadOrStore(key, value)
	return loaded
}
