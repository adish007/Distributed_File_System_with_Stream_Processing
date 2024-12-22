package cache

type CachedContent struct {
	Filename string
	Content  []byte
}

type Cache struct {
	Queue []CachedContent
	K     int
}

func (c *Cache) AddToCache(filename string, content []byte) {
	item := CachedContent{Filename: filename, Content: content}
	c.Queue = append(c.Queue, item)
	if len(c.Queue) > c.K {
		c.Queue = c.Queue[1:]
	}
}

func (c *Cache) RetrieveFromCache(filename string) []byte {
	for _, cc := range c.Queue {
		if cc.Filename == filename {
			return cc.Content
		}
	}
	return nil
}

func NewCache(k int) Cache {
	return Cache{Queue: []CachedContent{}, K: k}
}
