package memtable

import (
	"amethyst/internal/common"
	"sort" //sort keys in Flush
	"sync"
)

type Memtable interface {
	Put(key string, value []byte)
	Delete(key string)
	Get(key string) ([]byte, bool)

	ShouldFlush() bool
	Flush() []common.KVEntry
}

type memtable struct {
	data       []common.KVEntry //sorted order
	maxEntries int
	mu         sync.RWMutex
}

func NewMemtable(maxEntries int) Memtable {
	return &memtable{
		data:       make([]common.KVEntry, 0),
		maxEntries: maxEntries,
	}
}

func (m *memtable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	//binary search to find the correct insertion point
	i := sort.Search(len(m.data), func(i int) bool { return m.data[i].Key >= key })
	if i < len(m.data) && m.data[i].Key == key {
		m.data[i] = common.KVEntry{Key: key, Value: value, Tombstone: false}
	} else {
		// Insert while maintaining sort order
		m.data = append(m.data, common.KVEntry{})
		copy(m.data[i+1:], m.data[i:])
		m.data[i] = common.KVEntry{Key: key, Value: value, Tombstone: false}
	}
}

func (m *memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Binary search to find the correct insertion point
	i := sort.Search(len(m.data), func(i int) bool { return m.data[i].Key >= key })

	if i < len(m.data) && m.data[i].Key == key {
		m.data[i] = common.KVEntry{Key: key, Value: nil, Tombstone: true}
	} else {
		// Insert tombstone while maintaining sort order
		m.data = append(m.data, common.KVEntry{})
		copy(m.data[i+1:], m.data[i:])
		m.data[i] = common.KVEntry{Key: key, Value: nil, Tombstone: true}
	}
}

func (m *memtable) Get(key string) ([]byte, bool) {
	m.mu.RLock() // Request shared read access
	defer m.mu.RUnlock()

	i := sort.Search(len(m.data), func(i int) bool { return m.data[i].Key >= key })
	if i < len(m.data) && m.data[i].Key == key && !m.data[i].Tombstone {
		return m.data[i].Value, true
	}
	return nil, false
}

// returns true if mem is full
func (m *memtable) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data) >= m.maxEntries
}

// clears data and returns sorted for SSTable Writer
func (m *memtable) Flush() []common.KVEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If there's no data, return nil so main.go skips the write
	if len(m.data) == 0 {
		return nil //
	}

	oldData := m.data
	m.data = make([]common.KVEntry, 0)
	return oldData
}
