package segmentfile

import (
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

type SegmentFileManager interface {
	Append(data []byte) (offset int64, length int64, err error)
	ReadAt(offset int64, length int64) ([]byte, error)
	Delete(offset int64) error
	GetMmapData() ([]byte, error)
	ReleaseMmap() error
}

type localFileManager struct {
	file         *os.File
	path         string
	mu           sync.RWMutex
	mmapData     []byte
	mmappedSize  int64  // Track current mmap size
	isMMapped    bool
	activeReads  int32  // Reference count for active readers
}

func NewSegmentFileManager(path string) (SegmentFileManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &localFileManager{
		file: f,
		path: path,
	}, nil
}

func (s *localFileManager) Append(data []byte) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// DON'T unmap on every write - let it stay mapped
	// The OS will make writes visible through MAP_SHARED

	stat, err := s.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	offset := stat.Size()
	length := int64(len(data))

	_, err = s.file.Write(data)
	if err != nil {
		return 0, 0, err
	}

	// Force sync to ensure data is on disk
	if err := s.file.Sync(); err != nil {
		return 0, 0, err
	}

	return offset, length, nil
}

func (s *localFileManager) ReadAt(offset int64, length int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	buf := make([]byte, length)
	_, err := s.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *localFileManager) GetMmapData() ([]byte, error) {
	// Increment reader count
	atomic.AddInt32(&s.activeReads, 1)
	defer atomic.AddInt32(&s.activeReads, -1)

	s.mu.RLock()
	if s.isMMapped && s.mmapData != nil {
		// Check if file grew beyond current mapping
		stat, err := s.file.Stat()
		if err != nil {
			s.mu.RUnlock()
			return nil, err
		}
		
		// If file didn't grow much, reuse existing mmap
		if stat.Size() <= s.mmappedSize {
			defer s.mu.RUnlock()
			return s.mmapData, nil
		}
		s.mu.RUnlock()
		
		// Need to remap - upgrade to write lock
		s.mu.Lock()
		defer s.mu.Unlock()
		
		// Double-check after acquiring write lock
		if stat.Size() <= s.mmappedSize {
			return s.mmapData, nil
		}
		
		// Unmap old mapping
		if s.mmapData != nil {
			syscall.Munmap(s.mmapData)
		}
		
		// Remap with new size
		data, err := syscall.Mmap(int(s.file.Fd()), 0, int(stat.Size()), 
			syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			s.isMMapped = false
			return nil, err
		}
		
		s.mmapData = data
		s.mmappedSize = stat.Size()
		s.isMMapped = true
		return s.mmapData, nil
	}
	s.mu.RUnlock()

	// First time mapping
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check
	if s.isMMapped && s.mmapData != nil {
		return s.mmapData, nil
	}

	stat, err := s.file.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() == 0 {
		return []byte{}, nil
	}

	data, err := syscall.Mmap(int(s.file.Fd()), 0, int(stat.Size()), 
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	
	s.mmapData = data
	s.mmappedSize = stat.Size()
	s.isMMapped = true

	return s.mmapData, nil
}

func (s *localFileManager) ReleaseMmap() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Wait for active readers
	for atomic.LoadInt32(&s.activeReads) > 0 {
		s.mu.Unlock()
		// Brief sleep to let readers finish
		// time.Sleep(10 * time.Millisecond)
		s.mu.Lock()
	}

	if s.isMMapped && s.mmapData != nil {
		err := syscall.Munmap(s.mmapData)
		s.isMMapped = false
		s.mmapData = nil
		s.mmappedSize = 0
		return err
	}
	return nil
}

func (s *localFileManager) Delete(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isMMapped && s.mmapData != nil {
		syscall.Munmap(s.mmapData)
		s.isMMapped = false
		s.mmapData = nil
		s.mmappedSize = 0
	}

	if err := s.file.Close(); err != nil {
		return err
	}

	return os.Remove(s.path)
}