package segmentfile

import (
	"os"
	"sync"
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
	file      *os.File
	path      string //for Delete() to find file
	mu        sync.RWMutex
	mmapData  []byte
	isMMapped bool
}

func NewSegmentFileManager(path string) (SegmentFileManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &localFileManager{
		file: f,
		path: path, // Initialize the path
	}, nil
}

// adds data to the end of the file,returns location
func (s *localFileManager) Append(data []byte) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	       // Close mmap before writing to invalidate cache
	       if s.isMMapped && s.mmapData != nil {
		       s.releaseMmapLinux()
	       }

	//current size to know the start offset
	stat, err := s.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	offset := stat.Size()
	length := int64(len(data))

	//write the data to the end
	_, err = s.file.Write(data)
	if err != nil {
		return 0, 0, err
	}

	return offset, length, nil
}

// retrieves a part of data without reading the whole file
func (s *localFileManager) ReadAt(offset int64, length int64) ([]byte, error) {
	buf := make([]byte, length)
	_, err := s.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// GetMmapData returns memory-mapped data for efficient zero-copy access
func (s *localFileManager) GetMmapData() ([]byte, error) {
	s.mu.RLock()
	if s.isMMapped && s.mmapData != nil {
		defer s.mu.RUnlock()
		return s.mmapData, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
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

	data, err := syscall.Mmap(int(s.file.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	s.mmapData = data
	s.isMMapped = true

	return s.mmapData, nil
}

// ReleaseMmap closes the mmap
func (s *localFileManager) ReleaseMmap() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	       if s.isMMapped && s.mmapData != nil {
		       return s.releaseMmapLinux()
	       }
	       return nil
}

// marks segment for removal
func (s *localFileManager) Delete(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	       if s.isMMapped && s.mmapData != nil {
		       s.releaseMmapLinux()
	       }
// Helper method for proper mmap cleanup
func (s *localFileManager) releaseMmapLinux() error {
       if s.mmapData == nil {
	       s.isMMapped = false
	       return nil
       }
       err := syscall.Munmap(s.mmapData)
       s.isMMapped = false
       s.mmapData = nil
       return err
}

	if err := s.file.Close(); err != nil {
		return err
	}

	return os.Remove(s.path)
}
