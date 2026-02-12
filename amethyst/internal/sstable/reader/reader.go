package reader

import (
	"amethyst/internal/common"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"bytes"
	"encoding/binary"
	"fmt"
)

type SSTableReader interface {
	Get(meta *common.SegmentMeta, key string) ([]byte, bool)
	Scan(meta *common.SegmentMeta) (map[string][]byte, error)
}

type Reader struct {
	fileMgr segmentfile.SegmentFileManager
}

func NewReader(fileMgr segmentfile.SegmentFileManager) *Reader {
	return &Reader{fileMgr: fileMgr}
}

func (r *Reader) Get(meta *common.SegmentMeta, target string) ([]byte, bool) {
	//validate segment is not obsolete
	if meta == nil || meta.IsObsolete() {
		return nil, false
	}

	// Fast reject by key range
	if target < meta.MinKey || target > meta.MaxKey {
		return nil, false
	}

	idx, ok := meta.SparseIndex.(*sparseindex.SparseIndex)
	if !ok || idx == nil {
		return nil, false
	}

	// Get mmapped data
	mmapData, err := r.fileMgr.GetMmapData()
	if err != nil {
		return nil, false
	}

	//validate segment bounds b4 computing offsets
	if meta.Offset < 0 || meta.Length <= 0 {
		return nil, false
	}

	if meta.Offset+meta.Length > int64(len(mmapData)) {
		return nil, false
	}

	// Compute absolute start offset
	start := meta.Offset + meta.DataStartOffset + idx.Seek(target)
	end := meta.Offset + meta.SparseIndexOffset

	//validate computed bounds
	if start < 0 || end > int64(len(mmapData)) || start > end {
		return nil, false
	}

	// ensure we're within segment bounds
	if start < meta.Offset || end > meta.Offset+meta.Length {
		return nil, false
	}

	// Use direct slice from mmap - zero copy!
	data := mmapData[start:end]
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		var kLen uint32
		var vLen uint32
		var tomb byte

		if err := binary.Read(buf, binary.BigEndian, &kLen); err != nil {
			return nil, false
		}
		if err := binary.Read(buf, binary.BigEndian, &vLen); err != nil {
			return nil, false
		}
		if err := binary.Read(buf, binary.BigEndian, &tomb); err != nil {
			return nil, false
		}

		//validate lengths to prevent huge allocations
		if kLen > 1024*1024 || vLen > 10*1024*1024 {
			return nil, false
		}

		keyBytes := make([]byte, kLen)
		if _, err := buf.Read(keyBytes); err != nil {
			return nil, false
		}
		key := string(keyBytes)

		var valBytes []byte
		if vLen > 0 {
			valBytes = make([]byte, vLen)
			if _, err := buf.Read(valBytes); err != nil {
				return nil, false
			}
		}

		if key == target {
			if tomb == 1 {
				return nil, false
			}
			return valBytes, true
		}

		// Sorted order invariant: stop early
		if key > target {
			return nil, false
		}
	}

	return nil, false
}

func (r *Reader) Scan(meta *common.SegmentMeta) (map[string][]byte, error) {
	//validate segment before scanning
	if meta == nil {
		return nil, fmt.Errorf("nil segment metadata")
	}

	if meta.IsObsolete() {
		return nil, fmt.Errorf("cannot scan obsolete segment %s", meta.ID)
	}

	if meta.Offset < 0 || meta.Length <= 0 {
		return nil, fmt.Errorf("invalid segment bounds: offset=%d, length=%d", meta.Offset, meta.Length)
	}

	result := make(map[string][]byte)

	// Get mmapped data
	mmapData, err := r.fileMgr.GetMmapData()
	if err != nil {
		return nil, fmt.Errorf("failed to get mmap data: %w", err)
	}

	//validate segment is within file bounds
	if meta.Offset+meta.Length > int64(len(mmapData)) {
		return nil, fmt.Errorf("segment exceeds file bounds: offset=%d, length=%d, fileSize=%d",
			meta.Offset, meta.Length, len(mmapData))
	}

	start := meta.Offset + meta.DataStartOffset
	end := meta.Offset + meta.SparseIndexOffset

	//check bounds
	if start < 0 || end > int64(len(mmapData)) || start > end {
		return nil, fmt.Errorf("invalid data bounds: start=%d, end=%d, mmapSize=%d",
			start, end, len(mmapData))
	}

	//ensure we're in segment
	if start < meta.Offset || end > meta.Offset+meta.Length {
		return nil, fmt.Errorf("data range outside segment: start=%d, end=%d, segStart=%d, segEnd=%d",
			start, end, meta.Offset, meta.Offset+meta.Length)
	}

	//make a copy of the data to prevent use-after-modification
	//protects against the file being modified during scan
	dataLen := end - start
	if dataLen > 100*1024*1024 { //100MB max, change if doesnt make much sense
		return nil, fmt.Errorf("data too large: %d bytes", dataLen)
	}

	//re-validate before copy
	//catches the race where file grew between first check and now
	mmapData2, err := r.fileMgr.GetMmapData()
	if err != nil {
		return nil, fmt.Errorf("failed to re-fetch mmap data: %w", err)
	}
	//If file grew, use new data. If shrank, fail.
	if len(mmapData2) < len(mmapData) {
		return nil, fmt.Errorf("file shrank during scan")
	}
	if end > int64(len(mmapData2)) {
		return nil, fmt.Errorf("segment data out of bounds after concurrent write")
	}

	//now safe to copy using the new map
	dataCopy := make([]byte, dataLen)
	copy(dataCopy, mmapData2[start:end])

	buf := bytes.NewReader(dataCopy)

	for buf.Len() > 0 {
		var kLen, vLen uint32
		var tomb byte

		if err := binary.Read(buf, binary.BigEndian, &kLen); err != nil {
			break
		}
		if err := binary.Read(buf, binary.BigEndian, &vLen); err != nil {
			break
		}
		if err := binary.Read(buf, binary.BigEndian, &tomb); err != nil {
			break
		}

		//validate lengths in case of corrupted data
		if kLen > 1024*1024 || vLen > 10*1024*1024 {
			return nil, fmt.Errorf("invalid key/value length: kLen=%d, vLen=%d", kLen, vLen)
		}

		keyBytes := make([]byte, kLen)
		if _, err := buf.Read(keyBytes); err != nil {
			break
		}
		key := string(keyBytes)

		var valBytes []byte
		if vLen > 0 {
			valBytes = make([]byte, vLen)
			if _, err := buf.Read(valBytes); err != nil {
				break
			}
		}

		if tomb == 1 {
			result[key] = nil
		} else {
			result[key] = valBytes
		}
	}
	return result, nil
}
