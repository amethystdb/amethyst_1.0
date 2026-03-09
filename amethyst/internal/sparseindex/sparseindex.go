package sparseindex

import (
	"sort"
	"sync/atomic"
)

const DefaultStride = 16

type SparseIndex struct {
	Keys    []string
	Offsets []int64
}

type Builder interface {
	Build(keys []string, offsets []int64) *SparseIndex
}

type builder struct {
	stride int
}

func NewBuilder(stride int) Builder {
	if stride <= 0 {
		stride = DefaultStride
	}
	return &builder{stride: stride}
}

//sampling N-th key to construct sparse index

func (b *builder) Build(sortedKeys []string, offsets []int64) *SparseIndex {
	if len(sortedKeys) == 0 {
		return &SparseIndex{}
	}
	keys := make([]string, 0, (len(sortedKeys)/b.stride + 1))
	offs := make([]int64, 0, (len(sortedKeys)/b.stride + 1))

	for i := 0; i < len(sortedKeys); i += b.stride {
		keys = append(keys, sortedKeys[i])
		offs = append(offs, offsets[i])

	}
	return &SparseIndex{
		Keys:    keys,
		Offsets: offs,
	}
}

//seek function to find largest indexed key, for given object <=target
//if target is smaller than all indexed keys it returns 0 (keep in mind)

func (s *SparseIndex) Seek(target string, counter *int64) int64 {
	if len(s.Keys) == 0 {
		return 0
	}
 	i := sort.Search(len(s.Keys), func(i int) bool {
 		if counter != nil {
 			atomic.AddInt64(counter, 1) // Each step of binary search is 1 comparison
 		}
 		return s.Keys[i] > target
 	})
	if i == 0 {
		return 0
	}
	return s.Offsets[i-1]
}
