package engine

import (
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/metadata"
	"testing"
)

// --- MOCKS TO FIX COMPILER ERRORS ---

type MockController struct {
	NextStrategy common.CompactionType
}

func (m *MockController) ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	return true, m.NextStrategy, "Workshop Test"
}

type MockWriter struct{}

func (m *MockWriter) WriteSegment(data []common.KVEntry, strat common.CompactionType) (*common.SegmentMeta, error) {
	return &common.SegmentMeta{ID: "new-seg", Strategy: strat}, nil
}

type MockReader struct{}

func (m *MockReader) Get(meta *common.SegmentMeta, key string) ([]byte, bool) { return nil, false }
func (m *MockReader) Scan(meta *common.SegmentMeta) (map[string][]byte, error) {
	return map[string][]byte{"key": []byte("val")}, nil
}

// --- THE ACTUAL TEST ---

func TestFinal_EightToOneMerge(t *testing.T) {
	meta := metadata.NewTracker()
	// ctrl := &MockController{NextStrategy: common.LEVELED}
	director := compaction.NewDirector(meta, nil)

	executor := compaction.NewExecutor(meta, &MockReader{}, &MockWriter{})

	// Create 8 overlapping segments
	for i := 0; i < 8; i++ {
		meta.RegisterSegment(&common.SegmentMeta{
			ID:     string(rune(i)),
			MinKey: "a",
			MaxKey: "z",
		})
	}

	// 1. Check if Director finds all 8
	plan := director.MaybePlan()
	if plan == nil || len(plan.Inputs) != 8 {
		t.Fatalf("Director failed to find overlaps. Expected 8, got %d", len(plan.Inputs))
	}

	// 2. Execute Merge
	_, err := executor.Execute(plan)
	if err != nil {
		t.Fatalf("Executor failed: %v", err)
	}

	// 3. Final Check: Should only have 1 active segment left
	finalSegs := meta.GetAllSegments()
	if len(finalSegs) != 1 {
		t.Errorf("FAIL: Expected 1 segment, got %d", len(finalSegs))
	} else {
		t.Log("SUCCESS: 8 segments successfully merged into 1 leveled segment!")
	}
}
