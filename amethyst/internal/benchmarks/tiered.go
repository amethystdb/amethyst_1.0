package benchmarks

import (
	"amethyst/internal/common"
	"time"
)

type StaticTieredController struct{}

func NewTieredController() *StaticTieredController {
	return &StaticTieredController{}
}

func (c *StaticTieredController) ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	//current time to compare to last rewrite
	now := time.Now().Unix()

	//to prevent thrashing, returns false if touched less than a second ago
	if !meta.CooldownExpired(now, 1) {
		return false, common.TIERED, ""
	}

	//threshold of 50 here for merge can change
	if meta.GetWriteCount() > 50 {
		//returns true for rewrite, specifies tired
		return true, common.TIERED, "Baseline: Static Tiered merge"
	}

	//nothing done if no conditions met
	return false, common.TIERED, ""
}
