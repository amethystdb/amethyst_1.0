package adaptive

import (
	"amethyst/internal/common"
	"fmt"
	"time"
)

const (
	MinSegmentSize     = int64(4 * 1024)
	MinRewriteInterval = int64(2)        // 2 seconds — fast for benchmark
	MinSwitchInterval  = 2 * time.Second // 2 seconds between strategy switches
	WindowSize         = 5
)

type MetricSnapshot struct {
	Timestamp  int64
	ReadCount  int64
	WriteCount int64
}

type Controller interface {
	ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string)
}

type FSMController struct {
	segmentHistory map[string][]MetricSnapshot
	lastSwitch     time.Time
}

func NewFSMController() Controller {
	return &FSMController{
		segmentHistory: make(map[string][]MetricSnapshot),
		lastSwitch:     time.Now(),
	}
}

func (c *FSMController) ShouldRewrite(meta *common.SegmentMeta) (bool, common.CompactionType, string) {
	now := time.Now().Unix()

	if !meta.CooldownExpired(now, MinRewriteInterval) {
		return false, meta.Strategy, ""
	}

	if meta.Size() < MinSegmentSize {
		return false, meta.Strategy, ""
	}

	c.updateSegmentHistory(meta)
	history := c.segmentHistory[meta.ID]

	if len(history) < 3 {
		return false, meta.Strategy, ""
	}

	if time.Since(c.lastSwitch) < MinSwitchInterval {
		return false, meta.Strategy, ""
	}

	readTrend := c.calculateTrend(history, "read")
	writeTrend := c.calculateTrend(history, "write")

	switch meta.Strategy {
	case common.TIERED:
		if readTrend > 0.3 && meta.ReadCount > 50 {
			c.lastSwitch = time.Now()
			return true, common.LEVELED, fmt.Sprintf(
				"sustained_read_trend=%.1f%%, rc=%d (tiered→leveled)",
				readTrend*100, meta.ReadCount,
			)
		}

	case common.LEVELED:
		if writeTrend > 0.3 && meta.WriteCount > 10 {
			c.lastSwitch = time.Now()
			return true, common.TIERED, fmt.Sprintf(
				"sustained_write_trend=%.1f%%, wc=%d (leveled→tiered)",
				writeTrend*100, meta.WriteCount,
			)
		}
	}

	return false, meta.Strategy, ""
}

func (c *FSMController) updateSegmentHistory(meta *common.SegmentMeta) {
	snapshot := MetricSnapshot{
		Timestamp:  time.Now().Unix(),
		ReadCount:  meta.ReadCount,
		WriteCount: meta.WriteCount,
	}

	history := c.segmentHistory[meta.ID]
	history = append(history, snapshot)

	if len(history) > WindowSize {
		history = history[len(history)-WindowSize:]
	}

	c.segmentHistory[meta.ID] = history
}

func (c *FSMController) calculateTrend(history []MetricSnapshot, metric string) float64 {
	if len(history) < 2 {
		return 0
	}

	mid := len(history) / 2
	var oldSum, newSum int64

	for i := 0; i < mid; i++ {
		if metric == "read" {
			oldSum += history[i].ReadCount
		} else {
			oldSum += history[i].WriteCount
		}
	}

	for i := mid; i < len(history); i++ {
		if metric == "read" {
			newSum += history[i].ReadCount
		} else {
			newSum += history[i].WriteCount
		}
	}

	oldAvg := float64(oldSum) / float64(mid)
	newAvg := float64(newSum) / float64(len(history)-mid)

	if oldAvg == 0 {
		if newAvg > 0 {
			return 1.0
		}
		return 0
	}

	return (newAvg - oldAvg) / oldAvg
}
