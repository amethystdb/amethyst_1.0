package adaptive

import (
	"amethyst/internal/common"
	"fmt"
	"sync"
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
	mu             sync.RWMutex //protect concurrent map access
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

	// Lock for reading history
	c.mu.RLock()
	history := c.segmentHistory[meta.ID]
	c.mu.RUnlock()

	if len(history) < 3 {
		return false, meta.Strategy, ""
	}

	// Check switch cooldown with lock
	c.mu.RLock()
	timeSinceSwitch := time.Since(c.lastSwitch)
	c.mu.RUnlock()

	if timeSinceSwitch < MinSwitchInterval {
		return false, meta.Strategy, ""
	}

	readTrend := c.calculateTrend(history, "read")
	writeTrend := c.calculateTrend(history, "write")

	switch meta.Strategy {
	case common.TIERED:
		if readTrend > 0.3 && meta.GetReadCount() > 500 {
			c.mu.Lock()
			c.lastSwitch = time.Now()
			c.mu.Unlock()
			return true, common.LEVELED, fmt.Sprintf(
				"sustained_read_trend=%.1f%%, rc=%d (tiered→leveled)",
				readTrend*100, meta.GetReadCount(),
			)
		}
	case common.LEVELED:
		if writeTrend > 0.3 && meta.GetWriteCount() > 10 {
			c.mu.Lock()
			c.lastSwitch = time.Now()
			c.mu.Unlock()
			return true, common.TIERED, fmt.Sprintf(
				"sustained_write_trend=%.1f%%, wc=%d (leveled→tiered)",
				writeTrend*100, meta.GetWriteCount(),
			)
		}
	}

	return false, meta.Strategy, ""
}

func (c *FSMController) updateSegmentHistory(meta *common.SegmentMeta) {
	snapshot := MetricSnapshot{
		Timestamp:  time.Now().Unix(),
		ReadCount:  meta.GetReadCount(),
		WriteCount: meta.GetWriteCount(),
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	history := c.segmentHistory[meta.ID]
	history = append(history, snapshot)

	if len(history) > WindowSize {
		history = history[len(history)-WindowSize:]
	}

	c.segmentHistory[meta.ID] = history
}

func (c *FSMController) calculateTrend(history []MetricSnapshot, metric string) float64 {
        if len(history) == 0 {
                return 0
        }

        alpha := 0.2

        var ema float64

        if metric == "read" {
                ema = float64(history[0].ReadCount)
        } else {
                ema = float64(history[0].WriteCount)
        }

        var latest float64
        for i := 1; i < len(history); i++ {
                var value float64
                if metric == "read" {
                        value = float64(history[i].ReadCount)
                } else {
                        value = float64(history[i].WriteCount)
                }

                ema = alpha*value + (1-alpha)*ema
                latest = value
        }

        if latest == 0 {
                return 0
        }
        trend := (latest - ema) / latest
        if trend < 0 {
                return 0
        }
        return trend
}

