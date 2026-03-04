package compaction

import (
	"amethyst/internal/adaptive"
	"amethyst/internal/common"
	"amethyst/internal/metadata"
)

type Plan struct {
	Inputs         []*common.SegmentMeta
	OutputStrategy common.CompactionType
	Reason         string
	TargetLevel    int   // <-- ADDED
}

type Director interface {
	MaybePlan() *Plan
	UpdateMetrics(readAmp, writeAmp float64)
}

type director struct {
	meta          metadata.Tracker
	fsm           adaptive.Controller
	currentPolicy common.CompactionType
}

func NewDirector(meta metadata.Tracker, fsm adaptive.Controller) Director {
	return &director{
		meta:          meta,
		fsm:           fsm,
		currentPolicy: common.TIERED,
	}
}

// UpdateMetrics is kept for interface compatibility but no longer drives FSM decisions
func (d *director) UpdateMetrics(readAmp, writeAmp float64) {
	// No-op: FSM now uses per-segment ReadCount/WriteCount
}

func (d *director) MaybePlan() *Plan {
	segments := d.meta.GetAllSegments()

	if len(segments) == 0 {
		return nil
	}

	// Check each segment: ask FSM if it should be rewritten
	if d.fsm != nil {
		for _, seg := range segments {
			if seg.IsObsolete() {
				continue
			}

			shouldSwitch, newStrategy, reason := d.fsm.ShouldRewrite(seg)

			if shouldSwitch {
				d.currentPolicy = newStrategy

				var inputs []*common.SegmentMeta
				targetLevel := seg.Level

				if newStrategy == common.LEVELED {
					inputs = d.collectLimitedOverlaps(seg, 50)
					targetLevel = seg.Level + 1 // Push down on Leveled transition
				} else {
					inputs = []*common.SegmentMeta{seg}
				}
				return &Plan{
					Inputs:         inputs,
					OutputStrategy: newStrategy,
					Reason:         reason,
					TargetLevel:    targetLevel, // <-- ADDED
				        // Only pull in overlaps from the target's level OR the level below it.
				        if !seen[overlap.ID] && !overlap.IsObsolete() && (overlap.Level == target.Level || overlap.Level == target.Level+1) {
				            inputs = append(inputs, overlap)
				            seen[overlap.ID] = true
				            if len(inputs) >= maxSegments {
				                break
				            }
				        }
	case common.TIERED:
		if plan := d.planTieredCompaction(segments); plan != nil {
			return plan
		}
		// Also try leveled if tiered finds nothing (e.g. many overlapping segments)
		return d.planLeveledCompaction(segments)
	case common.LEVELED:
		return d.planLeveledCompaction(segments)
	}

	return nil
}

// planTieredCompaction selects similar-sized segments to merge
func (d *director) planTieredCompaction(segments []*common.SegmentMeta) *Plan {
	for i := 0; i < len(segments)-1; i++ {
		seg := segments[i]
		if seg.IsObsolete() {
			continue
		}

		for j := i + 1; j < len(segments); j++ {
			candidate := segments[j]
			if candidate.IsObsolete() {
				continue
			}
            // Only merge Tiered runs if they are on the same level!
            if seg.Level != candidate.Level {
                continue
            }

			sizeRatio := float64(seg.Length) / float64(candidate.Length)
			if sizeRatio < 2.0 && sizeRatio > 0.5 {
				return &Plan{
					Inputs:         []*common.SegmentMeta{seg, candidate},
					OutputStrategy: common.TIERED,
					Reason:         "tiered: merging similar-sized sorted runs",
					TargetLevel:    segments[i].Level, // Tiered stays at same level
				}
			}
		}
	}

	return nil
}

// planLeveledCompaction selects overlapping segments to merge
func (d *director) planLeveledCompaction(segments []*common.SegmentMeta) *Plan {
	if len(segments) == 0 {
		return nil
	}

	for _, seg := range segments {
		if seg.IsObsolete() {
			continue
		}

		overlaps := d.meta.GetOverlappingSegments(seg)
		if len(overlaps) > 0 {
			inputs := d.collectLimitedOverlaps(seg, 50)
			return &Plan{
				Inputs:         inputs,
				OutputStrategy: common.LEVELED,
				Reason:         "leveled: merging overlapping ranges",
				TargetLevel:    seg.Level + 1, // Leveled pushes down
			}
		}
	}

	return nil
}

// collectLimitedOverlaps prevents massive compactions by limiting segment count
func (d *director) collectLimitedOverlaps(target *common.SegmentMeta, maxSegments int) []*common.SegmentMeta {
	inputs := []*common.SegmentMeta{target}
	seen := make(map[string]bool)
	seen[target.ID] = true

	overlaps := d.meta.GetOverlappingSegments(target)

	for _, overlap := range overlaps {
		if !seen[overlap.ID] && !overlap.IsObsolete() {
			inputs = append(inputs, overlap)
			seen[overlap.ID] = true

			if len(inputs) >= maxSegments {
				break
			}
		}
	}

	return inputs
}

// GetCurrentPolicy returns the active compaction strategy
func (d *director) GetCurrentPolicy() common.CompactionType {
	return d.currentPolicy
}
