// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
	"strconv"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

func (s *Topom) ProcessSlotAction() error {
	table, err := s.tables()
	if err != nil {
		return  err
	}
	for tid, _ :=range table {
		for s.IsOnline() {
			var (
				marks = make(map[int]bool)
				plans = make(map[int]bool)
			)
			var accept = func(m *models.SlotMapping) bool {
				if marks[m.GroupId] || marks[m.Action.TargetId] {
					return false
				}
				if plans[m.Id] {
					return false
				}
				return true
			}
			var update = func(m *models.SlotMapping) bool {
				if m.GroupId != 0 {
					marks[m.GroupId] = true
				}
				marks[m.Action.TargetId] = true
				plans[m.Id] = true
				return true
			}
			var parallel = math2.MaxInt(1, s.config.MigrationParallelSlots)
			for parallel > len(plans) {
				_, ok, err := s.SlotActionPrepareFilter(accept, update, tid)
				if err != nil {
					return err
				} else if !ok {
					break
				}
			}
			if len(plans) == 0 {
		//		return nil
				break
			}
			var fut sync2.Future
			for _, v := range fut.Wait() {
				if v != nil {
					return v.(error)
				}
			}
			time.Sleep(time.Millisecond * 10)
		}

	}
	return nil
}

func (s *Topom) processSlotAction(tid int, sid int) error {
	for s.IsOnline() {
		if exec, err := s.newSlotActionExecutor(tid, sid); err != nil {
			return err
		} else if exec == nil {
			time.Sleep(time.Second)
		} else {
			n, nextdb, err := exec(tid)
			if err != nil {
				return err
			}
			log.Debugf("slot-[%d] action executor %d", sid, n)

			if n == 0 && nextdb == -1 {
				return s.SlotActionComplete(tid, sid)
			}

			if us := s.GetSlotActionInterval(); us != 0 {
				time.Sleep(time.Microsecond * time.Duration(us))
			}
		}
	}
	return nil
}

func (s *Topom) ProcessSyncAction() error {
	addr, err := s.SyncActionPrepare()
	if err != nil || addr == "" {
		return err
	}
	log.Warnf("sync-[%s] process action", addr)

	exec, err := s.newSyncActionExecutor(addr)
	if err != nil || exec == nil {
		return err
	}
	return s.SyncActionComplete(addr, exec() != nil)
}
