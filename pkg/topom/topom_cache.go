// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func (s *Topom) dirtySlotsCache(tid int, sid int) {
	s.cache.hooks.PushBack(func() {
		if s.cache.slots != nil {
			if s.cache.slots[tid] != nil {
				s.cache.slots[tid][sid] = nil
			}
		}
	})
}

func (s *Topom) dirtyGroupCache(gid int) {
	s.cache.hooks.PushBack(func() {
		if s.cache.group != nil {
			s.cache.group[gid] = nil
		}
	})
}

func (s *Topom) dirtyTableCache(tid int) {
	s.cache.hooks.PushBack(func() {
		if s.cache.table != nil {
			s.cache.table[tid] = nil
		}
		if s.cache.slots != nil {
			s.cache.slots[tid] = nil
		}
	})
}

func (s *Topom) dirtyTableMetaCache() {
	s.cache.hooks.PushBack(func() {
		s.cache.tableMeta = nil
	})
}

func (s *Topom) dirtyProxyCache(token string) {
	s.cache.hooks.PushBack(func() {
		if s.cache.proxy != nil {
			s.cache.proxy[token] = nil
		}
	})
}

func (s *Topom) dirtySentinelCache() {
	s.cache.hooks.PushBack(func() {
		s.cache.sentinel = nil
	})
}

func (s *Topom) dirtyCacheAll() {
	s.cache.hooks.PushBack(func() {
		s.cache.slots = nil
		s.cache.group = nil
		s.cache.proxy = nil
		s.cache.sentinel = nil
		s.cache.tableMeta = nil
	})
}

func (s *Topom) refillCache() error {
	for i := s.cache.hooks.Len(); i != 0; i-- {
		e := s.cache.hooks.Front()
		s.cache.hooks.Remove(e).(func())()
	}
	if tableMeta, err := s.refillCacheTableMeta(s.cache.tableMeta); err != nil {
		log.ErrorErrorf(err, "store: load table meta failed")
		return errors.Errorf("store: load table meta failed")
	} else {
		s.cache.tableMeta = tableMeta
	}

	if table,err := s.refillCacheTable(s.cache.table); err != nil {
		log.ErrorErrorf(err, "store: load table failed")
		return errors.Errorf("store: load table failed")
	} else {
		s.cache.table = table
	}
	if slots, err := s.refillCacheSlots(s.cache.slots, s.cache.table); err != nil {
		log.ErrorErrorf(err, "store: load slots failed")
		return errors.Errorf("store: load slots failed")
	} else {
		s.cache.slots = slots
	}
	if group, err := s.refillCacheGroup(s.cache.group); err != nil {
		log.ErrorErrorf(err, "store: load group failed")
		return errors.Errorf("store: load group failed")
	} else {
		s.cache.group = group
	}
	if proxy, err := s.refillCacheProxy(s.cache.proxy); err != nil {
		log.ErrorErrorf(err, "store: load proxy failed")
		return errors.Errorf("store: load proxy failed")
	} else {
		s.cache.proxy = proxy
	}
	if sentinel, err := s.refillCacheSentinel(s.cache.sentinel); err != nil {
		log.ErrorErrorf(err, "store: load sentinel failed")
		return errors.Errorf("store: load sentinel failed")
	} else {
		s.cache.sentinel = sentinel
	}
	return nil
}

func (s *Topom) refillCacheSlots(slots map[int][]*models.SlotMapping, table map[int]*models.Table) (map[int][]*models.SlotMapping, error) {
	var err error
	if slots == nil {
		slots := make(map[int][]*models.SlotMapping)
		for i, _ :=range table  {
			if table[i] !=nil {
				if slots[i], err = s.store.SlotMappings(i); err != nil {
					return nil, err
				}
			}
		}
		return slots, nil
	}
	for i, _ :=range slots {
		if slots[i] == nil {
			slots[i], err = s.store.SlotMappings(i)
			if err != nil  {
				return nil, err
			}
			if slots[i] == nil {
				delete(slots, i)
			}
		} else {
			for j, _ := range slots[i] {
				if slots[i][j] != nil {
					continue
				}
				m, err := s.store.LoadSlotMapping(i, j, false)
				if err != nil {
					return nil, err
				}
				if m != nil {
					slots[i][j] = m
				} else {
					slots[i][j] = &models.SlotMapping{Id: j}
				}
			}
		}
	}
	return slots, nil
}

func (s *Topom) refillCacheTable(table map[int]*models.Table) (map[int]*models.Table, error) {
	if table == nil {
		return s.store.ListTable()
	}
	for i, _ := range table {
		if table[i] != nil {
			continue
		}
		t, err := s.store.LoadTable(i, false)
		if err != nil {
			return nil, err
		}
		if t != nil {
			table[i] = t
		} else {
			delete(table, i)
		}
	}
	return table, nil
}

func (s *Topom) refillCacheTableMeta(tableMeta *models.TableMeta) (*models.TableMeta, error) {
	if tableMeta != nil {
		return tableMeta, nil
	}
	p, err := s.store.LoadTableMeta(false)
	if err != nil {
		return nil, err
	}
	if p != nil {
		return p, nil
	}
	return &models.TableMeta{Id:0}, nil
}

func (s *Topom) refillCacheGroup(group map[int]*models.Group) (map[int]*models.Group, error) {
	if group == nil {
		return s.store.ListGroup()
	}
	for i, _ := range group {
		if group[i] != nil {
			continue
		}
		g, err := s.store.LoadGroup(i, false)
		if err != nil {
			return nil, err
		}
		if g != nil {
			group[i] = g
		} else {
			delete(group, i)
		}
	}
	return group, nil
}

func (s *Topom) refillCacheProxy(proxy map[string]*models.Proxy) (map[string]*models.Proxy, error) {
	if proxy == nil {
		return s.store.ListProxy()
	}
	for t, _ := range proxy {
		if proxy[t] != nil {
			continue
		}
		p, err := s.store.LoadProxy(t, false)
		if err != nil {
			return nil, err
		}
		if p != nil {
			proxy[t] = p
		} else {
			delete(proxy, t)
		}
	}
	return proxy, nil
}

func (s *Topom) refillCacheSentinel(sentinel *models.Sentinel) (*models.Sentinel, error) {
	if sentinel != nil {
		return sentinel, nil
	}
	p, err := s.store.LoadSentinel(false)
	if err != nil {
		return nil, err
	}
	if p != nil {
		return p, nil
	}
	return &models.Sentinel{}, nil
}

func (s *Topom) storeCreateSlotMapping(tid int, m *models.SlotMapping) error {
	log.Warnf("create table-[%d] slot-[%d]:\n%s", tid, m.Id, m.Encode())
	if err := s.store.UpdateSlotMapping(tid, m); err != nil {
		log.ErrorErrorf(err, "store: create table-[%d] slot-[%d] failed", tid, m.Id)
		return errors.Errorf("store: create table-[%d] slot-[%d] failed", tid, m.Id)
	}
	return nil
}

func (s *Topom) storeUpdateSlotMapping(tid int, m *models.SlotMapping) error {
	log.Warnf("update table-[%d] slot-[%d]:\n%s", tid, m.Id, m.Encode())
	if err := s.store.UpdateSlotMapping(tid, m); err != nil {
		log.ErrorErrorf(err, "store: update table-[%d] slot-[%d] failed", tid, m.Id)
		return errors.Errorf("store: update table-[%d] slot-[%d] failed", tid, m.Id)
	}
	return nil
}

func (s *Topom) storeRemoveSlotMapping(tid int, m *models.SlotMapping) error {
	log.Warnf("remove table-[%d] slot-[%d]:\n%s", tid, m.Id, m.Encode())
	if err := s.store.RemoveSlotMapping(tid, m); err != nil {
		log.ErrorErrorf(err, "store: remove table-[%d] slot-[%d] failed", tid, m.Id)
		return errors.Errorf("store: remove table-[%d] slot-[%d] failed", tid, m.Id)
	}
	return nil
}

func (s *Topom) storeCreateGroup(g *models.Group) error {
	log.Warnf("create group-[%d]:\n%s", g.Id, g.Encode())
	if err := s.store.UpdateGroup(g); err != nil {
		log.ErrorErrorf(err, "store: create group-[%d] failed", g.Id)
		return errors.Errorf("store: create group-[%d] failed", g.Id)
	}
	return nil
}

func (s *Topom) storeUpdateGroup(g *models.Group) error {
	log.Warnf("update group-[%d]:\n%s", g.Id, g.Encode())
	if err := s.store.UpdateGroup(g); err != nil {
		log.ErrorErrorf(err, "store: update group-[%d] failed", g.Id)
		return errors.Errorf("store: update group-[%d] failed", g.Id)
	}
	return nil
}

func (s *Topom) storeRemoveGroup(g *models.Group) error {
	log.Warnf("remove group-[%d]:\n%s", g.Id, g.Encode())
	if err := s.store.DeleteGroup(g.Id); err != nil {
		log.ErrorErrorf(err, "store: remove group-[%d] failed", g.Id)
		return errors.Errorf("store: remove group-[%d] failed", g.Id)
	}
	return nil
}

func (s *Topom) storeCreateTable(t *models.Table) error {
	log.Warnf("create table-[%d]:\n%s", t.Id, t.Encode())
	if err := s.store.UpdateTable(t); err != nil {
		log.ErrorErrorf(err, "store: create table-[%d] failed", t.Id)
		return errors.Errorf("store: create table-[%d] failed", t.Id)
	}
	return nil
}

func (s *Topom) storeUpdateTable(t *models.Table) error {
	log.Warnf("create table-[%d]:\n%s", t.Id, t.Encode())
	if err := s.store.UpdateTable(t); err != nil {
		log.ErrorErrorf(err, "store: create table-[%d] failed", t.Id)
		return errors.Errorf("store: create table-[%d] failed", t.Id)
	}
	return nil
}

func (s *Topom) storeRemoveTable(t *models.Table) error {
	//TODO delete slot
	log.Warnf("remove table-[%d]:\n%s", t.Id, t.Encode())
	if err := s.store.DeleteTable(t.Id); err != nil {
		log.ErrorErrorf(err, "store: remove table-[%d] failed", t.Id)
		return errors.Errorf("store: remove table-[%d] failed", t.Id)
	}
	return nil
}

func (s *Topom) storeCreateTableMeta(t *models.TableMeta) error {
	log.Warnf("create table meta:\n%s", t.Encode())
	if err := s.store.UpdateTableMeta(t); err != nil {
		log.ErrorError(err, "store: create table meta failed")
		return errors.Errorf("store: create table meta failed")
	}
	return nil
}

func (s *Topom) storeUpdateTableMeta(t *models.TableMeta) error {
	log.Warnf("create table meta:\n%s", t.Encode())
	if err := s.store.UpdateTableMeta(t); err != nil {
		log.ErrorError(err, "store: create table meta failed")
		return errors.Errorf("store: create table meta failed")
	}
	return nil
}

func (s *Topom) storeCreateProxy(p *models.Proxy) error {
	log.Warnf("create proxy-[%s]:\n%s", p.Token, p.Encode())
	if err := s.store.UpdateProxy(p); err != nil {
		log.ErrorErrorf(err, "store: create proxy-[%s] failed", p.Token)
		return errors.Errorf("store: create proxy-[%s] failed", p.Token)
	}
	return nil
}

func (s *Topom) storeUpdateProxy(p *models.Proxy) error {
	log.Warnf("update proxy-[%s]:\n%s", p.Token, p.Encode())
	if err := s.store.UpdateProxy(p); err != nil {
		log.ErrorErrorf(err, "store: update proxy-[%s] failed", p.Token)
		return errors.Errorf("store: update proxy-[%s] failed", p.Token)
	}
	return nil
}

func (s *Topom) storeRemoveProxy(p *models.Proxy) error {
	log.Warnf("remove proxy-[%s]:\n%s", p.Token, p.Encode())
	if err := s.store.DeleteProxy(p.Token); err != nil {
		log.ErrorErrorf(err, "store: remove proxy-[%s] failed", p.Token)
		return errors.Errorf("store: remove proxy-[%s] failed", p.Token)
	}
	return nil
}

func (s *Topom) storeUpdateSentinel(p *models.Sentinel) error {
	log.Warnf("update sentinel:\n%s", p.Encode())
	if err := s.store.UpdateSentinel(p); err != nil {
		log.ErrorErrorf(err, "store: update sentinel failed")
		return errors.Errorf("store: update sentinel failed")
	}
	return nil
}
