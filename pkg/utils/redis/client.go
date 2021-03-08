// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"container/list"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/math2"

	redigo "github.com/garyburd/redigo/redis"
)

type Client struct {
	conn redigo.Conn
	Addr string
	Auth string

	Database int

	LastUse time.Time
	Timeout time.Duration

	Pipeline struct {
		Send, Recv uint64
	}
}

type InfoTable struct {
	Tid  int
	Slot map[int]*InfoSlot
}

type InfoSlot struct {
	Slot       int
	FileNum    int
	Offset     int
	Consensus  bool
	Term       int
	Index      int
	Role       string
	Slave      map[string]*Slave
	MasterAddr string
}

type Slave struct {
	Addr   string
	Status string
	Lag    int
}

type PiakInfoTable struct {
	Id                   int
	PartitionNum         int
	TotalCommandNum      int64
	TotalWriteCommandNum int64
	Qps                  int64
	WriteQps             int64
	ReadQps              int64
}

const InfoTableChunkLength int = 7

func stringToKv(s, sep string) (string, int, error) {
	kv := strings.SplitN(s, sep, 2)
	v, err := strconv.Atoi(strings.TrimPrefix(kv[1], " "))
	if err != nil {
		return "", 0, err
	}
	return strings.TrimPrefix(kv[0], " "), v, nil
}

func (c *Client) GetInfoTable() (map[int]*PiakInfoTable, error) {
	text, err := redigo.String(c.Do("pkcluster", "info", "table"))
	if err != nil {
		log.Infof("send pkcluster info err: %s", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	lineNum := strings.Count(text, "\r\n")
	chunkNum := lineNum / InfoTableChunkLength
	if lineNum == 0 || lineNum%InfoTableChunkLength != 0 {
		log.Info("pika pkcluster  info table format error")
	}
	line := strings.Split(text, "\r\n")
	infoTable := make(map[int]*PiakInfoTable)
	for c := 0; c < chunkNum; c++ {
		var info PiakInfoTable
		_, v, err := stringToKv(line[c*InfoTableChunkLength], ":")
		if err != nil {
			return nil, err
		} else {
			info.Id = v
		}
		_, v, err = stringToKv(line[c*InfoTableChunkLength+1], ":")
		if err != nil {
			return nil, err
		} else {
			info.PartitionNum = v
		}
		_, v, err = stringToKv(line[c*InfoTableChunkLength+2], ":")
		if err != nil {
			return nil, err
		} else {
			info.TotalCommandNum = int64(v)
		}
		_, v, err = stringToKv(line[c*InfoTableChunkLength+3], ":")
		if err != nil {
			return nil, err
		} else {
			info.TotalWriteCommandNum = int64(v)
		}
		/* don't use qps from pika. codis will compute it by itself
		_, v, err = stringToKv(line[c * InfoTableChunkLength + 4], ":")
		if err != nil {
			return nil, err
		} else {
			info.Qps = v
		}
		_, v, err = stringToKv(line[c * InfoTableChunkLength + 5], ":")
		if err != nil {
			return nil, err
		} else {
			info.WriteQps = v
		}
		_, v, err = stringToKv(line[c * InfoTableChunkLength + 6], ":")
		if err != nil {
			return nil, err
		} else {
			info.ReadQps = v
		}
		*/
		infoTable[info.Id] = &info
	}

	return infoTable, nil
}

func NewClientNoAuth(addr string, timeout time.Duration) (*Client, error) {
	return NewClient(addr, "", timeout)
}

func NewClient(addr string, auth string, timeout time.Duration) (*Client, error) {
	c, err := redigo.Dial("tcp", addr, []redigo.DialOption{
		redigo.DialConnectTimeout(math2.MinDuration(time.Second, timeout)),
		redigo.DialPassword(auth),
		redigo.DialReadTimeout(timeout), redigo.DialWriteTimeout(timeout),
	}...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Client{
		conn: c, Addr: addr, Auth: auth,
		LastUse: time.Now(), Timeout: timeout,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) isRecyclable() bool {
	switch {
	case c.conn.Err() != nil:
		return false
	case c.Pipeline.Send != c.Pipeline.Recv:
		return false
	case c.Timeout != 0 && c.Timeout <= time.Since(c.LastUse):
		return false
	}
	return true
}

func (c *Client) Do(cmd string, args ...interface{}) (interface{}, error) {
	r, err := c.conn.Do(cmd, args...)
	if err != nil {
		c.Close()
		return nil, errors.Trace(err)
	}
	c.LastUse = time.Now()

	if err, ok := r.(redigo.Error); ok {
		return nil, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) Send(cmd string, args ...interface{}) error {
	if err := c.conn.Send(cmd, args...); err != nil {
		c.Close()
		return errors.Trace(err)
	}
	c.Pipeline.Send++
	return nil
}

func (c *Client) Flush() error {
	if err := c.conn.Flush(); err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Receive() (interface{}, error) {
	r, err := c.conn.Receive()
	if err != nil {
		c.Close()
		return nil, errors.Trace(err)
	}
	c.Pipeline.Recv++

	c.LastUse = time.Now()

	if err, ok := r.(redigo.Error); ok {
		return nil, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) Select(database int) error {
	if c.Database == database {
		return nil
	}
	_, err := c.Do("SELECT", database)
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	c.Database = database
	return nil
}

func (c *Client) Shutdown() error {
	_, err := c.Do("SHUTDOWN")
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Info() (map[string]string, error) {
	text, err := redigo.String(c.Do("INFO"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	info := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		if key := strings.TrimSpace(kv[0]); key != "" {
			info[key] = strings.TrimSpace(kv[1])
		}
	}
	return info, nil
}

/* info slot
(db0:0) binlog_offset=0 0,safety_purge=none
  Role: Master
  connected_slaves: 2
  slave[0]: 127.0.0.1:9223
  replication_status: SlaveBinlogSync
  lag: 0
  slave[1]: 127.0.0.1:9222
  replication_status: SlaveBinlogSync
  lag: 0

*/
func (c *Client) InfoSlot() (map[int]*InfoTable, error) {
	text, err := redigo.String(c.Do("pkcluster", "info", "slot"))
	if err != nil {
		log.Infof("send pkcluster info err: %s", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	chunk := strings.Split(text, "\r\n\r\n")
	table := make(map[int]*InfoTable)
	var tid int
	for _, field := range chunk {
		line := strings.Split(field, "\r\n")
		if len(line) >= 3 {
			kv := strings.SplitN(line[0], ":", 2)
			t := strings.TrimPrefix(kv[0], "(db")
			skv := strings.SplitN(kv[1], ")", 2)
			s := skv[0]
			bkv := strings.SplitN(skv[1], ",", 2)
			fkv := strings.SplitN(strings.TrimPrefix(bkv[0], " binlog_offset="), " ", 2)
			fileNum := fkv[0]
			offset := fkv[1]
			infoSlot := InfoSlot{}
			if m, err := strconv.Atoi(t); err == nil {
				tid = m
				if _, ok := table[tid]; ok != true {
					slot := make(map[int]*InfoSlot)
					infoTable := InfoTable{Tid: tid, Slot: slot}
					table[tid] = &infoTable
				}
			} else {
				return nil, err
			}
			if m, err := strconv.Atoi(s); err == nil {
				infoSlot.Slot = m
			} else {
				return nil, err
			}
			if m, err := strconv.Atoi(fileNum); err == nil {
				infoSlot.FileNum = m
			} else {
				return nil, err
			}
			if m, err := strconv.Atoi(offset); err == nil {
				infoSlot.Offset = m
			} else {
				return nil, err
			}

			base := 0

			kv = strings.SplitN(line[1], "=", 2)
			if strings.TrimSpace(kv[0]) == "consensus_last_log" {
				base = 1
				infoSlot.Consensus = true
				item := strings.SplitN(kv[1], " ", 8)
				if t, err := strconv.Atoi(item[5]); err == nil {
					infoSlot.Term = t
				}
				if t, err := strconv.Atoi(item[7]); err == nil {
					infoSlot.Index = t
				}
			} else {
				base = 0
			}

			kv = strings.SplitN(line[base+1], ":", 2)
			infoSlot.Role = strings.TrimSpace(kv[1])
			if infoSlot.Role == "Master" {
				kv = strings.SplitN(line[base+2], ":", 2)
				slaves := strings.TrimSpace(kv[1])
				var slaveNum int
				if m, err := strconv.Atoi(slaves); err == nil {
					slaveNum = m
				} else {
					return nil, err
				}
				servers := make(map[string]*Slave)
				for i := 0; i < slaveNum; i++ {
					slave := &Slave{}
					kv := strings.SplitN(line[base+3+i*3], ":", 2)
					slave.Addr = strings.TrimSpace(kv[1])
					kv = strings.SplitN(line[base+3+i*3+1], ":", 2)
					slave.Status = strings.TrimSpace(kv[1])
					kv = strings.SplitN(line[base+3+i*3+2], ":", 2)
					lag := strings.TrimSpace(kv[1])
					if t, err := strconv.Atoi(lag); err == nil {
						slave.Lag = t
					} else {
						return nil, err
					}
					servers[slave.Addr] = slave
				}
				infoSlot.Slave = servers
			}
			table[tid].Slot[infoSlot.Slot] = &infoSlot
		}
	}
	return table, nil
}

func (c *Client) Ping() (bool, error) {
	text, err := redigo.String(c.Do("PING"))
	if err != nil {
		return false, errors.Trace(err)
	}
	switch text {
	case "pong", "PONG":
		return false, nil
	default:
		return true, nil
	}
}

func (c *Client) InfoKeySpace() (map[int]string, error) {
	text, err := redigo.String(c.Do("INFO", "keyspace"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	info := make(map[int]string)
	for _, line := range strings.Split(text, "\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		if key := strings.TrimSpace(kv[0]); key != "" && strings.HasPrefix(key, "db") {
			n, err := strconv.Atoi(key[2:])
			if err != nil {
				return nil, errors.Trace(err)
			}
			info[n] = strings.TrimSpace(kv[1])
		}
	}
	return info, nil
}

func (c *Client) InfoFull() (map[string]string, error) {
	if info, err := c.Info(); err != nil {
		return nil, errors.Trace(err)
	} else {
		host := info["master_host"]
		port := info["master_port"]
		if host != "" || port != "" {
			info["master_addr"] = net.JoinHostPort(host, port)
		}
		r, err := c.Do("CONFIG", "GET", "maxmemory")
		if err != nil {
			return nil, errors.Trace(err)
		}
		p, err := redigo.Values(r, nil)
		if err != nil || len(p) != 2 {
			return nil, errors.Errorf("invalid response = %v", r)
		}
		v, err := redigo.Int(p[1], nil)
		if err != nil {
			return nil, errors.Errorf("invalid response = %v", r)
		}
		info["maxmemory"] = strconv.Itoa(v)
		return info, nil
	}
}

func (c *Client) CreateTable(t, s int) error {
	if _, err := c.Do("pkcluster", "addtable", t, s); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) DeleteTable(addr string, t int) error {
	log.Warnf("addr-[%s] delete table-[%d]", addr, t)
	if _, err := c.Do("pkcluster", "deltable", t); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) AddSlots(addr string, tid, beg, end int) error {
	log.Warnf("addr-[%s]  table-[%d] add slots begin-[%d], end-[%d]", addr, tid, beg, end)
	buf := fmt.Sprintf("%d-%d", beg, end)
	if _, err := c.Do("pkcluster", "addslots", buf, tid); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) DelSlotsRange(addr string, tid, beg, end int) error {
	log.Warnf("addr-[%s]  table-[%d] delete slots begin-[%d], end-[%d]", addr, tid, beg, end)
	buf := fmt.Sprintf("%d-%d", beg, end)
	if _, err := c.Do("pkcluster", "delslots", buf, tid); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) DelSlots(addr string, tid int, slots string) error {
	log.Warnf("addr-[%s]  table-[%d] delete slots: %s", addr, tid, slots)
	if _, err := c.Do("pkcluster", "delslots", slots, tid); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) SlotSlaveof(addr string, s, t int) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err := c.Do("pkcluster", "slotsslaveof", host, port, s, t); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) SlotSlaveofAll(addr string, t int) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err := c.Do("pkcluster", "slotsslaveof", host, port, "all", t); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) SetMaster(master string) error {
	/*
		host, port, err := net.SplitHostPort(master)
		if err != nil {
			return errors.Trace(err)
		}
		if _, err := c.Do("CONFIG", "SET", "masterauth", c.Auth); err != nil {
			return errors.Trace(err)
		}
		if _, err := c.Do("SLAVEOF", host, port); err != nil {
			return errors.Trace(err)
		}
		c.Send("MULTI")
		c.Send("CONFIG", "SET", "masterauth", c.Auth)
		c.Send("SLAVEOF", host, port)
		c.Send("CONFIG", "REWRITE")
		c.Send("CLIENT", "KILL", "TYPE", "normal")
		values, err := redigo.Values(c.Do("EXEC"))
		if err != nil {
			return errors.Trace(err)
		}
		for _, r := range values {
			if err, ok := r.(redigo.Error); ok {
				return errors.Trace(err)
			}
		}
	*/
	return nil
}

func (c *Client) MigrateSlot(slot int, target string) (int, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return 0, errors.Trace(err)
	}
	mseconds := int(c.Timeout / time.Millisecond)
	if reply, err := c.Do("SLOTSMGRTTAGSLOT", host, port, mseconds, slot); err != nil {
		return 0, errors.Trace(err)
	} else {
		p, err := redigo.Ints(redigo.Values(reply, nil))
		if err != nil || len(p) != 2 {
			return 0, errors.Errorf("invalid response = %v", reply)
		}
		return p[1], nil
	}
}

type MigrateSlotAsyncOption struct {
	MaxBulks int
	MaxBytes int
	NumKeys  int
	Timeout  time.Duration
}

func (c *Client) MigrateSlotAsync(slot int, target string, option *MigrateSlotAsyncOption) (int, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if reply, err := c.Do("SLOTSMGRTTAGSLOT-ASYNC", host, port, int(option.Timeout/time.Millisecond),
		option.MaxBulks, option.MaxBytes, slot, option.NumKeys); err != nil {
		return 0, errors.Trace(err)
	} else {
		p, err := redigo.Ints(redigo.Values(reply, nil))
		if err != nil || len(p) != 2 {
			return 0, errors.Errorf("invalid response = %v", reply)
		}
		return p[1], nil
	}
}

func (c *Client) SlotsInfo() (map[int]int, error) {
	if reply, err := c.Do("SLOTSINFO"); err != nil {
		return nil, errors.Trace(err)
	} else {
		infos, err := redigo.Values(reply, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		slots := make(map[int]int)
		for i, info := range infos {
			p, err := redigo.Ints(info, nil)
			if err != nil || len(p) != 2 {
				return nil, errors.Errorf("invalid response[%d] = %v", i, info)
			}
			slots[p[0]] = p[1]
		}
		return slots, nil
	}
}

func (c *Client) Role() (string, error) {
	if reply, err := c.Do("ROLE"); err != nil {
		return "", err
	} else {
		values, err := redigo.Values(reply, nil)
		if err != nil {
			return "", errors.Trace(err)
		}
		if len(values) == 0 {
			return "", errors.Errorf("invalid response = %v", reply)
		}
		role, err := redigo.String(values[0], nil)
		if err != nil {
			return "", errors.Errorf("invalid response[0] = %v", values[0])
		}
		return strings.ToUpper(role), nil
	}
}

var ErrClosedPool = errors.New("use of closed redis pool")

type Pool struct {
	mu sync.Mutex

	auth string
	pool map[string]*list.List

	timeout time.Duration

	exit struct {
		C chan struct{}
	}

	closed bool
}

func NewPool(auth string, timeout time.Duration) *Pool {
	p := &Pool{
		auth: auth, timeout: timeout,
		pool: make(map[string]*list.List),
	}
	p.exit.C = make(chan struct{})

	if timeout != 0 {
		go func() {
			var ticker = time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-p.exit.C:
					return
				case <-ticker.C:
					p.Cleanup()
				}
			}
		}()
	}

	return p
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.exit.C)

	for addr, list := range p.pool {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			c.Close()
		}
		delete(p.pool, addr)
	}
	return nil
}

func (p *Pool) Cleanup() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosedPool
	}

	for addr, list := range p.pool {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			if !c.isRecyclable() {
				c.Close()
			} else {
				list.PushBack(c)
			}
		}
		if list.Len() == 0 {
			delete(p.pool, addr)
		}
	}
	return nil
}

func (p *Pool) GetClient(addr string) (*Client, error) {
	c, err := p.getClientFromCache(addr)
	if err != nil || c != nil {
		return c, err
	}
	return NewClient(addr, p.auth, p.timeout)
}

func (p *Pool) getClientFromCache(addr string) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosedPool
	}
	if list := p.pool[addr]; list != nil {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			if !c.isRecyclable() {
				c.Close()
			} else {
				return c, nil
			}
		}
	}
	return nil, nil
}

func (p *Pool) PutClient(c *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !c.isRecyclable() || p.closed {
		c.Close()
	} else {
		cache := p.pool[c.Addr]
		if cache == nil {
			cache = list.New()
			p.pool[c.Addr] = cache
		}
		cache.PushFront(c)
	}
}

func (p *Pool) Info(addr string) (_ map[string]string, err error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.Info()
}

func (p *Pool) InfoTable(addr string) (map[int]*PiakInfoTable, error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.GetInfoTable()
}

func (p *Pool) InfoFull(addr string) (_ map[string]string, err error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.InfoFull()
}

func (p *Pool) InfoSlot(addr string) (map[int]*InfoTable, error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.InfoSlot()
}

func (p *Pool) Ping(addr string) (bool, error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return false, err
	}
	defer p.PutClient(c)
	return c.Ping()
}

func (p *Pool) CreateTable(tid, slotNum int) func(string) error {
	return func(addr string) error {
		c, err := p.GetClient(addr)
		if err != nil {
			return err
		}
		defer p.PutClient(c)
		log.Warnf("addr-[%s]create table-[%d] slot num-[%d]", addr, tid, slotNum)
		return c.CreateTable(tid, slotNum)
	}
}

func (p *Pool) DeleteTable(tid int) func(string) error {
	return func(addr string) error {
		c, err := p.GetClient(addr)
		if err != nil {
			return err
		}
		defer p.PutClient(c)
		return c.DeleteTable(addr, tid)
	}
}

func (p *Pool) AddSlots(tid, beg, end int) func(string) error {
	return func(addr string) error {
		c, err := p.GetClient(addr)
		if err != nil {
			return err
		}
		defer p.PutClient(c)
		return c.AddSlots(addr, tid, beg, end)
	}
}

func (p *Pool) DelSlotsRange(tid, beg, end int) func(string) error {
	return func(addr string) error {
		c, err := p.GetClient(addr)
		if err != nil {
			return err
		}
		defer p.PutClient(c)
		return c.DelSlotsRange(addr, tid, beg, end)
	}
}

func (p *Pool) DelSlots(tid int, slots map[string]string) func(string) error {
	return func(addr string) error {
		c, err := p.GetClient(addr)
		if err != nil {
			return err
		}
		defer p.PutClient(c)
		return c.DelSlots(addr, tid, slots[addr])
	}
}

func (p *Pool) SlotSlaveof(addr, mAddr string, sid, tid int) error {
	c, err := p.GetClient(addr)
	if err != nil {
		return err
	}
	defer p.PutClient(c)
	return c.SlotSlaveof(mAddr, sid, tid)
}

func (p *Pool) SlotSlaveofAll(mAddr string, tid int) func(string) error {
	return func(addr string) error {
		c, err := p.GetClient(addr)
		if err != nil {
			return err
		}
		defer p.PutClient(c)
		log.Warnf("addr-[%s] slave of %s table-[%d]", addr, mAddr, tid)
		return c.SlotSlaveofAll(mAddr, tid)
	}
}

type InfoCache struct {
	mu sync.Mutex

	Auth string
	data map[string]map[string]string

	Timeout time.Duration
}

func (s *InfoCache) load(addr string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data != nil {
		return s.data[addr]
	}
	return nil
}

func (s *InfoCache) store(addr string, info map[string]string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]map[string]string)
	}
	if info != nil {
		s.data[addr] = info
	} else if s.data[addr] == nil {
		s.data[addr] = make(map[string]string)
	}
	return s.data[addr]
}

func (s *InfoCache) Get(addr string) map[string]string {
	info := s.load(addr)
	if info != nil {
		return info
	}
	info, _ = s.getSlow(addr)
	return s.store(addr, info)
}

func (s *InfoCache) GetRunId(addr string) string {
	return s.Get(addr)["run_id"]
}

func (s *InfoCache) getSlow(addr string) (map[string]string, error) {
	c, err := NewClient(addr, s.Auth, s.Timeout)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.Info()
}
