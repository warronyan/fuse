package fuse

import (
	"reflect"
	"sync"
)

func init() {
	implmap.Add("flowTokenManager", reflect.TypeOf((*Manager)(nil)))
}

type IFuseManager interface {
	AddInit(host string, proxy bool, logErr bool, initTokenNum int64, initFailLimit int64, triggerFailrate float64) *Fuse
	Add(host string, proxy bool, logErr bool) *Fuse
	Get(host string) *Fuse
	Del(host string)
}

type Manager struct {
	InitFailLimit int64 `inject:"flowTokenManagerInitFailLimit" cannil:"true"`
	InitTokenNum  int64 `inject:"flowTokenManagerInitTokenNum" cannil:"true"`

	fts sync.Map
}

func (m *Manager) Start() error {
	if m.InitFailLimit <= 0 {
		m.InitFailLimit = fail_limit
	}
	if m.InitTokenNum <= 0 {
		m.InitTokenNum = init_token_num
	}
	fmt.Printf("flow token manager start up %v", m)
	return nil
}

func (m *Manager) Close() {
	m.fts.Range(func(key, val interface{}) bool {
		vv, ok := val.(*Fuse)
		if ok && vv != nil {
			v := vv.GetSnapshot()
			fmt.Printf("flow token manager close k=%v,v=%v", key, v)
		}
		return true
	})
}

func (m *Manager) Get(host string) *Fuse {
	if ft, ok := m.fts.Load(host); ok {
		return ft.(*Fuse)
	}
	return nil
}

func (m *Manager) Del(host string) {
	m.fts.Delete(host)
}

func (m *Manager) Add(host string, proxy bool, logErr bool) *Fuse {
	return m.AddInit(host, proxy, logErr, m.InitTokenNum, m.InitFailLimit, 0)
}

func (m *Manager) AddInit(host string, proxy bool, logErr bool, initTokenNum int64, initFailLimit int64, triggerFailrate float64) *Fuse {
	if ft, ok := m.fts.Load(host); ok {
		return ft.(*Fuse)
	}
	ft := NewFuseIdInitProxy(host, initTokenNum, initFailLimit, proxy)
	ft.DoNotLogError = !logErr
	if triggerFailrate > 0 {
		ft.SetTriggerFailrate(triggerFailrate)
	}
	fmt.Printf("fuse manager add,h=%s,p=%t,le=%t,init=%d,failLimit=%d,triggerFailrate=%.2f", host, proxy, logErr, initTokenNum, initFailLimit, triggerFailrate)
	m.fts.Store(host, ft)
	return ft
}
