package fuse 

import (
	"math"
	"strings"
	"sync"
	"time"
)

const (
	init_token_num = 2000
	fail_limit     = 3
	log_format     = "[Fuse] %s. clientId=%s, currSec=%d, last_cwnd=%d, cwnd=%d, ssthresh=%d, request=%d, succ=%d, fail=%d, error=%d"
)

func IsFuseError(e error) (*FuseError, bool) {
	if e == nil {
		return nil, false
	}
	te, ok := e.(*FuseError)
	return te, ok
}

type FuseError struct {
	Snapshot *Snapshot
	Msg      string
}

func (e *FuseError) Error() string {
	s := ""
	sb, _ := json2.Marshal(e)
	if sb != nil {
		s = string(sb)
	}
	return s
}

type Token struct {
	TokenNum  int64
	flowToken *Fuse
}

type Fuse struct {
	clientId     string
	l            *sync.Mutex
	lastTimeSec  int64 // 上次请求时间
	tokenNum     int64 // 令牌控制
	cwnd         int64 // 滑动窗口
	ssthresh     int64 // 初始压力控制
	requestCount int64 // 请求次数（包括token fail情况）
	succCount    int64 // 成功次数
	failCount    int64 // 失败次数
	errorCount   int64 // 熔断次数
	tokenNumMax  int64 //令牌控制的上限
	initTokenNum int64
	failLimit    int64

	isProxy              bool
	triggerFailrate      float64
	DoNotLogError        bool
	passAsManyAsPossible bool
}

type Snapshot struct {
	ClientId     string
	LastTimeSec  int64 // 上次请求时间
	TokenNum     int64 // 令牌控制
	Cwnd         int64 // 滑动窗口
	Ssthresh     int64 // 初始压力控制
	RequestCount int64 // 请求次数（包括token fail情况）
	SuccCount    int64 // 成功次数
	FailCount    int64 // 失败次数
	ErrorCount   int64 // 熔断次数
}

/*
func NewFuse() *Fuse {
	return NewFuseId("")
}
*/

func NewProxyFuseId(clientId string) *Fuse {
	return NewFuseIdInitProxy(clientId, int64(init_token_num), int64(fail_limit), true)
}

func NewFuseId(clientId string) *Fuse {
	return NewFuseIdInit(clientId, int64(init_token_num), int64(fail_limit))
}

func NewFuseIdInit(clientId string, initTokenNum, failLimit int64) *Fuse {
	return NewFuseIdInitProxy(clientId, initTokenNum, failLimit, false)
}

func NewFuseIdInitProxy(clientId string, initTokenNum, failLimit int64, isProxy bool) *Fuse {
	if failLimit >= 200 {
		failLimit = 199
	}
	if failLimit <= 0 {
		failLimit = fail_limit
	}

	t := &Fuse{
		clientId: clientId,
		l:        &sync.Mutex{},
		//cwnd:     initTokenNum,
		ssthresh:        initTokenNum,
		initTokenNum:    initTokenNum,
		failLimit:       failLimit,
		isProxy:         isProxy,
		triggerFailrate: 0.02,
	}
	fmt.Printf("create flowtoken %s,proxy=%v", clientId, isProxy)
	return t
}

func (t *Fuse) SetPassAsManyAsPossible(passAsManyAsPossible bool) *Fuse {
	t.passAsManyAsPossible = passAsManyAsPossible
	return t
}

func (t *Fuse) SetDoNotLogError(doNotLogError bool) *Fuse {
	t.DoNotLogError = doNotLogError
	return t
}

func (t *Fuse) GetTriggerSuccrate() float64 {
	return 1 - t.triggerFailrate
}

func (t *Fuse) GetTriggerFailrate() float64 {
	return t.triggerFailrate
}

//proxy模式是不支持设置triggerFailrate的
func (t *Fuse) SetTriggerFailrate(triggerFailrate float64) *Fuse {
	if triggerFailrate >= 0.00001 && triggerFailrate < 1 {
		if triggerFailrate > 0.4 {
			triggerFailrate = 0.4
		}
		t.triggerFailrate = triggerFailrate
	}
	return t
}

//allFail is for ProxyFuse
func (t *Fuse) allProxyFail(succ, fail int64) bool {
	total := succ + fail
	if total == 0 {
		return false
	}

	if total < 200 {
		failRate := float64(fail) / float64(total)
		if failRate >= t.GetTriggerSuccrate() {
			return true
		}

		if succ == 0 && fail > 0 {
			return true
		}
		return fail >= t.failLimit && succ <= t.failLimit
	} else {
		failRate := float64(fail) / float64(total)
		if failRate >= t.GetTriggerSuccrate() {
			return true
		} else {
			return false
		}
	}
}

func (t *Fuse) allFail(succ, fail int64) bool {
	total := succ + fail
	if total == 0 {
		return false
	}

	if total < 200 {
		failRate := float64(fail) / float64(total)
		if failRate >= t.GetTriggerSuccrate() {
			return true
		}

		if succ == 0 && fail > 0 {
			return true
		}
		return fail >= t.failLimit && succ < t.failLimit
	} else {
		failRate := float64(fail) / float64(total)
		if failRate >= t.GetTriggerSuccrate() {
			return true
		} else {
			return false
		}
	}
}

func (t *Fuse) allSucc(succ, fail int64) bool {
	total := succ + fail
	if total < 200 {
		return fail < t.failLimit && succ >= t.failLimit
	} else {
		failRate := float64(fail) / float64(total)
		if failRate <= t.GetTriggerFailrate() {
			return true
		} else {
			return false
		}
	}
}

//如果请求量小于每秒钟200个，则每秒失败3个就开始降低阈值
//否则按百分比计算，2%及以上的请求失败则开始降低阈值
func (t *Fuse) partFail(succ, fail int64) bool {
	total := succ + fail
	if total < 200 {
		return fail >= t.failLimit
	} else {
		failRate := float64(fail) / float64(total)
		if failRate <= t.GetTriggerFailrate() {
			return false
		} else {
			return true
		}
	}
}

func (t *Fuse) initProxyToken(currSec int64) *Snapshot {
	//currSec := time.Now().Unix()
	if currSec > t.lastTimeSec { // 新的一秒开始
		osn := &Snapshot{
			ClientId:     t.clientId,
			LastTimeSec:  t.lastTimeSec,
			TokenNum:     t.tokenNum,
			Cwnd:         t.cwnd,
			Ssthresh:     t.ssthresh,
			RequestCount: t.requestCount,
			SuccCount:    t.succCount,
			FailCount:    t.failCount,
			ErrorCount:   t.errorCount,
		}

		succ := t.succCount
		fail := t.failCount
		er := t.errorCount
		t.requestCount = 0
		t.succCount = 0
		t.failCount = 0
		t.errorCount = 0
		if t.lastTimeSec == 0 {
			// 刚启动
			t.cwnd = t.initTokenNum
		} else if succ == 0 && fail == 0 {
			//do nothing
		} else if t.allProxyFail(succ, fail) {
			// 全失败，服务宕机
			//指数减小窗口
			t.cwnd >>= 1
			if t.cwnd < t.failLimit {
				t.cwnd = t.failLimit
			}
			//} else if fail < t.failLimit && succ >= t.failLimit {
		} else {
			// proxy模式下的flowtoken有个问题：
			// 当窗口快要关闭时，比如cwnd=4，此时有2个请求成功
			//2个请求失败，窗口马上会回到最大值，不会在当前窗口停留
			//FIXME
			// 全成功(偶尔失败也算)，快速启动算法
			if succ >= int64(float64(t.ssthresh)*t.GetTriggerSuccrate()) || er > 0 {
				// 成功数超过承受压力指数，表示可承受压力上涨，指数级别放大
				if succ < t.initTokenNum {
					if t.ssthresh < math.MaxInt32 {
						if t.ssthresh < t.initTokenNum {
							t.ssthresh += t.initTokenNum
						} else {
							t.ssthresh += succ
						}
					}
				} else {
					if t.ssthresh < math.MaxInt32 {
						t.ssthresh += succ
					}
				}
			}
			t.cwnd = t.ssthresh
			//} else if fail >= t.failLimit {
		}

		t.tokenNum = t.cwnd
		t.lastTimeSec = currSec

		return osn
	}

	return nil
}

func (t *Fuse) initToken(currSec int64) *Snapshot {
	if t.isProxy {
		return t.initProxyToken(currSec)
	}
	//currSec := time.Now().Unix()
	if currSec > t.lastTimeSec { // 新的一秒开始
		osn := &Snapshot{
			ClientId:     t.clientId,
			LastTimeSec:  t.lastTimeSec,
			TokenNum:     t.tokenNum,
			Cwnd:         t.cwnd,
			Ssthresh:     t.ssthresh,
			RequestCount: t.requestCount,
			SuccCount:    t.succCount,
			FailCount:    t.failCount,
			ErrorCount:   t.errorCount,
		}

		succ := t.succCount
		fail := t.failCount
		er := t.errorCount
		t.requestCount = 0
		t.succCount = 0
		t.failCount = 0
		t.errorCount = 0
		if t.lastTimeSec == 0 {
			// 刚启动
			t.cwnd = t.initTokenNum
		} else if t.allFail(succ, fail) {
			// 全失败，服务宕机
			//指数减小窗口
			t.cwnd >>= 1
			if t.cwnd < t.failLimit {
				t.cwnd = t.failLimit
			}
			//} else if fail < t.failLimit && succ >= t.failLimit {
		} else if t.allSucc(succ, fail) {
			// 全成功(偶尔失败也算)，快速启动算法
			if succ >= int64(float64(t.ssthresh)*t.GetTriggerSuccrate()) || er > 0 {
				// 成功数超过承受压力指数，表示可承受压力上涨，指数级别放大
				if succ < t.initTokenNum {
					if t.ssthresh < math.MaxInt32 {
						if t.ssthresh < t.initTokenNum {
							t.ssthresh += t.initTokenNum
						} else {
							t.ssthresh += succ
						}
					}
				} else {
					if t.ssthresh < math.MaxInt32 {
						t.ssthresh += succ
					}
				}
			}
			t.cwnd = t.ssthresh
			//} else if fail >= t.failLimit {
		} else if t.partFail(succ, fail) {
			// fail >= FAIL_LIMIT: 有成功也有失败请求，并且失败数较多，达到服务或网络可承受压力临界点
			//按失败比率减小
			/*
				total := succ + fail
				succRate := float64(succ) / float64(total)
				t.ssthresh = int64(float64(total) * succRate)
				t.cwnd = t.ssthresh
			*/

			//按失败次数减小
			if fail >= succ {
				// 失败占多数，指数减小滑动窗口
				t.cwnd >>= 1
			} else {
				// 小幅度波动，线性减小活动窗口，此时承受压力具有参考价值
				t.cwnd -= fail
				if t.ssthresh > succ {
					t.ssthresh = succ
				}
			}

			// 不是宕机，而是抖动，一次太少了，保证每秒有3次试探式的请求控制
			// 下次可能偶尔成功，可能全失败
			if t.cwnd < t.failLimit {
				t.cwnd = t.failLimit
			}
		} else if succ > 0 {
			// 偶尔成功，请求量太少了，但成功了1,2次
			// 如果cwnd也很小，但只要有成功，就设置临界点，期望下次能用上全成功，或者网络抖动又回来了
			if t.cwnd <= t.failLimit {
				t.cwnd = t.failLimit + succ
			}
			// else 如果cwnd也不小，要么超时居然没反馈，要么都被熔断了，先不管了
		}
		// else 没有成功，且fail=1,2次，属于窗口较小情况，或者请求本来就少

		if t.tokenNumMax > 0 && t.cwnd > t.tokenNumMax {
			t.tokenNum = t.tokenNumMax
		} else {
			t.tokenNum = t.cwnd
		}
		t.lastTimeSec = currSec

		return osn
	}

	return nil
}

func (fuse *Fuse) GetToken() (*Token, error) {
	currSec := time.Now().Unix()
	faillimit, osn, nsn, ft, err := fuse.getDetail(currSec)
	if err != nil {
		return nil, err
	}
	if osn != nil {
		//free log printing from locks
		if osn.Cwnd <= faillimit && nsn.Cwnd > faillimit {
			fmt.Printf(log_format, "startup", nsn.ClientId, currSec, osn.Cwnd, nsn.Cwnd, nsn.Ssthresh, osn.RequestCount, osn.SuccCount, osn.FailCount, osn.ErrorCount)
		} else if nsn.Cwnd <= faillimit && osn.ErrorCount > 0 {
			if fuse.DoNotLogError {
				fmt.Printf(log_format, "shutdown", nsn.ClientId, currSec, osn.Cwnd, nsn.Cwnd, nsn.Ssthresh, osn.RequestCount, osn.SuccCount, osn.FailCount, osn.ErrorCount)
			} else {
				log.WarnT("FLOWTOKEN_SHUTDOWN-"+nsn.ClientId, log_format, "shutdown", nsn.ClientId, currSec, osn.Cwnd, nsn.Cwnd, nsn.Ssthresh, osn.RequestCount, osn.SuccCount, osn.FailCount, osn.ErrorCount)
			}
		} else {
			fmt.Printf(log_format, "running", nsn.ClientId, currSec, osn.Cwnd, nsn.Cwnd, nsn.Ssthresh, osn.RequestCount, osn.SuccCount, osn.FailCount, osn.ErrorCount)
		}
	}
	return ft, err
}

func (t *Fuse) SetTokenNumMax(num int64) {
	t.l.Lock()
	defer t.l.Unlock()
	t.tokenNumMax = num
}

//这个方法一般是用户手工触发的（想看一下滑动窗口当前的状态），不会频繁调用
//所以暂时不加读写锁了，直接加锁
//TODO read write lock?
func (t *Fuse) GetSnapshot() *Snapshot {
	t.l.Lock()
	defer t.l.Unlock()
	return &Snapshot{
		ClientId:     t.clientId,
		LastTimeSec:  t.lastTimeSec,
		TokenNum:     t.tokenNum,
		Cwnd:         t.cwnd,
		Ssthresh:     t.ssthresh,
		RequestCount: t.requestCount,
		SuccCount:    t.succCount,
		FailCount:    t.failCount,
		ErrorCount:   t.errorCount,
	}
}

func (t *Fuse) getDetail(currSec int64) (int64, *Snapshot, *Snapshot, *Token, error) {
	t.l.Lock()
	defer t.l.Unlock()
	oldSnapshot := t.initToken(currSec)
	t.requestCount++
	if t.tokenNum <= 0 {
		t.errorCount++
		sn := &Snapshot{
			ClientId:     t.clientId,
			LastTimeSec:  t.lastTimeSec,
			TokenNum:     t.tokenNum,
			Cwnd:         t.cwnd,
			Ssthresh:     t.ssthresh,
			RequestCount: t.requestCount,
			SuccCount:    t.succCount,
			FailCount:    t.failCount,
			ErrorCount:   t.errorCount,
		}
		return 0, nil, nil, nil, &FuseError{
			Snapshot: sn,
			Msg:      t.clientId + " trigger fuse",
		}
	}
	t.tokenNum--
	token := &Token{
		TokenNum:  t.tokenNum,
		flowToken: t,
	}

	var newSnapshot *Snapshot
	if oldSnapshot != nil {
		newSnapshot = &Snapshot{
			ClientId:     t.clientId,
			LastTimeSec:  t.lastTimeSec,
			TokenNum:     t.tokenNum,
			Cwnd:         t.cwnd,
			Ssthresh:     t.ssthresh,
			RequestCount: t.requestCount,
			SuccCount:    t.succCount,
			FailCount:    t.failCount,
			ErrorCount:   t.errorCount,
		}
	}

	return t.failLimit, oldSnapshot, newSnapshot, token, nil
}

func (t *Fuse) ReportSucc() {
	t.reportSucc()
}

func (t *Fuse) reportSucc() {
	t.l.Lock()
	defer t.l.Unlock()
	t.succCount++
	//tokenNum++倾向于尽可能地放过更多的请求
	//去掉此行，flowtoken会严格地按tokenNum过滤每秒的请求
	if t.passAsManyAsPossible {
		t.tokenNum++
	}
}

func (t *Fuse) ReportFail() {
	t.reportFail()
}

func (t *Fuse) reportFail() {
	t.l.Lock()
	defer t.l.Unlock()
	t.failCount++
}

func (t *Token) Succ() {
	t.flowToken.reportSucc()
}

func (t *Token) Fail() {
	t.flowToken.reportFail()
}


//百分比，小数点以后4个0
const (
	percent_base     = float64(1000000)
	percent_base_int = 1000000
)

