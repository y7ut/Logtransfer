package entity

import (
	"log"
	"runtime"
	"sync"
	"time"
)

var life sync.Map

// 每个worker channel 最多可以阻塞的任务个数
var workerChanCap = func() int {
	// Use blocking workerChan if GOMAXPROCS=1.
	// This immediately switches Serve to WorkerFunc, which results
	// in higher performance (under go1.5 at least).
	//使用阻塞的workerChan if GOMAXPROCS = 1，
	//这会立即将Serve切换到WorkerFunc
	//在更高的性能(至少在go1.5下)。
	if runtime.GOMAXPROCS(0) == 1 {
		return 0
	}
	// Use non-blocking workerChan if GOMAXPROCS>1,
	// since otherwise the Serve caller (Acceptor) may lag accepting
	// new connections if WorkerFunc is CPU-bound.
	//使用非阻塞的workerChan if GOMAXPROCS>1，
	//否则服务调用方(接收方)可能会延迟接收
	//如果WorkerFunc是cpu绑定的新连接。
	return 1
}()

type ESWorkPool struct {
	WorkerFunc            func(matedatas []*Matedata) bool // Work的处理逻辑
	MaxWorkerCount        int                           // 最大Worker数量
	MaxIdleWorkerDuration time.Duration                 // Worker的最大空闲时间
	Lock                  sync.Mutex
	WorkerCount           int           // Work的数量
	mustStop              bool          // 停止的标志  用于通知 workerchan 的 ch!!(他们一旦进入工作，就要等干完了 才去能去获取这个标志)
	readyToWork           []*workerChan // 一个存储WorkerChan类似栈的FILO（现进后出）队列, 成员是指针
	stopChannel           chan struct{} // 结束信号的接收与发送channel
	workChanPool          sync.Pool     // 对象池
}

type workerChan struct {
	lastUseTime time.Time      // 上次工作时间
	ch          chan []*Matedata // 接收工作内容的channel
}

var Wait sync.WaitGroup

func (wp *ESWorkPool) Start() {
	// 创建一个停止信号Channel
	wp.stopChannel = make(chan struct{})
	stopCh := wp.stopChannel
	wp.workChanPool.New = func() interface{} {
		// 手动去判断 如果取出来的是个nil
		// 那就一个 worker Channel 的指针 这里 vch 还是 interface{}
		return &workerChan{
			ch: make(chan []*Matedata, workerChanCap),
		}
	}
	// 启动协程
	go func() {
		var scrath []*workerChan
		for {
			// 清理未使用的时间超过 最大空闲时间的WorkerChan
			// 不干活的就得死！
			wp.clean(&scrath)
			// 每隔一段时间检查一次 去进行清理操作，直到下班
			select {
			case <-stopCh:
				return
			default:
				time.Sleep(wp.MaxIdleWorkerDuration)
			}
		}
	}()
}

func (wp *ESWorkPool) Stop() {
	// 关闭 并移除stopChannel, 下班！
	close(wp.stopChannel)
	wp.stopChannel = nil

	// 关闭全部的reday slice中的全部WorkChan,并清空Ready
	wp.Lock.Lock()
	ready := wp.readyToWork // 获取当前全部的WorkChannel
	// 通知他们下班结束了
	for i, ch := range ready {

		ch.ch <- nil
		ready[i] = nil
	}
	// 清空WorkChannel
	wp.readyToWork = ready[:0]
	// 设置已经停止的标志
	wp.mustStop = true
	wp.Lock.Unlock()

}

func (wp *ESWorkPool) Serve(matedates []*Matedata) bool {
	// 获取可用的WorkerChan
	ch := wp.getCh()
	// 若果没有 就返回失败
	if ch == nil {
		return false
	}
	// 发送任务到workerChan
	ch.ch <- matedates
	return true
}

// 获取可用的workerChan
func (wp *ESWorkPool) getCh() *workerChan {
	var ch *workerChan
	// 默认是不需要重新创建Worker的
	createWorker := false
	wp.Lock.Lock()
	// 获取WorkerChan的Slice  全部的工作同道
	ready := wp.readyToWork
	// 获取Slice中元素的数量
	n := len(ready) - 1
	if n < 0 {
		// 若可用的数量为0, 有两种情况
		if wp.WorkerCount < wp.MaxWorkerCount {
			// 当前工作channerl的数量还没有达到最大worker Channerl 的数量
			// 所有的已经注册的worker都去工作了，
			// 这是一种ch为nil的情况 (1)
			createWorker = true
			wp.WorkerCount++
		}
		// 已经满了，不能再注册worker了， 并且已有workerChan 都分配出去了
		// 第二种ch为nil的情况 （2）
	} else {
		// 有可用的WorkerChan name就取slice的最后一个
		// 将ReadyToWork 移除出最后一个WorkChan
		// 这里可以看出来，拿出去干活的人， 就不在ReadytoWork的slice中了
		ch = ready[n]
		ready[n] = nil
		wp.readyToWork = ready[:n]
	}
	wp.Lock.Unlock()

	// 获取不到ch的时候
	if ch == nil {
		// 如果是第二种情况，也就是说满了的话，那就只能空手而归了，返回一个nil
		if !createWorker {
			return nil
		}
		// 若是第一种，可以新初始化一个worker Channel
		// 我们从对象池中获取一个
		// 这里之所以不 var 一个新的 worker Channel 是因为
		// 频繁地分配、回收内存会给 GC 带来一定的负担，而 sync.Pool 可以将暂时不用的对象缓存起来
		// 待下次需要的时候直接使用，不用再次经过内存分配，复用对象的内存，减轻 GC 的压力，提升系统的性能。
		vch := wp.workChanPool.Get()
		// 可以的 提前给Channel POOL 一个NEW方法
		// 把vch再转换为 worker Channel 的指针 这种类型
		ch = vch.(*workerChan)

		now, _ := life.Load("new")
		change := 1
		if now != nil {
			change = now.(int) + 1
		}
		life.Store("new", change)


		// 在协程中 去开启这个worker 的ch
		go func() {
			// 阻塞的去接收 worker Channel 的工作内容
			wp.workerFunc(ch)
			// 然后把这个 vch （注意不是ch 是interface类型的里面是 worker Channel 指针）放回sync.Pool中
			wp.workChanPool.Put(vch)
		}()

	}
	// 顺利返回workerchan
	return ch
}

func (wp *ESWorkPool) workerFunc(ch *workerChan) {
	var matedatas []*Matedata
	// 阻塞等待 woker chan 中的接收 job的 ch收到 job
	for matedatas = range ch.ch {
		// 有人close 了他 或者发送nil了 就可以 退出了
		if matedatas == nil {

			now, _ := life.Load("die")
			change := 1
			if now != nil {
				change = now.(int) + 1
			}
			life.Store("die", change)

			break
		}
		// do the job!!!!
		if wp.WorkerFunc(matedatas) {
			// 完事了！
			matedatas = matedatas[:0]
		}
		// 去释放这个workerChan
		// 因为他在干活的时候是不知道外面发生了什么的
		// 发现wp 已经都must stop了 那就直接下班了
		if !wp.release(ch) {
			break
		}
	}
	// 退出的时候 顺手给工作自己的存在注销掉
	wp.Lock.Lock()
	count := wp.WorkerCount
	wp.WorkerCount--
	wp.Lock.Unlock()
	log.Println("当前Worker还剩:", (count - 1))
}

func (wp *ESWorkPool) release(ch *workerChan) bool {
	// 记录一下时间
	ch.lastUseTime = time.Now()
	wp.Lock.Lock()
	// 若wp已经退出了 返回false
	if wp.mustStop {
		// 若worker Pool都结束了，就直接下班了
		wp.Lock.Unlock()
		return false
	}
	// 不然还是要放回去的， 放回ready to work slice中
	wp.readyToWork = append(wp.readyToWork, ch)
	wp.Lock.Unlock()
	return true
}

// 定期的去清理长时间不用的 不干活的 因为活跃的都在后方 这个时候就需要砍掉前面的元素！
func (wp *ESWorkPool) clean(scratch *[]*workerChan) {
	maxIdleWorkerDuration := wp.MaxIdleWorkerDuration
	current := time.Now()

	wp.Lock.Lock()
	ready := wp.readyToWork
	n := len(ready)

	i := 0
	// 当前的workerChan 有空闲，并很久没用了
	for i < n && current.Sub(ready[i].lastUseTime) > maxIdleWorkerDuration {
		// 计数+1
		i++
	}

	*scratch = append((*scratch)[:0], ready[:i]...)
	// 去掉对应数量 不经常用的 他们在slice的前方 （别忘了 FILO）
	if i > 0 {
		m := copy(ready, ready[i:])

		for i = m; i < n; i++ {
			ready[i] = nil
		}
		wp.readyToWork = ready[:m]

	}
	wp.Lock.Unlock()

	// 最后别忘了 解散那个worker channel!!!
	tmp := *scratch

	for i := range tmp {
	
		tmp[i].ch <- nil
		tmp[i] = nil
	}
}
