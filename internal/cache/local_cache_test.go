package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/zombiecd/slink/internal/model"
)

// localCache 是 LinkCache 的进程内 L1，
// 用 hashicorp/golang-lru/v2/expirable 做底层（容量 + TTL 一站式）。
//
// entry 语义：
//   - localEntry{link: l}  → 命中 link
//   - localEntry{link: nil} → 命中 missing 标记（DB 已确认不存在）
//
// 这些测试是纯单测，不依赖 Redis。

func TestLocalCache_PutGetHit(t *testing.T) {
	c := newLocalCache(8, time.Minute)
	link := &model.Link{Code: "abc", LongURL: "https://x.com"}
	c.Put("abc", localEntry{link: link})

	got, ok := c.Get("abc")
	if !ok {
		t.Fatalf("Get(abc): want hit, got miss")
	}
	if got.link != link {
		t.Errorf("Get(abc).link: got %p, want %p", got.link, link)
	}
}

func TestLocalCache_GetMiss(t *testing.T) {
	c := newLocalCache(8, time.Minute)
	if _, ok := c.Get("nope"); ok {
		t.Errorf("Get(nope): want miss, got hit")
	}
}

func TestLocalCache_MissingMarker(t *testing.T) {
	// missing 标记：localEntry{link: nil}
	c := newLocalCache(8, time.Minute)
	c.Put("ghost", localEntry{link: nil})

	got, ok := c.Get("ghost")
	if !ok {
		t.Fatalf("missing marker: want hit, got miss")
	}
	if got.link != nil {
		t.Errorf("missing marker: link should be nil, got %+v", got.link)
	}
}

func TestLocalCache_TTLExpire(t *testing.T) {
	// 100ms TTL → 等 200ms 应过期
	c := newLocalCache(8, 100*time.Millisecond)
	c.Put("k", localEntry{link: &model.Link{Code: "k"}})

	time.Sleep(200 * time.Millisecond)

	if _, ok := c.Get("k"); ok {
		t.Errorf("after TTL: want miss, got hit")
	}
}

func TestLocalCache_CapacityEvict(t *testing.T) {
	// size=2 → 放第 3 个时最久没用的应该被踢
	c := newLocalCache(2, time.Minute)
	c.Put("a", localEntry{link: &model.Link{Code: "a"}})
	c.Put("b", localEntry{link: &model.Link{Code: "b"}})
	// 让 a 变最近访问（b 变最久）
	_, _ = c.Get("a")
	c.Put("c", localEntry{link: &model.Link{Code: "c"}})

	if _, ok := c.Get("b"); ok {
		t.Errorf("b should be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Errorf("a should still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Errorf("c should be present")
	}
}

func TestLocalCache_Del(t *testing.T) {
	c := newLocalCache(8, time.Minute)
	c.Put("k", localEntry{link: &model.Link{Code: "k"}})
	c.Del("k")
	if _, ok := c.Get("k"); ok {
		t.Errorf("after Del: want miss, got hit")
	}
}

func TestLocalCache_Stats(t *testing.T) {
	c := newLocalCache(8, time.Minute)
	c.Put("k", localEntry{link: &model.Link{Code: "k"}})

	_, _ = c.Get("k")    // hit
	_, _ = c.Get("k")    // hit
	_, _ = c.Get("nope") // miss

	s := c.Stats()
	if s.Hits != 2 {
		t.Errorf("Hits: got %d, want 2", s.Hits)
	}
	if s.Misses != 1 {
		t.Errorf("Misses: got %d, want 1", s.Misses)
	}
}

func TestLocalCache_ConcurrentSafe(t *testing.T) {
	// 主要验证不 panic / 不 race（go test -race 会抓）
	c := newLocalCache(64, time.Minute)

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N * 2)

	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			key := "k" + string(rune('a'+i%26))
			c.Put(key, localEntry{link: &model.Link{Code: key}})
		}(i)
		go func(i int) {
			defer wg.Done()
			key := "k" + string(rune('a'+i%26))
			_, _ = c.Get(key)
		}(i)
	}
	wg.Wait()
}

// nil 配置 = 不启用：Get 永远 miss，Put 是 noop（避免 LinkCache 里到处 nil 检查）
func TestLocalCache_NilSafe(t *testing.T) {
	var c *localCache // nil
	c.Put("k", localEntry{link: &model.Link{Code: "k"}})
	if _, ok := c.Get("k"); ok {
		t.Errorf("nil cache: Get should always miss")
	}
	c.Del("k")           // 不 panic
	_ = c.Stats()        // 不 panic
}
