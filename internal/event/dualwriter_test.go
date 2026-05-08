package event

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// fakeEventer 是测试用 Eventer：可注入 err，统计调用次数。
//
// 不放在生产代码里 — DualWriter 用 Eventer 接口，测试侧自由替换。
type fakeEventer struct {
	err   error
	calls atomic.Int64
}

func (f *fakeEventer) Enqueue(_ context.Context, _ ClickEvent) error {
	f.calls.Add(1)
	return f.err
}

func TestDualWriter_PrimaryAndSecondaryBothOK(t *testing.T) {
	primary := &fakeEventer{}
	secondary := &fakeEventer{}
	dw := NewDualWriter(primary, secondary)

	if err := dw.Enqueue(context.Background(), ClickEvent{}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := primary.calls.Load(); got != 1 {
		t.Errorf("primary calls = %d, want 1", got)
	}
	if got := secondary.calls.Load(); got != 1 {
		t.Errorf("secondary calls = %d, want 1", got)
	}
	if got := dw.BothFailed(); got != 0 {
		t.Errorf("BothFailed = %d, want 0", got)
	}
}

func TestDualWriter_PrimaryFailSecondaryOK(t *testing.T) {
	primaryErr := errors.New("kafka: timeout")
	primary := &fakeEventer{err: primaryErr}
	secondary := &fakeEventer{}
	dw := NewDualWriter(primary, secondary)

	// primary 失败仍要调 secondary —— 决策稿 §8.1 互不影响
	if err := dw.Enqueue(context.Background(), ClickEvent{}); err != nil {
		t.Fatalf("expected nil error (secondary OK), got %v", err)
	}
	if got := primary.calls.Load(); got != 1 {
		t.Errorf("primary calls = %d, want 1", got)
	}
	if got := secondary.calls.Load(); got != 1 {
		t.Errorf("secondary calls = %d, want 1 (must run despite primary fail)", got)
	}
	if got := dw.BothFailed(); got != 0 {
		t.Errorf("BothFailed = %d, want 0 (only primary failed)", got)
	}
}

func TestDualWriter_PrimaryOKSecondaryFail(t *testing.T) {
	secondaryErr := ErrBufferFull
	primary := &fakeEventer{}
	secondary := &fakeEventer{err: secondaryErr}
	dw := NewDualWriter(primary, secondary)

	if err := dw.Enqueue(context.Background(), ClickEvent{}); err != nil {
		t.Fatalf("expected nil error (primary OK), got %v", err)
	}
	if got := primary.calls.Load(); got != 1 {
		t.Errorf("primary calls = %d, want 1", got)
	}
	if got := secondary.calls.Load(); got != 1 {
		t.Errorf("secondary calls = %d, want 1", got)
	}
	if got := dw.BothFailed(); got != 0 {
		t.Errorf("BothFailed = %d, want 0 (only secondary failed)", got)
	}
}

func TestDualWriter_BothFailReturnsPrimaryError(t *testing.T) {
	primaryErr := errors.New("kafka: broker unreachable")
	secondaryErr := ErrBufferFull
	primary := &fakeEventer{err: primaryErr}
	secondary := &fakeEventer{err: secondaryErr}
	dw := NewDualWriter(primary, secondary)

	err := dw.Enqueue(context.Background(), ClickEvent{})
	if err == nil {
		t.Fatal("expected non-nil error when both fail")
	}
	// 决策稿 §8.1：双失败时返回 primary error（信息更具体）
	if !errors.Is(err, primaryErr) {
		t.Errorf("expected primary error %v, got %v", primaryErr, err)
	}
	if got := dw.BothFailed(); got != 1 {
		t.Errorf("BothFailed = %d, want 1", got)
	}
}

func TestDualWriter_NilPanicsAtConstruction(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil primary, got none")
		}
	}()
	NewDualWriter(nil, &fakeEventer{})
}

func TestDualWriter_NilSecondaryPanicsAtConstruction(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil secondary, got none")
		}
	}()
	NewDualWriter(&fakeEventer{}, nil)
}

func TestDualWriter_BothFailedCounterAccumulates(t *testing.T) {
	primary := &fakeEventer{err: errors.New("e1")}
	secondary := &fakeEventer{err: errors.New("e2")}
	dw := NewDualWriter(primary, secondary)

	for i := 0; i < 5; i++ {
		_ = dw.Enqueue(context.Background(), ClickEvent{})
	}
	if got := dw.BothFailed(); got != 5 {
		t.Errorf("BothFailed = %d, want 5", got)
	}
}
