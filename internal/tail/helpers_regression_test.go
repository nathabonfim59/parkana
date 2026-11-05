package tail

import (
	"testing"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Two websocket dials may schedule their upstream subscriptions in either
// order. waitSubscribe must retain events for tenants it was not asked
// about; consuming and dropping them makes a later wait time out.
func TestWaitSubscribeRetainsOutOfOrderTenantEvents(t *testing.T) {
	t1 := tenant.ID("tenant-one")
	t2 := tenant.ID("tenant-two")
	for _, tc := range []struct {
		name    string
		arrival []tenant.ID
		waits   []tenant.ID
	}{
		{"arrival t2,t1 waits t1,t2", []tenant.ID{t2, t1}, []tenant.ID{t1, t2}},
		{"arrival t1,t2 waits t2,t1", []tenant.ID{t1, t2}, []tenant.ID{t2, t1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeIngester(t, fakeIngesterOptions{})
			for _, id := range tc.arrival {
				f.tenants <- id
			}
			for _, id := range tc.waits {
				f.waitSubscribe(t, id)
			}

			// Each event is claimed exactly once: nothing stays pending
			// and the channel holds no event a wait already consumed.
			f.mu.Lock()
			n := len(f.pending)
			f.mu.Unlock()
			if n != 0 {
				t.Fatalf("%d subscription event(s) still pending", n)
			}
			select {
			case id := <-f.tenants:
				t.Fatalf("subscription event for %q consumed more than once", id)
			default:
			}
		})
	}
}

// A wait for a tenant with no subscription event must not consume events
// belonging to other tenants; they stay pending and remain claimable.
func TestWaitSubscribeKeepsNonmatchingEventsPending(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	t1 := tenant.ID("tenant-one")
	t2 := tenant.ID("tenant-two")

	f.tenants <- t2
	if f.claimSubscribe(t1) {
		t.Fatalf("claimed %q's subscription event for %q", t2, t1)
	}
	f.tenants <- t1
	f.waitSubscribe(t, t1)
	f.waitSubscribe(t, t2)
}

// A tenant may subscribe more than once (reconnects); every event stays
// pending until a wait claims it, so counts are not collapsed.
func TestWaitSubscribeCountsRepeatedTenantEvents(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	id := tenant.ID("tenant-x")

	f.tenants <- id
	f.tenants <- id
	f.waitSubscribe(t, id)
	f.waitSubscribe(t, id)
	if f.claimSubscribe(id) {
		t.Fatalf("claimed a third %q subscription that never happened", id)
	}
}
