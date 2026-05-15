// Copyright 2026 The p9 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// race_repro_test.go reproduces the connState teardown race fixed in
// this commit. handleRequest at server.go:508 spawned an auxiliary
// `go cs.handleRequests()` without first calling pendingWg.Add(1).
// The Add only ran when the spawned goroutine entered handleRequest;
// in the scheduling window before that, cs.stop()'s pendingWg.Wait()
// could return with count = 0 and proceed to close cs.r/cs.t and walk
// cs.fids while the auxiliary handleRequests goroutine was still alive.
//
// The race detector flags it as a struct-field race; the underlying
// Go runtime can also panic with "sync: WaitGroup is reused before
// previous Wait has returned" when the spawned goroutine's late
// Add(1) lands after Wait has unblocked.
//
// Run me with `go test -race -count=200 ./p9/ -run TestRaceOnTeardown`.
// On busy schedulers (CI arm64 / Linux) the failure reliably triggers
// in a few hundred iterations against the unpatched server.

package p9_test

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/hugelgupf/p9/fsimpl/templatefs"
	"github.com/hugelgupf/p9/p9"
)

type noopAttacher struct{}

func (noopAttacher) Attach() (p9.File, error) { return &noopFile{}, nil }

type noopFile struct {
	templatefs.NoopFile
}

// Walk on the root must succeed so the client's Attach completes and
// the server hits the auxiliary-goroutine spawn at server.go:508.
func (n *noopFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &noopFile{}, nil
	}
	return nil, nil, nil
}

// TestRaceOnTeardown opens a client, performs one Attach round-trip,
// and tears the conn down. Repeat in parallel and the race detector
// flags the connState teardown race against the unpatched server.
func TestRaceOnTeardown(t *testing.T) {
	const conns = 32
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			oneConnAndClose(t)
		}()
	}
	wg.Wait()
}

func oneConnAndClose(t *testing.T) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := p9.NewServer(noopAttacher{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.ServeContext(ctx, lis)
	}()

	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c, err := p9.NewClient(conn)
	if err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}

	// One Attach round-trip forces the server-side handleRequest path
	// to reach the auxiliary-goroutine spawn at server.go:508.
	if f, err := c.Attach("/"); err == nil {
		_ = f.Close()
	}

	// Close the client. The server's recv loop sees EOF, returns from
	// handleRequests, and Handle's deferred cs.stop() runs while the
	// auxiliary goroutine from line 508 may not have been scheduled
	// yet — that's the race window.
	_ = c.Close()
	cancel()
	<-done
}
