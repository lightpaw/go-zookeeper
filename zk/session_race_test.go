// +build !race

package zk

import (
	. "github.com/onsi/gomega"
	"testing"
	"time"
)

// Contains a race to clear connection passwd
func TestConn_ChangePasswdAndDisconnect_MustExpire(t *testing.T) {
	RegisterTestingT(t)

	ts, err := StartTestCluster(1, nil, logWriter{t: t, p: "[ZKERR] "})
	Ω(err).Should(Succeed())
	defer ts.Stop()

	conn, eventChan, err := ts.ConnectWithOptions(500*time.Millisecond, WithDialer(stopDialer), WithSessionExpireAndQuit())
	Ω(err).Should(Succeed())

	go func() {
		for range eventChan {
			// consume events
		}
	}()

	Eventually(conn.SessionID, 3*time.Second).Should(BeNumerically(">", 0))

	// clear passwd
	for i := range conn.passwd {
		conn.passwd[i] = 0
	}

	// disconnect
	conn.conn.Close()

	Eventually(conn.shouldQuit, time.Duration(conn.sessionTimeoutMs*2)*time.Millisecond).Should(BeClosed())
}
