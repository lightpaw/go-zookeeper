package zk

import (
	"errors"
	. "github.com/onsi/gomega"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tests session expiration notify

func GetEventState(e Event) State {
	return e.State
}

func TestConn_Reconnect(t *testing.T) {
	RegisterTestingT(t)
	timeout := time.Millisecond * 500
	ts, err := StartTestCluster(3, nil, logWriter{t: t, p: "[ZKERR] "})
	Ω(err).Should(Succeed())
	defer ts.Stop()

	conn, eventChan, err := ts.ConnectWithOptions(timeout, WithDialer(stopDialer), WithSessionExpireAndQuit())
	Ω(err).Should(Succeed())

	Ω(receive(eventChan, 1*time.Second)).Should(WithTransform(GetEventState, Equal(StateConnecting)))
	Ω(receive(eventChan, 1*time.Second)).Should(WithTransform(GetEventState, Equal(StateConnected)))
	Ω(receive(eventChan, 1*time.Second)).Should(WithTransform(GetEventState, Equal(StateHasSession)))
	Ω(eventChan).ShouldNot(Receive())

	sessionID := conn.SessionID()

	Ω(sessionID).Should(BeNumerically(">", 0))
	timeout = time.Duration(conn.sessionTimeoutMs) * time.Millisecond

	// simulate link fail, should timeout

	conn.conn.(*stopReadWriteCloser).stop()

	Ω(receive(eventChan, timeout)).Should(WithTransform(GetEventState, Equal(StateDisconnected)))
	Ω(receive(eventChan, timeout)).Should(WithTransform(GetEventState, Equal(StateConnecting)))
	Ω(receive(eventChan, timeout)).Should(WithTransform(GetEventState, Equal(StateConnected)))
	Ω(receive(eventChan, timeout)).Should(WithTransform(GetEventState, Equal(StateHasSession)))

	Ω(conn.SessionID()).Should(Equal(sessionID), "reconnect should have same session id")
}

func TestConn_Expire(t *testing.T) {
	RegisterTestingT(t)

	timeout := time.Millisecond * 500
	ts, err := StartTestCluster(1, nil, logWriter{t: t, p: "[ZKERR] "})
	Ω(err).Should(Succeed())
	defer ts.Stop()

	conn, eventChan, err := ts.ConnectWithOptions(timeout, WithDialer(stopDialer), WithSessionExpireAndQuit())
	Ω(err).Should(Succeed())

	Ω(receive(eventChan, 1*time.Second)).Should(WithTransform(GetEventState, Equal(StateConnecting)))
	Ω(receive(eventChan, 1*time.Second)).Should(WithTransform(GetEventState, Equal(StateConnected)))
	Ω(receive(eventChan, 1*time.Second)).Should(WithTransform(GetEventState, Equal(StateHasSession)))
	Eventually(conn.SessionID, 1*time.Second).Should(BeNumerically(">", 0))
	timeout = time.Duration(conn.sessionTimeoutMs) * time.Millisecond

	// close server
	ts.Stop()

	Ω(receive(eventChan, timeout)).Should(WithTransform(GetEventState, Equal(StateDisconnected)))
	Eventually(eventChan, timeout).Should(Receive(WithTransform(GetEventState, Equal(StateExpired))))

	Eventually(conn.shouldQuit, timeout).Should(BeClosed())
	Eventually(conn.eventChan, 2*time.Second).Should(BeClosed())
}

func receive(ch <-chan Event, timeout time.Duration) (Event, error) {
	select {
	case result := <-ch:
		return result, nil
	case <-time.After(timeout):
		return Event{}, errors.New("receive channel timeout")
	}
}

func stopDialer(network, address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return conn, err
	}

	return &stopReadWriteCloser{Conn: conn}, nil
}

type stopReadWriteCloser struct {
	net.Conn

	stopped int32
	closed  int32

	timeoutLock  sync.RWMutex
	readTimeout  time.Time
	writeTimeout time.Time
}

func (s *stopReadWriteCloser) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) == 1
}

func (s *stopReadWriteCloser) isClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

func (s *stopReadWriteCloser) stop() {
	atomic.StoreInt32(&s.stopped, 1)
}

func (s *stopReadWriteCloser) Read(p []byte) (int, error) {
	for s.isStopped() && !s.isClosed() {

		s.timeoutLock.RLock()
		if s.readTimeout.After(time.Now()) {
			s.timeoutLock.RUnlock()
			return 0, errors.New("read timeout")
		}
		s.timeoutLock.RUnlock()
		time.Sleep(time.Second)
	}

	if s.isClosed() {
		return 0, io.ErrClosedPipe
	}

	return s.Conn.Read(p)
}

func (s *stopReadWriteCloser) Write(p []byte) (int, error) {
	for s.isStopped() && !s.isClosed() {
		s.timeoutLock.RLock()
		if s.writeTimeout.After(time.Now()) {
			s.timeoutLock.RUnlock()
			return 0, errors.New("write timeout")
		}
		s.timeoutLock.RUnlock()
		time.Sleep(time.Second)
	}

	if s.isClosed() {
		return 0, io.ErrClosedPipe
	}

	return s.Conn.Write(p)
}

func (s *stopReadWriteCloser) Close() error {
	atomic.StoreInt32(&s.closed, 1)
	return s.Conn.Close()
}

func (s *stopReadWriteCloser) SetDeadline(t time.Time) error {
	s.timeoutLock.Lock()
	s.readTimeout = t
	s.writeTimeout = t
	s.timeoutLock.Unlock()
	return s.Conn.SetDeadline(t)
}

func (s *stopReadWriteCloser) SetReadDeadline(t time.Time) error {
	s.timeoutLock.Lock()
	s.readTimeout = t
	s.timeoutLock.Unlock()
	return s.Conn.SetReadDeadline(t)
}

func (s *stopReadWriteCloser) SetWriteDeadline(t time.Time) error {
	s.timeoutLock.Lock()
	s.writeTimeout = t
	s.timeoutLock.Unlock()
	return s.Conn.SetWriteDeadline(t)
}
