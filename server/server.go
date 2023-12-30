package server

import (
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type TcpServer struct {
	addr string
	l    net.Listener
	wg   sync.WaitGroup
	quit chan interface{}
}

func NewTcpServer(addr string) *TcpServer {
	return &TcpServer{
		addr: addr,
		wg:   sync.WaitGroup{},
		quit: make(chan interface{}),
	}
}

func (s *TcpServer) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.l = l
	s.wg.Add(1)

	go s.acceptLoop()

	return nil
}

func (s *TcpServer) Stop() error {
	close(s.quit)
	err := s.l.Close()
	s.wg.Wait()
	return err
}

func (s *TcpServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.l.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Println("accept error: ", err)
			}
			continue
		}

		s.wg.Add(1)
		go func() {
			s.handleConn(conn)
			defer s.wg.Done()
		}()
	}
}

func (s *TcpServer) handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 2048)
ReadLoop:
	for {
		select {
		case <-s.quit:
			return
		default:
			conn.SetDeadline(time.Now().Add(200 * time.Millisecond))
			n, err := conn.Read(buf)
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue ReadLoop
				} else if err != io.EOF {
					log.Println("read error: ", err)
					return
				}
			}
			if n == 0 {
				return
			}

			log.Printf("received from %v: %s", conn.RemoteAddr(), string(buf[:n]))
		}
	}
}
