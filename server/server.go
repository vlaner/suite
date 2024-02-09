package server

import (
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/vlaner/suite/broker"
)

type TcpServer struct {
	addr     string
	l        net.Listener
	wg       sync.WaitGroup
	quit     chan interface{}
	clients  map[net.Conn]*Client
	exchange *broker.Exchange
}

func NewTcpServer(addr string, exchange *broker.Exchange) *TcpServer {
	return &TcpServer{
		addr:     addr,
		wg:       sync.WaitGroup{},
		quit:     make(chan interface{}),
		clients:  make(map[net.Conn]*Client),
		exchange: exchange,
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

		client := NewClient(10, conn)
		s.clients[conn] = client

		s.wg.Add(1)
		go func() {
			s.handleConn(conn)
			defer s.wg.Done()
		}()
	}
}

func (s *TcpServer) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		delete(s.clients, conn)
	}()

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

func (s *TcpServer) getClientById(id int) *Client {
	for _, c := range s.clients {
		if c.id == id {
			return c
		}
	}
	return nil
}
