package server

import (
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/vlaner/suite/broker"
)

type TcpServer struct {
	addr      string
	l         net.Listener
	wg        sync.WaitGroup
	quit      chan interface{}
	clients   map[net.Conn]*Client
	exchange  *broker.Exchange
	clientIds int
}

func NewTcpServer(addr string, exchange *broker.Exchange) *TcpServer {
	return &TcpServer{
		addr:      addr,
		wg:        sync.WaitGroup{},
		quit:      make(chan interface{}),
		clients:   make(map[net.Conn]*Client),
		exchange:  exchange,
		clientIds: 0,
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
		s.clientIds++
		client := NewClient(s.clientIds, conn, -1, s.exchange)
		s.clients[conn] = client

		s.wg.Add(1)
		go func() {
			s.handleConn(conn, s.clientIds)
			defer s.wg.Done()
		}()
	}
}

func (s *TcpServer) handleConn(conn net.Conn, id int) {
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

			log.Printf("received from %v: %s, ID: %d", conn.RemoteAddr(), string(buf[:n]), id)

			command := string(buf[:n])
			if strings.HasPrefix(command, "consume ") {
				c := s.getClientById(id)
				c.makeConsumer()
				topic := strings.Split(command, " ")[1]
				s.exchange.Subscribe(broker.Topic(topic), c)
			}

			if strings.HasPrefix(command, "producer ") {
				c := s.getClientById(id)
				c.makeProducer()
				topic := strings.Split(command, " ")[1]
				s.exchange.Subscribe(broker.Topic(topic), c)
			}

			if strings.HasPrefix(command, "publish ") {
				c := s.getClientById(id)
				topic := strings.Split(command, " ")[1]
				data := strings.Split(command, " ")[2:]

				var payload []byte
				for _, d := range data {
					payload = append(payload[:], []byte(d)...)
				}

				c.Publish(broker.Topic(topic), payload)
			}
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
