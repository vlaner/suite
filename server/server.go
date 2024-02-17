package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/vlaner/suite/broker"
	"github.com/vlaner/suite/database"
)

type TcpServer struct {
	addr      string
	l         net.Listener
	wg        sync.WaitGroup
	quit      chan interface{}
	clients   map[net.Conn]*Client
	mu        sync.RWMutex
	exchange  *broker.Exchange
	database  *database.Database
	clientIds int
}

func NewTcpServer(addr string, exchange *broker.Exchange, db *database.Database) *TcpServer {
	return &TcpServer{
		addr:      addr,
		wg:        sync.WaitGroup{},
		quit:      make(chan interface{}),
		clients:   make(map[net.Conn]*Client),
		exchange:  exchange,
		database:  db,
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
		client := NewClient(s.clientIds, conn, s.exchange)
		s.mu.Lock()
		s.clients[conn] = client
		s.mu.Unlock()

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
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
	}()

	r := bufio.NewReader(conn)
ReadLoop:
	for {
		select {
		case <-s.quit:
			return
		default:
			conn.SetDeadline(time.Now().Add(200 * time.Millisecond))
			b, _, err := r.ReadLine()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue ReadLoop
				} else if err != io.EOF {
					log.Println("read error: ", err)
					return
				}
			}
			if len(b) == 0 {
				return
			}

			log.Printf("received from %v: %s, ID: %d", conn.RemoteAddr(), string(b), id)

			command := string(b)
			if strings.HasPrefix(command, "consume ") {
				c := s.getClientById(id)
				c.makeConsumer()
				topic := strings.Split(command, " ")[1]
				s.exchange.Subscribe(broker.Topic(topic), c)
			}

			if strings.HasPrefix(command, "producer ") {
				c := s.getClientById(id)
				c.makeProducer()
			}

			if strings.HasPrefix(command, "publish ") {
				c := s.getClientById(id)
				topic := strings.Split(command, " ")[1]
				c.Publish(broker.Topic(topic), []byte(b[len("publish ")+len(topic)+1:]))
			}

			if strings.HasPrefix(command, "get ") {
				entry, err := s.database.Get(b[len("gen "):])
				if err != nil {
					conn.Write([]byte("key not found\n"))
					continue ReadLoop
				}
				conn.Write([]byte(fmt.Sprintf("key: %s; value: %s\n", entry.Key, entry.Value)))
			}

			if strings.HasPrefix(command, "set ") {
				key := strings.Split(command, " ")[1]
				value := []byte(b[len("set ")+len(key)+1:])

				err := s.database.Set([]byte(key), []byte(value))
				if err != nil {
					conn.Write([]byte("cannot set key\n"))
					continue ReadLoop
				}
			}

			if strings.HasPrefix(command, "del ") {
				key := strings.Split(command, " ")[1]
				err := s.database.Delete([]byte(key))
				if err != nil {
					conn.Write([]byte("cannot delete key\n"))
					continue ReadLoop
				}
			}
		}
	}
}

func (s *TcpServer) getClientById(id int) *Client {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, c := range s.clients {
		if c.id == id {
			return c
		}
	}
	return nil
}
