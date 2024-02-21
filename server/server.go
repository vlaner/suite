package server

import (
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vlaner/suite/broker"
	"github.com/vlaner/suite/database"
	"github.com/vlaner/suite/protocol"
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

	r := protocol.NewProtoReader(conn)
	w := protocol.NewProtoWriter(conn)
ReadLoop:
	for {
		select {
		case <-s.quit:
			return
		default:
			conn.SetDeadline(time.Now().Add(200 * time.Millisecond))
			protoVal, err := r.ParseInput()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue ReadLoop
				} else if err != io.EOF {
					log.Println("read error: ", err)
					return
				}
			}
			if protoVal == nil {
				return
			}

			log.Printf("received from %v: %+v, ID: %d", conn.RemoteAddr(), protoVal, id)

			command := protoVal.Array[0]
			if command.Str == "consume" {
				c := s.getClientById(id)
				c.makeConsumer()
				topic := protoVal.Array[1].Str
				s.exchange.Subscribe(broker.Topic(topic), c)
			}

			if command.Str == "producer" {
				c := s.getClientById(id)
				c.makeProducer()
			}

			if command.Str == "publish" {
				c := s.getClientById(id)
				topic := protoVal.Array[1].Str
				c.Publish(broker.Topic(topic), []byte(protoVal.Array[2].Str))
			}

			if command.Str == "ack" {
				c := s.getClientById(id)
				topic := protoVal.Array[1].Str
				msgId := protoVal.Array[2].Str
				c.Ack(broker.Topic(topic), uuid.MustParse(msgId))
			}

			if command.Str == "unsub" {
				c := s.getClientById(id)
				topic := protoVal.Array[1].Str
				c.Unsubscribe(broker.Topic(topic))
			}

			if command.Str == "get" {
				entry, err := s.database.Get([]byte(protoVal.Array[1].Str))
				if err != nil {
					if err := w.Write(protocol.Value{ValType: protocol.ERROR, Str: "key not found"}); err != nil {
						log.Println("error writing to connection", err)
						return
					}
					continue ReadLoop
				}
				if err := w.Write(protocol.Value{ValType: protocol.ARRAY, Array: []protocol.Value{
					{ValType: protocol.BINARY_STRING, Str: "realm"},
					{ValType: protocol.BINARY_STRING, Str: "database"},
					{ValType: protocol.BINARY_STRING, Str: string(entry.Key)},
					{ValType: protocol.BINARY_STRING, Str: string(entry.Value)},
				}}); err != nil {
					log.Println("error writing to connection", err)
					return
				}
			}

			if command.Str == "set" {
				key := protoVal.Array[1].Str
				value := protoVal.Array[2].Str

				err := s.database.Set([]byte(key), []byte(value))
				if err != nil {
					if err := w.Write(protocol.Value{ValType: protocol.ERROR, Str: "cannot set key"}); err != nil {
						log.Println("error writing to connection", err)
						return
					}

					continue ReadLoop
				}
			}

			if command.Str == "del" {
				key := protoVal.Array[1].Str
				err := s.database.Delete([]byte(key))
				if err != nil {
					if err := w.Write(protocol.Value{ValType: protocol.ERROR, Str: "cannot delete key"}); err != nil {
						log.Println("error writing to connection", err)
						return
					}
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
