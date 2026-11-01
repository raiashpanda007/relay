package relay

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
)

type RelayServer struct {
	listener        net.Listener
	connectionArray map[string]net.Conn
}

func initRelayServer(listener net.Listener) *RelayServer {
	return &RelayServer{
		listener:        listener,
		connectionArray: make(map[string]net.Conn),
	}
}

func (r *RelayServer) NewConnection(conn net.Conn) (string, error) {
	id := uuid.New().String()
	r.connectionArray[id] = conn

	_, err := conn.Write([]byte(id))
	if err != nil {
		return id, err
	}
	return id, nil
}

func (r *RelayServer) RemoveConnection(id string) {
	delete(r.connectionArray, id)
}

func NewServer(socketPath string, msgSize int, onServer func(data ...any) any) (*RelayServer, error) {
	listener, err := net.Listen("unix", socketPath)
	var rlSvr *RelayServer
	if err != nil {
		return rlSvr, err
	}

	relayServer := initRelayServer(listener)

	defer listener.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		os.Remove(socketPath)
		os.Exit(0)
	}()

	onServer()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return relayServer, err
		}

		id, err := relayServer.NewConnection(conn)

		if err != nil {
			relayServer.RemoveConnection(id)
			return relayServer, err
		}

	}
}
