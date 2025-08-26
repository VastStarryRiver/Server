package server

import (
	"net"
	"sync"
)

type ServerManager struct {
	serverSocket net.Listener
	clientItems  map[string]*ClientItem
	mu           sync.Mutex
}

var serverManager_instance *ServerManager
var once sync.Once

// 单例模式，保证只有一个 ServerManager 实例
func GetServerManager() *ServerManager {
	once.Do(func() {
		serverManager_instance = &ServerManager{
			clientItems: make(map[string]*ClientItem),
		}
	})
	return serverManager_instance
}

func (s *ServerManager) Start() {
	// 监听所有客户端请求
	listener, err := net.Listen("tcp", "0.0.0.0:2000")
	if err != nil {
		PrintLog("Error starting server:", err)
		return
	}
	s.serverSocket = listener
	PrintLog("服务器已启动，监听端口 2000")

	// 处理客户端连接
	go s.acceptConnections()
}

func (s *ServerManager) acceptConnections() {
	for {
		conn, err := s.serverSocket.Accept()
		if err != nil {
			PrintLog("Error accepting connection:", err)
			return
		}

		// 为每个连接创建新的 ClientItem
		client := NewClientItem(conn)
		s.mu.Lock()
		s.clientItems[client.conn.RemoteAddr().String()] = client
		s.mu.Unlock()

		PrintLog("客户端", client.conn.RemoteAddr(), "已连接")

		// 处理客户端的消息
		go s.handleClientMessages(client)
	}
}

func (s *ServerManager) handleClientMessages(client *ClientItem) {
	defer client.conn.Close()

	for {
		// 接收客户端消息
		buf := make([]byte, 1024)
		n, err := client.conn.Read(buf)
		if err != nil {
			PrintLog("客户端", client.conn.RemoteAddr(), "断开连接:", err)
			s.mu.Lock()
			delete(s.clientItems, client.conn.RemoteAddr().String())
			s.mu.Unlock()
			return
		}

		message := string(buf[:n])
		PrintLog("从客户端", client.conn.RemoteAddr(), "收到消息:", message)

		// 根据消息判断处理方式
		s.processMessage(message, client)
	}
}

func (s *ServerManager) processMessage(message string, client *ClientItem) {
	// 示例：如果消息包含 "|", 分割并处理
	if len(message) > 1 && message[1] == '|' {
		parts := splitMessage(message)
		if len(parts) == 2 {
			// 处理 id 查找逻辑
			if clientToSend, exists := s.clientItems[parts[0]]; exists {
				clientToSend.SendData([]byte(parts[1]))
			} else {
				client.SendData([]byte("没有对应id"))
			}
		}
	} else {
		client.SendData([]byte(message))
	}
}

func (s *ServerManager) Stop() {
	s.serverSocket.Close()
	PrintLog("服务器已关闭")
}