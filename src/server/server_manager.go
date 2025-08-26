package server

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type ServerManager struct {
	clientItems  map[string]*ClientItem
	mu           sync.Mutex
	upgrader     websocket.Upgrader
}

var serverManager_instance *ServerManager
var once sync.Once

// 单例模式，保证只有一个 ServerManager 实例
func GetServerManager() *ServerManager {
	once.Do(func() {
		serverManager_instance = &ServerManager{
			clientItems: make(map[string]*ClientItem),
			upgrader: websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool {
					return true // 允许所有跨域请求
				},
			},
		}
	})
	return serverManager_instance
}

func (s *ServerManager) Start() {
	http.HandleFunc("/ws", s.handleWebSocket)
	PrintLog("WebSocket服务器已启动，监听端口 2000")
	
	err := http.ListenAndServe(":2000", nil)
	if err != nil {
		PrintLog("Error starting server:", err)
		return
	}
}

func (s *ServerManager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 升级HTTP连接到WebSocket
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		PrintLog("Error upgrading to WebSocket:", err)
		return
	}

	// 为每个连接创建新的 ClientItem
	client := NewClientItem(conn)
	clientAddr := conn.RemoteAddr().String()
	
	s.mu.Lock()
	s.clientItems[clientAddr] = client
	s.mu.Unlock()

	PrintLog("客户端", clientAddr, "已连接")

	// 处理客户端的消息
	go s.handleClientMessages(client)
}

func (s *ServerManager) handleClientMessages(client *ClientItem) {
	defer func() {
		client.conn.Close()
		s.mu.Lock()
		delete(s.clientItems, client.conn.RemoteAddr().String())
		s.mu.Unlock()
	}()

	for {
		// 接收客户端消息
		messageType, message, err := client.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				PrintLog("客户端", client.conn.RemoteAddr(), "断开连接:", err)
			}
			return
		}

		// 只处理文本消息
		if messageType == websocket.TextMessage {
			msg := string(message)
			PrintLog("从客户端", client.conn.RemoteAddr(), "收到消息:", msg)
			
			// 根据消息判断处理方式
			s.processMessage(msg, client)
		}
	}
}

func (s *ServerManager) processMessage(message string, client *ClientItem) {
	// 示例：如果消息包含 "|", 分割并处理
	if len(message) > 1 && message[1] == '|' {
		parts := splitMessage(message)
		if len(parts) == 2 {
			// 处理 id 查找逻辑
			s.mu.Lock()
			clientToSend, exists := s.clientItems[parts[0]]
			s.mu.Unlock()
			
			if exists {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 关闭所有客户端连接
	for _, client := range s.clientItems {
		client.conn.Close()
	}
	
	PrintLog("服务器已关闭")
}