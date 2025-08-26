package server

import (
	"github.com/gorilla/websocket"
	"time"
)

type ClientItem struct {
	conn *websocket.Conn
}

// 创建新的 ClientItem
func NewClientItem(conn *websocket.Conn) *ClientItem {
	return &ClientItem{conn: conn}
}

// 向客户端发送数据
func (c *ClientItem) SendData(data []byte) {
	err := c.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		PrintLog("向客户端", c.conn.RemoteAddr(), "发送数据失败:", err)
	}
}

// 设置读取超时时间
func (c *ClientItem) SetReadTimeout(timeout time.Duration) {
	c.conn.SetReadDeadline(time.Now().Add(timeout))
}

// 设置写入超时时间
func (c *ClientItem) SetWriteTimeout(timeout time.Duration) {
	c.conn.SetWriteDeadline(time.Now().Add(timeout))
}