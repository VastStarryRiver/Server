package server

import (
	"net"
)

type ClientItem struct {
	conn net.Conn
}

// 创建新的 ClientItem
func NewClientItem(conn net.Conn) *ClientItem {
	return &ClientItem{conn: conn}
}

// 向客户端发送数据
func (c *ClientItem) SendData(data []byte) {
	_, err := c.conn.Write(data)
	if err != nil {
		PrintLog("向客户端", c.conn.RemoteAddr(), "发送数据失败:", err)
	}
}