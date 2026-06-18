package server

import (
	"encoding/binary"
	"errors"
	"log"
	"net"
	"runtime"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	// STUN 头固定 20 字节：类型、长度、magic cookie 和 12 字节 transaction id。
	stunHeaderSize = 20
	// 当前只实现 Binding Request/Success Response，足够 WebRTC 获取公网映射地址。
	stunBindingRequest         = 0x0001
	stunBindingSuccessResponse = 0x0101
	// XOR-MAPPED-ADDRESS 是 RFC 5389 推荐返回的地址属性，浏览器 ICE 能直接识别。
	stunAttrXORMappedAddress = 0x0020
	// magic cookie 用于区分新版 STUN 报文，并参与 XOR 地址混淆。
	stunMagicCookie = 0x2112A442
	// stunRateLimit 限制单 IP 每秒 STUN 请求数，防止被用作 DDoS 反射器。
	stunRateLimit      = 20
	stunRateBurst      = 10
	stunLimiterTTL     = 5 * time.Minute
)

// STUNServer 是内置的最小 RFC 5389 Binding 服务。
// 它只告诉浏览器"服务端看到你的 UDP 来源 IP/端口是什么"，不记录房间密钥、不处理业务数据，也不承担 TURN relay。
type STUNServer struct {
	conn      net.PacketConn
	done      chan struct{}
	limiterMu sync.Mutex
	limiters  map[string]*stunLimiter
}

type stunLimiter struct {
	limiter *rate.Limiter
	lastUse time.Time
}

// StartSTUNServer 在与 HTTP/WS 相同的 addr 上启动 UDP STUN 监听。
// TCP 和 UDP 可以复用同一个端口号，因此 127.0.0.1:8787 能同时服务 HTTP/WS 和 STUN。
func StartSTUNServer(addr string) (*STUNServer, error) {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	server := &STUNServer{conn: conn, done: make(chan struct{}), limiters: make(map[string]*stunLimiter)}
	// 多 worker 共享同一个 PacketConn，利用多核并行处理 STUN 请求。
	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}
	for i := 0; i < workers; i++ {
		go server.serve()
	}
	// 定期清理过期的限流器。
	go server.cleanupLoop()
	log.Printf("stun listening udp=%s workers=%d", conn.LocalAddr(), workers)
	return server, nil
}

// Close 关闭 UDP 监听和限流器清理 goroutine；serve 循环会收到 net.ErrClosed 后自然退出。
func (s *STUNServer) Close() error {
	close(s.done)
	return s.conn.Close()
}

// cleanupLoop 定期清理超过 TTL 未使用的 IP 限流器，防止内存泄漏。
func (s *STUNServer) cleanupLoop() {
	ticker := time.NewTicker(stunLimiterTTL)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.limiterMu.Lock()
			now := time.Now()
			for ip, lim := range s.limiters {
				if now.Sub(lim.lastUse) > stunLimiterTTL {
					delete(s.limiters, ip)
				}
			}
			s.limiterMu.Unlock()
		}
	}
}

// serve 是 UDP 处理循环。STUN 请求很小，固定 1500 字节缓冲足以覆盖普通 MTU 内的 Binding Request。
func (s *STUNServer) serve() {
	buf := make([]byte, 1500)
	for {
		n, remote, err := s.conn.ReadFrom(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("stun read failed err=%v", err)
			}
			return
		}
		// IP 维度速率限制，防止被用作 DDoS 反射器。
		ip := remoteAddrIP(remote)
		if !s.allow(ip) {
			log.Printf("stun rate_limited remote=%s", ip)
			continue
		}
		// 非 STUN、非 Binding Request 或格式不完整的 UDP 包直接忽略，不返回业务错误。
		response, ok := buildSTUNBindingResponse(buf[:n], remote)
		if !ok {
			continue
		}
		if _, err := s.conn.WriteTo(response, remote); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("stun write failed remote=%s err=%v", remote, err)
		}
	}
}

func (s *STUNServer) allow(ip string) bool {
	s.limiterMu.Lock()
	lim, ok := s.limiters[ip]
	if !ok {
		lim = &stunLimiter{limiter: rate.NewLimiter(rate.Limit(stunRateLimit), stunRateBurst)}
		s.limiters[ip] = lim
	}
	lim.lastUse = time.Now()
	s.limiterMu.Unlock()
	return lim.limiter.Allow()
}

func remoteAddrIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	switch a := addr.(type) {
	case *net.UDPAddr:
		return a.IP.String()
	case *net.TCPAddr:
		return a.IP.String()
	default:
		return addr.String()
	}
}

// buildSTUNBindingResponse 解析 Binding Request 并构造 Success Response。
// 这里只做最小合法性校验：消息类型、长度对齐、magic cookie 和 UDP 远端地址。
func buildSTUNBindingResponse(packet []byte, remote net.Addr) ([]byte, bool) {
	if len(packet) < stunHeaderSize {
		return nil, false
	}
	if binary.BigEndian.Uint16(packet[0:2]) != stunBindingRequest {
		return nil, false
	}
	// STUN 属性区按 4 字节对齐；长度越过 UDP 包尾说明请求无效。
	messageLength := int(binary.BigEndian.Uint16(packet[2:4]))
	if messageLength%4 != 0 || stunHeaderSize+messageLength > len(packet) {
		return nil, false
	}
	if binary.BigEndian.Uint32(packet[4:8]) != stunMagicCookie {
		return nil, false
	}
	udpAddr, ok := remote.(*net.UDPAddr)
	if !ok {
		return nil, false
	}
	// Success Response 必须复用原请求的 transaction id，浏览器靠它匹配请求和响应。
	transactionID := packet[8:20]
	xorMappedAddress, ok := buildXORMappedAddressAttribute(udpAddr, transactionID)
	if !ok {
		return nil, false
	}

	response := make([]byte, stunHeaderSize+len(xorMappedAddress))
	binary.BigEndian.PutUint16(response[0:2], stunBindingSuccessResponse)
	binary.BigEndian.PutUint16(response[2:4], uint16(len(xorMappedAddress)))
	binary.BigEndian.PutUint32(response[4:8], stunMagicCookie)
	copy(response[8:20], transactionID)
	copy(response[20:], xorMappedAddress)
	return response, true
}

// buildXORMappedAddressAttribute 将服务端看到的 UDP 源地址写入 XOR-MAPPED-ADDRESS。
// IPv4 用 magic cookie 异或；IPv6 用 magic cookie + transaction id 组成 16 字节 mask。
func buildXORMappedAddressAttribute(addr *net.UDPAddr, transactionID []byte) ([]byte, bool) {
	ip4 := addr.IP.To4()
	if ip4 != nil {
		// 属性头 4 字节 + value 8 字节；value = 0、family、x-port、x-address。
		attr := make([]byte, 12)
		binary.BigEndian.PutUint16(attr[0:2], stunAttrXORMappedAddress)
		binary.BigEndian.PutUint16(attr[2:4], 8)
		attr[5] = 0x01
		binary.BigEndian.PutUint16(attr[6:8], uint16(addr.Port)^uint16(stunMagicCookie>>16))
		cookie := make([]byte, 4)
		binary.BigEndian.PutUint32(cookie, stunMagicCookie)
		for i := range ip4 {
			attr[8+i] = ip4[i] ^ cookie[i]
		}
		return attr, true
	}

	ip16 := addr.IP.To16()
	if ip16 == nil || len(transactionID) != 12 {
		return nil, false
	}
	// IPv6 属性 value 长 20 字节：0、family、x-port、16 字节 x-address。
	attr := make([]byte, 24)
	binary.BigEndian.PutUint16(attr[0:2], stunAttrXORMappedAddress)
	binary.BigEndian.PutUint16(attr[2:4], 20)
	attr[5] = 0x02
	binary.BigEndian.PutUint16(attr[6:8], uint16(addr.Port)^uint16(stunMagicCookie>>16))
	mask := make([]byte, 16)
	binary.BigEndian.PutUint32(mask[0:4], stunMagicCookie)
	copy(mask[4:], transactionID)
	for i := range ip16 {
		attr[8+i] = ip16[i] ^ mask[i]
	}
	return attr, true
}