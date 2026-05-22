package server

import (
	"encoding/binary"
	"net"
	"testing"
)

func TestBuildSTUNBindingResponseIPv4(t *testing.T) {
	transactionID := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	request := makeSTUNBindingRequest(transactionID)
	remote := &net.UDPAddr{IP: net.ParseIP("203.0.113.9"), Port: 54321}

	response, ok := buildSTUNBindingResponse(request, remote)
	if !ok {
		t.Fatal("expected STUN binding response")
	}
	assertSTUNHeader(t, response, transactionID, 12)
	if got := binary.BigEndian.Uint16(response[20:22]); got != stunAttrXORMappedAddress {
		t.Fatalf("unexpected attribute type: 0x%x", got)
	}
	if got := binary.BigEndian.Uint16(response[22:24]); got != 8 {
		t.Fatalf("unexpected attribute length: %d", got)
	}
	if got := response[25]; got != 0x01 {
		t.Fatalf("unexpected address family: %d", got)
	}
	port := binary.BigEndian.Uint16(response[26:28]) ^ uint16(stunMagicCookie>>16)
	if port != uint16(remote.Port) {
		t.Fatalf("decoded port=%d, want %d", port, remote.Port)
	}
	cookie := make([]byte, 4)
	binary.BigEndian.PutUint32(cookie, stunMagicCookie)
	gotIP := make(net.IP, 4)
	for i := range gotIP {
		gotIP[i] = response[28+i] ^ cookie[i]
	}
	if !gotIP.Equal(remote.IP.To4()) {
		t.Fatalf("decoded ip=%s, want %s", gotIP, remote.IP)
	}
}

func TestBuildSTUNBindingResponseIPv6(t *testing.T) {
	transactionID := []byte{11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	request := makeSTUNBindingRequest(transactionID)
	remote := &net.UDPAddr{IP: net.ParseIP("2001:db8::21"), Port: 60000}

	response, ok := buildSTUNBindingResponse(request, remote)
	if !ok {
		t.Fatal("expected STUN binding response")
	}
	assertSTUNHeader(t, response, transactionID, 24)
	if got := binary.BigEndian.Uint16(response[22:24]); got != 20 {
		t.Fatalf("unexpected attribute length: %d", got)
	}
	if got := response[25]; got != 0x02 {
		t.Fatalf("unexpected address family: %d", got)
	}
	mask := make([]byte, 16)
	binary.BigEndian.PutUint32(mask[0:4], stunMagicCookie)
	copy(mask[4:], transactionID)
	gotIP := make(net.IP, 16)
	for i := range gotIP {
		gotIP[i] = response[28+i] ^ mask[i]
	}
	if !gotIP.Equal(remote.IP.To16()) {
		t.Fatalf("decoded ip=%s, want %s", gotIP, remote.IP)
	}
}

func TestBuildSTUNBindingResponseRejectsNonSTUN(t *testing.T) {
	if _, ok := buildSTUNBindingResponse([]byte("not stun"), &net.UDPAddr{}); ok {
		t.Fatal("non-STUN packet should be ignored")
	}
}

func makeSTUNBindingRequest(transactionID []byte) []byte {
	request := make([]byte, stunHeaderSize)
	binary.BigEndian.PutUint16(request[0:2], stunBindingRequest)
	binary.BigEndian.PutUint32(request[4:8], stunMagicCookie)
	copy(request[8:20], transactionID)
	return request
}

func assertSTUNHeader(t *testing.T, response []byte, transactionID []byte, wantLength uint16) {
	t.Helper()
	if len(response) != stunHeaderSize+int(wantLength) {
		t.Fatalf("response length=%d, want %d", len(response), stunHeaderSize+int(wantLength))
	}
	if got := binary.BigEndian.Uint16(response[0:2]); got != stunBindingSuccessResponse {
		t.Fatalf("unexpected response type: 0x%x", got)
	}
	if got := binary.BigEndian.Uint16(response[2:4]); got != wantLength {
		t.Fatalf("unexpected message length=%d, want %d", got, wantLength)
	}
	if got := binary.BigEndian.Uint32(response[4:8]); got != stunMagicCookie {
		t.Fatalf("unexpected magic cookie: 0x%x", got)
	}
	if got := response[8:20]; string(got) != string(transactionID) {
		t.Fatalf("transaction id changed: %v", got)
	}
}
