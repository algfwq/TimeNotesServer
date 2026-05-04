package protocol

import "testing"

func TestBytesBase64RoundTrip(t *testing.T) {
	encoded := EncodeBytes([]byte{1, 2, 3, 4})
	decoded, err := DecodeBytes(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded) != string([]byte{1, 2, 3, 4}) {
		t.Fatalf("unexpected decoded bytes: %v", decoded)
	}
}
