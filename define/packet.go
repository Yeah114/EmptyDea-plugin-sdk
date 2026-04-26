package define

type PacketEvent struct {
	ID     uint32
	Fields map[string]any
}

type PacketBytesEvent struct {
	ID      uint32
	Payload []byte
}
