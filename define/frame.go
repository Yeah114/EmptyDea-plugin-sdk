package define

import "context"

type Frame interface {
	PluginFrame

	GetPacketNameIDMapping(ctx context.Context) (map[string]uint32, error)
	PacketNameByID(packetID uint32) (string, bool)
	PacketIDByName(packetName string) (uint32, bool)
	SendPacketData(ctx context.Context, packetID uint32, fields any) error
	SendPacketBytes(ctx context.Context, packetID uint32, payload []byte) error
	ListenPacketData(ctx context.Context, packetIDs []uint32, handler func(PacketEvent, error)) (string, error)
	ListenPacketBytes(ctx context.Context, packetIDs []uint32, handler func(PacketBytesEvent, error)) (string, error)
	DestroyPacketListener(listenerID string)

	ListModules() map[string]Module
	GetModule(name string) (Module, bool)
}
