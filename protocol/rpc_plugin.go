package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"net/rpc"
	"sync"

	"github.com/hashicorp/go-plugin"

	"github.com/Yeah114/EmptyDea-plugin-sdk/api"
	sdkdefine "github.com/Yeah114/EmptyDea-plugin-sdk/define"
)

// RPCPlugin is the minimal surface exposed over go-plugin for api.Plugin.
// It intentionally only supports Init/Load/Unload to keep the protocol small.
type RPCPlugin interface {
	Init(frame sdkdefine.Frame, id string, config map[string]interface{}) error
	Load() error
	Unload() error
}

// DynamicRPCPlugin is the go-plugin wrapper.
// - On the plugin (server) side, set Impl.
// - On the host (client) side, Impl is unused.
type DynamicRPCPlugin struct {
	plugin.NetRPCUnsupportedPlugin
	Impl api.Plugin
}

type InitArgs struct {
	ID            string
	Config        map[string]interface{}
	FrameBrokerID uint32
}

type Empty struct{}

type rpcServer struct {
	Impl   api.Plugin
	broker *plugin.MuxBroker
}

type frameModuleStub struct {
	name string
}

func (m frameModuleStub) Name() string { return m.name }

type ListModulesResp struct {
	Names []string
}

type GetModuleArgs struct {
	Name string
}

type GetModuleResp struct {
	Exists         bool
	Name           string
	ModuleKind     string
	ModuleBrokerID uint32
}

type GetPluginConfigArgs struct {
	ID string
}

type GetPluginConfigResp struct {
	Exists bool
	Config sdkdefine.PluginConfig
}

type UpgradePluginConfigArgs struct {
	ID     string
	Config map[string]interface{}
}

type UpgradePluginConfigResp struct{}

type UpgradePluginFullConfigArgs struct {
	ID     string
	Config sdkdefine.PluginConfig
}

type UpgradePluginFullConfigResp struct{}

type RegisterWhenActivateArgs struct {
	CallbackBrokerID uint32
}

type RegisterWhenActivateResp struct {
	ListenerID string
}

type UnregisterWhenActivateArgs struct {
	ListenerID string
}

type UnregisterWhenActivateResp struct {
	OK bool
}

type PacketNameIDMappingResp struct {
	Mapping map[string]uint32
}

type SendPacketDataArgs struct {
	PacketID uint32
	Fields   map[string]any
}

type SendPacketBytesArgs struct {
	PacketID uint32
	Payload  []byte
}

type PacketListenArgs struct {
	PacketIDs        []uint32
	CallbackBrokerID uint32
}

type DestroyPacketListenerArgs struct {
	ListenerID string
}

type PacketListenerResp struct {
	ListenerID string
}

type PacketDataCallbackEvent struct {
	Event sdkdefine.PacketEvent
	Error string
}

type PacketBytesCallbackEvent struct {
	Event sdkdefine.PacketBytesEvent
	Error string
}

type packetDataCallbackServer struct {
	handler func(sdkdefine.PacketEvent, error)
}

func (s *packetDataCallbackServer) OnPacket(args *PacketDataCallbackEvent, _ *Empty) error {
	if s == nil || s.handler == nil {
		return nil
	}
	if args == nil {
		s.handler(sdkdefine.PacketEvent{}, nil)
		return nil
	}
	if args.Error != "" {
		s.handler(sdkdefine.PacketEvent{}, errors.New(args.Error))
		return nil
	}
	s.handler(args.Event, nil)
	return nil
}

type packetBytesCallbackServer struct {
	handler func(sdkdefine.PacketBytesEvent, error)
}

func (s *packetBytesCallbackServer) OnPacket(args *PacketBytesCallbackEvent, _ *Empty) error {
	if s == nil || s.handler == nil {
		return nil
	}
	if args == nil {
		s.handler(sdkdefine.PacketBytesEvent{}, nil)
		return nil
	}
	if args.Error != "" {
		s.handler(sdkdefine.PacketBytesEvent{}, errors.New(args.Error))
		return nil
	}
	s.handler(args.Event, nil)
	return nil
}

type packetDataCallbackClient struct {
	c  *rpc.Client
	mu sync.Mutex
}

func (c *packetDataCallbackClient) Close() error {
	if c == nil || c.c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.Close()
}

func (c *packetDataCallbackClient) OnPacket(event sdkdefine.PacketEvent, err error) error {
	if c == nil || c.c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	msg := &PacketDataCallbackEvent{Event: event}
	if err != nil {
		msg.Error = err.Error()
	}
	return c.c.Call("Plugin.OnPacket", msg, &Empty{})
}

type packetBytesCallbackClient struct {
	c  *rpc.Client
	mu sync.Mutex
}

func (c *packetBytesCallbackClient) Close() error {
	if c == nil || c.c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.Close()
}

func (c *packetBytesCallbackClient) OnPacket(event sdkdefine.PacketBytesEvent, err error) error {
	if c == nil || c.c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	msg := &PacketBytesCallbackEvent{Event: event}
	if err != nil {
		msg.Error = err.Error()
	}
	return c.c.Call("Plugin.OnPacket", msg, &Empty{})
}

type frameRPCServer struct {
	Frame  sdkdefine.Frame
	broker *plugin.MuxBroker

	packetMu             sync.Mutex
	packetDataCallbacks  map[string]*packetDataCallbackClient
	packetBytesCallbacks map[string]*packetBytesCallbackClient
}

func (s *frameRPCServer) ListModules(_ *Empty, resp *ListModulesResp) error {
	if resp == nil {
		return nil
	}
	resp.Names = nil
	if s == nil || s.Frame == nil {
		return nil
	}
	mods := s.Frame.ListModules()
	if mods == nil {
		return nil
	}
	resp.Names = make([]string, 0, len(mods))
	for name := range mods {
		resp.Names = append(resp.Names, name)
	}
	return nil
}

func (s *frameRPCServer) GetModule(args *GetModuleArgs, resp *GetModuleResp) error {
	if resp == nil {
		return nil
	}
	resp.Exists = false
	resp.Name = ""
	resp.ModuleKind = ""
	resp.ModuleBrokerID = 0
	if s == nil || s.Frame == nil || args == nil {
		return nil
	}
	mod, ok := s.Frame.GetModule(args.Name)
	if !ok || mod == nil {
		return nil
	}
	resp.Exists = true
	resp.Name = mod.Name()

	if s.broker == nil {
		return nil
	}
	if chatMod, ok := any(mod).(api.ChatModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &ChatModuleRPCServer{Impl: chatMod, broker: s.broker})
		resp.ModuleKind = api.NameChatModule
		resp.ModuleBrokerID = id
		return nil
	}
	if cmdsMod, ok := any(mod).(api.CommandsModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &CommandsModuleRPCServer{Impl: cmdsMod})
		resp.ModuleKind = api.NameCommandsModule
		resp.ModuleBrokerID = id
		return nil
	}
	if flexMod, ok := any(mod).(api.FlexModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &FlexModuleRPCServer{Impl: flexMod, broker: s.broker})
		resp.ModuleKind = api.NameFlexModule
		resp.ModuleBrokerID = id
		return nil
	}
	if uqMod, ok := any(mod).(api.UQHolderModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &UQHolderModuleRPCServer{Impl: uqMod})
		resp.ModuleKind = api.NameUQHolderModule
		resp.ModuleBrokerID = id
		return nil
	}
	if gmMod, ok := any(mod).(api.GameMenuModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &GameMenuModuleRPCServer{Impl: gmMod, broker: s.broker})
		resp.ModuleKind = api.NameGameMenuModule
		resp.ModuleBrokerID = id
		return nil
	}
	if tmMod, ok := any(mod).(api.TerminalMenuModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &TerminalMenuModuleRPCServer{Impl: tmMod, broker: s.broker})
		resp.ModuleKind = api.NameTerminalMenuModule
		resp.ModuleBrokerID = id
		return nil
	}
	if terminalMod, ok := any(mod).(api.TerminalModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &TerminalModuleRPCServer{Impl: terminalMod, broker: s.broker})
		resp.ModuleKind = api.NameTerminalModule
		resp.ModuleBrokerID = id
		return nil
	}
	if playersMod, ok := any(mod).(api.PlayersModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &PlayersModuleRPCServer{Impl: playersMod, broker: s.broker})
		resp.ModuleKind = api.NamePlayersModule
		resp.ModuleBrokerID = id
		return nil
	}
	if loggerMod, ok := any(mod).(api.LoggerModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &LoggerModuleRPCServer{Impl: loggerMod})
		resp.ModuleKind = api.NameLoggerModule
		resp.ModuleBrokerID = id
		return nil
	}
	if dbMod, ok := any(mod).(api.DatabaseModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &DatabaseModuleRPCServer{Impl: dbMod, broker: s.broker})
		resp.ModuleKind = api.NameDatabaseModule
		resp.ModuleBrokerID = id
		return nil
	}
	if spMod, ok := any(mod).(api.StoragePathModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &StoragePathModuleRPCServer{Impl: spMod})
		resp.ModuleKind = api.NameStoragePathModule
		resp.ModuleBrokerID = id
		return nil
	}
	if brainMod, ok := any(mod).(api.BrainModule); ok {
		id := s.broker.NextId()
		go acceptAndServeMuxBroker(s.broker, id, &BrainModuleRPCServer{Impl: brainMod, broker: s.broker})
		resp.ModuleKind = api.NameBrainModule
		resp.ModuleBrokerID = id
	}
	return nil
}

func (s *frameRPCServer) GetPluginConfig(args *GetPluginConfigArgs, resp *GetPluginConfigResp) error {
	if resp == nil {
		return nil
	}
	resp.Exists = false
	resp.Config = sdkdefine.PluginConfig{}
	if s == nil || s.Frame == nil || args == nil || args.ID == "" {
		return nil
	}
	cfg, ok := s.Frame.GetPluginConfig(args.ID)
	if !ok {
		return nil
	}
	resp.Exists = true
	resp.Config = cfg
	return nil
}

func (s *frameRPCServer) UpgradePluginConfig(args *UpgradePluginConfigArgs, resp *UpgradePluginConfigResp) error {
	_ = resp
	if s == nil || s.Frame == nil || args == nil || args.ID == "" {
		return nil
	}
	return s.Frame.UpgradePluginConfig(args.ID, args.Config)
}

func (s *frameRPCServer) UpgradePluginFullConfig(args *UpgradePluginFullConfigArgs, resp *UpgradePluginFullConfigResp) error {
	_ = resp
	if s == nil || s.Frame == nil || args == nil || args.ID == "" {
		return nil
	}
	return s.Frame.UpgradePluginFullConfig(args.ID, args.Config)
}

func (s *frameRPCServer) RegisterWhenActivate(args *RegisterWhenActivateArgs, resp *RegisterWhenActivateResp) error {
	if resp == nil {
		return nil
	}
	resp.ListenerID = ""
	if s == nil || s.Frame == nil || args == nil {
		return nil
	}
	if s.broker == nil || args.CallbackBrokerID == 0 {
		return nil
	}

	id, err := s.Frame.RegisterWhenActivate(func() {
		conn, dialErr := s.broker.Dial(args.CallbackBrokerID)
		if dialErr != nil || conn == nil {
			return
		}
		client := rpc.NewClient(conn)
		_ = client.Call("Plugin.Activate", &Empty{}, &Empty{})
		_ = client.Close()
	})
	if err != nil {
		return err
	}
	resp.ListenerID = id
	return nil
}

func (s *frameRPCServer) UnregisterWhenActivate(args *UnregisterWhenActivateArgs, resp *UnregisterWhenActivateResp) error {
	if resp == nil {
		return nil
	}
	resp.OK = false
	if s == nil || s.Frame == nil || args == nil || args.ListenerID == "" {
		return nil
	}
	resp.OK = s.Frame.UnregisterWhenActivate(args.ListenerID)
	return nil
}

func (s *frameRPCServer) GetPacketNameIDMapping(_ *Empty, resp *PacketNameIDMappingResp) error {
	if resp == nil {
		return nil
	}
	resp.Mapping = map[string]uint32{}
	if s == nil || s.Frame == nil {
		return nil
	}
	mapping, err := s.Frame.GetPacketNameIDMapping(context.Background())
	if err != nil {
		return err
	}
	if mapping != nil {
		resp.Mapping = mapping
	}
	return nil
}

func (s *frameRPCServer) SendPacketData(args *SendPacketDataArgs, _ *Empty) error {
	if s == nil || s.Frame == nil || args == nil {
		return nil
	}
	return s.Frame.SendPacketData(context.Background(), args.PacketID, args.Fields)
}

func (s *frameRPCServer) SendPacketBytes(args *SendPacketBytesArgs, _ *Empty) error {
	if s == nil || s.Frame == nil || args == nil {
		return nil
	}
	return s.Frame.SendPacketBytes(context.Background(), args.PacketID, args.Payload)
}

func (s *frameRPCServer) ListenPacketData(args *PacketListenArgs, resp *PacketListenerResp) error {
	if resp == nil {
		return nil
	}
	resp.ListenerID = ""
	if s == nil || s.Frame == nil || s.broker == nil || args == nil {
		return nil
	}
	if args.CallbackBrokerID == 0 {
		return errors.New("frameRPCServer.ListenPacketData: callback broker id is 0")
	}
	conn, err := s.broker.Dial(args.CallbackBrokerID)
	if err != nil {
		return err
	}
	cb := &packetDataCallbackClient{c: rpc.NewClient(conn)}
	listenerID, err := s.Frame.ListenPacketData(context.Background(), args.PacketIDs, func(event sdkdefine.PacketEvent, listenErr error) {
		_ = cb.OnPacket(event, listenErr)
	})
	if err != nil {
		_ = cb.Close()
		return err
	}
	s.packetMu.Lock()
	if s.packetDataCallbacks == nil {
		s.packetDataCallbacks = make(map[string]*packetDataCallbackClient)
	}
	s.packetDataCallbacks[listenerID] = cb
	s.packetMu.Unlock()
	resp.ListenerID = listenerID
	return nil
}

func (s *frameRPCServer) ListenPacketBytes(args *PacketListenArgs, resp *PacketListenerResp) error {
	if resp == nil {
		return nil
	}
	resp.ListenerID = ""
	if s == nil || s.Frame == nil || s.broker == nil || args == nil {
		return nil
	}
	if args.CallbackBrokerID == 0 {
		return errors.New("frameRPCServer.ListenPacketBytes: callback broker id is 0")
	}
	conn, err := s.broker.Dial(args.CallbackBrokerID)
	if err != nil {
		return err
	}
	cb := &packetBytesCallbackClient{c: rpc.NewClient(conn)}
	listenerID, err := s.Frame.ListenPacketBytes(context.Background(), args.PacketIDs, func(event sdkdefine.PacketBytesEvent, listenErr error) {
		_ = cb.OnPacket(event, listenErr)
	})
	if err != nil {
		_ = cb.Close()
		return err
	}
	s.packetMu.Lock()
	if s.packetBytesCallbacks == nil {
		s.packetBytesCallbacks = make(map[string]*packetBytesCallbackClient)
	}
	s.packetBytesCallbacks[listenerID] = cb
	s.packetMu.Unlock()
	resp.ListenerID = listenerID
	return nil
}

func (s *frameRPCServer) DestroyPacketListener(args *DestroyPacketListenerArgs, resp *BoolResp) error {
	if resp != nil {
		resp.OK = false
	}
	if s == nil || s.Frame == nil || args == nil || args.ListenerID == "" {
		return nil
	}
	s.Frame.DestroyPacketListener(args.ListenerID)
	if resp != nil {
		resp.OK = true
	}
	s.packetMu.Lock()
	dataCB := s.packetDataCallbacks[args.ListenerID]
	delete(s.packetDataCallbacks, args.ListenerID)
	bytesCB := s.packetBytesCallbacks[args.ListenerID]
	delete(s.packetBytesCallbacks, args.ListenerID)
	s.packetMu.Unlock()
	if dataCB != nil {
		_ = dataCB.Close()
	}
	if bytesCB != nil {
		_ = bytesCB.Close()
	}
	return nil
}

type frameRPCClient struct {
	c      *rpc.Client
	broker *plugin.MuxBroker
	mu     sync.Mutex

	packetInitOnce sync.Once
	packetInitErr  error
	packetMu       sync.RWMutex
	packetNameID   map[string]uint32
	packetIDName   map[uint32]string
}

type activateCallbackRPCServer struct {
	Handler func()
}

func (s *activateCallbackRPCServer) Activate(_ *Empty, _ *Empty) error {
	if s == nil || s.Handler == nil {
		return nil
	}
	s.Handler()
	return nil
}

func (c *frameRPCClient) ListModules() map[string]sdkdefine.Module {
	if c == nil || c.c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var resp ListModulesResp
	if err := c.c.Call("Plugin.ListModules", &Empty{}, &resp); err != nil {
		return nil
	}
	out := make(map[string]sdkdefine.Module, len(resp.Names))
	for _, name := range resp.Names {
		if name == "" {
			continue
		}
		out[name] = frameModuleStub{name: name}
	}
	return out
}

func (c *frameRPCClient) GetModule(name string) (sdkdefine.Module, bool) {
	if c == nil || c.c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var resp GetModuleResp
	if err := c.c.Call("Plugin.GetModule", &GetModuleArgs{Name: name}, &resp); err != nil {
		return nil, false
	}
	if !resp.Exists || resp.Name == "" {
		return nil, false
	}

	if resp.ModuleBrokerID != 0 && c.broker != nil {
		if conn, err := c.broker.Dial(resp.ModuleBrokerID); err == nil && conn != nil {
			switch resp.ModuleKind {
			case api.NameChatModule:
				if m := newChatModuleRPCClient(conn, c.broker); m != nil {
					return m, true
				}
			case api.NameCommandsModule:
				if m := newCommandsModuleRPCClient(conn); m != nil {
					return m, true
				}
			case api.NameFlexModule:
				if m := newFlexModuleRPCClient(conn, c.broker); m != nil {
					return m, true
				}
			case api.NameUQHolderModule:
				if m := newUQHolderModuleRPCClient(conn); m != nil {
					return m, true
				}
			case api.NameGameMenuModule:
				if m := newGameMenuModuleRPCClient(conn, c.broker); m != nil {
					return m, true
				}
			case api.NameTerminalMenuModule:
				if m := newTerminalMenuModuleRPCClient(conn, c.broker); m != nil {
					return m, true
				}
			case api.NameTerminalModule:
				if m := newTerminalModuleRPCClient(conn, c.broker); m != nil {
					return m, true
				}
			case api.NamePlayersModule:
				if m := newPlayersModuleRPCClient(conn, c.broker); m != nil {
					return m, true
				}
			case api.NameLoggerModule:
				if m := newLoggerModuleRPCClient(conn); m != nil {
					return m, true
				}
			case api.NameDatabaseModule:
				if m := newDatabaseModuleRPCClient(conn, c.broker); m != nil {
					return m, true
				}
			case api.NameStoragePathModule:
				if m := newStoragePathModuleRPCClient(conn); m != nil {
					return m, true
				}
			case api.NameBrainModule:
				if m := newBrainModuleRPCClient(conn, c.broker); m != nil {
					return m, true
				}
			default:
				_ = conn.Close()
			}
		}
	}

	return frameModuleStub{name: resp.Name}, true
}

func (c *frameRPCClient) GetPluginConfig(id string) (sdkdefine.PluginConfig, bool) {
	if c == nil || c.c == nil || id == "" {
		return sdkdefine.PluginConfig{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var resp GetPluginConfigResp
	if err := c.c.Call("Plugin.GetPluginConfig", &GetPluginConfigArgs{ID: id}, &resp); err != nil {
		return sdkdefine.PluginConfig{}, false
	}
	if !resp.Exists {
		return sdkdefine.PluginConfig{}, false
	}
	return resp.Config, true
}

func (c *frameRPCClient) UpgradePluginConfig(id string, config map[string]interface{}) error {
	if c == nil || c.c == nil || id == "" {
		return nil
	}
	if config == nil {
		config = map[string]interface{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.Call("Plugin.UpgradePluginConfig", &UpgradePluginConfigArgs{ID: id, Config: config}, &UpgradePluginConfigResp{})
}

func (c *frameRPCClient) UpgradePluginFullConfig(id string, config sdkdefine.PluginConfig) error {
	if c == nil || c.c == nil || id == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.Call("Plugin.UpgradePluginFullConfig", &UpgradePluginFullConfigArgs{ID: id, Config: config}, &UpgradePluginFullConfigResp{})
}

func (c *frameRPCClient) RegisterWhenActivate(handler func()) (string, error) {
	if c == nil || c.c == nil || handler == nil {
		return "", nil
	}
	if c.broker == nil {
		return "", nil
	}

	brokerID := c.broker.NextId()
	go acceptAndServeMuxBroker(c.broker, brokerID, &activateCallbackRPCServer{Handler: handler})

	c.mu.Lock()
	defer c.mu.Unlock()

	var resp RegisterWhenActivateResp
	if err := c.c.Call("Plugin.RegisterWhenActivate", &RegisterWhenActivateArgs{CallbackBrokerID: brokerID}, &resp); err != nil {
		return "", err
	}
	return resp.ListenerID, nil
}

func (c *frameRPCClient) UnregisterWhenActivate(listenerID string) bool {
	if c == nil || c.c == nil || listenerID == "" {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var resp UnregisterWhenActivateResp
	if err := c.c.Call("Plugin.UnregisterWhenActivate", &UnregisterWhenActivateArgs{ListenerID: listenerID}, &resp); err != nil {
		return false
	}
	return resp.OK
}

func (c *frameRPCClient) refreshPacketMappings() error {
	if c == nil || c.c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var resp PacketNameIDMappingResp
	if err := c.c.Call("Plugin.GetPacketNameIDMapping", &Empty{}, &resp); err != nil {
		return err
	}
	nameID := make(map[string]uint32, len(resp.Mapping))
	idName := make(map[uint32]string, len(resp.Mapping))
	for name, id := range resp.Mapping {
		nameID[name] = id
		idName[id] = name
	}
	c.packetMu.Lock()
	c.packetNameID = nameID
	c.packetIDName = idName
	c.packetMu.Unlock()
	return nil
}

func (c *frameRPCClient) ensurePacketMappings() error {
	if c == nil {
		return nil
	}
	c.packetInitOnce.Do(func() {
		c.packetInitErr = c.refreshPacketMappings()
	})
	return c.packetInitErr
}

func (c *frameRPCClient) GetPacketNameIDMapping(_ context.Context) (map[string]uint32, error) {
	if err := c.ensurePacketMappings(); err != nil {
		return nil, err
	}
	c.packetMu.RLock()
	defer c.packetMu.RUnlock()
	out := make(map[string]uint32, len(c.packetNameID))
	for name, id := range c.packetNameID {
		out[name] = id
	}
	return out, nil
}

func (c *frameRPCClient) PacketNameByID(packetID uint32) (string, bool) {
	if err := c.ensurePacketMappings(); err != nil {
		return "", false
	}
	c.packetMu.RLock()
	defer c.packetMu.RUnlock()
	name, ok := c.packetIDName[packetID]
	return name, ok
}

func (c *frameRPCClient) PacketIDByName(packetName string) (uint32, bool) {
	if err := c.ensurePacketMappings(); err != nil {
		return 0, false
	}
	c.packetMu.RLock()
	defer c.packetMu.RUnlock()
	id, ok := c.packetNameID[packetName]
	return id, ok
}

func (c *frameRPCClient) SendPacketData(_ context.Context, packetID uint32, fields any) error {
	if c == nil || c.c == nil {
		return nil
	}
	fieldMap, _ := fields.(map[string]any)
	if fieldMap == nil && fields != nil {
		raw, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		_ = json.Unmarshal(raw, &fieldMap)
	}
	if fieldMap == nil {
		fieldMap = map[string]any{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.Call("Plugin.SendPacketData", &SendPacketDataArgs{PacketID: packetID, Fields: fieldMap}, &Empty{})
}

func (c *frameRPCClient) SendPacketBytes(_ context.Context, packetID uint32, payload []byte) error {
	if c == nil || c.c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.Call("Plugin.SendPacketBytes", &SendPacketBytesArgs{PacketID: packetID, Payload: append([]byte(nil), payload...)}, &Empty{})
}

func (c *frameRPCClient) ListenPacketData(_ context.Context, packetIDs []uint32, handler func(sdkdefine.PacketEvent, error)) (string, error) {
	if c == nil || c.c == nil || handler == nil || c.broker == nil {
		return "", nil
	}
	brokerID := c.broker.NextId()
	go acceptAndServeMuxBroker(c.broker, brokerID, &packetDataCallbackServer{handler: handler})

	c.mu.Lock()
	defer c.mu.Unlock()
	var resp PacketListenerResp
	if err := c.c.Call("Plugin.ListenPacketData", &PacketListenArgs{
		PacketIDs:        append([]uint32(nil), packetIDs...),
		CallbackBrokerID: brokerID,
	}, &resp); err != nil {
		return "", err
	}
	return resp.ListenerID, nil
}

func (c *frameRPCClient) ListenPacketBytes(_ context.Context, packetIDs []uint32, handler func(sdkdefine.PacketBytesEvent, error)) (string, error) {
	if c == nil || c.c == nil || handler == nil || c.broker == nil {
		return "", nil
	}
	brokerID := c.broker.NextId()
	go acceptAndServeMuxBroker(c.broker, brokerID, &packetBytesCallbackServer{handler: handler})

	c.mu.Lock()
	defer c.mu.Unlock()
	var resp PacketListenerResp
	if err := c.c.Call("Plugin.ListenPacketBytes", &PacketListenArgs{
		PacketIDs:        append([]uint32(nil), packetIDs...),
		CallbackBrokerID: brokerID,
	}, &resp); err != nil {
		return "", err
	}
	return resp.ListenerID, nil
}

func (c *frameRPCClient) DestroyPacketListener(listenerID string) {
	if c == nil || c.c == nil || listenerID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.c.Call("Plugin.DestroyPacketListener", &DestroyPacketListenerArgs{ListenerID: listenerID}, &BoolResp{})
}

func (s *rpcServer) Init(args *InitArgs, _ *Empty) error {
	if s == nil || s.Impl == nil {
		return nil
	}
	id := ""
	cfg := map[string]interface{}{}
	if args != nil {
		id = args.ID
		if args.Config != nil {
			cfg = args.Config
		}
	}
	var frame sdkdefine.Frame
	if args != nil && args.FrameBrokerID != 0 && s.broker != nil {
		if conn, err := s.broker.Dial(args.FrameBrokerID); err == nil && conn != nil {
			frameClient := &frameRPCClient{c: rpc.NewClient(conn), broker: s.broker}
			_ = frameClient.ensurePacketMappings()
			frame = frameClient
		}
	}
	s.Impl.Init(frame, id, cfg)
	return nil
}

func (s *rpcServer) Load(_ *Empty, _ *Empty) error {
	if s == nil || s.Impl == nil {
		return nil
	}
	return s.Impl.Load(context.Background())
}

func (s *rpcServer) Unload(_ *Empty, _ *Empty) error {
	if s == nil || s.Impl == nil {
		return nil
	}
	return s.Impl.Unload(context.Background())
}

type rpcClient struct {
	c      *rpc.Client
	broker *plugin.MuxBroker
}

func (c *rpcClient) Init(frame sdkdefine.Frame, id string, config map[string]interface{}) error {
	if c == nil || c.c == nil {
		return nil
	}
	var brokerID uint32
	if c.broker != nil {
		brokerID = c.broker.NextId()
		go acceptAndServeMuxBroker(c.broker, brokerID, &frameRPCServer{Frame: frame, broker: c.broker})
	}
	return c.c.Call("Plugin.Init", &InitArgs{ID: id, Config: config, FrameBrokerID: brokerID}, &Empty{})
}

func (c *rpcClient) Load() error {
	return c.c.Call("Plugin.Load", &Empty{}, &Empty{})
}

func (c *rpcClient) Unload() error {
	return c.c.Call("Plugin.Unload", &Empty{}, &Empty{})
}

func (p *DynamicRPCPlugin) Server(b *plugin.MuxBroker) (interface{}, error) {
	return &rpcServer{Impl: p.Impl, broker: b}, nil
}

func (p *DynamicRPCPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &rpcClient{c: c, broker: b}, nil
}
