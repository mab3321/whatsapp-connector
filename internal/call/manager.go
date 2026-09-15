package call

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/livekit/protocol/livekit"
	"github.com/mab3321/whatsapp-connector/internal/livekitbridge"
	"github.com/mab3321/whatsapp-connector/internal/media"
	"github.com/mab3321/whatsapp-connector/internal/meta"
)

type Config struct {
	LiveKitURL, APIKey, APISecret string
	PublicIP                      netip.Addr
	PortMin, PortMax              int
	SetupTimeout, MediaTimeout    time.Duration
}

type AcceptParams struct {
	PhoneID, APIToken, APIVersion, CallID, SDP string
	RoomName, Identity, Name, Metadata         string
	Attributes                                 map[string]string
	Agents                                     []*livekit.RoomAgentDispatch
	Wait                                       bool
	Timeout                                    time.Duration
}

type Manager struct {
	log      *slog.Logger
	conf     Config
	meta     *meta.Client
	ports    media.PortAllocator
	mu       sync.Mutex
	calls    map[string]*Call
	draining bool
	wg       sync.WaitGroup
}

type Call struct {
	log                                 *slog.Logger
	params                              AcceptParams
	room, identity                      string
	ctx                                 context.Context
	cancel                              context.CancelFunc
	transport                           *media.Transport
	bridge                              *livekitbridge.Bridge
	answer                              string
	ready, done                         chan struct{}
	mu                                  sync.Mutex
	err                                 error
	terminate, userTerminated, signaled bool
}

func NewManager(log *slog.Logger, conf Config, mc *meta.Client) *Manager {
	return &Manager{log: log, conf: conf, meta: mc, ports: media.PortAllocator{Min: conf.PortMin, Max: conf.PortMax}, calls: make(map[string]*Call)}
}

func (m *Manager) Accept(ctx context.Context, p AcceptParams) (string, error) {
	if p.PhoneID == "" || p.APIToken == "" || p.APIVersion == "" || p.CallID == "" || p.SDP == "" {
		return "", errors.New("required WhatsApp call fields are missing")
	}
	m.mu.Lock()
	if m.draining {
		m.mu.Unlock()
		return "", errors.New("connector is draining")
	}
	if c := m.calls[p.CallID]; c != nil {
		m.mu.Unlock()
		if p.Wait {
			return c.room, c.wait(ctx)
		}
		return c.room, nil
	}
	m.mu.Unlock()
	if p.RoomName == "" {
		p.RoomName = "whatsapp-" + uuid.NewString()
	}
	if p.Identity == "" {
		p.Identity = "whatsapp-" + uuid.NewString()
	}
	udp, err := m.ports.Listen()
	if err != nil {
		return "", err
	}
	cert, err := media.NewCertificate()
	if err != nil {
		udp.Close()
		return "", err
	}
	port := udp.LocalAddr().(*net.UDPAddr).Port
	n, err := media.ParseOfferAndAnswer(p.SDP, cert, m.conf.PublicIP, port)
	if err != nil {
		udp.Close()
		return "", err
	}
	callCtx, cancel := context.WithCancel(context.Background())
	c := &Call{
		log:    m.log.With("call", callLabel(p.CallID), "room", p.RoomName),
		params: p, room: p.RoomName, identity: p.Identity, ctx: callCtx, cancel: cancel,
		answer: n.Answer, ready: make(chan struct{}), done: make(chan struct{}),
	}
	setupTimeout := m.conf.SetupTimeout
	if p.Timeout > 0 && p.Timeout < setupTimeout {
		setupTimeout = p.Timeout
	}
	c.transport = media.NewTransport(callCtx, udp, n, setupTimeout)
	m.mu.Lock()
	if old := m.calls[p.CallID]; old != nil {
		m.mu.Unlock()
		_ = c.transport.Close()
		cancel()
		if p.Wait {
			return old.room, old.wait(ctx)
		}
		return old.room, nil
	}
	m.calls[p.CallID] = c
	m.wg.Add(1)
	m.mu.Unlock()
	go m.run(c, n.PayloadType)
	if p.Wait {
		err := c.wait(ctx)
		if err != nil {
			c.stopBusiness()
		}
		return c.room, err
	}
	return c.room, nil
}

func (m *Manager) run(c *Call, payload uint8) {
	defer func() {
		c.cleanup(m.meta)
		m.mu.Lock()
		delete(m.calls, c.params.CallID)
		m.mu.Unlock()
		m.wg.Done()
		close(c.done)
	}()
	b, err := livekitbridge.Connect(c.ctx, c.log, livekitbridge.Config{
		URL: m.conf.LiveKitURL, APIKey: m.conf.APIKey, APISecret: m.conf.APISecret,
		RoomName: c.room, Identity: c.identity, Name: c.params.Name, Metadata: c.params.Metadata,
		Attributes: c.params.Attributes, Agents: c.params.Agents, PayloadType: payload,
		WriteToMeta: c.transport.WriteRTP, OnDisconnected: c.stopBusiness,
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.bridge = b
	if err = m.meta.PreAccept(c.ctx, c.params.APIVersion, c.params.PhoneID, c.params.APIToken, c.params.CallID, c.answer); err != nil {
		c.fail(err)
		return
	}
	c.mu.Lock()
	c.signaled = true
	c.mu.Unlock()
	c.transport.Start()
	if err = c.transport.Wait(); err != nil {
		c.fail(err)
		return
	}
	if err = m.meta.Accept(c.ctx, c.params.APIVersion, c.params.PhoneID, c.params.APIToken, c.params.CallID, c.answer); err != nil {
		c.fail(err)
		return
	}
	c.signalReady(nil)
	c.log.Info("WhatsApp call connected")
	errCh := make(chan error, 1)
	go func() {
		for {
			p, readErr := c.transport.ReadRTP()
			if readErr != nil {
				errCh <- readErr
				return
			}
			if writeErr := c.bridge.WriteFromMeta(p); writeErr != nil {
				errCh <- writeErr
				return
			}
		}
	}()
	select {
	case <-c.ctx.Done():
	case <-b.Done():
		c.stopBusiness()
	case err = <-errCh:
		if err != nil {
			c.log.Warn("media ended", "error", err)
		}
		c.stopBusiness()
	}
}

func (c *Call) signalReady(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.ready:
		return
	default:
		c.err = err
		close(c.ready)
	}
}
func (c *Call) fail(err error) {
	c.log.Error("call setup failed", "error", err)
	c.signalReady(err)
	c.stopBusiness()
}
func (c *Call) wait(ctx context.Context) error {
	select {
	case <-c.ready:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *Call) stopBusiness() {
	c.mu.Lock()
	if !c.userTerminated {
		c.terminate = true
	}
	c.mu.Unlock()
	c.cancel()
}
func (c *Call) stopUser() {
	c.mu.Lock()
	c.userTerminated = true
	c.terminate = false
	c.mu.Unlock()
	c.cancel()
}
func (c *Call) cleanup(mc *meta.Client) {
	c.signalReady(context.Canceled)
	c.mu.Lock()
	term, signaled, token := c.terminate, c.signaled, c.params.APIToken
	c.mu.Unlock()
	if term && signaled {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := mc.Terminate(ctx, c.params.APIVersion, c.params.PhoneID, token, c.params.CallID); err != nil {
			c.log.Warn("Meta terminate failed", "error", err)
		}
		cancel()
	}
	if c.bridge != nil {
		_ = c.bridge.Close()
	}
	if c.transport != nil {
		_ = c.transport.Close()
	}
}

func (m *Manager) Disconnect(callID string, user bool, apiToken string) error {
	m.mu.Lock()
	c := m.calls[callID]
	m.mu.Unlock()
	if c == nil {
		return nil
	}
	if apiToken != "" {
		c.mu.Lock()
		c.params.APIToken = apiToken
		c.mu.Unlock()
	}
	if user {
		c.stopUser()
	} else {
		c.stopBusiness()
	}
	return nil
}
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.draining = true
	for _, c := range m.calls {
		c.stopBusiness()
	}
	m.mu.Unlock()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func callLabel(id string) string { v := sha256.Sum256([]byte(id)); return fmt.Sprintf("%x", v[:6]) }
