package jobserver

import (
	"encoding/json"
	"fmt"

	"github.com/wow-look-at-my/go-ipc"
)

// ipcCallType is the message type the jobserver service carries: a JSON Request in, a JSON Response out.
const ipcCallType uint32 = 1

// startIPC serves the API over a go-ipc service. A local process enqueues
// and reads status through shared memory with no socket and no HTTP.
func (s *Server) startIPC() error {
	if !s.cfg.IPC || s.cfg.IPCName == "" {
		return nil
	}
	name := s.cfg.IPCName
	svc, err := ipc.Serve(name, ipc.HandlerFunc(s.ipcCall))
	if err != nil {
		return fmt.Errorf("go-jobserver: ipc %s: %w", name, err)
	}
	s.addCloser(svc)
	s.cfg.Logf("go-jobserver: ipc service %s", name)
	return nil
}

// ipcCall answers one call from one client process.
func (s *Server) ipcCall(_ *ipc.Session, typ uint32, payload []byte) (uint32, []byte, error) {
	if typ != ipcCallType {
		return 0, nil, fmt.Errorf("go-jobserver: unknown ipc message type %d", typ)
	}
	var req Request
	if err := json.Unmarshal(payload, &req); err != nil {
		return 0, nil, fmt.Errorf("go-jobserver: bad request: %w", err)
	}
	resp := s.Handle(req)
	body, err := json.Marshal(resp)
	if err != nil {
		return 0, nil, err
	}
	return ipcCallType, body, nil
}

// IPCName is the service name derived from a directory.
func IPCName(dir string) string {
	if dir == "" {
		dir = DefaultDir()
	}
	return "go-jobserver-" + nameSuffix(dir)
}
