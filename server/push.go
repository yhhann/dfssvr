package server

import (
	"log"
	"strings"
	"time"

	"jingoal.com/dfs/proto/discovery"
)

// GetDfsServers gets a list of DfsServer from server.
func (s *DFSServer) GetDfsServers(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	clientId := strings.Join([]string{req.GetClient().Id, getPeerAddressString(stream.Context())}, "/")

	observer := make(chan struct{}, 100)
	s.register.AddObserver(observer, clientId)

	log.Printf("Client %s connected.", clientId)

	ticker := time.NewTicker(time.Duration(*heartbeatInterval) * time.Second)
outLoop:
	for {
		select {
		case <-observer:
			if err := s.sendDfsServerMap(req, stream); err != nil {
				break outLoop
			}

		case <-ticker.C:
			if err := s.sendHeartbeat(req, stream); err != nil {
				break outLoop
			}
		}
	}

	ticker.Stop()
	s.register.RemoveObserver(observer)
	log.Printf("Client connection closed, client: %s", clientId)

	return nil
}

func (s *DFSServer) sendHeartbeat(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	rep := &discovery.GetDfsServersRep{
		GetDfsServerUnion: &discovery.GetDfsServersRep_Hb{
			Hb: &discovery.Heartbeat{
				Timestamp: time.Now().Unix(),
			},
		},
	}

	if err := stream.Send(rep); err != nil {
		return err
	}

	return nil
}

func (s *DFSServer) sendDfsServerMap(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	sm := s.register.GetDfsServerMap()
	ss := make([]*discovery.DfsServer, 0, len(sm))
	for _, pd := range sm {
		// If we detect a server offline, we set its value to nil,
		// so we must filter nil values out.
		if pd != nil {
			ss = append(ss, pd)
		}
	}

	rep := &discovery.GetDfsServersRep{
		GetDfsServerUnion: &discovery.GetDfsServersRep_Sl{
			Sl: &discovery.DfsServerList{
				Server: ss,
			},
		},
	}

	if err := stream.Send(rep); err != nil {
		return err
	}

	clientId := strings.Join([]string{req.GetClient().Id, getPeerAddressString(stream.Context())}, "/")
	log.Printf("Succeeded to send dfs server list to client: %s, Servers:", clientId)
	for i, s := range ss {
		log.Printf("\t\t%d. DfsServer: %s\n", i+1, strings.Join([]string{s.Id, s.Uri, s.Status.String()}, "/"))
	}

	return nil
}
