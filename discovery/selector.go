package discovery

import (
	dpb "jingoal.com/dfs/proto/discovery"
)

// Selector is an interface which represents the DfsServer Selector
type Selector interface {
	GetPerfectServer() (*dpb.DfsServer, error)
}
