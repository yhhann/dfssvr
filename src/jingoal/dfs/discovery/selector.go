package discovery

// Selector is an interface which represents the DfsServer Selector
type Selector interface {
	GetPerfectServer() (*DfsServer, error)
}
