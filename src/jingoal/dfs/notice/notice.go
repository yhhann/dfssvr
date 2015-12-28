// Package notice processes the event fired by infrastructure.
package notice

// Notice is a interface process the notice operator.
type Notice interface {
	// CheckChildren checks the path, returned chan will be noticed
	// when its children changed.
	CheckChildren(path string) (<-chan []string, <-chan error)

	// CheckDataChange checks the path, returned chan will be noticed
	// when its data changed.
	CheckDataChange(path string) (<-chan []byte, <-chan error)

	// GetData returns the data of given path
	GetData(path string) ([]byte, error)

	// Register registers a server. returned chan will be noticed
	// when its sibling nodes changed. If check is true,
	// will start a routine to process the nodes change, otherwise not.
	Register(prefix string, data []byte, check bool) (string, <-chan []byte, <-chan error)
}
