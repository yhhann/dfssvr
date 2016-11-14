package conf

const (
	// Flag to enable/disable store to weedfs.
	FlagKeyAsyncStoreToWeed = "async-store-to-weed"

	// Flag to enable/disable read from weedfs.
	FlagKeyReadFromWeed = "read-from-weed"
)

func init() {
	features = make(map[string]*FeatureFlag)

	PutFlag(&FeatureFlag{
		Key:        FlagKeyAsyncStoreToWeed,
		Enabled:    false,
		Users:      []uint32{},
		Groups:     []string{},
		Percentage: uint32(0),
	})

	PutFlag(&FeatureFlag{
		Key:        FlagKeyReadFromWeed,
		Enabled:    false,
		Users:      []uint32{},
		Groups:     []string{},
		Percentage: uint32(0),
	})
}
