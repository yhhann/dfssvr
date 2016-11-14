package conf

import (
	"fmt"
	"sync"
)

var (
	features map[string]*FeatureFlag
	flock    sync.RWMutex
)

// PutFlag puts a feature flag into configuration context.
func PutFlag(f *FeatureFlag) error {
	flock.Lock()
	defer flock.Unlock()

	if _, ok := features[f.Key]; ok {
		return fmt.Errorf("feature already existed")
	}
	features[f.Key] = f

	return nil
}

// GetFlag gets a feature flag from configuration context.
func GetFlag(key string) (*FeatureFlag, error) {
	flock.RLock()
	defer flock.RUnlock()

	f, ok := features[key]
	if !ok {
		return nil, fmt.Errorf("feature not exist")
	}

	return f, nil
}

// UpdateFlag updates an existed feature flag.
// for example:
// set/create /shard/conf/dfs.svr.featureflag.foo
//  {"key":"foo","enabled":false,"percentage":100,"users":[1,2,3],
//  "groups":["a","b","c"]}
func UpdateFlag(f *FeatureFlag) error {
	flock.Lock()
	defer flock.Unlock()

	if _, ok := features[f.Key]; !ok {
		return fmt.Errorf("feature not exist")
	}
	features[f.Key] = f

	return nil
}
