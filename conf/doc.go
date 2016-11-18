package conf

// We use zookeeper to store the flag value, and update value of the flag
// once there is a changing of the value stored in zookeeper.

// For example, declaring a flag such as:
// var testFlag = flag.Int("test-flag", 10, "test flag usage.")

// We can modify value of testFlag with following command in zookddper.
// create(set) /shard/conf/dfs.svr.test-flag 15
// carete(set) /shard/conf/dfs.svr.${dfs-node-name}.test-flag 20

// Import feature flag from
// https://github.com/AntoineAugusti/feature-flags/blob/master/models/feature.go
// We can create or set a feature flag through following zk command:
// set /shard/conf/dfs.svr.featureflag.backstore {"key":"backstore","enabled":false,"percentage":0,"users":[2,3]}
