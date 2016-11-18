package conf

// The content of this file is copy from
// https://github.com/AntoineAugusti/feature-flags/blob/master/models/feature.go
// Thanks for the original author.

import (
	"fmt"
	"hash/crc32"
	"regexp"
)

// Represents a feature flag
type FeatureFlag struct {
	// The key of a feature flag
	Key string `json:"key"`
	// Tell if a feature flag is enabled. If set to false,
	// the feature flag can still be partially enabled thanks to
	// the Users, Groups and Percentage properties
	Enabled bool `json:"enabled"`
	// Gives access to a feature to specific user IDs
	Users []uint32 `json:"users"`
	// Gives access to a feature to specific groups
	Groups []string `json:"groups"`
	// Gives access to a feature to a percentage of users
	Percentage uint32 `json:"percentage"`
}

// Self validate the properties of a feature flag
func (f FeatureFlag) validate() error {
	// Validate percentage
	if f.Percentage < 0 || f.Percentage > 100 {
		return fmt.Errorf("Percentage must be between 0 and 100")
	}

	// Validate key
	if len(f.Key) < 3 || len(f.Key) > 50 {
		return fmt.Errorf("Feature key must be between 3 and 50 characters")
	}

	if !regexp.MustCompile(`^[a-z0-9_]*$`).MatchString(f.Key) {
		return fmt.Errorf("Feature key must only contain digits, lowercase letters and underscores")
	}
	return nil
}

// Check if a feature flag is enabled
func (f FeatureFlag) isEnabled() bool {
	return f.Enabled || f.Percentage == 100
}

// Check if a feature flag is partially enabled
func (f FeatureFlag) isPartiallyEnabled() bool {
	return !f.isEnabled() && (f.hasUsers() || f.hasGroups() || f.hasPercentage())
}

// Check if a group has access to a feature
func (f FeatureFlag) GroupHasAccess(group string) bool {
	return f.isEnabled() || (f.isPartiallyEnabled() && f.groupInGroups(group))
}

// Check if a user has access to a feature
func (f FeatureFlag) UserHasAccess(user uint32) bool {
	// A user has access:
	// - if the feature is enabled
	// - if the feature is partially enabled and he has been given access explicity
	// - if the feature is partially enabled and he is in the allowed percentage
	return f.isEnabled() || (f.isPartiallyEnabled() && (f.userInUsers(user) || f.userIsAllowedByPercentage(user)))
}

// Tell if specific users have access to the feature
func (f FeatureFlag) hasUsers() bool {
	return len(f.Users) > 0
}

// Tell if specific groups have access to the feature
func (f FeatureFlag) hasGroups() bool {
	return len(f.Groups) > 0
}

// Tell if a specific percentage of users has access to the feature
func (f FeatureFlag) hasPercentage() bool {
	return f.Percentage > 0
}

// Check if a user has access to the feature thanks to the percentage value
func (f FeatureFlag) userIsAllowedByPercentage(user uint32) bool {
	return crc32.ChecksumIEEE(Uint32ToBytes(user))%100 < f.Percentage
}

// Check if a user is in the list of allowed users
func (f FeatureFlag) userInUsers(user uint32) bool {
	return IntInSlice(user, f.Users)
}

// Check if a group is in the list of allowed groups
func (f FeatureFlag) groupInGroups(group string) bool {
	return StringInSlice(group, f.Groups)
}
