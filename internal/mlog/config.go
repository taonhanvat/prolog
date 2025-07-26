package mlog

import "github.com/hashicorp/raft"

const (
	defaultIndexMaxSize = 4096
	defaultStoreMaxSize = 1024 * 1024
)

type Config struct {
	Raft struct {
		StreamLayer    *StreamLayer
		IsBootstrapper bool
		raft.Config
	}
	Internal struct {
		Store struct {
			MaxSize    uint64
			InitOffset uint64
		}
		Index struct {
			MaxSize uint64
		}
	}
}
