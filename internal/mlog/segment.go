package mlog

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/taonhanvat/prolog/api"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	baseOffset uint64
	nextOffset uint64
	store      *store
	index      *index
}

func newSegment(dirName string, baseOffset uint64, c Config) (*segment, error) {
	fmt.Printf("create segment with base: %d\n", baseOffset)
	segment := &segment{baseOffset: baseOffset}

	storeFile, err := os.OpenFile(
		path.Join(dirName, fmt.Sprintf("%d.store", baseOffset)),
		os.O_RDWR|os.O_APPEND|os.O_CREATE,
		0666,
	)
	if err != nil {
		return nil, err
	}
	store, err := newStore(storeFile, c)
	if err != nil {
		return nil, err
	}
	segment.store = store

	indexFile, err := os.OpenFile(
		path.Join(dirName, fmt.Sprintf("%d.index", baseOffset)),
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		return nil, err
	}
	indexFileStat, err := os.Stat(indexFile.Name())
	if err != nil {
		return nil, err
	}
	segment.nextOffset = uint64(indexFileStat.Size()) / indexLength
	index, err := newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}
	segment.index = index

	return segment, nil
}

func (s *segment) append(record *api.Record) (uint64, error) {
	data, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.append(data)
	if err != nil {
		log.Printf("append store error: %s", err)
		return 0, err
	}
	indexedOffset, err := s.index.append(pos, s.nextOffset)
	if err != nil {
		fmt.Printf("index err: %v\n", err)
		return 0, err
	}
	defer func() {
		s.nextOffset = indexedOffset
	}()
	return s.nextOffset, nil
}

func (s *segment) read(offset uint64) (*api.Record, error) {
	record := &api.Record{}
	pos, err := s.index.read(int(offset))
	if err != nil {
		return nil, fmt.Errorf("reading index error: %v", err)
	}
	data, err := s.store.readAt(pos)
	if err != nil {
		return nil, fmt.Errorf("reading store error: %v", err)
	}
	err = proto.Unmarshal(data, record)
	if err != nil {
		return nil, err
	}
	record.Offset = offset
	return record, nil
}

func (s *segment) close() error {
	if err := s.index.close(s.nextOffset); err != nil {
		return err
	}
	if err := s.store.close(); err != nil {
		return err
	}
	return nil
}
