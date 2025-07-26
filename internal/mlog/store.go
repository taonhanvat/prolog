package mlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	byteOrder           = binary.BigEndian
	errExceedStoreLimit = errors.New("exceed store limit")
)

const (
	// Store uint64
	headerLength = 8
)

type store struct {
	file    *os.File
	buf     *bytes.Buffer
	size    uint64
	maxSize uint64
}

func newStore(file *os.File, c Config) (*store, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return &store{}, fmt.Errorf("got this error when checking file stat: %v", err)
	}
	return &store{
		file:    file,
		buf:     &bytes.Buffer{},
		size:    uint64(fileInfo.Size()),
		maxSize: c.Internal.Store.MaxSize,
	}, nil
}

func (s *store) append(data []byte) (n int, storePos uint64, err error) {
	storePos = s.size
	newPos := uint64(int(s.size) + headerLength + len(data))
	if newPos > s.maxSize {
		err = errExceedStoreLimit
		return
	}
	if err = binary.Write(s.buf, byteOrder, uint64(len(data))); err != nil {
		return 0, 0, err
	}
	n, err = s.buf.Write(data)
	if err != nil {
		return 0, 0, err
	}
	s.size = newPos
	return headerLength + n, storePos, nil
}

func (s *store) commit() error {
	if _, err := s.file.Write(s.buf.Bytes()); err != nil {
		return err
	}
	s.buf.Reset()
	return nil
}

func (s *store) readAt(pos uint64) ([]byte, error) {
	if err := s.commit(); err != nil {
		return nil, fmt.Errorf("error committing store before read at position %d: %w", pos, err)
	}
	bodyLength := make([]byte, headerLength)
	if _, err := s.file.ReadAt(bodyLength, int64(pos)); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("eof while reading header at position %d (store size: %d)", pos, s.size)
		}
		return nil, fmt.Errorf("error reading header at position %d (store size: %d): %w", pos, s.size, err)
	}
	b := make([]byte, byteOrder.Uint64(bodyLength))
	_, err := s.file.ReadAt(b, int64(pos+headerLength))
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("eof while reading body at position %d (store size: %d, body length: %d)",
				pos+headerLength, s.size, len(b))
		}
		return nil, fmt.Errorf("error reading body at position %d (store size: %d, body length: %d): %w",
			pos+headerLength, s.size, len(b), err)
	}
	return b, nil
}

func (s *store) close() error {
	if err := s.commit(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *store) reader() io.Reader {
	return &reader{store: s}
}

type reader struct {
	store  *store
	offset int
}

func (r *reader) Read(b []byte) (int, error) {
	data, err := r.store.readAt(uint64(r.offset))
	n := copy(b, data)
	if err != nil {
		return 0, err
	}
	r.offset += n
	return n, nil
}
