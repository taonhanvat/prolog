package mlog

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	indexLength         uint64 = 8
	errExceedIndexLimit        = errors.New("exceed index limit")
)

type index struct {
	file       *os.File
	mappedFile gommap.MMap
}

func newIndex(file *os.File, c Config) (*index, error) {
	index := &index{file: file}

	os.Truncate(file.Name(), int64(c.Internal.Index.MaxSize))
	fd := file.Fd()
	mappedFile, err := gommap.Map(fd, gommap.PROT_WRITE|gommap.PROT_READ, gommap.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("got this error when trying to map your provided file with a memory address: %v", err)
	}
	index.mappedFile = mappedFile

	return index, nil
}

func (index *index) append(data uint64, nextOffset uint64) (uint64, error) {
	if uint64(len(index.mappedFile)) < scaleOffset(nextOffset+1) {
		return 0, errExceedIndexLimit
	}
	byteOrder.PutUint64(
		index.mappedFile[scaleOffset(nextOffset):scaleOffset(nextOffset+1)],
		data,
	)
	return nextOffset + 1, nil
}

func (index *index) read(offset int) (storePosition uint64, err error) {
	if len(index.mappedFile) < offset {
		err = io.EOF
		return
	} else {
		storePosition =
			byteOrder.Uint64(
				index.mappedFile[scaleOffset(uint64(offset)):scaleOffset(uint64(offset+1))],
			)
	}

	return storePosition, nil
}

func scaleOffset(offset uint64) uint64 {
	return offset * indexLength
}

func (index *index) close(nextOffset uint64) error {
	if err := index.mappedFile.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := index.file.Sync(); err != nil {
		return err
	}
	if err := index.file.Truncate(int64(scaleOffset(nextOffset))); err != nil {
		return err
	}
	return index.file.Close()
}
