package mlog

import (
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/taonhanvat/prolog/api"
)

type Log struct {
	mu            sync.RWMutex
	activeSegment *segment
	segments      []*segment
	dirName       string
	Config        Config
}

func NewLog(dirName string, config Config) (*Log, error) {
	config = handleConfig(config)
	log := &Log{Config: config, dirName: dirName}

	// Read all persisted index files to initialize all representation entities(segments, indexes, stores)
	var baseOffsets []uint64
	dir, err := os.OpenFile(dirName, os.O_RDONLY, 0666)
	if err != nil {
		return log, err
	}
	files, err := dir.ReadDir(0)
	if err != nil {
		return log, err
	}
	for _, file := range files {
		if strings.Contains(file.Name(), "index") {
			offsetString := strings.TrimSuffix(
				file.Name(),
				path.Ext(file.Name()),
			)
			offset, _ := strconv.ParseInt(offsetString, 10, 0)
			baseOffsets = append(baseOffsets, uint64(offset))
		}
	}

	slices.Sort(baseOffsets)
	for _, baseOffset := range baseOffsets {
		segment, err := newSegment(dirName, baseOffset, config)
		if err != nil {
			return log, err
		}
		log.segments = append(log.segments, segment)
	}

	if log.segments == nil {
		segment, err := newSegment(dirName, config.Internal.Store.InitOffset, config)
		if err != nil {
			return log, err
		} else {
			log.segments = append(log.segments, segment)
			log.activeSegment = segment
		}
	} else {
		log.activeSegment = log.segments[len(log.segments)-1]
	}
	return log, nil
}

func (l *Log) Append(r *api.Record) (offset uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	offset, err = l.activeSegment.append(r)
	if err == errExceedStoreLimit || err == errExceedIndexLimit {
		initOffsetSegment := l.activeSegment.baseOffset + l.activeSegment.nextOffset + 1
		newSegment, err := newSegment(l.dirName, initOffsetSegment, l.Config)
		if err != nil {
			return 0, err
		}
		l.activeSegment = newSegment
		l.segments = append(l.segments, l.activeSegment)
		offset, err = l.activeSegment.append(r)
		return offset + l.activeSegment.baseOffset, err
	} else if err != nil {
		return 0, err
	}
	return offset + l.activeSegment.baseOffset, err
}

func (l *Log) Read(offset uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, segment := range l.segments {
		if segment.baseOffset <= offset && segment.baseOffset+segment.nextOffset >= offset {
			record, err := segment.read(offset - segment.baseOffset)
			if err != nil {
				if err == io.EOF {
					return nil, fmt.Errorf("eof while reading store at offset %d (segment base: %d, relative offset: %d)",
						offset, segment.baseOffset, offset-segment.baseOffset)
				}
				return nil, fmt.Errorf("error reading segment at offset %d (segment base: %d, relative offset: %d): %w",
					offset, segment.baseOffset, offset-segment.baseOffset, err)
			}
			record.Offset = record.Offset + segment.baseOffset
			return record, nil
		}
	}
	return nil, fmt.Errorf("offset %d is not existed in any segment (segments: %v)", offset, l.segments)
}

func (l *Log) FirstIndex() uint64 {
	return l.segments[0].baseOffset
}

func (l *Log) LastIndex() uint64 {
	return l.segments[len(l.segments)-1].nextOffset
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Truncate(until uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, segment := range l.segments {
		if segment.nextOffset <= until {
			if err := segment.close(); err != nil {
				return err
			}
		} else {
			segments = append(segments, segment)
		}
	}
	l.segments = segments
	l.activeSegment = segments[len(segments)-1]
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.dirName)
}

func (l *Log) reader() io.Reader {
	var readers []io.Reader
	for _, segment := range l.segments {
		readers = append(readers, segment.store.reader())
	}
	return io.MultiReader(readers...)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.dirName)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains dup for index and store, so we skip the dup
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(l.Config.Internal.Store.InitOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.dirName, off, l.Config)
	if err != nil {
		return err
	}
	newSegment := s
	l.segments = append(l.segments, newSegment)
	l.activeSegment = newSegment
	return nil
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	highestSegment := l.segments[len(l.segments)-1]
	off := highestSegment.nextOffset + highestSegment.baseOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func handleConfig(c Config) Config {
	if c.Internal.Index.MaxSize == 0 {
		c.Internal.Index.MaxSize = defaultIndexMaxSize
	}
	if c.Internal.Store.MaxSize == 0 {
		c.Internal.Store.MaxSize = defaultStoreMaxSize
	}
	return c
}
