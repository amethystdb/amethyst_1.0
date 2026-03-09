package wal

//package main

import (
	"amethyst/internal/common"
	"encoding/binary"
	"io"   //ReadFull
	"os"   //FileHandling
	"sync" //Mutex
)

// interface for wal
type WAL interface {
	LogPut(key string, value []byte) error
	LogDelete(key string) error
	ReadAll() ([]common.WALEntry, error)
	Truncate() error
}

type diskWAL struct {
	file *os.File //file obj on hard drive
	path string
	mu   sync.Mutex //mutex lock, only one at a time
}

// creates new or uses existing
func NewDiskWAL(path string) (WAL, error) {
	//O_Append means append only, O_Create - make if missing and O_RDWR- read write
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &diskWAL{file: f, path: path}, nil
}

func (w *diskWAL) LogPut(key string, value []byte) error {
	//create walentry struc and adds to write func
	return w.write(common.WALEntry{Key: key, Value: value, Tombstone: false})
}

func (w *diskWAL) LogDelete(key string) error {
	//adds a tombstone=true
	return w.write(common.WALEntry{Key: key, Tombstone: true})
}

// write func
func (w *diskWAL) write(entry common.WALEntry) error {
	w.mu.Lock()         //locked mutex
	defer w.mu.Unlock() //unlock mutex when over

	// Format: KeyLen(4)| ValLen(4)| Tombstone(1)| KeyBytes| ValBytes
	header := make([]byte, 9)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(entry.Key)))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(entry.Value)))
	if entry.Tombstone {
		header[8] = 1
	} else {
		header[8] = 0
	}

	//writing header then key then value
	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if _, err := w.file.WriteString(entry.Key); err != nil {
		return err
	}
	if _, err := w.file.Write(entry.Value); err != nil {
		return err
	}

	//write to phy disk
	return nil // Don't sync on every write - too slow for benchmarks
}

// on start to reconstruct db
func (w *diskWAL) ReadAll() ([]common.WALEntry, error) {
	//mutex lock and unlock
	w.mu.Lock()
	defer w.mu.Unlock()

	//move marker to thr front
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}
	var entries []common.WALEntry

	//till EOF
	for {
		header := make([]byte, 9)
		//checks for 9 bytes, if lesser err
		if _, err := io.ReadFull(w.file, header); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		//get each parts length
		kLen := binary.BigEndian.Uint32(header[0:4])
		vLen := binary.BigEndian.Uint32(header[4:8])
		isTomb := header[8] == 1

		//buffers to hold the key value
		keyBuf := make([]byte, kLen)
		valBuf := make([]byte, vLen)
		//read them
		if _, err := io.ReadFull(w.file, keyBuf); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(w.file, valBuf); err != nil {
			return nil, err
		}

		//add completed entry to list
		entries = append(entries, common.WALEntry{Key: string(keyBuf), Value: valBuf, Tombstone: isTomb})
	}
	return entries, nil //return full list
}

// clear
func (w *diskWAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	// Clear the file to size 0 (but keep it open)
	if err := w.file.Truncate(0); err != nil {
		return err
	}

	// Seek back to beginning for next writes
	_, err := w.file.Seek(0, 0)
	return err
}
