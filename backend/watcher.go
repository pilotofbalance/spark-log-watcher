package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

var linesRead = make(map[string]int64)
var mu sync.Mutex

func WatchForLogs() {
	// Directory to watch for log files.
	dirToWatch := DIR_WATCH
	// Create/ensure the directory exists
	if err := os.MkdirAll(dirToWatch, 0755); err != nil {
		log.Fatalf("Failed to create directory %q: %v\n", dirToWatch, err)
	}

	// Create the fsnotify watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Failed to create watcher: %v\n", err)
	}
	defer watcher.Close()

	// Start watching the directory for changes.
	if err = watcher.Add(dirToWatch); err != nil {
		log.Fatalf("Failed to watch directory %q: %v\n", dirToWatch, err)
	}

	// Use a channel to block the main goroutine.
	done := make(chan bool)

	// Event handling loop.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				// We care about creates and writes.
				if event.Op&(fsnotify.Create|fsnotify.Write) != 0 {
					// Check if the file ends in ".gz".
					if filepath.Ext(event.Name) == ".gz" {
						go readNewLinesFromGzip(event.Name)
					}
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				fmt.Println("Watcher error:", err)
			}
		}
	}()

	fmt.Printf("Watching directory %q for new or updated .gz files...\n", dirToWatch)
	// Block forever (Ctrl+C to stop).
	<-done
}

// readNewLinesFromGzip reads a GZIP‐compressed file from the beginning,
// skips lines already processed previously, and prints only newly added lines.
func readNewLinesFromGzip(path string) {
	mu.Lock()
	defer mu.Unlock()

	// Give the writer a small grace period if the file is still being written.
	// (Sometimes the WRITE event can fire before the file is fully closed.)
	time.Sleep(200 * time.Millisecond)

	// Open the GZIP file.
	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v\n", path, err)
		return
	}
	defer f.Close()

	// Wrap in a GZIP reader.
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Printf("Failed to create GZIP reader for %s: %v\n", path, err)
		return
	}
	defer gz.Close()

	// Create a scanner to read the GZIP‐decompressed lines.
	scanner := bufio.NewScanner(gz)
	var currentLine int64

	for scanner.Scan() {
		currentLine++
		// If we have never seen this line before, write it.
		if currentLine > linesRead[path] {
			rawLine := scanner.Bytes()
			var ev SparkEvent
			if err := json.Unmarshal(rawLine, &ev); err != nil {
				// Log or handle unmarshalling error, but continue scanning next line.
				fmt.Printf("JSON unmarshal error at line %d in %q: %v\n", currentLine, path, err)
				continue
			}

			err = WriteToParquet(path, &ev)
			if err != nil {
				fmt.Println("failed to write parquet: %w", err)
			}

		}
	}

	// Update how many lines we've fully processed.
	linesRead[path] = currentLine
}
