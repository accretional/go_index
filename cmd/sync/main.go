package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	pb "github.com/fred/go_index/proto/goindex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	goIndexURL = "https://index.golang.org/index"
	syncDir    = "sync"
	indexFile  = "index.txt"
	pageSize   = 2000 // max entries per request from the Go index
)

// indexEntry represents a single JSON line from the Go module index.
type indexEntry struct {
	Path      string    `json:"Path"`
	Version   string    `json:"Version"`
	Timestamp time.Time `json:"Timestamp"`
}

// findResumePoint checks for existing .pb files and returns the starting offset
// and the timestamp to resume from.
func findResumePoint() (startOffset int, since string) {
	entries, err := os.ReadDir(syncDir)
	if err != nil || len(entries) == 0 {
		return 0, ""
	}

	// Find the last .pb file
	var pbFiles []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".pb") {
			pbFiles = append(pbFiles, e.Name())
		}
	}
	if len(pbFiles) == 0 {
		return 0, ""
	}
	sort.Strings(pbFiles)
	lastPB := pbFiles[len(pbFiles)-1]

	// Read the last protobuf to get its timestamp
	data, err := os.ReadFile(filepath.Join(syncDir, lastPB))
	if err != nil {
		return 0, ""
	}
	var m pb.Module
	if err := proto.Unmarshal(data, &m); err != nil {
		return 0, ""
	}

	offset := len(pbFiles)
	ts := m.Timestamp.AsTime().Format(time.RFC3339Nano)
	return offset, ts
}

func main() {
	serverAddr := flag.String("server", "", "gRPC server address (e.g. localhost:50051). If empty, only writes local files.")
	limit := flag.Int("limit", 0, "maximum number of entries to fetch (0 = unlimited, fetch entire index)")
	resume := flag.Bool("resume", true, "resume from last synced position")
	flag.Parse()

	if err := os.MkdirAll(syncDir, 0o755); err != nil {
		log.Fatalf("failed to create sync dir: %v", err)
	}

	var (
		since      string
		total      int
		pageNum    int
		client     pb.GoIndexServiceClient
		grpcConn   *grpc.ClientConn
	)

	// Check for resume point
	if *resume {
		offset, ts := findResumePoint()
		if offset > 0 {
			total = offset
			since = ts
			log.Printf("Resuming from entry %d (since=%s)", total, since)
		}
	}

	// Open index.txt in append mode if resuming, create otherwise
	var indexF *os.File
	var err error
	if total > 0 {
		indexF, err = os.OpenFile(indexFile, os.O_APPEND|os.O_WRONLY, 0o644)
	} else {
		indexF, err = os.Create(indexFile)
	}
	if err != nil {
		log.Fatalf("failed to open %s: %v", indexFile, err)
	}
	defer indexF.Close()
	indexW := bufio.NewWriter(indexF)
	defer indexW.Flush()

	if *serverAddr != "" {
		var err error
		grpcConn, err = grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect to gRPC server: %v", err)
		}
		defer grpcConn.Close()
		client = pb.NewGoIndexServiceClient(grpcConn)
	}

	log.Printf("Fetching entire Go module index from %s (limit=%d)", goIndexURL, *limit)

	for {
		pageNum++
		url := goIndexURL
		if since != "" {
			url = fmt.Sprintf("%s?since=%s", goIndexURL, since)
		}

		entries, err := fetchPage(url)
		if err != nil {
			log.Fatalf("failed to fetch page %d: %v", pageNum, err)
		}

		if len(entries) == 0 {
			break
		}

		for _, entry := range entries {
			m := &pb.Module{
				Path:      entry.Path,
				Version:   entry.Version,
				Timestamp: timestamppb.New(entry.Timestamp),
			}

			// Write protobuf to sync/
			data, err := proto.Marshal(m)
			if err != nil {
				log.Printf("warning: failed to marshal %s: %v", m.Path, err)
				continue
			}
			pbFile := filepath.Join(syncDir, fmt.Sprintf("%09d.pb", total))
			if err := os.WriteFile(pbFile, data, 0o644); err != nil {
				log.Printf("warning: failed to write %s: %v", pbFile, err)
				continue
			}

			// Write URL to index.txt
			fmt.Fprintf(indexW, "https://pkg.go.dev/%s@%s\n", m.Path, m.Version)

			// Optionally send to gRPC server
			if client != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := client.AddModule(ctx, &pb.AddModuleRequest{Module: m})
				cancel()
				if err != nil {
					log.Printf("warning: failed to add %s@%s to server: %v", m.Path, m.Version, err)
				}
			}

			total++
			if *limit > 0 && total >= *limit {
				break
			}
		}

		// Flush index periodically
		if pageNum%50 == 0 {
			indexW.Flush()
		}

		log.Printf("Page %d: fetched %d entries (total: %d)", pageNum, len(entries), total)

		if *limit > 0 && total >= *limit {
			break
		}

		// Use the last entry's timestamp as the pagination cursor
		since = entries[len(entries)-1].Timestamp.Format(time.RFC3339Nano)

		// If we got fewer than a full page, we've reached the end
		if len(entries) < pageSize {
			break
		}
	}

	indexW.Flush()
	log.Printf("Done. Total: %d modules, written to %s/ and %s", total, syncDir, indexFile)
}

// fetchPage fetches a single page of entries from the given URL.
func fetchPage(url string) ([]indexEntry, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status from %s: %s", url, resp.Status)
	}

	var entries []indexEntry
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry indexEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			log.Printf("skipping malformed line: %v", err)
			continue
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}
	return entries, nil
}

func loadToServer(addr string, modules []*pb.Module) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	client := pb.NewGoIndexServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for _, m := range modules {
		if _, err := client.AddModule(ctx, &pb.AddModuleRequest{Module: m}); err != nil {
			log.Printf("warning: failed to add %s@%s: %v", m.Path, m.Version, err)
		}
	}

	log.Printf("Loaded %d modules into server at %s", len(modules), addr)
	return nil
}
