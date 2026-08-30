package logsvc

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// LogSearcher provides log file querying capabilities.
type LogSearcher struct {
	logDir string
}

// NewLogSearcher creates a new LogSearcher.
// logDir is the directory containing log files (from logging config).
func NewLogSearcher(logDir string) *LogSearcher {
	if logDir == "" {
		logDir = "."
	}
	return &LogSearcher{logDir: logDir}
}

// QueryResult holds the result of a log query.
type QueryResult struct {
	TotalMatches  int      `json:"total_matches"`
	ReturnedLines int      `json:"returned_lines"`
	Lines         []string `json:"lines"`
	File          string   `json:"file"`
}

// QueryParams defines the parameters for a log query.
type QueryParams struct {
	Lines   int    // max lines to return, default 100, max 1000
	Keyword string // case-insensitive keyword search
	Regex   string // regex pattern search
	Level   string // level filter: debug/info/warn/error
	File    string // specific log file (relative to logDir), default "app-log.log"
}

// Query searches the log file and returns matching lines.
func (s *LogSearcher) Query(params QueryParams) (*QueryResult, error) {
	fileName := params.File
	if fileName == "" {
		fileName = "app-log.log"
	}
	filePath := filepath.Join(s.logDir, fileName)

	maxLines := params.Lines
	if maxLines <= 0 {
		maxLines = 100
	}
	if maxLines > 1000 {
		maxLines = 1000
	}

	lines, err := s.readLastLines(filePath, maxLines*10) // read extra to allow filtering
	if err != nil {
		return nil, err
	}

	var levelRegex *regexp.Regexp
	if params.Level != "" {
		levelRegex = regexp.MustCompile(`(?i)"level"\s*:\s*"` + regexp.QuoteMeta(params.Level) + `"`)
	}

	var keywordLower string
	if params.Keyword != "" {
		keywordLower = strings.ToLower(params.Keyword)
	}

	var patternRegex *regexp.Regexp
	if params.Regex != "" {
		patternRegex, err = regexp.Compile(params.Regex)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %w", err)
		}
	}

	var matched []string
	for _, line := range lines {
		if levelRegex != nil && !levelRegex.MatchString(line) {
			continue
		}
		if keywordLower != "" && !strings.Contains(strings.ToLower(line), keywordLower) {
			continue
		}
		if patternRegex != nil && !patternRegex.MatchString(line) {
			continue
		}
		matched = append(matched, line)
	}

	totalMatches := len(matched)
	if len(matched) > maxLines {
		matched = matched[len(matched)-maxLines:]
	}

	return &QueryResult{
		TotalMatches:  totalMatches,
		ReturnedLines: len(matched),
		Lines:         matched,
		File:          filePath,
	}, nil
}

// readLastLines reads the last N lines from a file efficiently.
func (s *LogSearcher) readLastLines(filePath string, maxLines int) ([]string, error) {
	f, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("log file not found: %s", filePath)
		}
		return nil, err
	}
	defer f.Close()

	// Read all lines, keep last maxLines
	var lines []string
	scanner := bufio.NewScanner(f)
	// Use a larger buffer for long log lines
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > maxLines {
			lines = lines[1:]
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

// LogFileInfo describes a log file.
type LogFileInfo struct {
	Name       string `json:"name"`
	SizeBytes  int64  `json:"size_bytes"`
	ModifiedAt int64  `json:"modified_at"`
}

// ListFiles returns all log files in the log directory.
func (s *LogSearcher) ListFiles() ([]LogFileInfo, string, error) {
	entries, err := os.ReadDir(s.logDir)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read log directory %s: %w", s.logDir, err)
	}

	var files []LogFileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, LogFileInfo{
			Name:       entry.Name(),
			SizeBytes:  info.Size(),
			ModifiedAt: info.ModTime().UnixMilli(),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].ModifiedAt > files[j].ModifiedAt
	})

	currentFile := filepath.Join(s.logDir, "app-log.log")
	return files, currentFile, nil
}
