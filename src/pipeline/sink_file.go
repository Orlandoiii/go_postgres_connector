package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
)

type FileSinkFactory struct {
	outputDir string
	logger    observability.Logger
	mu        sync.Mutex
	files     map[string]*os.File
}

func NewFileSinkFactory(outputDir string, logger observability.Logger) *FileSinkFactory {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		logger.Error(context.Background(), "Error creando directorio de salida", err)
	}

	return &FileSinkFactory{
		outputDir: outputDir,
		logger:    logger,
		files:     make(map[string]*os.File),
	}
}

func (fsf *FileSinkFactory) CreateSink(tableKey string) (EventSink, error) {
	fsf.mu.Lock()
	defer fsf.mu.Unlock()

	if file, exists := fsf.files[tableKey]; exists {
		return &FileSink{
			file:   file,
			logger: fsf.logger,
		}, nil
	}

	fileName := fsf.getFileName(tableKey)
	filePath := fmt.Sprintf("%s/%s", fsf.outputDir, fileName)

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fsf.files[tableKey] = file

	return &FileSink{
		file:   file,
		logger: fsf.logger,
	}, nil
}

func (fsf *FileSinkFactory) getFileName(tableKey string) string {
	idx := -1
	for i := len(tableKey) - 1; i >= 0; i-- {
		if tableKey[i] == ':' {
			idx = i
			break
		}
	}

	if idx >= 0 {
		topic := tableKey[idx+1:]
		return fmt.Sprintf("%s.json", topic)
	}

	return fmt.Sprintf("%s.json", tableKey)
}

func (fsf *FileSinkFactory) Close() error {
	fsf.mu.Lock()
	defer fsf.mu.Unlock()

	for _, file := range fsf.files {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

type FileSink struct {
	file   *os.File
	logger observability.Logger
	mu     sync.Mutex
}

func (fs *FileSink) PersistEvent(ctx context.Context,
	changeEvent *ChangeEventSink, txEvent *TransactionEvent) error {
	if changeEvent == nil || txEvent == nil {
		return nil
	}

	if !txEvent.IsCommit {
		return nil
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	jsonData, err := json.Marshal(changeEvent)
	if err != nil {
		return fmt.Errorf("serialize event: %w", err)
	}

	if _, err := fs.file.Write(jsonData); err != nil {
		return fmt.Errorf("write to file: %w", err)
	}

	if _, err := fs.file.Write([]byte("\n")); err != nil {
		return fmt.Errorf("write newline: %w", err)
	}

	return nil
}

func (fs *FileSink) Close() error {
	return nil
}
