package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	parquet "github.com/fraugster/parquet-go"
	parquetfile "github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

var parquetWriters = make(map[string]*parquet.FileWriter)
var parquetFiles = make(map[string]*os.File)

func WriteToParquet(path string, ev *SparkEvent) error {
	pqPath := gzipPathToParquetPath(path)
	w, err := getOrCreateParquetWriter(pqPath)
	if err != nil {
		return err
	}
	extraBytes, err := json.Marshal(ev.ExtraData)
	if err != nil {
		fmt.Println("failed to marshal ExtraData: %w", err)
	}
	// Build the final map to match your Parquet schema exactly:
	data := map[string]interface{}{
		"event":            ev.Event,
		"timestamp":        ev.Timestamp,
		"spark_version":    ev.SparkVersion,
		"app_name":         ev.AppName,
		"app_id":           ev.AppId,
		"user":             ev.User,
		"stage_id":         int32(ev.StageID),
		"stage_attempt_id": int32(ev.StageAttemptID),
		"task_id":          int64(ev.TaskInfo.TaskID),
		"index":            int64(ev.TaskInfo.Index),
		"attempt":          int64(ev.TaskInfo.Attempt),
		"job_id":           int32(ev.JobID),
		"executor_id":      ev.ExecutorID,
		"extra_data":       string(extraBytes),
	}
	// Add the new row to the Parquet writer.
	if err := w.AddData(data); err != nil {
		return err
	}
	w.Close()
	return nil
}

func gzipPathToParquetPath(gzPath string) string {
	if strings.HasSuffix(gzPath, ".gz") {
		return strings.TrimSuffix(gzPath, ".gz") + ".parquet"
	}
	return gzPath + ".parquet"
}

// getOrCreateParquetWriter returns an open FileWriter for pqPath, creating it if needed.
func getOrCreateParquetWriter(pqPath string) (*parquet.FileWriter, error) {
	if w, ok := parquetWriters[pqPath]; ok {
		return w, nil
	}

	// Create or overwrite a new .parquet file
	f, err := os.Create(pqPath)
	if err != nil {
		return nil, fmt.Errorf("cannot create parquet file %q: %w", pqPath, err)
	}

	schema := `
		message autogen_schema {
			optional binary event (UTF8);
			optional int64 timestamp;
			optional binary spark_version (UTF8);
			optional binary app_name (UTF8);
			optional binary app_id (UTF8);
			optional binary user (UTF8);
			optional int32 stage_id;
			optional int32 stage_attempt_id;
			optional int64 task_id;
			optional int64 index;
			optional int64 attempt;
			optional int32 job_id;
			optional binary executor_id (UTF8);
			optional binary extra_data (UTF8);
		  }
        `

	sd, err := parquetschema.ParseSchemaDefinition(schema)
	if err != nil {
		log.Fatalf("Failed to parse schema: %v", err)
	}
	// Create a file writer using the schema we built from SparkEvent
	fw := parquet.NewFileWriter(f, parquet.WithSchemaDefinition(sd), parquet.WithCompressionCodec(parquetfile.CompressionCodec_SNAPPY))

	// Cache it so we can append again when the .gz file grows
	parquetFiles[pqPath] = f
	parquetWriters[pqPath] = fw

	return fw, nil
}
