package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	parquet "github.com/fraugster/parquet-go"
)

func ReadParquets() ([]SparkEvent, error) {
	folderPath := DIR_WATCH // Replace with your folder path
	records := []SparkEvent{}
	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".parquet" {
			records, err = readParquetFile(path)
			if err != nil {
				log.Printf("Error reading %s: %v\n", path, err)
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking the path %q: %v\n", folderPath, err)
	}
	return records, nil
}

func readParquetFile(filePath string) ([]SparkEvent, error) {
	fr, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %q: %w", filePath, err)
	}
	defer fr.Close()

	pr, err := parquet.NewFileReader(fr)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader for %q: %w", filePath, err)
	}

	records := []SparkEvent{}
	for {
		row, err := pr.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading record failed: %w", err)
		}

		record := SparkEvent{}
		record.Event = string(row["event"].([]uint8))
		record.Timestamp = row["timestamp"].(int64)
		record.SparkVersion = string(row["spark_version"].([]uint8))
		record.AppName = string(row["app_name"].([]uint8))
		record.AppId = string(row["app_id"].([]uint8))
		record.User = string(row["user"].([]uint8))
		record.StageID = row["stage_id"].(int32)
		record.StageAttemptID = row["stage_attempt_id"].(int32)
		record.TaskInfo.TaskID = row["task_id"].(int64)
		record.TaskInfo.Index = row["index"].(int64)
		record.TaskInfo.Attempt = row["attempt"].(int64)
		record.JobID = row["job_id"].(int32)
		record.ExecutorID = string(row["executor_id"].([]uint8))
		record.ExtraData = string(row["extra_data"].([]uint8))
		records = append(records, record)
	}

	return records, nil
}
