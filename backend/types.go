package main

// SparkEvent matches your `json structure, with optional parquet tags.
type SparkEvent struct {
	Event          string   `json:"Event"`
	Timestamp      int64    `json:"Timestamp"`
	SparkVersion   string   `json:"SparkVersion"`
	AppName        string   `json:"AppName"`
	AppId          string   `json:"AppId"`
	User           string   `json:"User"`
	StageID        int32    `json:"StageID"`
	StageAttemptID int32    `json:"StageAttemptID"`
	TaskInfo       TaskInfo `json:"TaskInfo"`
	JobID          int32    `json:"JobID"`
	ExecutorID     string   `json:"ExecutorID"`
	ExtraData      string   `json:"ExtraData"`
}

// TaskInfo is the nested portion of SparkEvent; note the parquet struct tag with the repeated fields.
type TaskInfo struct {
	TaskID  int64 `json:"TaskID"`
	Index   int64 `json:"Index"`
	Attempt int64 `json:"Attempt"`
}

// SparkEvent parquet schema.
type SparkEvenSchema struct {
	Event          string `parquet:"name=event, type=UTF8"`
	Timestamp      int64  `parquet:"name=timestamp, type=INT64"`
	SparkVersion   string `parquet:"name=spark_version, type=UTF8"`
	AppName        string `parquet:"name=app_name, type=UTF8"`
	AppId          string `parquet:"name=app_id, type=UTF8"`
	User           string `parquet:"name=user, type=UTF8"`
	StageID        int32  `parquet:"name=stage_id, type=INT32"`
	StageAttemptID int32  `parquet:"name=stage_attempt_id, type=INT32"`
	TaskID         int64  `parquet:"name=task_id, type=INT64"`
	Index          int64  `parquet:"name=index, type=INT64"`
	Attempt        int64  `parquet:"name=attempt, type=INT64"`
	JobID          int32  `parquet:"name=job_id, type=INT32"`
	ExecutorID     string `parquet:"name=executor_id, type=UTF8"`
	ExtraData      string `parquet:"name=extra_data, type=UTF8"`
}

type RequestData struct {
	Page  int64 `json:"page"`
	Limit int64 `json:"limit"`
}

type ResponseData struct {
	Total int          `json:"total"`
	Page  int64        `json:"page"`
	Limit int64        `json:"limit"`
	Data  []SparkEvent `json:"data,omitempty"`
}

const DIR_WATCH = "../spark-events"
