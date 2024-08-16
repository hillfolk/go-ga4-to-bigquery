package reports

import (
	"cloud.google.com/go/bigquery"
	ga "google.golang.org/api/analyticsdata/v1beta"
)

// Rport 관련 부분에서 복잡한 부분은 Build 패턴으로 해볼까?

type SchemaGenerator interface {
	Schema() bigquery.Schema
}

type ReportRequester interface {
	ReportRequestFunc(propertyId, startDate, endDate string) *ga.RunReportRequest
}

type Transformer interface {
	TransformFunc(result *ga.RunReportResponse) ([]bigquery.ValueSaver, error)
}

type CsvWriter interface {
	CsvWriter(filePath string) error
}

type Report interface {
	ReportRequester
	Transformer
	SchemaGenerator
	CsvWriter
	ReportTitle() string
}

type Item interface {
	Save() (map[string]bigquery.Value, string, error)
	Row() []string
}
