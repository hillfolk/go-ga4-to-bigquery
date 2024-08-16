package impl

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	ga "google.golang.org/api/analyticsdata/v1beta"

	"go-ga4-to-bigquery/internal/reports"
)

// 브라우저 별 사용자 보고서

type UserTechnologyReport struct {
	Items []reports.Item
}

func (r UserTechnologyReport) ReportRequestFunc(propertyId, startDate, endDate string) *ga.RunReportRequest {
	return &ga.RunReportRequest{
		Property: "properties/" + propertyId,
		DateRanges: []*ga.DateRange{
			{
				StartDate: startDate,
				EndDate:   endDate,
			},
		},
		Dimensions: []*ga.Dimension{
			{Name: "browser"},
			{Name: "operatingSystem"},
			{Name: "platform"},
			{Name: "deviceCategory"},
			{Name: "date"},
		},
		Metrics: []*ga.Metric{
			{
				Name: "activeUsers",
			},
			{
				Name: "sessions",
			},
		},
	}
}

func (a UserTechnologyReport) ReportTitle() string {
	return fmt.Sprintf("user_technology_%s", time.Now().Format("20060102"))
}

type UserTechnologyReportItem struct {
	Browser         string `json:"browser"`
	OperatingSystem string `json:"operating_system"`
	Platform        string `json:"platform"`
	DeviceCategory  string `json:"device_category"`
	Date            string `json:"date"`
	ActiveUsers     int    `json:"active_users"`
	Session         int    `json:"session"`
}

func (a UserTechnologyReportItem) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = map[string]bigquery.Value{
		"browser":          a.Browser,
		"operating_system": a.OperatingSystem,
		"platform":         a.Platform,
		"device_category":  a.DeviceCategory,
		"date":             a.Date,
		"active_users":     a.ActiveUsers,
		"session":          a.Session,
	}
	return row, bigquery.NoDedupeID, nil
}

func (a UserTechnologyReportItem) Row() (row []string) {
	var values []string
	values = append(values, a.Browser)
	values = append(values, a.OperatingSystem)
	values = append(values, a.Platform)
	values = append(values, a.DeviceCategory)
	values = append(values, a.Date)
	values = append(values, strconv.Itoa(a.ActiveUsers))
	values = append(values, strconv.Itoa(a.Session))
	return values
}

func (a UserTechnologyReport) CsvWriter(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Writing the header
	if err := writer.Write([]string{"Country", "Region", "City", "Date", "ActiveUsers"}); err != nil {
		return err
	}

	// Writing data
	for _, item := range a.Items {
		if err := writer.Write(item.Row()); err != nil {
			return err
		}
	}

	log.Printf("Data successfully saved to %s", filePath)
	return nil
}

func (UserTechnologyReport) TransformFunc(result *ga.RunReportResponse) ([]bigquery.ValueSaver, error) {
	var transformedData []bigquery.ValueSaver
	for _, row := range result.Rows {
		if len(row.DimensionValues) > 0 && len(row.MetricValues) > 0 {
			activeUsers, err := strconv.Atoi(row.MetricValues[0].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert activeUsers to int")
			}

			session, err := strconv.Atoi(row.MetricValues[1].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert newUsers to int")
			}

			transformedData = append(transformedData, UserTechnologyReportItem{
				Browser:         row.DimensionValues[0].Value,
				OperatingSystem: row.DimensionValues[1].Value,
				Platform:        row.DimensionValues[2].Value,
				DeviceCategory:  row.DimensionValues[3].Value,
				Date:            row.DimensionValues[4].Value,
				ActiveUsers:     activeUsers,
				Session:         session,
			})
		}
	}
	return transformedData, nil
}

func (UserTechnologyReport) Schema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "browser", Required: true, Type: bigquery.StringFieldType},
		{Name: "operating_system", Required: true, Type: bigquery.StringFieldType},
		{Name: "platform", Required: true, Type: bigquery.StringFieldType},
		{Name: "device_category", Required: true, Type: bigquery.StringFieldType},
		{Name: "date", Required: true, Type: bigquery.StringFieldType},
		{Name: "active_users", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "session", Required: true, Type: bigquery.IntegerFieldType},
	}
}
