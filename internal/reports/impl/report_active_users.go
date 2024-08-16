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

type ActiveUsersReport struct {
	Items []reports.Item
}

func (a ActiveUsersReport) ReportRequestFunc(propertyId, startDate, endDate string) *ga.RunReportRequest {
	return &ga.RunReportRequest{
		Property: "properties/" + propertyId,
		DateRanges: []*ga.DateRange{
			{
				StartDate: startDate,
				EndDate:   endDate,
			},
		},
		Dimensions: []*ga.Dimension{
			{Name: "country"},
			{Name: "region"},
			{Name: "city"},
			{Name: "date"},
		},
		Metrics: []*ga.Metric{
			{
				Name: "activeUsers",
			},
			{
				Name: "newUsers",
			},
			{
				Name: "sessions",
			},
			{
				Name: "totalUsers",
			},
			{
				Name: "active1DayUsers",
			},
		},
	}
}

func (a ActiveUsersReport) ReportTitle() string {
	return fmt.Sprintf("daily_active_users_%s", time.Now().Format("20060102"))
}

type ActiveUsersReportItem struct {
	Country         string `json:"country"`
	Region          string `json:"region"`
	City            string `json:"city"`
	Date            string `json:"date"`
	ActiveUsers     int    `json:"active_users"`
	NewUsers        int    `json:"new_users"`
	Session         int    `json:"session"`
	TotalUsers      int    `json:"total_users"`
	Active1DayUsers int    `json:"active_1day_users"`
}

func (a ActiveUsersReportItem) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = map[string]bigquery.Value{
		"country":           a.Country,
		"region":            a.Region,
		"city":              a.City,
		"date":              a.Date,
		"active_users":      a.ActiveUsers,
		"new_users":         a.NewUsers,
		"session":           a.Session,
		"total_users":       a.TotalUsers,
		"active_1day_users": a.Active1DayUsers,
	}
	return row, bigquery.NoDedupeID, nil
}

func (a ActiveUsersReportItem) Row() (row []string) {
	var values []string
	values = append(values, a.City)
	return values
}

func (a ActiveUsersReport) CsvWriter(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Writing the header
	if err := writer.Write([]string{"Country", "Region", "City", "Date", "ActiveUsers", "Session", "total_users", "active_1day"}); err != nil {
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

func (ActiveUsersReport) TransformFunc(result *ga.RunReportResponse) ([]bigquery.ValueSaver, error) {
	var transformedData []bigquery.ValueSaver
	for _, row := range result.Rows {
		if len(row.DimensionValues) > 0 && len(row.MetricValues) > 0 {
			activeUsers, err := strconv.Atoi(row.MetricValues[0].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert activeUsers to int")
			}

			newUsers, err := strconv.Atoi(row.MetricValues[1].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert newUsers to int")
			}

			session, err := strconv.Atoi(row.MetricValues[2].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert session to int")
			}
			totalUsers, err := strconv.Atoi(row.MetricValues[3].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert totalUsers to int")
			}

			active1Day, err := strconv.Atoi(row.MetricValues[4].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert active1Day to int")
			}

			transformedData = append(transformedData, ActiveUsersReportItem{
				Country:         row.DimensionValues[0].Value,
				Region:          row.DimensionValues[1].Value,
				City:            row.DimensionValues[2].Value,
				Date:            row.DimensionValues[3].Value,
				ActiveUsers:     activeUsers,
				NewUsers:        newUsers,
				Session:         session,
				TotalUsers:      totalUsers,
				Active1DayUsers: active1Day,
			})
		}
	}
	return transformedData, nil
}

func (ActiveUsersReport) Schema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "country", Required: true, Type: bigquery.StringFieldType},
		{Name: "region", Required: true, Type: bigquery.StringFieldType},
		{Name: "city", Required: true, Type: bigquery.StringFieldType},
		{Name: "date", Required: true, Type: bigquery.StringFieldType},
		{Name: "active_users", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "new_users", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "session", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "total_users", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "active_1day_users", Required: true, Type: bigquery.IntegerFieldType},
	}
}
