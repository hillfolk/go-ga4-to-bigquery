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

type UserChannelGroupingReport struct {
	Items []reports.Item
}

func (r UserChannelGroupingReport) ReportRequestFunc(propertyId, startDate, endDate string) *ga.RunReportRequest {
	return &ga.RunReportRequest{
		Property: "properties/" + propertyId,
		DateRanges: []*ga.DateRange{
			{
				StartDate: startDate,
				EndDate:   endDate,
			},
		},
		Dimensions: []*ga.Dimension{

			{Name: "defaultChannelGrouping"},
			{Name: "date"},
		},
		Metrics: []*ga.Metric{
			{
				Name: "activeUsers",
			},
		},
	}
}

func (r UserChannelGroupingReport) ReportTitle() string {
	return fmt.Sprintf("user_channel_grouping_%s", time.Now().Format("20060102"))
}

type UserChannelGroupingReportItem struct {
	DefaultChannelGrouping string `json:"default_channel_grouping"`
	Date                   string `json:"date"`
	ActiveUsers            int    `json:"active_users"`
}

func (a UserChannelGroupingReportItem) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = map[string]bigquery.Value{
		"default_channel_grouping": a.DefaultChannelGrouping,
		"date":                     a.Date,
		"active_users":             a.ActiveUsers,
	}
	return row, bigquery.NoDedupeID, nil
}

func (a UserChannelGroupingReportItem) Row() (row []string) {
	var values []string
	values = append(values, a.DefaultChannelGrouping)
	values = append(values, a.Date)
	values = append(values, strconv.Itoa(a.ActiveUsers))
	return values
}

func (a UserChannelGroupingReport) CsvWriter(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Writing the header
	if err := writer.Write([]string{"DefaultChannelGrouping", "Date", "ActiveUsers", "NewUsers"}); err != nil {
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

func (UserChannelGroupingReport) TransformFunc(result *ga.RunReportResponse) ([]bigquery.ValueSaver, error) {
	var transformedData []bigquery.ValueSaver
	for _, row := range result.Rows {
		if len(row.DimensionValues) > 0 && len(row.MetricValues) > 0 {
			activeUsers, err := strconv.Atoi(row.MetricValues[0].Value)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert activeUsers to int")
			}

			transformedData = append(transformedData, UserChannelGroupingReportItem{
				DefaultChannelGrouping: row.DimensionValues[0].Value,
				Date:                   row.DimensionValues[1].Value,
				ActiveUsers:            activeUsers,
			})
		}
	}
	return transformedData, nil
}

func (UserChannelGroupingReport) Schema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "default_channel_grouping", Required: true, Type: bigquery.StringFieldType},
		{Name: "date", Required: true, Type: bigquery.StringFieldType},
		{Name: "active_users", Required: true, Type: bigquery.IntegerFieldType},
	}
}
