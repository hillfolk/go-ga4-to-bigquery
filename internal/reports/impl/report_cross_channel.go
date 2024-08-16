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

type CrossChannelReport struct {
	Items []reports.Item
}

func (a CrossChannelReport) ReportRequestFunc(propertyId, startDate, endDate string) *ga.RunReportRequest {
	return &ga.RunReportRequest{
		Property: "properties/" + propertyId,
		DateRanges: []*ga.DateRange{
			{
				StartDate: startDate,
				EndDate:   endDate,
			},
		},
		Dimensions: []*ga.Dimension{
			{Name: "sessionCampaignId"},
			{Name: "sessionCampaignName"},
			{Name: "sessionDefaultChannelGroup"},
			{Name: "sessionMedium"},
			{Name: "sessionSource"},
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
		},
	}
}

func (a CrossChannelReport) ReportTitle() string {
	return fmt.Sprintf("daily_cross_channel_%s", time.Now().Format("20060102"))
}

type CrossChannelReportItem struct {
	SessionCampaignId          string `json:"session_campaign_id"`
	SessionCampaignName        string `json:"session_campaign_name"`
	SessionDefaultChannelGroup string `json:"session_default_channel_group"`
	SessionMedium              string `json:"session_medium"`
	SessionSource              string `json:"session_source"`
	Date                       string `json:"date"`
	ActiveUsers                int    `json:"active_users"`
	NewUsers                   int    `json:"new_users"`
	Session                    int    `json:"session"`
	TotalUsers                 int    `json:"total_users"`
}

func (a CrossChannelReportItem) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = map[string]bigquery.Value{
		"session_campaign_id":           a.SessionCampaignId,
		"session_campaign_name":         a.SessionCampaignName,
		"session_default_channel_group": a.SessionDefaultChannelGroup,
		"session_medium":                a.SessionMedium,
		"session_source":                a.SessionSource,
		"date":                          a.Date,
		"active_users":                  a.ActiveUsers,
		"new_users":                     a.NewUsers,
		"session":                       a.Session,
		"total_users":                   a.TotalUsers,
	}
	return row, bigquery.NoDedupeID, nil
}

func (a CrossChannelReportItem) Row() (row []string) {
	var values []string
	values = append(values, a.SessionCampaignId)
	return values
}

func (a CrossChannelReport) CsvWriter(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Writing the header
	if err := writer.Write([]string{
		"SessionCampaignId",
		"SessionCampaignName",
		"SessionDefaultChannelGroup",
		"SessionMedium",
		"SessionSource",
		"Date",
		"ActiveUsers",
		"NewUsers",
		"Session",
		"TotalUsers",
	}); err != nil {
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

func (CrossChannelReport) TransformFunc(result *ga.RunReportResponse) ([]bigquery.ValueSaver, error) {
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

			transformedData = append(transformedData, CrossChannelReportItem{
				SessionCampaignId:          row.DimensionValues[0].Value,
				SessionCampaignName:        row.DimensionValues[1].Value,
				SessionDefaultChannelGroup: row.DimensionValues[2].Value,
				SessionMedium:              row.DimensionValues[3].Value,
				SessionSource:              row.DimensionValues[4].Value,
				Date:                       row.DimensionValues[5].Value,
				ActiveUsers:                activeUsers,
				NewUsers:                   newUsers,
				Session:                    session,
				TotalUsers:                 totalUsers,
			})
		}
	}
	return transformedData, nil
}

func (CrossChannelReport) Schema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "session_campaign_id", Required: true, Type: bigquery.StringFieldType},
		{Name: "session_campaign_name", Required: true, Type: bigquery.StringFieldType},
		{Name: "session_default_channel_group", Required: true, Type: bigquery.StringFieldType},
		{Name: "session_medium", Required: true, Type: bigquery.StringFieldType},
		{Name: "session_source", Required: true, Type: bigquery.StringFieldType},
		{Name: "date", Required: true, Type: bigquery.StringFieldType},
		{Name: "active_users", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "new_users", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "session", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "total_users", Required: true, Type: bigquery.IntegerFieldType},
	}
}
