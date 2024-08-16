package impl

import (
	"encoding/csv"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	ga "google.golang.org/api/analyticsdata/v1beta"

	"go-ga4-to-bigquery/internal/reports"
)

type EventsReport struct {
	Items []reports.Item
}
type EventReportItem struct {
	EventName         string `json:"event_name"`           // ct_active_users
	IsConversion      string `json:"is_conversion"`        // None
	EventDate         string `json:"event_date"`           // Dimension
	ChannelGroup      string `json:"channel_group"`        // Dimension
	EventCount        string `json:"event_count"`          // Metric
	EventCountPerUser string `json:"event_count_per_user"` // Metric
	EventsPerSession  string `json:"events_per_session"`   // Metric
	EventType         string `json:"event_type"`           // Traffic
}

func (e EventsReport) ReportRequestFunc(propertyId, startDate, endDate string) *ga.RunReportRequest {
	return &ga.RunReportRequest{
		Property: "properties/" + propertyId,
		DateRanges: []*ga.DateRange{
			{
				StartDate: startDate,
				EndDate:   endDate,
			},
		},
		Dimensions: []*ga.Dimension{
			{Name: "eventName"},                  // event_name
			{Name: "isConversionEvent"},          // is_conversion
			{Name: "date"},                       // eventDate
			{Name: "sessionDefaultChannelGroup"}, // channelGroup
		},
		Metrics: []*ga.Metric{
			{
				Name: "eventCount",
			},
			{
				Name: "eventCountPerUser",
			},
			{
				Name: "eventsPerSession",
			},
		},
	}
}

func (e EventsReport) ReportTitle() string {
	return "daily_events_report_" + time.Now().Format("20060102")
}

func (e EventReportItem) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = map[string]bigquery.Value{
		"event_name":           e.EventName,
		"is_conversion":        e.IsConversion,
		"event_date":           e.EventDate,
		"channel_group":        e.ChannelGroup,
		"event_count":          e.EventCount,
		"event_count_per_user": e.EventCountPerUser,
		"events_per_session":   e.EventsPerSession,
		"event_type":           e.EventType,
	}
	return row, bigquery.NoDedupeID, nil
}

func (e EventReportItem) Row() (row []string) {
	var values []string
	values = append(values, e.EventName, e.IsConversion, e.EventDate, e.ChannelGroup, e.EventCount, e.EventCountPerUser, e.EventsPerSession, e.EventType)
	return values
}

func (e EventsReport) CsvWriter(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Writing the header
	if err := writer.Write([]string{"EventName", "EventValue", "IsConversion", "EventDate", "ChannelGroup", "EventCount", "EventCountPerUser", "EventCountPerSession", "EventType"}); err != nil {
		return err
	}

	// Writing data
	for _, item := range e.Items {
		if err := writer.Write(item.Row()); err != nil {
			return err
		}
	}

	log.Printf("Data successfully saved to %s", filePath)
	return nil
}

/*
{Name: "eventName"},                  // event_name
			{Name: "isConversionEvent"},          // is_conversion
			{Name: "date"},                       // eventDate
			{Name: "sessionDefaultChannelGroup"}, // channelGroup
			{Name: "screenResolution"},           // screenResolution
*/

func (EventsReport) TransformFunc(result *ga.RunReportResponse) ([]bigquery.ValueSaver, error) {
	var transformedData []bigquery.ValueSaver
	for _, row := range result.Rows {
		if len(row.DimensionValues) > 0 && len(row.MetricValues) > 0 {
			transformedData = append(transformedData, EventReportItem{
				EventName:         row.DimensionValues[0].Value,
				IsConversion:      row.DimensionValues[1].Value,
				EventDate:         row.DimensionValues[2].Value,
				ChannelGroup:      row.DimensionValues[3].Value,
				EventCount:        row.MetricValues[0].Value,
				EventCountPerUser: row.MetricValues[1].Value,
				EventsPerSession:  row.MetricValues[2].Value,
				EventType:         "Traffic",
			})
		}
	}
	return transformedData, nil
}

func (EventsReport) Schema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "event_name", Required: true, Type: bigquery.StringFieldType},
		{Name: "is_conversion", Required: true, Type: bigquery.StringFieldType},
		{Name: "event_date", Required: true, Type: bigquery.StringFieldType},
		{Name: "channel_group", Required: true, Type: bigquery.StringFieldType},
		{Name: "event_count", Required: true, Type: bigquery.IntegerFieldType},
		{Name: "event_count_per_user", Required: true, Type: bigquery.FloatFieldType},
		{Name: "events_per_session", Required: true, Type: bigquery.FloatFieldType},
		{Name: "event_type", Required: true, Type: bigquery.StringFieldType},
	}
}
