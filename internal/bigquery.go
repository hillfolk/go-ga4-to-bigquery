package internal

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"

	"go-ga4-to-bigquery/internal/reports"
)

type BigQueryDataLoader struct {
	bqClient *bigquery.Client
}

func NewBigQueryDataLoader(client *bigquery.Client) *BigQueryDataLoader {
	return &BigQueryDataLoader{
		bqClient: client,
	}
}

// QueryTable queries a table in BigQuery
func selectQuery(ctx context.Context, bqClient *bigquery.Client, fullTableId string) error {
	var idx int = 0
	query := fmt.Sprintf("SELECT * FROM %s;", fullTableId)

	q := bqClient.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to read query result")
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err != nil {
			break
		}
		fmt.Printf("Row %d: %v\n", idx, row)
		idx++
	}
	return nil
}

func (b *BigQueryDataLoader) QueryTable(ctx context.Context, client *bigquery.Client, datasetID, tableID string, data interface{}) error {

	fullTableId := fmt.Sprintf("%s.%s.%s", client.Project(), datasetID, tableID)
	err := selectQuery(ctx, client, fullTableId)
	if err != nil {
		return errors.Wrap(err, "failed to query table")
	}
	return nil
}

type BigQueryDateInserter struct {
	bqClient *bigquery.Client
}

func NewBigQueryDateInsert(client *bigquery.Client) *BigQueryDateInserter {
	return &BigQueryDateInserter{
		bqClient: client,
	}
}

func (b *BigQueryDateInserter) InsertData(ctx context.Context, client *bigquery.Client, datasetID, tableID string, schemaGen reports.SchemaGenerator, data []bigquery.ValueSaver) error {
	if err := client.Dataset(datasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{
		Schema:   schemaGen.Schema(),
		Location: "US",
	}); err != nil {
		return errors.Wrap(err, "failed to create table")
	}

	if err := client.Dataset(datasetID).Table(tableID).Inserter().Put(ctx, data); err != nil {
		return errors.Wrap(err, "failed to insert data")
	}
	return nil
}

type BigQueryDataInserter struct {
}
