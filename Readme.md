# Go GA4 to BigQuery

This is a simple golang cli  to export data from Google Analytics 4 to BigQuery.

## Reference
- https://github.com/aliasoblomov/Backfill-GA4-to-BigQuery 
- [Google Analytics 4 API](https://developers.google.com/analytics/devguides/reporting/data/v1)
- [Google Cloud BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest)
- [Google Cloud BigQuery Go Client](https://pkg.go.dev/cloud.google.com/go/bigquery)

## Requirements
Go 1.16 or later
Google Cloud SDK
Google Analytics 4 Property
Google Cloud Project with BigQuery enabled

## Setup
1. Create a Google Cloud Project
2. Enable BigQuery API
3. Create a BigQuery Dataset
4. Create a Google Analytics 4 Property


## Usage
1. Clone the repository
2. Run the following command to build the binary

```bash
go build -o go-ga4-to-bigquery
```
3. Run the binary
```bash
./go-ga4-to-bigquery run-report --config ./config.json
```

4. Supported Report Types
	- daily-active-users
    - daily-events
    - daily-user-technology
    - daily-user-channel-grouping
    - daily-cross-channel
   