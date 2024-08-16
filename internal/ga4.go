package internal

import (
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	ga "google.golang.org/api/analyticsdata/v1beta"
)

// Ga4DataFetcher fetches data from Google Analytics
type Ga4DataFetcher struct {
	service     *ga.Service
	requestFunc func(propertyId, startDate, endDate string) *ga.RunReportRequest
}

func NewGa4DataFetcher(service *ga.Service) *Ga4DataFetcher {
	return &Ga4DataFetcher{
		service: service,
	}
}

// Ga4DataTransformer transforms data from Google Analytics
type Ga4DataTransformer struct {
}

func NewGa4DataTransformer() *Ga4DataTransformer {
	return &Ga4DataTransformer{}
}

func (t Ga4DataTransformer) TransformData(result *ga.RunReportResponse, transformer TransformerF) ([]bigquery.ValueSaver, error) {
	if result == nil {
		return nil, errors.New("result is nil")
	}

	transformedData, err := transformer(result)
	if err != nil {
		return nil, errors.Wrap(err, "failed to transform data")
	}
	log.Println("Transformed data:", transformedData)
	return transformedData, nil
}

type TransformerF = func(result *ga.RunReportResponse) ([]bigquery.ValueSaver, error)
type ReportRequestF = func(propertyId, startDate, endDtate string) *ga.RunReportRequest

// TODO : 요청을 복수로 처리하고 모두 처리된 결과값을 리턴하도록 수정하고 테라포머에서도 복수의 결과값을 처리하도록 수정
// GetGADataFetcher fetches data from Google Analytics
func (g *Ga4DataFetcher) GetGADataFetcher(propertyId, start, end string, requestFunc ReportRequestF) (*ga.RunReportResponse, error) {
	// Define the Google Analytics request
	request := requestFunc(propertyId, start, end)
	response, err := g.service.Properties.RunReport("properties/"+propertyId, request).Do()
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute Google Analytics request")
	}
	// Execute the Google Analytics request
	return response, nil
}
