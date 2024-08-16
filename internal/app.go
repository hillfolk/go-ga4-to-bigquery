package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	ga "google.golang.org/api/analyticsdata/v1beta"
	"google.golang.org/api/option"

	"go-ga4-to-bigquery/internal/reports"
	"go-ga4-to-bigquery/internal/reports/impl"
)

type Config struct {
	ReportTypes          []string `json:"REPORT_TYPES"`
	ClientSecretFile     string   `json:"CLIENT_SECRET_FILE"`
	ServiceAccountFile   string   `json:"SERVICE_ACCOUNT_FILE"`
	Scopes               []string `json:"SCOPES"`
	PropertyID           string   `json:"PROPERTY_ID"`
	InitialFetchFromDate string   `json:"INITIAL_FETCH_FROM_DATE"`
	FetchToDate          string   `json:"FETCH_TO_DATE"`
	ProjectId            string   `json:"PROJECT_ID"`
	DatasetID            string   `json:"DATASET_ID"`
	TablePrefix          string   `json:"TABLE_PREFIX"`
	PartitionBy          string   `json:"PARTITION_BY"`
	ClusterBy            string   `json:"CLUSTER_BY"`
}

func (c Config) AllConfig() string {
	return fmt.Sprintf("%+v", c)
}

type App struct {
	cfg                *Config
	ga4DataFetcher     *Ga4DataFetcher
	ga4DataTransformer *Ga4DataTransformer
	bigQueryDateInsert *BigQueryDateInserter
}

func NewApp() *App {
	return &App{}
}

func (a *App) SetConfig(cmd *cobra.Command, args []string) error {
	var err error

	cfgFile, err := cmd.Flags().GetString("config")
	if err != nil {
		return err
	}
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.Getwd()
		cobra.CheckErr(err)

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("json")
		viper.SetConfigName(".local")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
		a.cfg = &Config{
			ReportTypes:          viper.GetStringSlice("REPORT_TYPES"),
			ClientSecretFile:     viper.GetString("CLIENT_SECRET_FILE"),
			ServiceAccountFile:   viper.GetString("SERVICE_ACCOUNT_FILE"),
			Scopes:               viper.GetStringSlice("SCOPES"),
			PropertyID:           viper.GetString("PROPERTY_ID"),
			ProjectId:            viper.GetString("PROJECT_ID"),
			InitialFetchFromDate: viper.GetString("INITIAL_FETCH_FROM_DATE"),
			FetchToDate:          viper.GetString("FETCH_TO_DATE"),
			DatasetID:            viper.GetString("DATASET_ID"),
			TablePrefix:          viper.GetString("TABLE_PREFIX"),
		}
		fmt.Println(a.cfg.AllConfig())
	} else {
		return errors.Wrap(err, "failed to read config")
	}

	return nil
}

// Run runs the Ga4DataFetcher
func (a *App) RunE(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Create a new Google Analytics Data service
	gaService, err := ga.NewService(ctx, option.WithCredentialsFile(a.cfg.ServiceAccountFile))
	if err != nil {
		log.Printf("Failed to create Google Analytics service: %v", err)
	}
	// Settup GA4 client
	a.ga4DataFetcher = NewGa4DataFetcher(gaService)

	// Create a new Transformer
	a.ga4DataTransformer = NewGa4DataTransformer()

	// Create a new BigQuery client
	bqClient, err := bigquery.NewClient(ctx, a.cfg.ProjectId, option.WithCredentialsFile(a.cfg.ServiceAccountFile))
	if err != nil {
		log.Printf("Failed to create BigQuery client: %v", err)
	}

	// BigQuery client
	a.bigQueryDateInsert = NewBigQueryDateInsert(bqClient)

	// 상위 Command는 Google Analytics Data API를 이용하여 데이터를 조회 하는 방식을 결정합니다.
	switch cmd.Use {
	case "run-report":
		return a.Run()
	default:
		errors.Wrap(err, "invalid command")
	}
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc
	return nil
}

func (a *App) Run() error {
	for _, reportType := range a.cfg.ReportTypes {
		report, err := SelectReport(REPORT_TYPE(reportType))
		if err != nil {
			return errors.Wrap(err, "failed to select report")
		}
		err = a.runReport(report)
		if err != nil {
			return errors.Wrap(err, "failed to run report")
		}
	}
	return nil
}

func createServiceClient(ctx context.Context, serviceAccountFilePath string) (*ga.Service, error) {
	// Use the service account file to authenticate and create a service client
	service, err := ga.NewService(ctx, option.WithCredentialsFile(serviceAccountFilePath))
	if err != nil {
		log.Printf("Failed to create service: %v", err)
		return nil, err
	}

	return service, nil
}

func (a *App) Auth(ctx context.Context, serviceAccountFilePath string) (*ga.Service, error) {
	return createServiceClient(ctx, serviceAccountFilePath)
}

func (a *App) runReport(report reports.Report) error {
	// Get the data from Google Analytics
	result, err := a.ga4DataFetcher.GetGADataFetcher(a.cfg.PropertyID, a.cfg.InitialFetchFromDate, a.cfg.FetchToDate, report.ReportRequestFunc)
	if err != nil {
		return errors.Wrap(err, "failed to get GA data")
	}

	// Transform the data
	transformedData, err := a.ga4DataTransformer.TransformData(result, report.TransformFunc)
	if err != nil {
		return errors.Wrap(err, "failed to transform data")
	}

	//Load the data into BigQuery
	fullTableID := a.cfg.TablePrefix + report.ReportTitle()

	err = a.bigQueryDateInsert.InsertData(context.Background(), a.bigQueryDateInsert.bqClient, a.cfg.DatasetID, fullTableID, report, transformedData)
	if err != nil {
		return errors.Wrap(err, "failed to load data into BigQuery")
	}
	return nil
}

type REPORT_TYPE string

const (
	ACTIVE_USERS          REPORT_TYPE = "daily-active-users" // active-users, user-technology, events
	EVENTS                REPORT_TYPE = "daily-events"
	USER_TECHNOLOGY       REPORT_TYPE = "daily-user-technology"
	USER_CHANNEL_GROUPING REPORT_TYPE = "daily-user-channel-grouping"
	CROSS_CAMPAIGN        REPORT_TYPE = "daily-cross-channel"
)

func SelectReport(rType REPORT_TYPE) (reports.Report, error) {
	switch rType {
	case ACTIVE_USERS:

		return &impl.ActiveUsersReport{}, nil
	case EVENTS:

		return &impl.EventsReport{}, nil
	case USER_TECHNOLOGY:

		return &impl.UserTechnologyReport{}, nil
	case USER_CHANNEL_GROUPING:

		return &impl.UserChannelGroupingReport{}, nil
	case CROSS_CAMPAIGN:
		return &impl.CrossChannelReport{}, nil
	default:
		return nil, errors.New("invalid report type")
	}
}
