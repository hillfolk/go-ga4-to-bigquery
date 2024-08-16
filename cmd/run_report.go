package cmd

import "github.com/spf13/cobra"

// event represents the event command
var RunReportCmd = &cobra.Command{
	Use:     "run-report",
	Short:   "기본 리포트를 생성 및 전송합니다.",
	Long:    `기본 리포트를 생성 및 전송합니다.`,
	PreRunE: app.SetConfig,
	RunE:    app.RunE,
}

func init() {
	RunReportCmd.Flags().StringVar(&cfgFile, "config", "", "config file (default is config.json)")
	rootCmd.AddCommand(RunReportCmd)
}
