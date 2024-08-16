/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"go-ga4-to-bigquery/internal"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	app     = internal.NewApp()
	cfgFile string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "go-ga4-to-bigquery",
	Short: "A brief description of your application",
	Long:  "A longer description that spans multiple lines and likely contains examples and usage of using your application. For example:\n\nCobra is a CLI library for Go that empowers applications.\nThis application is a tool to generate the needed files to quickly create a Cobra application.",

	//PersistentPreRun: func(cmd *cobra.Command, args []string) {
	//	fmt.Printf("Inside rootCmd PersistentPreRun with args: %v\n", args)
	//},
	PreRunE: app.SetConfig,
	RunE:    app.RunE,
	//PostRun: func(cmd *cobra.Command, args []string) {
	//	fmt.Printf("Inside rootCmd PostRun with args: %v\n", args)
	//},
	//PersistentPostRun: func(cmd *cobra.Command, args []string) {
	//	fmt.Printf("Inside rootCmd PersistentPostRun with args: %v\n", args)
	//}
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is config.json)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}

func initConfig() {
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
		fmt.Println("config:", viper.AllSettings())
	}
}
