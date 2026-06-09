package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/vshn/chancla/cmd"
)

const (
	textAlertmanagerHost        = `The Host of the Alertmanager to query for alerts.`
	textAlertmanagerUseTLS      = `Wether to use TLS when connecting to the Alertmanager.`
	textAlertmanagerInsecureTLS = `Whether to skip TLS verification when connecting to the Alertmanager.`

	textMetricsAddr = `The address the metrics endpoint binds to.
Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.`
	textProbeAddr      = `The address the probe endpoint binds to.`
	textEnableElection = `Enable leader election for controller manager.
Enabling this will ensure there is only one active controller manager.`
	textSecureMetrics = `If set, the metrics endpoint is served securely via HTTPS.
Use --metrics-secure=false to use HTTP instead.`
	textEnableHTTP2 = `If set, HTTP/2 will be enabled for the metrics and webhook servers`
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "chancla",
	Short: "Manages Runbooks on Kubernetes.",
	Long: `This application watches for alerts in Alertmanager.
If an alert matches the Runbook, the controller will
create a Job according to the provided configuration.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("nothing to do here")
	},
}

// StartCmd will execute the controller-manager.
var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the controller-manager.",
	Run:   cmd.Start,
}

// TestCmd will test some things.
var TestCmd = &cobra.Command{
	Use:   "test",
	Short: "Tests stuff.",
	Run:   cmd.Test,
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().String("alertmanager-host", "localhost:9093", textAlertmanagerHost)
	RootCmd.PersistentFlags().Bool("alertmanager-use-tls", true, textAlertmanagerUseTLS)
	RootCmd.PersistentFlags().Bool("alertmanager-insecure-tls", false, textAlertmanagerInsecureTLS)

	StartCmd.PersistentFlags().String("metrics-address", "0", textMetricsAddr)
	StartCmd.PersistentFlags().String("probe-address", ":8081", textProbeAddr)
	StartCmd.PersistentFlags().Bool("metrics-secure", true, textSecureMetrics)
	StartCmd.PersistentFlags().Bool("enable-election", false, textEnableElection)
	StartCmd.PersistentFlags().Bool("enable-http2", false, textEnableHTTP2)

	// TestCmd.Flags().String("kubeconfig", "$HOME/.kube/config", "Path to the kubeconfig file.")
	TestCmd.Flags().String("alertmanager-token", "", "Token for authenticating with the Alertmanager API.")

	for _, err := range []error{
		viper.BindPFlag("alertmanager-host", RootCmd.PersistentFlags().Lookup("alertmanager-host")),
		viper.BindPFlag("alertmanager-use-tls", RootCmd.PersistentFlags().Lookup("alertmanager-use-tls")),
		viper.BindPFlag("alertmanager-insecure-tls", RootCmd.PersistentFlags().Lookup("alertmanager-insecure-tls")),

		viper.BindPFlag("metrics-address", StartCmd.PersistentFlags().Lookup("metrics-address")),
		viper.BindPFlag("probe-address", StartCmd.PersistentFlags().Lookup("probe-address")),
		viper.BindPFlag("metrics-secure", StartCmd.PersistentFlags().Lookup("metrics-secure")),
		viper.BindPFlag("enable-election", StartCmd.PersistentFlags().Lookup("enable-election")),
		viper.BindPFlag("enable-http2", StartCmd.PersistentFlags().Lookup("enable-http2")),

		// viper.BindPFlag("kubeconfig", ValidateCmd.Flags().Lookup("kubeconfig")),
		viper.BindPFlag("alertmanager-token", TestCmd.Flags().Lookup("alertmanager-token")),
	} {
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

	}

	RootCmd.AddCommand(StartCmd)
	RootCmd.AddCommand(TestCmd)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv() // read in environment variables that match
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
