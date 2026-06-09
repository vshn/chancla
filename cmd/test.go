package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/bombsimon/logrusr/v4"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	chanclavshniov1alpha1 "github.com/vshn/chancla/api/v1alpha1"
	"github.com/vshn/chancla/internal/alertmanager"
)

func Test(cmd *cobra.Command, args []string) {
	l := logrusr.New(log)

	rb := &chanclavshniov1alpha1.Runbook{
		Spec: chanclavshniov1alpha1.RunbookSpec{
			Matchers: []string{
				// `alertname="MyAwesomeAlert"`,
				`OnCall="true"`,
			},
		},
	}

	config := &alertmanager.AlertmanagerConfig{
		Host:        viper.GetString("alertmanager-host"),
		Token:       viper.GetString("alertmanager-token"),
		UseTLS:      viper.GetBool("alertmanager-use-tls"),
		InsecureTLS: viper.GetBool("alertmanager-insecure-tls"),
	}

	amClient, err := alertmanager.GetClient(config)
	if err != nil {
		l.Error(err, "failed to create alertmanager client")
		return
	}

	alerts, err := amClient.AlertsFromMatchers(rb.Spec.Matchers)
	if err != nil {
		l.Error(err, "failed to query alerts")
		return
	}

	if len(alerts) == 0 {
		l.Info("no alerts found")
		return
	}

	data, err := json.Marshal(alerts)
	if err != nil {
		l.Error(err, "failed to marshal alerts")
		return
	}

	fmt.Println(string(data))
}
