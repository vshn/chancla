package alertmanager

import (
	"github.com/bombsimon/logrusr/v4"
	openapiclient "github.com/go-openapi/runtime/client"
	alertmanagerclient "github.com/prometheus/alertmanager/api/v2/client"
	"github.com/prometheus/alertmanager/api/v2/client/alert"
	alertmanagermodels "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/sirupsen/logrus"

	chanclavshniov1alpha1 "github.com/vshn/chancla/api/v1alpha1"
)

type AlertmanagerClient struct {
	*alertmanagerclient.AlertmanagerAPI
}

type Alerts []*alertmanagermodels.GettableAlert

type AlertmanagerConfig struct {
	Host        string
	Token       string
	UseTLS      bool
	InsecureTLS bool
}

var (
	log = logrus.New()
)

func GetClient(config *AlertmanagerConfig) (*AlertmanagerClient, error) {
	l := logrusr.New(log)

	runtime := openapiclient.New(config.Host, alertmanagerclient.DefaultBasePath, []string{"http"})
	if config.UseTLS {
		opts := openapiclient.TLSClientOptions{
			InsecureSkipVerify: config.InsecureTLS,
		}

		tlsClient, err := openapiclient.TLSClient(opts)
		if err != nil {
			l.Error(err, "failed to create TLS client")
			return nil, err
		}

		runtime = openapiclient.NewWithClient(config.Host, alertmanagerclient.DefaultBasePath, []string{"https"}, tlsClient)
	}

	if config.Token != "" {
		runtime.DefaultAuthentication = openapiclient.BearerToken(config.Token)
	}

	amClient := &AlertmanagerClient{
		AlertmanagerAPI: alertmanagerclient.New(runtime, nil),
	}

	return amClient, nil
}

func (c *AlertmanagerClient) AlertsFromMatchers(matchers []string) ([]*chanclavshniov1alpha1.RunbookStatusFiringAlert, error) {
	l := logrusr.New(log)

	_true := true
	_false := false

	params := alert.NewGetAlertsParams().
		WithActive(&_true).
		WithSilenced(&_false).
		WithFilter(matchers)

	resp, err := c.Alert.GetAlerts(params)
	if err != nil {
		l.Error(err, "failed to query alerts")
		return nil, err
	}

	alerts := []*chanclavshniov1alpha1.RunbookStatusFiringAlert{}
	for _, a := range resp.Payload {
		alerts = append(alerts, &chanclavshniov1alpha1.RunbookStatusFiringAlert{
			Fingerprint: *a.Fingerprint,
			StartsAt:    a.StartsAt.String(),
			UpdatedAt:   a.UpdatedAt.String(),
			EndsAt:      a.EndsAt.String(),
			Annotations: a.Annotations,
			Labels:      a.Labels,
		})
	}

	return alerts, nil
}
