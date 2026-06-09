package controller

import (
	"context"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	chanclavshniov1alpha1 "github.com/vshn/chancla/api/v1alpha1"
)

type IntervalRunnable struct {
	client   client.Client
	interval time.Duration
	ch       chan event.GenericEvent
}

func (t *IntervalRunnable) Start(ctx context.Context) error {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			var list chanclavshniov1alpha1.RunbookList
			if err := t.client.List(ctx, &list); err != nil {
				continue
			}
			for _, item := range list.Items {
				t.ch <- event.GenericEvent{Object: item.DeepCopy()}
			}
		}
	}
}
