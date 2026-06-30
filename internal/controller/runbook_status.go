package controller

import (
	"context"
	"fmt"
	"time"

	chanclavshniov1alpha1 "github.com/vshn/chancla/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Definitions to manage status conditions types
const (
	typeAvailable    = "Available"
	typeFiringAlerts = "FiringAlerts"
	typeRunningJobs  = "RunningJobs"
	typeDegraded     = "Degraded"
)

// Definitions to manage status conditions reasons
const (
	reasonAlertsFiring   = "AlertsFiring"
	reasonNoAlertsFiring = "NoAlertsFiring"
	reasonRunningJobs    = "JobsActive"
	reasonNoRunningJobs  = "NoJobsActive"
	reasonFailedJobs     = "JobsFailing"
	reasonNoFailedJobs   = "NoJobsFailing"
	reasonStruggling     = "StrugglingButOk"
)

type RunbookReconcilerStatus struct {
	Result            ctrl.Result
	Error             error
	FiringAlerts      []chanclavshniov1alpha1.RunbookStatusFiringAlert
	RunningJobs       []chanclavshniov1alpha1.RunbookStatusRunningJob
	FiringAlertsCount int
	RunningJobsCount  int
	FailedJobsCount   int
}

func (rs *RunbookReconcilerStatus) HasError() bool {
	if rs.Error != nil {
		return true
	}
	return false
}

func (rs *RunbookReconcilerStatus) ReconcileResult() (ctrl.Result, error) {
	return rs.Result, rs.Error
}

func (rs *RunbookReconcilerStatus) WithError(err error) *RunbookReconcilerStatus {
	rs.Error = err
	return rs
}

func (rs *RunbookReconcilerStatus) WithDuration(duration time.Duration) *RunbookReconcilerStatus {
	rs.Result.RequeueAfter = duration
	return rs
}

// 👇 TODO: maybe better to have this in runbook_controller? or not?
func (rs *RunbookReconcilerStatus) updateRunbookStatus(ctx context.Context, clt client.Client, rb chanclavshniov1alpha1.Runbook) error {
	rb.Status.FiringAlerts = rs.FiringAlerts
	rb.Status.RunningJobs = rs.RunningJobs

	rb.Status.FiringAlertsCount = ptr.To(int32(rs.FiringAlertsCount))
	rb.Status.RunningJobsCount = ptr.To(int32(rs.RunningJobsCount))
	rb.Status.FailedJobsCount = ptr.To(int32(rs.FailedJobsCount))

	if rs.FiringAlertsCount > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeFiringAlerts,
			Status:  metav1.ConditionTrue,
			Reason:  reasonAlertsFiring,
			Message: fmt.Sprintf("%d alerts are currently active", rs.FiringAlertsCount),
		})
		// 👇 TODO: maybe a more formal message here
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailable,
			Status:  metav1.ConditionTrue,
			Reason:  reasonStruggling,
			Message: "Doing my job Ok...",
		})
	} else {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeFiringAlerts,
			Status:  metav1.ConditionFalse,
			Reason:  reasonNoAlertsFiring,
			Message: "Currently no matching alerts active",
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailable,
			Status:  metav1.ConditionTrue,
			Reason:  reasonNoAlertsFiring,
			Message: "Currently no matching alerts active",
		})
	}

	if rs.RunningJobsCount > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeRunningJobs,
			Status:  metav1.ConditionTrue,
			Reason:  reasonRunningJobs,
			Message: fmt.Sprintf("%d jobs are currently running", rs.RunningJobsCount),
		})
	} else {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeRunningJobs,
			Status:  metav1.ConditionFalse,
			Reason:  reasonNoRunningJobs,
			Message: "Currently no jobs active",
		})
	}

	if rs.FailedJobsCount > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reasonFailedJobs,
			Message: fmt.Sprintf("%d jobs have failed", rs.FailedJobsCount),
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailable,
			Status:  metav1.ConditionFalse,
			Reason:  reasonFailedJobs,
			Message: fmt.Sprintf("%d jobs have failed", rs.FailedJobsCount),
		})
	} else {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeDegraded,
			Status:  metav1.ConditionFalse,
			Reason:  reasonNoFailedJobs,
			Message: "Currently no jobs failed",
		})
	}

	return clt.Status().Update(ctx, &rb)
}
