package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chanclavshniov1alpha1 "github.com/vshn/chancla/api/v1alpha1"
	"github.com/vshn/chancla/internal/alertmanager"
)

// Definitions to manage status conditions
const (
	// typeAvailableRunbook represents the status of the Runbook reconciliation
	typeAvailableRunbook = "Available"
	// typeProgressingRunbook represents the status used when the Runbook is being reconciled
	typeProgressingRunbook = "Progressing"
	// typeDegradedRunbook represents the status used when the Runbook has encountered an error
	typeDegradedRunbook = "Degraded"
)

const (
	scheduledTimeAnnotation = "chancla.vshn.io/scheduled-at"
	managedJobLabel         = "chancla.vshn.io/managed-by"

	failedJobsHistoryLimit     = 3
	successfulJobsHistoryLimit = 10

	defaultRequeueAfter = 30 * time.Second
)

// RunbookReconciler reconciles a Runbook object
type RunbookReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Alertmanager *alertmanager.AlertmanagerClient
}

var (
	aHundredYearsAgo = time.Now().AddDate(-100, 0, 0)
)

// +kubebuilder:rbac:groups=chancla.vshn.io,namespace=chancla-system,resources=runbooks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chancla.vshn.io,namespace=chancla-system,resources=runbooks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=chancla.vshn.io,namespace=chancla-system,resources=runbooks/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *RunbookReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	result, err := r.reconcile(ctx, req)
	if err != nil || result != (ctrl.Result{}) {
		return result, err
	}

	// Requeue with default value if not a specific value provided.
	return ctrl.Result{RequeueAfter: defaultRequeueAfter}, nil
}

func (r *RunbookReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Get the Runbook resource and handle not found and deletion cases
	rb := chanclavshniov1alpha1.Runbook{}
	if fetchErr := r.Get(ctx, req.NamespacedName, &rb); fetchErr != nil {
		if apierrors.IsNotFound(fetchErr) {
			return ctrl.Result{}, nil
		}

		// Error reading the object - requeue the request.
		l.Error(fetchErr, "Failed to fetch the Runbook")
		return ctrl.Result{}, fetchErr
	}
	if !rb.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Initialize status conditions if not yet present
	if len(rb.Status.Conditions) == 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeProgressingRunbook,
			Status:  metav1.ConditionUnknown,
			Reason:  "Reconciling",
			Message: "Starting reconciliation",
		})
		if err := r.Status().Update(ctx, &rb); err != nil {
			l.Error(err, "failed to update Runbook status")
			return ctrl.Result{}, err
		}
	}

	// List all active jobs, and update the status
	childJobs := &batchv1.JobList{}
	if err := r.List(ctx, childJobs, client.InNamespace(req.Namespace), client.MatchingFields{".metadata.controller": req.Name}); err != nil {
		l.Error(err, "Unable to list child Jobs")

		// Update status condition to reflect the error
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeDegradedRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "ReconciliationError",
			Message: fmt.Sprintf("Failed to list child jobs: %v", err),
		})
		if statusErr := r.Status().Update(ctx, &rb); statusErr != nil {
			l.Error(statusErr, "Failed to update Runbook status")
		}
		return ctrl.Result{}, err
	}

	// find the active list of jobs
	activeJobs := []batchv1.Job{}
	successfulJobs := []batchv1.Job{}
	failedJobs := []batchv1.Job{}
	mostRecentTime := aHundredYearsAgo // set to 100y ago, so we defenitly have a valid comparsion

	// categorise child Jobs
	for i, job := range childJobs.Items {
		_, finishedType := jobIsFinished(&job)
		switch finishedType {
		case "": // ongoing
			activeJobs = append(activeJobs, childJobs.Items[i])
		case batchv1.JobFailed:
			failedJobs = append(failedJobs, childJobs.Items[i])
		case batchv1.JobComplete:
			successfulJobs = append(successfulJobs, childJobs.Items[i])
		}

		// We'll store the launch time in an annotation, so we'll reconstitute that from
		// the active jobs themselves.
		scheduledTimeForJob, err := jobScheduledTime(job)
		if err != nil {
			l.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}
		if mostRecentTime.Before(scheduledTimeForJob) {
			mostRecentTime = scheduledTimeForJob
		}
	}

	if mostRecentTime != aHundredYearsAgo {
		rb.Status.LastScheduleTime = &metav1.Time{Time: mostRecentTime}
	} else {
		rb.Status.LastScheduleTime = nil
	}

	// 👇 TODO: Do we need a reference to the last active Job?
	rb.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, &activeJob)
		if err != nil {
			l.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		rb.Status.Active = append(rb.Status.Active, *jobRef)
	}

	// Update status conditions based on current state
	// 👇 TODO: evaluate if all these stati are relevant for a Runbook.
	if len(failedJobs) > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeDegradedRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionFalse,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
	} else if len(activeJobs) > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeProgressingRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("%d job(s) are currently active", len(activeJobs)),
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("CronJob is progressing with %d active job(s)", len(activeJobs)),
		})
	} else {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "AllJobsCompleted",
			Message: "All jobs have completed successfully",
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeProgressingRunbook,
			Status:  metav1.ConditionFalse,
			Reason:  "NoJobsActive",
			Message: "No jobs are currently active",
		})
	}
	if statusErr := r.Status().Update(ctx, &rb); statusErr != nil {
		l.Error(statusErr, "unable to update CronJob status")
		return ctrl.Result{}, statusErr
	}

	// Clean up old jobs according to the history limit.
	// Deleting these are "best effort" -- if we fail on a particular one,
	// we won't requeue just to finish the deleting.
	slices.SortStableFunc(failedJobs, jobSortFunc)
	if err := jobDeleteOutsideOfHistory(ctx, r, failedJobs, failedJobsHistoryLimit); err != nil {
		l.Error(err, "failed to delete old failed jobs")
	}

	slices.SortStableFunc(successfulJobs, jobSortFunc)
	if err := jobDeleteOutsideOfHistory(ctx, r, successfulJobs, successfulJobsHistoryLimit); err != nil {
		l.Error(err, "failed to delete old successful jobs")
	}

	// Retrieve the alerts from Alertmanager based on the matchers defined in the Runbook spec
	alerts, err := r.Alertmanager.AlertsFromMatchers(rb.Spec.Matchers)
	if err != nil {
		l.Error(err, "failed to query alerts")
		return ctrl.Result{}, err
	}

	// Update status firingAlerts based on current alerts
	if !firingAlertsEqual(rb.Status.FiringAlerts, alerts) {
		rb.Status.FiringAlerts = alerts
		if err := r.Status().Update(ctx, &rb); err != nil {
			l.Error(err, "Failed to update Runbook status")
			return ctrl.Result{}, err
		}
		// 👇 TODO: should we return if alerts are different?
		// return ctrl.Result{}, nil
	}

	// We now have the current state of "the world",
	// now we must decide how to proceed
	//
	// * if an alert is firing:
	//   * check if a Job is already running
	//	 * check for how long the running Job is running
	//	 * if no Job is running, check when the last Job was running
	//	 * decide if we can safely run another job
	// * if no alert is firing, requeue the Runbook for Runbook.Spec.Intervall

	// Requeue if a Job is already running
	if len(activeJobs) > 0 {
		l.Info("job is already running")
		return ctrl.Result{}, nil
	}

	// Requeue if the last run was in Runbook.Spec.GracePeriodLastRun
	if time.Since(mostRecentTime) < rb.Spec.GracePeriodLastRun.Duration {
		return ctrl.Result{}, nil
	}

	// 👇 TODO: What if the last Job failed?

	// 👇 TODO: What if the last Job was successfull but Runbook.Spec.Interval not passed?

	// Create a Kubernetes Job based on the Runbook spec and the retrieved alerts
	data, err := json.Marshal(rb.Status.FiringAlerts)
	if err != nil {
		l.Error(err, "failed to marshal alerts")
		return ctrl.Result{}, err
	}

	job := constructJobForRunbook(&rb, string(data))
	if err := ctrl.SetControllerReference(&rb, job, r.Scheme); err != nil {
		l.Error(err, "failed to set ownership")
		return ctrl.Result{}, err
	}

	// ...and create it on the cluster
	if err := r.Create(ctx, job); err != nil {
		l.Error(err, "unable to create Job for Runbook", "job", job)

		// Update status condition to reflect the error
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeDegradedRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobCreationFailed",
			Message: fmt.Sprintf("Failed to create job: %v", err),
		})
		if statusErr := r.Status().Update(ctx, &rb); statusErr != nil {
			l.Error(statusErr, "Failed to update Runbook status")
		}
		return ctrl.Result{}, err
	}
	l.Info("created Job for Runbook run", "job", job)

	// Update status condition to reflect successful job creation
	meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
		Type:    typeProgressingRunbook,
		Status:  metav1.ConditionTrue,
		Reason:  "JobCreated",
		Message: fmt.Sprintf("Created job %s", job.Name),
	})
	if err := r.Status().Update(ctx, &rb); err != nil {
		l.Error(err, "Failed to update CronJob status")
		return ctrl.Result{}, err
	}

	// Requeue after Runbook.Spec.Interval
	return ctrl.Result{RequeueAfter: rb.Spec.Interval.Duration}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RunbookReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &batchv1.Job{}, ".metadata.controller", func(rawObj client.Object) []string {
		// grab the job object, extract the owner...
		job := rawObj.(*batchv1.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		// ...make sure it's a Runbook...
		if owner.APIVersion != chanclavshniov1alpha1.GroupVersion.String() || owner.Kind != "Runbook" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&chanclavshniov1alpha1.Runbook{}).
		Owns(&batchv1.Job{}).
		Named("runbook").
		Complete(r)
}

// A job is "finished" if it has a "Complete" or "Failed" condition marked as true.
func jobIsFinished(job *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}

	return false, ""
}

// A helper to extract the scheduled time from the annotation
func jobScheduledTime(job batchv1.Job) (time.Time, error) {
	timeRaw := job.Annotations[scheduledTimeAnnotation]
	if len(timeRaw) == 0 {
		return aHundredYearsAgo, nil
	}

	timeParsed, err := time.Parse(time.RFC3339, timeRaw)
	if err != nil {
		return aHundredYearsAgo, err
	}
	return timeParsed, nil
}

func jobSortFunc(a, b batchv1.Job) int {
	if a.Status.StartTime == nil && b.Status.StartTime != nil {
		return 1
	}
	if a.Status.StartTime.Before(b.Status.StartTime) {
		return -1
	} else if b.Status.StartTime.Before(a.Status.StartTime) {
		return 1
	}
	return 0
}

func jobDeleteOutsideOfHistory(ctx context.Context, clt client.Writer, jobs []batchv1.Job, historyLimit int) error {
	for i, job := range jobs {
		if i >= len(jobs)-historyLimit {
			break
		}
		if err := clt.Delete(ctx, &job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			return err
		}
	}

	return nil
}

func firingAlertsEqual(a, b []*chanclavshniov1alpha1.RunbookStatusFiringAlert) bool {
	if len(a) != len(b) {
		return false
	}
	for i, _ := range a {
		if a[i].Fingerprint != b[i].Fingerprint {
			return false
		} else if a[i].StartsAt != b[i].StartsAt {
			return false
		} else if a[i].UpdatedAt != b[i].UpdatedAt {
			return false
		}
	}

	return true
}

func constructJobForRunbook(rb *chanclavshniov1alpha1.Runbook, alerts string) *batchv1.Job {
	// We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
	name := fmt.Sprintf("%s-%d", rb.Name, time.Now().UTC().Unix())
	if len(name) > 63 {
		digest := sha256.Sum256([]byte(name))
		name = name[0:52] + "-" + hex.EncodeToString(digest[0:])[0:10]
	}

	tmpl := rb.Spec.Template.DeepCopy()

	labels := make(map[string]string)
	maps.Copy(labels, tmpl.Labels)
	maps.Copy(labels, map[string]string{managedJobLabel: rb.Name})

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   rb.Namespace,
			Annotations: tmpl.Annotations,
			Labels:      labels,
			// Finalizers:  []string{UpgradeJobHookJobTrackerFinalizer},
		},
		Spec: tmpl.Spec,
	}

	// Inject the alerts payload as an environment variable into the job template.
	env := corev1.EnvVar{
		Name:  "ALERTS_JSON",
		Value: alerts,
	}
	for i := range tmpl.Spec.Template.Spec.Containers {
		tmpl.Spec.Template.Spec.Containers[i].Env = append(tmpl.Spec.Template.Spec.Containers[i].Env, env)
	}

	return job
}
