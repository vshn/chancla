package controller

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	alertmanagermodels "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/thanhpk/randstr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chanclavshniov1alpha1 "github.com/vshn/chancla/api/v1alpha1"
	"github.com/vshn/chancla/internal/alertmanager"
)

const (
	scheduledTimeAnnotation    = "chancla.vshn.io/scheduled-at"
	alertFingerprintAnnotation = "chancla.vshn.io/alert-fingerprint"
	managedJobLabel            = "chancla.vshn.io/managed-by"
)

// RunbookReconciler reconciles a Runbook object
type RunbookReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Alertmanager        alertmanager.AlertmanagerReader
	DefaultRequeueAfter time.Duration
	MinimumRequeuAfter  time.Duration
}

var (
	aHundredYearsAgo = time.Now().AddDate(-100, 0, 0)
)

// +kubebuilder:rbac:groups=chancla.vshn.io,namespace=chancla-system,resources=runbooks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chancla.vshn.io,namespace=chancla-system,resources=runbooks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=chancla.vshn.io,namespace=chancla-system,resources=runbooks/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *RunbookReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Get the Runbook resource and handle not found and deletion cases
	rb := chanclavshniov1alpha1.Runbook{}
	if err := r.Get(ctx, req.NamespacedName, &rb); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	status := r.reconcile(ctx, req, rb)
	if err := status.updateRunbookStatus(ctx, r.Client, rb); err != nil {
		return ctrl.Result{}, err
	}

	return status.ReconcileResult()
}

func (r *RunbookReconciler) reconcile(ctx context.Context, req ctrl.Request, rb chanclavshniov1alpha1.Runbook) *RunbookReconcilerStatus {
	status := &RunbookReconcilerStatus{}

	if !rb.DeletionTimestamp.IsZero() {
		return status
	}

	// Gathering the state of the world
	// -------------------------------------------------------------------------

	// Retrieve the firing alerts from Alertmanager based on the matchers defined in the Runbook
	alerts, err := r.getAlertsFromAlertmanager(ctx, rb.Spec.Matchers)
	if err != nil {
		return status.WithDuration(r.MinimumRequeuAfter).WithError(err)
	}

	status.FiringAlerts = convertAlertsToStatusFiringAlerts(alerts)
	status.FiringAlertsCount = len(alerts)

	// List all owned jobs, running and completed
	jobs := batchv1.JobList{}
	if err := r.List(ctx, &jobs, client.InNamespace(req.Namespace), client.MatchingFields{".metadata.controller": req.Name}); err != nil {
		return status.WithDuration(r.MinimumRequeuAfter).WithError(err)
	}

	activeJobs, failedJobs, successfulJobs, lastScheduledTime := splitJobListBasedOnType(jobs)

	status.RunningJobs = convertJobsMapToStatusRunningJobs(activeJobs)
	status.RunningJobsCount = countJobsInMappedList(activeJobs)
	status.FailedJobsCount = countJobsInMappedList(failedJobs)

	// Processing the state of the world
	//
	// We now have the current state of "the world",
	// now we must decide how to proceed.
	// -------------------------------------------------------------------------

	// Clean up old jobs according to the history limit.
	// We won't requeue just to finish the deleting.
	status.FailedJobsCount -= r.deleteJobsOutsideOfHistory(ctx, failedJobs, int(*rb.Spec.FailedJobsHistoryLimit)) // 👈 TODO: well, thats ugly
	_ = r.deleteJobsOutsideOfHistory(ctx, successfulJobs, int(*rb.Spec.SuccessfulJobsHistoryLimit))

	// If the Runbook is suspended we dont want to create any Jobs.
	if rb.Spec.Suspend != nil && *rb.Spec.Suspend {
		log.FromContext(ctx).Info("Runbook is suspended, skipping")
		// The status is already set, nothing more to do here.
		return status
	}

	// Calculate when the next earliest time would be we could reconcile
	// this Runbook according to past Jobs.
	// 👇 TODO: this is probably not correct if the last rerun of a job was _not_ a fail
	durationToEarliestRerun := max(rb.Spec.ReconcileInterval.Duration, r.MinimumRequeuAfter)
	for _, lst := range lastScheduledTime {
		earliestRerunForAlert := max(time.Since(lst)-rb.Spec.GracePeriodLastRun.Duration, r.MinimumRequeuAfter)
		durationToEarliestRerun = min(durationToEarliestRerun, earliestRerunForAlert)
	}

	// We have cleaned up past Jobs according to the history limits,
	// if there are no firing alerts we can return and reconcile next
	// according to the previously determined earliestRerun,
	// to process jobs that might still be running.
	// 👇 TODO: this is can be ditched
	if status.FiringAlertsCount == 0 {
		// The status is already set, nothing more to do here.
		return status.WithDuration(durationToEarliestRerun)
	}

	// 👇 TODO: What if the last Job failed?

	// 👇 TODO: What if the last Job was successful but Runbook.Spec.Interval not passed?

	// Create new Jobs for each firing alert that is matched by the Runbook.
	scheduledTime := time.Now()
	countJobsFailed := 0
	for _, a := range alerts {
		// Skip if an active Job is running
		if len(activeJobs[*a.Fingerprint]) > 0 {
			break
		}

		// Construct a new Job based on the firing alert
		job, err := convertGettableAlertToJob(rb, a, scheduledTime)
		// JOHO: implement here
		if err != nil {
			log.FromContext(ctx).Error(err, "failed to convert alert", "alert", a)
			continue
		}

		// ...and set the controller reference
		// since this is a new Job we can ignore the error
		_ = ctrl.SetControllerReference(&rb, &job, r.Scheme)

		// ...and create it on the cluster
		if err := r.Create(ctx, &job); err != nil {
			log.FromContext(ctx).Error(err, "unable to create Job for Runbook", "job", job)
			countJobsFailed++
			continue
		}

		log.FromContext(ctx).Info("created Job for Runbook run", "name", job.Name, "namespace", job.Namespace)
	}

	return status.WithDuration(durationToEarliestRerun)
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

// getAlertsFromAlertmanager gets a list of alerts from Alertmanager
func (r *RunbookReconciler) getAlertsFromAlertmanager(ctx context.Context, matchers []string) (alertmanagermodels.GettableAlerts, error) {
	alerts, err := r.Alertmanager.AlertsFromMatchers(ctx, matchers)
	if err != nil {
		return nil, err
	}

	return alerts, nil
}

// deleteJobsOutsideOfHistory deletes jobs outside of the desired history limit
// The deletion is checked for every mapped list individually,
// any potential errors are logged but not acted upon.
func (r *RunbookReconciler) deleteJobsOutsideOfHistory(ctx context.Context, mappedJobs map[string][]batchv1.Job, historyLimit int) int {
	deletedJobs := 0
	for _, jobs := range mappedJobs {
		slices.SortStableFunc(jobs, sorterFuncJobs)
		for i, job := range jobs {
			if i >= len(jobs)-historyLimit {
				break
			}
			if err := r.Delete(ctx, &job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				log.FromContext(ctx).Error(err, "failed to delete job", "job", job)
			}
			deletedJobs++
		}
	}

	return deletedJobs
}

// Random functions
//
// Various helper functions for all kind of stuff.
// -----------------------------------------------------------------------------

// conditionTypeFromJobStatus returns the JobConditionType from a Job
// If the Job is still active the JobConditionType is "".
func conditionTypeFromJobStatus(job batchv1.Job) batchv1.JobConditionType {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return c.Type
		}
	}

	return ""
}

// countJobsInMappedList returns the total number of jobs in a mapped list.
func countJobsInMappedList(list map[string][]batchv1.Job) int {
	count := 0
	for _, jobs := range list {
		count += len(jobs)
	}

	return count
}

// extractScheduledTimeFromJob extracts the scheduled time from a Jobs annotation
func extractScheduledTimeFromJob(job batchv1.Job) (time.Time, error) {
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

// splitJobListBasedOnType returns separate mapped lists of Jobs based on their conditionType and their last scheduled time
// The returned mapped lists are active, failed and successful jobs.
// The last returned value is a map of each last scheduled time based on the alerts fingerprint.
func splitJobListBasedOnType(jobs batchv1.JobList) (map[string][]batchv1.Job, map[string][]batchv1.Job, map[string][]batchv1.Job, map[string]time.Time) {
	activeJobs := make(map[string][]batchv1.Job)
	failedJobs := make(map[string][]batchv1.Job)
	successfulJobs := make(map[string][]batchv1.Job)
	lastScheduledTime := make(map[string]time.Time)
	for _, job := range jobs.Items {
		conditionType := conditionTypeFromJobStatus(job)
		fingerprint := job.Annotations[alertFingerprintAnnotation]
		if fingerprint == "" {
			// Skip the Job if it has no fingerprint annotation
			continue
		}

		switch conditionType {
		case "": // ongoing
			activeJobs[fingerprint] = append(activeJobs[fingerprint], job)
		case batchv1.JobFailed:
			failedJobs[fingerprint] = append(failedJobs[fingerprint], job)
		case batchv1.JobComplete:
			successfulJobs[fingerprint] = append(successfulJobs[fingerprint], job)
		}

		// ...and extract the scheduledTime from the Job
		lst := lastScheduledTime[fingerprint]
		scheduledTimeForJob, err := extractScheduledTimeFromJob(job)
		if err != nil {
			scheduledTimeForJob = aHundredYearsAgo
		}

		// ...and update the Jobs last scheduled time
		if lst.IsZero() {
			lastScheduledTime[fingerprint] = scheduledTimeForJob
			continue
		}

		if lastScheduledTime[fingerprint].Before(scheduledTimeForJob) {
			lastScheduledTime[fingerprint] = scheduledTimeForJob
		}
	}

	return activeJobs, failedJobs, successfulJobs, lastScheduledTime
}

// Sorter functions
//
// Helper functions to sort various lists.
// -----------------------------------------------------------------------------

// sorterFuncStatusFiringAlerts provides the comparison function for sorting RunbookStatusFiringAlerts
func sorterFuncStatusFiringAlerts(a, b chanclavshniov1alpha1.RunbookStatusFiringAlert) int {
	// Alertmanager computes the fingerprint from the label set of the alert,
	// specifically the combination of all label key-value pairs.
	// Nothing else (annotations, startsAt, endsAt, generatorURL) is included.
	return cmp.Compare(a.Fingerprint, b.Fingerprint)
}

// sorterFuncStatusRunningJobs provides the comparison function for sorting RunbookStatusRunningJobs
func sorterFuncStatusRunningJobs(a, b chanclavshniov1alpha1.RunbookStatusRunningJob) int {
	if a.Fingerprint != b.Fingerprint {
		return cmp.Compare(a.Fingerprint, b.Fingerprint)
	}
	if a.Namespace != b.Namespace {
		return cmp.Compare(a.Namespace, b.Namespace)
	}
	if a.Name != b.Name {
		return cmp.Compare(a.Name, b.Name)
	}
	return 0
}

// sorterFuncJobs provides the comparison function for sorting Jobs based on their actual start time
func sorterFuncJobs(a, b batchv1.Job) int {
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

// Converter functions
//
// Helper functions to convert various things to other things.
// -----------------------------------------------------------------------------

// convertAlertsToStatusFiringAlerts converts a list of alerts to the corresponding RunbookStatusFiringAlerts
func convertAlertsToStatusFiringAlerts(alerts alertmanagermodels.GettableAlerts) []chanclavshniov1alpha1.RunbookStatusFiringAlert {
	statusFiringAlerts := make([]chanclavshniov1alpha1.RunbookStatusFiringAlert, 0, len(alerts))
	for _, a := range alerts {
		statusFiringAlerts = append(statusFiringAlerts, chanclavshniov1alpha1.RunbookStatusFiringAlert{
			Fingerprint: *a.Fingerprint,
			StartsAt:    a.StartsAt.String(),
			UpdatedAt:   a.UpdatedAt.String(),
			Annotations: a.Annotations,
			Labels:      a.Labels,
		})
	}

	slices.SortStableFunc(statusFiringAlerts, sorterFuncStatusFiringAlerts)
	return statusFiringAlerts
}

// convertJobsMapToStatusRunningJobs converts a map of active jobs to the corresponding RunbookStatusRunningJobs
func convertJobsMapToStatusRunningJobs(activeJobs map[string][]batchv1.Job) []chanclavshniov1alpha1.RunbookStatusRunningJob {
	statusRunningJobs := make([]chanclavshniov1alpha1.RunbookStatusRunningJob, countJobsInMappedList(activeJobs))
	for fingerprint, jobs := range activeJobs {
		for _, job := range jobs {
			statusRunningJobs = append(statusRunningJobs, chanclavshniov1alpha1.RunbookStatusRunningJob{
				Fingerprint: fingerprint,
				APIVersion:  job.APIVersion,
				Kind:        job.Kind,
				Name:        job.Name,
				Namespace:   job.Namespace,
			})
		}
	}

	slices.SortStableFunc(statusRunningJobs, sorterFuncStatusRunningJobs)
	return statusRunningJobs
}

// convertGettableAlertToJob converts an alert to a Job for scheduling on the cluster
func convertGettableAlertToJob(rb chanclavshniov1alpha1.Runbook, alert *alertmanagermodels.GettableAlert, scheduledTime time.Time) (batchv1.Job, error) {
	name := fmt.Sprintf("%s-%s-%s", rb.Name, *alert.Fingerprint, randstr.Hex(8))
	if len(name) > 63 {
		digest := sha256.Sum256([]byte(name))
		name = name[0:52] + "-" + hex.EncodeToString(digest[0:])[0:10]
	}

	tmpl := rb.Spec.Template.DeepCopy()

	annotations := make(map[string]string)
	maps.Copy(annotations, tmpl.Annotations)
	maps.Copy(annotations, map[string]string{
		scheduledTimeAnnotation:    scheduledTime.Format(time.RFC3339),
		alertFingerprintAnnotation: *alert.Fingerprint},
	)

	labels := make(map[string]string)
	maps.Copy(labels, tmpl.Labels)
	maps.Copy(labels, map[string]string{managedJobLabel: rb.Name})

	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        strings.ToLower(name),
			Namespace:   rb.Namespace,
			Annotations: annotations,
			Labels:      labels,
			// Finalizers:  []string{UpgradeJobHookJobTrackerFinalizer},
		},
		Spec: tmpl.Spec,
	}

	// Inject the alerts payload as an environment variable into the job template.
	data, err := json.Marshal(alert.Labels)
	if err != nil {
		return job, err
	}

	env := []corev1.EnvVar{
		{
			Name:  "ALERT_STARTED_AT",
			Value: alert.StartsAt.String(),
		},
		{
			Name:  "ALERT_LABELS_JSON",
			Value: string(data),
		},
	}
	for i := range tmpl.Spec.Template.Spec.Containers {
		tmpl.Spec.Template.Spec.Containers[i].Env = append(tmpl.Spec.Template.Spec.Containers[i].Env, env...)
	}

	return job, nil
}
