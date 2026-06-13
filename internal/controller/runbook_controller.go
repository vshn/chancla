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
	scheduledTimeAnnotation    = "chancla.vshn.io/scheduled-at"
	alertFingerprintAnnotation = "chancla.vshn.io/alert-fingerprint"
	managedJobLabel            = "chancla.vshn.io/managed-by"

	defaultRequeueAfter = 30 * time.Second
	minimumRequeuAfter  = 5 * time.Second // 👈 TODO: might not be needed
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
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;update;patch

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

// nolint:gocyclo // for now ignore complexity of this function 😬
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

	// Gathering the state of the world
	// -------------------------------------------------------------------------

	// Retrieve the alerts from Alertmanager based on the matchers defined in the Runbook
	alerts, err := r.Alertmanager.SortedAlertsFromMatchers(rb.Spec.Matchers)
	if err != nil {
		return ctrl.Result{}, err
	}

	// ...and update status firingAlerts if they differ
	if !rb.CompareStatusFiringAlerts(alerts) {
		rb.Status.FiringAlerts = alerts
		if err := r.Status().Update(ctx, &rb); err != nil {
			return ctrl.Result{}, err
		}
	}
	// ...and update status condition
	meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
		Type:    typeProgressingRunbook,
		Status:  metav1.ConditionTrue,
		Reason:  "AlertsFiring",
		Message: fmt.Sprintf("%d firing alerts", len(alerts)),
	})
	if statusErr := r.Status().Update(ctx, &rb); statusErr != nil {
		return ctrl.Result{}, err
	}

	// List all owned jobs, running and completed
	childJobs := batchv1.JobList{}
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{".metadata.controller": req.Name}); err != nil {
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

	// ...and categorize them according to their state
	activeJobs := make(map[string][]batchv1.Job)
	failedJobs := make(map[string][]batchv1.Job)
	successfulJobs := make(map[string][]batchv1.Job)
	lastScheduledTime := make(map[string]time.Time)
	for _, job := range childJobs.Items {
		_, finishedType := jobIsFinished(&job)
		fingerprint := jobFingerprint(&job)
		if fingerprint == "" {
			continue // Skip the Job if it has no fingerprint annotation
		}

		switch finishedType {
		case "": // ongoing
			activeJobs[fingerprint] = append(activeJobs[fingerprint], job)
		case batchv1.JobFailed:
			failedJobs[fingerprint] = append(failedJobs[fingerprint], job)
		case batchv1.JobComplete:
			successfulJobs[fingerprint] = append(successfulJobs[fingerprint], job)
		}

		// ...and extract the scheduledTime from the Job
		lst := lastScheduledTime[fingerprint]
		scheduledTimeForJob, err := jobScheduledTime(&job)
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

	// ...and create a list of runningJobs
	statusRunningJobs := []*chanclavshniov1alpha1.RunbookStatusRunningJob{}
	for fingerprint, jobs := range activeJobs {
		for _, job := range jobs {
			jobRef, err := ref.GetReference(r.Scheme, &job)
			if err != nil {
				l.Error(err, "unable to make reference to active job", "job", job)
				continue
			}

			statusRunningJobs = append(statusRunningJobs, &chanclavshniov1alpha1.RunbookStatusRunningJob{
				Fingerprint:  fingerprint,
				JobReference: *jobRef,
			})
		}
	}

	// ...and update status runningJobs if they differ
	slices.SortStableFunc(statusRunningJobs, statusRunningJobSortFunc)
	if !rb.CompareStatusRunningJobs(statusRunningJobs) {
		rb.Status.FiringAlerts = alerts
		if err := r.Status().Update(ctx, &rb); err != nil {
			return ctrl.Result{}, err // 👈 TODO: maybe not return here
		}
	}

	// Update the status condition of the Runbook
	//
	// After gathering the current state of the world
	// update the status of the Runbook, before processing further.
	// -------------------------------------------------------------------------

	// First set the condition based on firing alerts,
	// could be there are currently no Jobs to schedule.
	if len(alerts) > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeProgressingRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "AlertsFiring",
			Message: fmt.Sprintf("%d alert(s) are currently firing", len(alerts)),
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "AlertsFiring",
			Message: fmt.Sprintf("Runbook is progressing with %d active alert(s)", len(alerts)),
		})
	} else {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeProgressingRunbook,
			Status:  metav1.ConditionFalse,
			Reason:  "NoFiringAlerts",
			Message: "No firing alerts matched by this Runbook",
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "NoFiringAlerts",
			Message: "No firing alerts matched by this Runbook",
		})
	}

	// ...if there are active Jobs, update the condition accordingly
	count := jobInMapListCount(&activeJobs)
	if jobInMapListCount(&activeJobs) > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeProgressingRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("%d job(s) are currently active", count),
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("Runbook is progressing with %d active job(s)", count),
		})
	}

	// ...if there are failed Jobs, update the condition accordingly
	// Only failed Jobs decide if the Runbook is degraded at this point.
	count = jobInMapListCount(&failedJobs)
	if jobInMapListCount(&failedJobs) > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeDegradedRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", count),
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionFalse,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", count),
		})
	} else {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeDegradedRunbook,
			Status:  metav1.ConditionFalse,
			Reason:  "NoFailedJobs",
			Message: "No Jobs of this Runbook are failing",
		})
	}

	// ...if the Runbook is suspended
	if rb.Spec.Suspend != nil && *rb.Spec.Suspend {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionFalse,
			Reason:  "Suspended",
			Message: "Runbook is suspended",
		})
	}

	// ...and finally update the Runbook
	if err := r.Status().Update(ctx, &rb); err != nil {
		return ctrl.Result{}, err
	}

	// Processing the current state of the world
	//
	// We now have the current state of "the world",
	// now we must decide how to proceed.
	// -------------------------------------------------------------------------

	// Clean up old jobs according to the history limit.
	// We won't requeue just to finish the deleting.
	for _, jobs := range failedJobs {
		slices.SortStableFunc(jobs, jobSortFunc)
		jobDeleteOutsideOfHistory(ctx, r, jobs, int(*rb.Spec.FailedJobsHistoryLimit))
	}
	for _, jobs := range successfulJobs {
		slices.SortStableFunc(jobs, jobSortFunc)
		jobDeleteOutsideOfHistory(ctx, r, jobs, int(*rb.Spec.SuccessfulJobsHistoryLimit))
	}

	// If the Runbook is suspended we dont want to create any Jobs.
	if rb.Spec.Suspend != nil && *rb.Spec.Suspend {
		l.Info("Runbook is suspended, skipping")
		// The status is already set, nothing more to do here.
		return ctrl.Result{}, nil
	}

	// Calculate when the next earliest time would be we could reconcile
	// this Runbook according to past Jobs.
	durationToEarliestRerun := rb.Spec.ReconcileInterval.Duration
	for _, lst := range lastScheduledTime {
		earliestRerunForAlert := min(time.Since(lst)-rb.Spec.GracePeriodLastRun.Duration, minimumRequeuAfter)
		durationToEarliestRerun = min(durationToEarliestRerun, earliestRerunForAlert)
	}

	// We have cleaned up past Jobs according to the history limits,
	// if there are no firing alerts we can return and reconcile next
	// according to the previously determined earliestRerun,
	// to process jobs that might still be running.
	if len(alerts) == 0 {
		// The status is already set, nothing more to do here.
		return ctrl.Result{RequeueAfter: durationToEarliestRerun}, nil
	}

	// 👇 TODO: What if the last Job failed?

	// 👇 TODO: What if the last Job was successful but Runbook.Spec.Interval not passed?

	// Create new Jobs for each firing alert that is matched by the Runbook.
	scheduledTime := time.Now()
	countJobsFailed := 0
	for _, a := range alerts {
		// Skip if an active Job is running
		if len(activeJobs[a.Fingerprint]) > 0 {
			break
		}

		// Construct a new Job based on the firing alert
		job, err := constructJobForAlert(&rb, a, scheduledTime)
		if err != nil {
			l.Error(err, "failed to construct Job", "job", job)
			continue
		}

		// ...and set the controller reference
		// since this is a new Job we can ignore the error
		_ = ctrl.SetControllerReference(&rb, &job, r.Scheme)

		// ...and create it on the cluster
		if err := r.Create(ctx, &job); err != nil {
			l.Error(err, "unable to create Job for Runbook", "job", job)
			countJobsFailed++
			continue
		}
		l.Info("created Job for Runbook run", "job", job)

		// 👇 TODO: Do we need a reference to the last running Job?
		jobRef, err := ref.GetReference(r.Scheme, &job)
		if err != nil {
			l.Error(err, "unable to make reference to created job", "job", job)
			continue
		}
		statusRunningJobs = append(statusRunningJobs, &chanclavshniov1alpha1.RunbookStatusRunningJob{
			Fingerprint:  a.Fingerprint,
			JobReference: *jobRef,
		})
	}

	// ...and update status runningJobs if they differ
	slices.SortStableFunc(statusRunningJobs, statusRunningJobSortFunc)
	if !rb.CompareStatusRunningJobs(statusRunningJobs) {
		rb.Status.FiringAlerts = alerts
		if err := r.Status().Update(ctx, &rb); err != nil {
			return ctrl.Result{}, err // 👈 TODO: maybe not return here
		}
	}

	// Update the status condition of the Runbook
	//
	// After processing the Runbook do a final update of the status
	// to refelect any potential newly created Jobs.
	// -------------------------------------------------------------------------

	// Update status condition to reflect successful job creation
	if len(statusRunningJobs) > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeProgressingRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("%d job(s) are currently active", len(statusRunningJobs)),
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("Runbook is progressing with %d active job(s)", len(statusRunningJobs)),
		})
	}
	if countJobsFailed > 0 {
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeDegradedRunbook,
			Status:  metav1.ConditionTrue,
			Reason:  "JobCreationFailed",
			Message: fmt.Sprintf(" Failed createing %d jobs", countJobsFailed),
		})
		meta.SetStatusCondition(&rb.Status.Conditions, metav1.Condition{
			Type:    typeAvailableRunbook,
			Status:  metav1.ConditionFalse,
			Reason:  "JobCreationFailed",
			Message: fmt.Sprintf(" Failed createing %d jobs", countJobsFailed),
		})
	}
	if err := r.Status().Update(ctx, &rb); err != nil {
		return ctrl.Result{}, err // 👈 TODO: maybe not return here
	}

	return ctrl.Result{RequeueAfter: durationToEarliestRerun}, nil
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

// jobisFinished returns true if the Job is finished and the batchv1.JobConditionType.
// If the Job is still active the batchv1.JobConditionType is "".
func jobIsFinished(job *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}

	return false, ""
}

// jobFingerprint returns the fingerprint annotation.
func jobFingerprint(job *batchv1.Job) string {
	return job.Annotations[alertFingerprintAnnotation]
}

// jobInMapListCount returns the total number of jobs in a mapped list.
func jobInMapListCount(list *map[string][]batchv1.Job) int {
	count := 0
	for _, jobs := range *list {
		count += len(jobs)
	}

	return count
}

// A helper to extract the scheduled time from the annotation
func jobScheduledTime(job *batchv1.Job) (time.Time, error) {
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

func statusRunningJobSortFunc(a, b *chanclavshniov1alpha1.RunbookStatusRunningJob) int {
	if a.Fingerprint != b.Fingerprint {
		return cmp.Compare(a.Fingerprint, b.Fingerprint)
	}
	if a.JobReference.Namespace != b.JobReference.Namespace {
		return cmp.Compare(a.JobReference.Namespace, b.JobReference.Namespace)
	}
	if a.JobReference.Name != b.JobReference.Name {
		return cmp.Compare(a.JobReference.Name, b.JobReference.Name)
	}
	return 0
}

func jobDeleteOutsideOfHistory(ctx context.Context, clt client.Writer, jobs []batchv1.Job, historyLimit int) {
	l := log.FromContext(ctx)

	for i, job := range jobs {
		if i >= len(jobs)-historyLimit {
			break
		}
		if err := clt.Delete(ctx, &job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			l.Error(err, "failed to delete job", "job", job)
		}
	}
}

func constructJobForAlert(rb *chanclavshniov1alpha1.Runbook, alert *chanclavshniov1alpha1.RunbookStatusFiringAlert, scheduledTime time.Time) (batchv1.Job, error) {
	// We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
	name := fmt.Sprintf("%s-%s-%d", rb.Name, alert.Fingerprint, time.Now().UTC().Unix())
	if len(name) > 63 {
		digest := sha256.Sum256([]byte(name))
		name = name[0:52] + "-" + hex.EncodeToString(digest[0:])[0:10]
	}

	tmpl := rb.Spec.Template.DeepCopy()

	annotations := make(map[string]string)
	maps.Copy(annotations, tmpl.Annotations)
	maps.Copy(annotations, map[string]string{
		scheduledTimeAnnotation:    scheduledTime.Format(time.RFC3339),
		alertFingerprintAnnotation: "le fingerprint"}, // 👈 TODO: add proper fingerprint
	)

	labels := make(map[string]string)
	maps.Copy(labels, tmpl.Labels)
	maps.Copy(labels, map[string]string{managedJobLabel: rb.Name})

	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   rb.Namespace,
			Annotations: annotations,
			Labels:      labels,
			// Finalizers:  []string{UpgradeJobHookJobTrackerFinalizer},
		},
		Spec: tmpl.Spec,
	}

	// Inject the alerts payload as an environment variable into the job template.
	data, err := json.Marshal(rb.Status.FiringAlerts)
	if err != nil {
		return job, err
	}

	env := corev1.EnvVar{
		Name:  "ALERT_JSON",
		Value: string(data),
	}
	for i := range tmpl.Spec.Template.Spec.Containers {
		tmpl.Spec.Template.Spec.Containers[i].Env = append(tmpl.Spec.Template.Spec.Containers[i].Env, env)
	}

	return job, nil
}
