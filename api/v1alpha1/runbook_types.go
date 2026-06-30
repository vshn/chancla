package v1alpha1

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RunbookSpec defines the desired state of Runbook
type RunbookSpec struct {
	// reconcileInterval defines the interval at which the Runbook should be reconciled.
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="15m"
	// +optional
	ReconcileInterval metav1.Duration `json:"reconcileInterval,omitempty"`

	// gracePeriodLastRun defines the grace period on which to wait before
	// the Runbook is executed again after the last run.
	// The grace period will be respected even if a matching alert is still
	// active and not resolved.
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="5m"
	// +optional
	GracePeriodLastRun metav1.Duration `json:"gracePeriodLastRun,omitempty"`

	// failedJobsHistoryLimit defines the number of failed finished jobs to retain.
	// This is a pointer to distinguish between explicit zero and not specified.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`

	// successfulJobsHistoryLimit defines the number of successful finished jobs to retain.
	// This is a pointer to distinguish between explicit zero and not specified.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=5
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// Matchers is a list of labels on which to match in Alertmanager API to trigger the Runbook.
	Matchers []string `json:"matchers,omitempty"`

	// suspend tells the controller to suspend subsequent executions, it does
	// not apply to already started executions.  Defaults to false.
	// +optional
	// +kubebuilder:default=false
	Suspend *bool `json:"suspend,omitempty"`

	// Template defines the job that will be created when executing a Runbook.
	// +required
	Template batchv1.JobTemplateSpec `json:"template"`
}

type RunbookStatusFiringAlert struct {
	// fingerprint represents the Alertmanagers fingerprint of the alert.
	// +optional
	Fingerprint string `json:"fingerprint,omitempty"`

	// startsAt defines the time the alert started.
	// +optional
	StartsAt string `json:"startsAt,omitempty"`

	// updatedAt defines the time the alert was updated.
	// +optional
	UpdatedAt string `json:"updatedAt,omitempty"`

	// annotations represents the annotations from the alert.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// labels defines the labels from the alert.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

type RunbookStatusRunningJob struct {
	// fingerprint represents the corresponding alert.
	// +optional
	Fingerprint string `json:"fingerprint,omitempty"`

	// API version of the referent.
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,5,opt,name=apiVersion"`

	// Kind of the referent.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	Kind string `json:"kind,omitempty" protobuf:"bytes,1,opt,name=kind"`

	// Name of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,3,opt,name=name"`

	// Namespace of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,2,opt,name=namespace"`
}

// RunbookStatus defines the observed state of Runbook.
type RunbookStatus struct {
	// conditions represent the current state of the Runbook resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// firingAlerts represent the currently matching active alerts in Alertmanager.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=0
	FiringAlerts []RunbookStatusFiringAlert `json:"firingAlerts,omitempty"`

	// firingAlertsCount represents the number of currently firing alerts.
	// +optional
	FiringAlertsCount *int32 `json:"firingAlertsCount,omitempty"`

	// runningJobs defines a list of pointers to currently running jobs.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=0
	RunningJobs []RunbookStatusRunningJob `json:"runningJobs,omitempty"`

	// runningJobsCount represents the number of currently running jobs.
	// +optional
	RunningJobsCount *int32 `json:"runningJobsCount,omitempty"`

	// failedJobsCount represents the number of currently failed jobs.
	// +optional
	FailedJobsCount *int32 `json:"failedJobsCount,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Runbook is the Schema for the runbooks API
// +kubebuilder:printcolumn:name="Firing Alerts",type="integer",JSONPath=".status.firingAlertsCount"
// +kubebuilder:printcolumn:name="Running Jobs",type="integer",JSONPath=".status.runningJobsCount"
// +kubebuilder:printcolumn:name="Failed Jobs",type="integer",JSONPath=".status.failedJobsCount"
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type=='Available')].reason"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Runbook struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Runbook
	// +required
	Spec RunbookSpec `json:"spec"`

	// status defines the observed state of Runbook
	// +optional
	Status RunbookStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RunbookList contains a list of Runbook
type RunbookList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Runbook `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Runbook{}, &RunbookList{})
}

// CompareStatusFiringAlerts to a given slice of alerts.
// Requires the given slices to be sorted.
// Fingerprint is calculated over the Labels, there is no need to
// compare the LabelSet.
func (rb *Runbook) CompareStatusFiringAlerts(to []RunbookStatusFiringAlert) bool {
	if len(rb.Status.FiringAlerts) != len(to) {
		return false
	}
	for i, this := range rb.Status.FiringAlerts {
		// if this == nil || to[i] == nil {
		// 	if this != to[i] {
		// 		return false
		// 	}
		// 	continue
		// }
		if this.Fingerprint != to[i].Fingerprint {
			return false
		}
		if this.StartsAt != to[i].StartsAt {
			return false
		}
		if this.UpdatedAt != to[i].UpdatedAt {
			return false
		}
	}

	return true
}

// CompareStatusRunningJobs to a given slice of jobs.
// Requires the given slices to be sorted.
// Fingerprint is calculated over the Labels, there is no need to
// compare the LabelSet.
func (rb *Runbook) CompareStatusRunningJobs(to []RunbookStatusRunningJob) bool {
	if len(rb.Status.RunningJobs) != len(to) {
		return false
	}
	for i, this := range rb.Status.RunningJobs {
		// if this == nil || to[i] == nil {
		// 	if this != to[i] {
		// 		return false
		// 	}
		// 	continue
		// }
		if this.Fingerprint != to[i].Fingerprint {
			return false
		}
		if this.Name != to[i].Name {
			return false
		}
		if this.Namespace != to[i].Namespace {
			return false
		}
	}

	return true
}
