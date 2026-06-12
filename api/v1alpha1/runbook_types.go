package v1alpha1

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RunbookSpec defines the desired state of Runbook
type RunbookSpec struct {
	// interval defines the interval at which the Runbook should be reconciled.
	// +kubebuilder:validation:Format=duration
	Interval metav1.Duration `json:"interval,omitempty"`

	// gracePeriodLastRun defines the grace period on which to wait before
	// the Runbook is executed again after the last run.
	// The grace period will be respected even if a matching alert is still
	// active and not resolved.
	// +kubebuilder:validation:Format=duration
	GracePeriodLastRun metav1.Duration `json:"gracePeriodLastRun,omitempty"`

	// Matchers is a list of labels on which to match in Alertmanager API to trigger the Runbook.
	Matchers []string `json:"matchers,omitempty"`

	// Template is the job template that is executed.
	Template batchv1.JobTemplateSpec `json:"template,omitempty"`
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

// RunbookStatus defines the observed state of Runbook.
type RunbookStatus struct {
	// active defines a list of pointers to currently running jobs.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=10
	Active []corev1.ObjectReference `json:"active,omitempty"`

	// lastScheduleTime defines when was the last time the job was successfully scheduled.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

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
	FiringAlerts []*RunbookStatusFiringAlert `json:"firingAlerts,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Runbook is the Schema for the runbooks API
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
