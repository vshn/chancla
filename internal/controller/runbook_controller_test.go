package controller

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	alertmanagermodels "github.com/prometheus/alertmanager/api/v2/models"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	chanclavshniov1alpha1 "github.com/vshn/chancla/api/v1alpha1"
)

type MockAlertmanagerClient struct {
	Alerts alertmanagermodels.GettableAlerts
	Err    error
}

var (
	alertmanagerClient = &MockAlertmanagerClient{}
)

func (m *MockAlertmanagerClient) UpdateAlerts(alerts alertmanagermodels.GettableAlerts) {
	m.Alerts = alerts
}

func (m *MockAlertmanagerClient) AlertsFromMatchers(ctx context.Context, matchers []string) (alertmanagermodels.GettableAlerts, error) {
	return m.Alerts, m.Err
}

var (
	alert11 = &alertmanagermodels.GettableAlert{
		Fingerprint: ptr.To("ABC00000011"),
		StartsAt:    ptr.To(strfmt.DateTime(time.Date(2026, 1, 1, 0, 11, 0, 0, time.UTC))),
		UpdatedAt:   ptr.To(strfmt.DateTime(time.Date(2026, 1, 1, 0, 11, 0, 0, time.UTC))),
		Annotations: alertmanagermodels.LabelSet{
			"alertname": "TestAlert1",
		},
		Alert: alertmanagermodels.Alert{
			Labels: alertmanagermodels.LabelSet{
				"alertname": "TestAlert1",
				"severity":  "warning",
			},
		},
	}
	alert12 = &alertmanagermodels.GettableAlert{
		Fingerprint: ptr.To("ABC00000012"),
		StartsAt:    ptr.To(strfmt.DateTime(time.Date(2026, 1, 1, 0, 12, 0, 0, time.UTC))),
		UpdatedAt:   ptr.To(strfmt.DateTime(time.Date(2026, 1, 1, 0, 12, 0, 0, time.UTC))),
		Annotations: alertmanagermodels.LabelSet{
			"alertname": "TestAlert1",
		},
		Alert: alertmanagermodels.Alert{
			Labels: alertmanagermodels.LabelSet{
				"alertname": "TestAlert1",
				"severity":  "critical",
			},
		},
	}
	alert21 = &alertmanagermodels.GettableAlert{
		Fingerprint: ptr.To("ABC00000021"),
		StartsAt:    ptr.To(strfmt.DateTime(time.Date(2026, 1, 1, 0, 21, 0, 0, time.UTC))),
		UpdatedAt:   ptr.To(strfmt.DateTime(time.Date(2026, 1, 1, 0, 21, 0, 0, time.UTC))),
		Annotations: alertmanagermodels.LabelSet{
			"alertname": "TestAlert2",
		},
		Alert: alertmanagermodels.Alert{
			Labels: alertmanagermodels.LabelSet{
				"alertname": "TestAlert2",
				"severity":  "critical",
			},
		},
	}
)

func updateJobActive(job batchv1.Job) *batchv1.Job {
	job.Status.StartTime = ptr.To(metav1.Now())
	job.Status.Active = 1
	job.Status.Succeeded = 0
	job.Status.Conditions = []batchv1.JobCondition{}
	return &job
}
func updateJobCompleted(job batchv1.Job) *batchv1.Job {
	job.Status.CompletionTime = ptr.To(metav1.Now())
	job.Status.Active = 0
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{
			Type:               batchv1.JobSuccessCriteriaMet,
			Status:             corev1.ConditionTrue,
			LastProbeTime:      metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "CompletionsReached",
			Message:            "Reached expected number of succeeded pods",
		},
		{
			Type:               batchv1.JobComplete,
			Status:             corev1.ConditionTrue,
			LastProbeTime:      metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "CompletionsReached",
			Message:            "Reached expected number of succeeded pods",
		},
	}
	return &job
}

var _ = Describe("Runbook Controller", func() {
	Context("When reconciling a resource", func() {
		typeNamespacedName := types.NamespacedName{
			Name:      "test-resource",
			Namespace: "default",
		}
		// eventualTimeout := 2 * time.Minute
		eventualTimeout := 20 * time.Second

		ctx := context.Background()
		runbook := chanclavshniov1alpha1.Runbook{}
		jobList := batchv1.JobList{}

		BeforeEach(func() {
			By("Creating the custom resource for the Kind Runbook")
			err := k8sClient.Get(ctx, typeNamespacedName, &runbook)
			if err != nil && errors.IsNotFound(err) {
				resource := &chanclavshniov1alpha1.Runbook{
					ObjectMeta: metav1.ObjectMeta{
						Name:      typeNamespacedName.Name,
						Namespace: typeNamespacedName.Namespace,
					},
					Spec: chanclavshniov1alpha1.RunbookSpec{
						ReconcileInterval:  metav1.Duration{Duration: 5 * time.Second},
						GracePeriodLastRun: metav1.Duration{Duration: 5 * time.Second},
						Matchers: []string{
							"alertname=SomethingDoesNotMatter",
						},
						Template: batchv1.JobTemplateSpec{
							Spec: batchv1.JobSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										RestartPolicy: "OnFailure",
										Containers: []corev1.Container{
											{
												Name:  "test",
												Image: "test",
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			// resource := &chanclavshniov1alpha1.Runbook{}
			// err := k8sClient.Get(ctx, typeNamespacedName, resource)
			// Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Runbook")
			err := k8sClient.Get(ctx, typeNamespacedName, &runbook)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Runbook")
			Expect(k8sClient.Delete(ctx, &runbook)).To(Succeed())

			By("Cleanup potentially spawned Jobs")
			// Expect(k8sClient.DeleteAllOf(ctx, &batchv1.Job{})).To(Succeed())
		})

		It("should successfully reconcile the resource with 0 alerts", func() {
			By("Reconciling the Runbook")
			// reconciler := &RunbookReconciler{
			// 	Client: k8sManager.GetClient(),
			// 	Scheme: k8sManager.GetClient().Scheme(),
			// 	Alertmanager: &MockAlertmanagerClient{
			// 		Alerts: alertmanagermodels.GettableAlerts{},
			// 	},
			// 	DefaultRequeueAfter: 3 * time.Second,
			// 	MinimumRequeuAfter:  1 * time.Second,
			// }
			// _, _ = reconciler.Reconcile(ctx, reconcile.Request{
			// 	NamespacedName: typeNamespacedName,
			// })
			// Expect(err).NotTo(HaveOccurred())

			By("Checking the Runbooks firing alerts")
			Eventually(func(g Gomega) {
				g.Expect(k8sManager.GetClient().Get(ctx, typeNamespacedName, &runbook)).To(Succeed())
				// g.Expect(runbook.Status.FiringAlerts).To(HaveLen(0))
				// g.Expect(runbook.Status.RunningJobs).To(HaveLen(0))
				// g.Expect(runbook.Status.Conditions).To(HaveLen(3))
			}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

			By("Checking the Runbooks status conditions")
			Eventually(func(g Gomega) {
				g.Expect(k8sManager.GetClient().Get(ctx, typeNamespacedName, &runbook)).To(Succeed())
				// condAvailable := meta.FindStatusCondition(runbook.Status.Conditions, typeAvailableRunbook)
				// Expect(condAvailable.Status).To(Equal(metav1.ConditionTrue))
				// Expect(condAvailable.Reason).To(Equal("NoFiringAlerts"))
				// condProgressing := meta.FindStatusCondition(runbook.Status.Conditions, typeProgressingRunbook)
				// Expect(condProgressing.Status).To(Equal(metav1.ConditionFalse))
				// Expect(condProgressing.Reason).To(Equal("NoFiringAlerts"))
				// condDegraded := meta.FindStatusCondition(runbook.Status.Conditions, typeDegradedRunbook)
				// Expect(condDegraded.Status).To(Equal(metav1.ConditionFalse))
				// Expect(condDegraded.Reason).To(Equal("NoFailedJobs"))
			}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())
		})

		It("should successfully reconcile the resource with 1 alerts", func() {
			By("Updating the potentially spawned Jobs to Active")
			Eventually(func(g Gomega) {
				g.Expect(k8sManager.GetClient().List(ctx, &jobList, client.MatchingFields{".metadata.controller": runbook.Name})).To(Succeed())
				g.Expect(jobList.Items).To(HaveLen(1))
			}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

			Expect(k8sClient.Status().Update(ctx, updateJobActive(jobList.Items[0]))).To(Succeed())

			Eventually(func(g Gomega) {
				g.Expect(k8sManager.GetClient().List(ctx, &jobList, client.MatchingFields{".metadata.controller": runbook.Name})).To(Succeed())
				g.Expect(jobList.Items[0].Status.Active).To(Equal(int32(1)))
			}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

			By("Updating the potentially spawned Jobs to Completed")
			Expect(k8sClient.Status().Update(ctx, updateJobCompleted(jobList.Items[0]))).To(Succeed())

			By("Check if another Job was spawned")
			Eventually(func(g Gomega) {
				g.Expect(k8sManager.GetClient().List(ctx, &jobList, client.MatchingFields{".metadata.controller": runbook.Name})).To(Succeed())
				g.Expect(jobList.Items).To(HaveLen(2))
			}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

			By("Checking the Runbooks firing alerts")
			Eventually(func(g Gomega) {
				g.Expect(k8sManager.GetClient().Get(ctx, typeNamespacedName, &runbook)).To(Succeed())
				g.Expect(runbook.Status.FiringAlertsCount).To(Equal(ptr.To(int32(1))))
				g.Expect(runbook.Status.RunningJobsCount).To(Equal(ptr.To(int32(1))))
			}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

			By("Checking the Runbooks status conditions")
			condAlerts := meta.FindStatusCondition(runbook.Status.Conditions, typeFiringAlerts)
			Expect(condAlerts.Status).To(Equal(metav1.ConditionTrue))
			Expect(condAlerts.Reason).To(Equal(reasonAlertsFiring))

			condJobs := meta.FindStatusCondition(runbook.Status.Conditions, typeRunningJobs)
			Expect(condJobs.Status).To(Equal(metav1.ConditionTrue))
			Expect(condJobs.Reason).To(Equal(reasonRunningJobs))

			condDegraded := meta.FindStatusCondition(runbook.Status.Conditions, typeDegraded)
			Expect(condDegraded.Status).To(Equal(metav1.ConditionFalse))
			Expect(condDegraded.Reason).To(Equal(reasonNoFailedJobs))

			condAvailable := meta.FindStatusCondition(runbook.Status.Conditions, typeAvailable)
			Expect(condAvailable.Status).To(Equal(metav1.ConditionTrue))
			Expect(condAvailable.Reason).To(Equal(reasonStruggling))

		})
		/*
			It("should successfully reconcile the resource with 3 alerts", func() {
				By("Reconciling the Runbook")
				reconciler := &RunbookReconciler{
					Client: k8sManager.GetClient(),
					Scheme: k8sManager.GetClient().Scheme(),
					Alertmanager: &MockAlertmanagerClient{
						Alerts: alertmanagermodels.GettableAlerts{alert21, alert11, alert12},
					},
					DefaultRequeueAfter: 10 * time.Second,
					MinimumRequeuAfter:  10 * time.Second,
				}
				_, _ = reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				// Expect(err).NotTo(HaveOccurred())

				By("Updating the potentially spawned Jobs")
				Eventually(func(g Gomega) {
					g.Expect(k8sManager.GetClient().List(ctx, &jobList, client.MatchingFields{".metadata.controller": runbook.Name})).To(Succeed())
					g.Expect(jobList.Items).To(HaveLen(3))
				}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

				Expect(k8sClient.Status().Update(ctx, updateJobActive(jobList.Items[0]))).To(Succeed())
				Expect(k8sClient.Status().Update(ctx, updateJobActive(jobList.Items[1]))).To(Succeed())
				Expect(k8sClient.Status().Update(ctx, updateJobActive(jobList.Items[2]))).To(Succeed())

				Eventually(func(g Gomega) {
					g.Expect(k8sManager.GetClient().List(ctx, &jobList, client.MatchingFields{".metadata.controller": runbook.Name})).To(Succeed())
					g.Expect(jobList.Items[0].Status.Active).To(Equal(int32(1)))
					g.Expect(jobList.Items[1].Status.Active).To(Equal(int32(1)))
					g.Expect(jobList.Items[2].Status.Active).To(Equal(int32(1)))
				}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

				By("Checking the Runbooks firing alerts")
				Eventually(func(g Gomega) {
					g.Expect(k8sManager.GetClient().Get(ctx, typeNamespacedName, &runbook)).To(Succeed())
					// g.Expect(runbook.Status.FiringAlerts).To(HaveLen(1))
					// g.Expect(runbook.Status.RunningJobs).To(HaveLen(1))
					// g.Expect(runbook.Status.Conditions).To(HaveLen(3))
				}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())

				By("Checking the Runbooks status conditions")
				Eventually(func(g Gomega) {
					g.Expect(k8sManager.GetClient().Get(ctx, typeNamespacedName, &runbook)).To(Succeed())
					// condAvailable := meta.FindStatusCondition(runbook.Status.Conditions, typeAvailableRunbook)
					// g.Expect(condAvailable.Status).To(Equal(metav1.ConditionTrue))
					// g.Expect(condAvailable.Reason).To(Equal("JobsActive"))
					// condProgressing := meta.FindStatusCondition(runbook.Status.Conditions, typeProgressingRunbook)
					// g.Expect(condProgressing.Status).To(Equal(metav1.ConditionTrue))
					// g.Expect(condProgressing.Reason).To(Equal("JobsActive"))
					// condDegraded := meta.FindStatusCondition(runbook.Status.Conditions, typeDegradedRunbook)
					// g.Expect(condDegraded.Status).To(Equal(metav1.ConditionFalse))
					// g.Expect(condDegraded.Reason).To(Equal("NoFailedJobs"))
				}).WithTimeout(eventualTimeout).WithPolling(100 * time.Millisecond).Should(Succeed())
			})
		*/
	})
})

// Moar tests
/*
	rb := chanclavshniov1alpha1.Runbook{}
	Eventually(func(g Gomega) {
		g.Expect(k8sManager.GetClient().Get(ctx, typeNamespacedName, &rb)).To(Succeed())
		// g.Expect(rb.Status.Conditions).To(ContainElement(...))
		g.Expect(rb.Status.RunningJobs).To(HaveLen(1))
	}).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	time.Sleep(10 * time.Second)

	rb := &chanclavshniov1alpha1.Runbook{}
	err = k8sManager.GetClient().Get(ctx, typeNamespacedName, rb)
	Expect(err).NotTo(HaveOccurred())

	jobs := batchv1.JobList{}
	err = k8sManager.GetClient().List(ctx, &jobs, client.MatchingFields{".metadata.controller": resourceName})
	Expect(err).NotTo(HaveOccurred())

	// Checking the resources
	// Expect(rb.Status.RunningJobs).To(HaveLen(2))
	Expect(jobs.Items).To(HaveLen(2))
*/
