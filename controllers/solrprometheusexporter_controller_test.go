/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controllers

import (
	"crypto/md5"
	"fmt"
	solr "github.com/apache/solr-operator/api/v1beta1"
	"github.com/apache/solr-operator/controllers/util"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
	"testing"
	"time"
)

var _ reconcile.Reconciler = &SolrPrometheusExporterReconciler{}

var (
	expectedMetricsRequest = reconcile.Request{NamespacedName: types.NamespacedName{Name: "foo-met", Namespace: "default"}}
	metricsDKey            = types.NamespacedName{Name: "foo-met-solr-metrics", Namespace: "default"}
	metricsSKey            = types.NamespacedName{Name: "foo-met-solr-metrics", Namespace: "default"}
	metricsCMKey           = types.NamespacedName{Name: "foo-met-solr-metrics", Namespace: "default"}

	testExporterConfig = "THis is a test config."
)

func TestMetricsReconcileWithoutExporterConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	instance := &solr.SolrPrometheusExporter{
		ObjectMeta: metav1.ObjectMeta{Name: expectedMetricsRequest.Name, Namespace: expectedMetricsRequest.Namespace},
		Spec: solr.SolrPrometheusExporterSpec{
			CustomKubeOptions: solr.CustomExporterKubeOptions{
				PodOptions: &solr.PodOptions{
					EnvVariables:                  extraVars,
					PodSecurityContext:            &podSecurityContext,
					Volumes:                       extraVolumes,
					Affinity:                      affinity,
					Resources:                     resources,
					SidecarContainers:             extraContainers2,
					InitContainers:                extraContainers1,
					ImagePullSecrets:              testAdditionalImagePullSecrets,
					TerminationGracePeriodSeconds: &testTerminationGracePeriodSeconds,
					LivenessProbe:                 testProbeLivenessNonDefaults,
					ReadinessProbe:                testProbeReadinessNonDefaults,
				},
			},
			ExporterEntrypoint: "/test/entry-point",
			Image: &solr.ContainerImage{
				ImagePullSecret: testImagePullSecretName,
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrPrometheusExporterReconciler := &SolrPrometheusExporterReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrPrometheusExporter"),
	}
	newRec, requests := SetupTestReconcile(solrPrometheusExporterReconciler)
	g.Expect(solrPrometheusExporterReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	// Create the SolrPrometheusExporter object and expect the Reconcile and Deployment to be created
	err = testClient.Create(context.TODO(), instance)
	// The instance object may not be a valid object because it might be missing some required fields.
	// Please modify the instance object by adding required fields and then remove the following if statement.
	if apierrors.IsInvalid(err) {
		t.Logf("failed to create object, got an invalid object error: %v", err)
		return
	}
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	expectNoConfigMap(g, metricsCMKey)

	deployment := expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, "")

	// Check extra containers
	assert.Equal(t, 3, len(deployment.Spec.Template.Spec.Containers), "PrometheusExporter deployment requires the exporter container plus the desired sidecars.")
	assert.EqualValues(t, extraContainers2, deployment.Spec.Template.Spec.Containers[1:])

	assert.Equal(t, 2, len(deployment.Spec.Template.Spec.InitContainers), "PrometheusExporter deployment requires the additional specified init containers.")
	assert.EqualValues(t, extraContainers1, deployment.Spec.Template.Spec.InitContainers)

	// Pod Options Checks
	assert.Equal(t, extraVars, deployment.Spec.Template.Spec.Containers[0].Env, "Extra Env Vars are not the same as the ones provided in podOptions")
	assert.Equal(t, podSecurityContext, *deployment.Spec.Template.Spec.SecurityContext, "PodSecurityContext is not the same as the one provided in podOptions")
	assert.Equal(t, affinity, deployment.Spec.Template.Spec.Affinity, "Affinity is not the same as the one provided in podOptions")
	assert.Equal(t, resources.Limits, deployment.Spec.Template.Spec.Containers[0].Resources.Limits, "Resources.Limits is not the same as the one provided in podOptions")
	assert.Equal(t, resources.Requests, deployment.Spec.Template.Spec.Containers[0].Resources.Requests, "Resources.Requests is not the same as the one provided in podOptions")
	extraVolumes[0].DefaultContainerMount.Name = extraVolumes[0].Name
	assert.Equal(t, len(extraVolumes), len(deployment.Spec.Template.Spec.Containers[0].VolumeMounts), "Container has wrong number of volumeMounts")
	assert.Equal(t, *extraVolumes[0].DefaultContainerMount, deployment.Spec.Template.Spec.Containers[0].VolumeMounts[0], "Additional Volume from podOptions not mounted into container properly.")
	assert.Equal(t, len(extraVolumes), len(deployment.Spec.Template.Spec.Volumes), "Pod has wrong number of volumes")
	assert.Equal(t, extraVolumes[0].Name, deployment.Spec.Template.Spec.Volumes[0].Name, "Additional Volume from podOptions not loaded into pod properly.")
	assert.Equal(t, extraVolumes[0].Source, deployment.Spec.Template.Spec.Volumes[0].VolumeSource, "Additional Volume from podOptions not loaded into pod properly.")
	assert.ElementsMatch(t, append(testAdditionalImagePullSecrets, corev1.LocalObjectReference{Name: testImagePullSecretName}), deployment.Spec.Template.Spec.ImagePullSecrets, "Incorrect imagePullSecrets")
	assert.EqualValues(t, &testTerminationGracePeriodSeconds, deployment.Spec.Template.Spec.TerminationGracePeriodSeconds, "Incorrect terminationGracePeriodSeconds")

	testPodProbe(t, testProbeLivenessNonDefaults, deployment.Spec.Template.Spec.Containers[0].LivenessProbe, "liveness")
	testPodProbe(t, testProbeReadinessNonDefaults, deployment.Spec.Template.Spec.Containers[0].ReadinessProbe, "readiness")
	assert.Nilf(t, deployment.Spec.Template.Spec.Containers[0].StartupProbe, "%s probe should be nil since it was not specified", "startup")

	// Check the Service
	service := expectService(t, g, requests, expectedMetricsRequest, metricsSKey, deployment.Spec.Template.Labels)
	assert.Equal(t, "true", service.Annotations["prometheus.io/scrape"], "Metrics Service Prometheus scraping is not enabled.")
	assert.EqualValues(t, "solr-metrics", service.Spec.Ports[0].Name, "Wrong port name on common Service")
}

func TestMetricsReconcileWithExporterConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	instance := &solr.SolrPrometheusExporter{
		ObjectMeta: metav1.ObjectMeta{Name: expectedMetricsRequest.Name, Namespace: expectedMetricsRequest.Namespace},
		Spec: solr.SolrPrometheusExporterSpec{
			Config: testExporterConfig,
			CustomKubeOptions: solr.CustomExporterKubeOptions{
				PodOptions: &solr.PodOptions{
					Annotations:       testPodAnnotations,
					Labels:            testPodLabels,
					Volumes:           extraVolumes,
					Tolerations:       testTolerationsPromExporter,
					NodeSelector:      testNodeSelectors,
					PriorityClassName: testPriorityClass,
					StartupProbe:      testProbeStartup,
				},
				DeploymentOptions: &solr.DeploymentOptions{
					Annotations: testDeploymentAnnotations,
					Labels:      testDeploymentLabels,
				},
				ServiceOptions: &solr.ServiceOptions{
					Annotations: testMetricsServiceAnnotations,
					Labels:      testMetricsServiceLabels,
				},
				ConfigMapOptions: &solr.ConfigMapOptions{
					Annotations: testConfigMapAnnotations,
					Labels:      testConfigMapLabels,
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrPrometheusExporterReconciler := &SolrPrometheusExporterReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrPrometheusExporter"),
	}
	newRec, requests := SetupTestReconcile(solrPrometheusExporterReconciler)
	g.Expect(solrPrometheusExporterReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrPrometheusExporter object and expect the Reconcile and Deployment to be created
	err = testClient.Create(context.TODO(), instance)
	// The instance object may not be a valid object because it might be missing some required fields.
	// Please modify the instance object by adding required fields and then remove the following if statement.
	if apierrors.IsInvalid(err) {
		t.Logf("failed to create object, got an invalid object error: %v", err)
		return
	}
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	configMap := expectConfigMap(t, g, requests, expectedMetricsRequest, metricsCMKey, map[string]string{util.PrometheusExporterConfigMapKey: testExporterConfig})
	testMapsEqual(t, "configMap labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testConfigMapLabels), configMap.Labels)
	testMapsEqual(t, "configMap annotations", testConfigMapAnnotations, configMap.Annotations)

	deployment := expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, configMap.Name)
	expectedDeploymentLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"technology": solr.SolrPrometheusExporterTechnologyLabel})
	testMapsEqual(t, "deployment labels", util.MergeLabelsOrAnnotations(expectedDeploymentLabels, testDeploymentLabels), deployment.Labels)
	testMapsEqual(t, "deployment annotations", testDeploymentAnnotations, deployment.Annotations)
	testMapsEqual(t, "pod labels", util.MergeLabelsOrAnnotations(expectedDeploymentLabels, testPodLabels), deployment.Spec.Template.ObjectMeta.Labels)
	expectedMd5 := fmt.Sprintf("%x", md5.Sum([]byte(testExporterConfig)))
	testPodAnnotations[util.PrometheusExporterConfigXmlMd5Annotation] = expectedMd5
	testMapsEqual(t, "pod annotations", testPodAnnotations, deployment.Spec.Template.ObjectMeta.Annotations)
	assert.EqualValues(t, testPriorityClass, deployment.Spec.Template.Spec.PriorityClassName, "Incorrect Priority class name for Pod Spec")

	// Test tolerations and node selectors
	testMapsEqual(t, "pod node selectors", testNodeSelectors, deployment.Spec.Template.Spec.NodeSelector)
	testPodTolerations(t, testTolerationsPromExporter, deployment.Spec.Template.Spec.Tolerations)

	// Other Pod Options
	extraVolumes[0].DefaultContainerMount.Name = extraVolumes[0].Name
	assert.Equal(t, len(extraVolumes)+1, len(deployment.Spec.Template.Spec.Containers[0].VolumeMounts), "Container has wrong number of volumeMounts")
	assert.EqualValues(t, *extraVolumes[0].DefaultContainerMount, deployment.Spec.Template.Spec.Containers[0].VolumeMounts[1], "Additional Volume from podOptions not mounted into container properly.")
	assert.Equal(t, len(extraVolumes)+1, len(deployment.Spec.Template.Spec.Volumes), "Pod has wrong number of volumes")
	assert.Equal(t, extraVolumes[0].Name, deployment.Spec.Template.Spec.Volumes[1].Name, "Additional Volume from podOptions not loaded into pod properly.")
	assert.Equal(t, extraVolumes[0].Source, deployment.Spec.Template.Spec.Volumes[1].VolumeSource, "Additional Volume from podOptions not loaded into pod properly.")

	testPodProbe(t, &corev1.Probe{
		Handler: corev1.Handler{
			HTTPGet: &corev1.HTTPGetAction{
				Scheme: corev1.URISchemeHTTP,
				Path:   "/metrics",
				Port:   intstr.FromInt(util.SolrMetricsPort),
			},
		},
		InitialDelaySeconds: 20,
		TimeoutSeconds:      1,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}, deployment.Spec.Template.Spec.Containers[0].LivenessProbe, "liveness")
	testPodProbe(t, testProbeStartup, deployment.Spec.Template.Spec.Containers[0].StartupProbe, "startup")
	assert.Nilf(t, deployment.Spec.Template.Spec.Containers[0].ReadinessProbe, "%s probe should be nil since it was not specified", "readiness")

	// Check the Service
	expectedServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "metrics"})
	expectedServiceAnnotations := map[string]string{"prometheus.io/path": "/metrics", "prometheus.io/port": "80", "prometheus.io/scheme": "http", "prometheus.io/scrape": "true"}
	service := expectService(t, g, requests, expectedMetricsRequest, metricsSKey, expectedDeploymentLabels)
	assert.Equal(t, "true", service.Annotations["prometheus.io/scrape"], "Metrics Service Prometheus scraping is not enabled.")
	testMapsEqual(t, "service labels", util.MergeLabelsOrAnnotations(expectedServiceLabels, testMetricsServiceLabels), service.Labels)
	testMapsEqual(t, "service annotations", util.MergeLabelsOrAnnotations(expectedServiceAnnotations, testMetricsServiceAnnotations), service.Annotations)
	assert.EqualValues(t, "solr-metrics", service.Spec.Ports[0].Name, "Wrong port name on common Service")
}

func TestMetricsReconcileWithGivenZkAcls(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	instance := &solr.SolrPrometheusExporter{
		ObjectMeta: metav1.ObjectMeta{Name: expectedMetricsRequest.Name, Namespace: expectedMetricsRequest.Namespace},
		Spec: solr.SolrPrometheusExporterSpec{
			Config: testExporterConfig,
			SolrReference: solr.SolrReference{
				Cloud: &solr.SolrCloudReference{
					ZookeeperConnectionInfo: &solr.ZookeeperConnectionInfo{
						InternalConnectionString: "host:7271",
						AllACL: &solr.ZookeeperACL{
							SecretRef:   "secret-name",
							UsernameKey: "user",
							PasswordKey: "pass",
						},
						ReadOnlyACL: &solr.ZookeeperACL{
							SecretRef:   "read-secret-name",
							UsernameKey: "read-only-user",
							PasswordKey: "read-only-pass",
						},
					},
				},
			},
			CustomKubeOptions: solr.CustomExporterKubeOptions{
				PodOptions: &solr.PodOptions{
					EnvVariables:       extraVars,
					Annotations:        testPodAnnotations,
					Labels:             testPodLabels,
					Volumes:            extraVolumes,
					Tolerations:        testTolerationsPromExporter,
					NodeSelector:       testNodeSelectors,
					ServiceAccountName: testServiceAccountName,
				},
				DeploymentOptions: &solr.DeploymentOptions{
					Annotations: testDeploymentAnnotations,
					Labels:      testDeploymentLabels,
				},
				ServiceOptions: &solr.ServiceOptions{
					Annotations: testMetricsServiceAnnotations,
					Labels:      testMetricsServiceLabels,
				},
				ConfigMapOptions: &solr.ConfigMapOptions{
					Annotations: testConfigMapAnnotations,
					Labels:      testConfigMapLabels,
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrPrometheusExporterReconciler := &SolrPrometheusExporterReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrPrometheusExporter"),
	}
	newRec, requests := SetupTestReconcile(solrPrometheusExporterReconciler)
	g.Expect(solrPrometheusExporterReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrPrometheusExporter object and expect the Reconcile and Deployment to be created
	err = testClient.Create(context.TODO(), instance)
	// The instance object may not be a valid object because it might be missing some required fields.
	// Please modify the instance object by adding required fields and then remove the following if statement.
	if apierrors.IsInvalid(err) {
		t.Logf("failed to create object, got an invalid object error: %v", err)
		return
	}
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	configMap := expectConfigMap(t, g, requests, expectedMetricsRequest, metricsCMKey, map[string]string{util.PrometheusExporterConfigMapKey: testExporterConfig})
	testMapsEqual(t, "configMap labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testConfigMapLabels), configMap.Labels)
	testMapsEqual(t, "configMap annotations", testConfigMapAnnotations, configMap.Annotations)

	deployment := expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, configMap.Name)
	expectedDeploymentLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"technology": solr.SolrPrometheusExporterTechnologyLabel})
	testMapsEqual(t, "deployment labels", util.MergeLabelsOrAnnotations(expectedDeploymentLabels, testDeploymentLabels), deployment.Labels)
	testMapsEqual(t, "deployment annotations", testDeploymentAnnotations, deployment.Annotations)
	testMapsEqual(t, "pod labels", util.MergeLabelsOrAnnotations(expectedDeploymentLabels, testPodLabels), deployment.Spec.Template.ObjectMeta.Labels)

	expectedMd5 := fmt.Sprintf("%x", md5.Sum([]byte(testExporterConfig)))
	testPodAnnotations[util.PrometheusExporterConfigXmlMd5Annotation] = expectedMd5
	testMapsEqual(t, "pod annotations", testPodAnnotations, deployment.Spec.Template.ObjectMeta.Annotations)

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"JAVA_OPTS": "$(SOLR_ZK_CREDS_AND_ACLS)",
	}
	foundEnv := deployment.Spec.Template.Spec.Containers[0].Env
	f := false
	zkAclEnvVars := []corev1.EnvVar{
		{
			Name: "ZK_ALL_ACL_USERNAME",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "secret-name"},
					Key:                  "user",
					Optional:             &f,
				},
			},
		},
		{
			Name: "ZK_ALL_ACL_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "secret-name"},
					Key:                  "pass",
					Optional:             &f,
				},
			},
		},
		{
			Name: "ZK_READ_ACL_USERNAME",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "read-secret-name"},
					Key:                  "read-only-user",
					Optional:             &f,
				},
			},
		},
		{
			Name: "ZK_READ_ACL_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "read-secret-name"},
					Key:                  "read-only-pass",
					Optional:             &f,
				},
			},
		},
		{
			Name:      "SOLR_ZK_CREDS_AND_ACLS",
			Value:     "-DzkACLProvider=org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider -DzkCredentialsProvider=org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider -DzkDigestUsername=$(ZK_ALL_ACL_USERNAME) -DzkDigestPassword=$(ZK_ALL_ACL_PASSWORD) -DzkDigestReadonlyUsername=$(ZK_READ_ACL_USERNAME) -DzkDigestReadonlyPassword=$(ZK_READ_ACL_PASSWORD)",
			ValueFrom: nil,
		},
	}
	assert.Equal(t, zkAclEnvVars, foundEnv[0:5], "ZK ACL Env Vars are not correct")
	assert.Equal(t, extraVars, foundEnv[5:len(foundEnv)-1], "Extra Env Vars are not the same as the ones provided in podOptions")
	// Note that this check changes the variable foundEnv, so the values are no longer valid afterwards.
	// TODO: Make this not invalidate foundEnv
	testMetricsPodEnvVariables(t, expectedEnvVars, foundEnv[len(foundEnv)-1:])

	// Test tolerations and node selectors
	testMapsEqual(t, "pod node selectors", testNodeSelectors, deployment.Spec.Template.Spec.NodeSelector)
	testPodTolerations(t, testTolerationsPromExporter, deployment.Spec.Template.Spec.Tolerations)

	// Other Pod Options
	extraVolumes[0].DefaultContainerMount.Name = extraVolumes[0].Name
	assert.Equal(t, len(extraVolumes)+1, len(deployment.Spec.Template.Spec.Containers[0].VolumeMounts), "Container has wrong number of volumeMounts")
	assert.Equal(t, *extraVolumes[0].DefaultContainerMount, deployment.Spec.Template.Spec.Containers[0].VolumeMounts[1], "Additional Volume from podOptions not mounted into container properly.")
	assert.Equal(t, len(extraVolumes)+1, len(deployment.Spec.Template.Spec.Volumes), "Pod has wrong number of volumes")
	assert.Equal(t, extraVolumes[0].Name, deployment.Spec.Template.Spec.Volumes[1].Name, "Additional Volume from podOptions not loaded into pod properly.")
	assert.Equal(t, extraVolumes[0].Source, deployment.Spec.Template.Spec.Volumes[1].VolumeSource, "Additional Volume from podOptions not loaded into pod properly.")
	assert.Equal(t, testServiceAccountName, deployment.Spec.Template.Spec.ServiceAccountName, "Incorrect serviceAccountName.")

	expectedServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "metrics"})
	expectedServiceAnnotations := map[string]string{"prometheus.io/path": "/metrics", "prometheus.io/port": "80", "prometheus.io/scheme": "http", "prometheus.io/scrape": "true"}
	service := expectService(t, g, requests, expectedMetricsRequest, metricsSKey, expectedDeploymentLabels)
	assert.Equal(t, "true", service.Annotations["prometheus.io/scrape"], "Metrics Service Prometheus scraping is not enabled.")
	testMapsEqual(t, "service labels", util.MergeLabelsOrAnnotations(expectedServiceLabels, testMetricsServiceLabels), service.Labels)
	testMapsEqual(t, "service annotations", util.MergeLabelsOrAnnotations(expectedServiceAnnotations, testMetricsServiceAnnotations), service.Annotations)
}

func TestMetricsReconcileWithSolrZkAcls(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	instance := &solr.SolrPrometheusExporter{
		ObjectMeta: metav1.ObjectMeta{Name: expectedMetricsRequest.Name, Namespace: expectedMetricsRequest.Namespace},
		Spec: solr.SolrPrometheusExporterSpec{
			Config: testExporterConfig,
			SolrReference: solr.SolrReference{
				Cloud: &solr.SolrCloudReference{
					Name: expectedCloudRequest.Name,
				},
			},
			CustomKubeOptions: solr.CustomExporterKubeOptions{
				PodOptions: &solr.PodOptions{
					EnvVariables: extraVars,
				},
			},
		},
	}

	solrRef := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
					AllACL: &solr.ZookeeperACL{
						SecretRef:   "secret-name",
						UsernameKey: "user",
						PasswordKey: "pass",
					},
					ReadOnlyACL: &solr.ZookeeperACL{
						SecretRef:   "read-secret-name",
						UsernameKey: "read-only-user",
						PasswordKey: "read-only-pass",
					},
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrPrometheusExporterReconciler := &SolrPrometheusExporterReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrPrometheusExporter"),
	}
	newRec, requests := SetupTestReconcile(solrPrometheusExporterReconciler)
	g.Expect(solrPrometheusExporterReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	cloudRec, cloudRequests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, cloudRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrCloud object that the PrometheusExporter will reference
	err = testClient.Create(context.TODO(), solrRef)
	if apierrors.IsInvalid(err) {
		t.Logf("failed to create object, got an invalid object error: %v", err)
		return
	}
	defer testClient.Delete(context.TODO(), solrRef)
	g.Eventually(cloudRequests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	// Add an additional check for reconcile, so that the services will have IP addresses for the hostAliases to use
	// Otherwise the reconciler will have 'blockReconciliationOfStatefulSet' set to true, and the stateful set will not be created
	g.Eventually(cloudRequests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Create the SolrPrometheusExporter object and expect the Reconcile and Deployment to be created
	err = testClient.Create(context.TODO(), instance)
	// The instance object may not be a valid object because it might be missing some required fields.
	// Please modify the instance object by adding required fields and then remove the following if statement.
	if apierrors.IsInvalid(err) {
		t.Logf("failed to create object, got an invalid object error: %v", err)
		return
	}
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	configMap := expectConfigMap(t, g, requests, expectedMetricsRequest, metricsCMKey, map[string]string{util.PrometheusExporterConfigMapKey: testExporterConfig})
	testMapsEqual(t, "configMap labels", instance.SharedLabelsWith(instance.Labels), configMap.Labels)

	deployment := expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, configMap.Name)
	expectedDeploymentLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"technology": solr.SolrPrometheusExporterTechnologyLabel})
	testMapsEqual(t, "deployment labels", expectedDeploymentLabels, deployment.Labels)
	testMapsEqual(t, "pod labels", expectedDeploymentLabels, deployment.Spec.Template.ObjectMeta.Labels)

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"JAVA_OPTS": "$(SOLR_ZK_CREDS_AND_ACLS)",
	}
	foundEnv := deployment.Spec.Template.Spec.Containers[0].Env
	f := false
	zkAclEnvVars := []corev1.EnvVar{
		{
			Name: "ZK_ALL_ACL_USERNAME",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "secret-name"},
					Key:                  "user",
					Optional:             &f,
				},
			},
		},
		{
			Name: "ZK_ALL_ACL_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "secret-name"},
					Key:                  "pass",
					Optional:             &f,
				},
			},
		},
		{
			Name: "ZK_READ_ACL_USERNAME",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "read-secret-name"},
					Key:                  "read-only-user",
					Optional:             &f,
				},
			},
		},
		{
			Name: "ZK_READ_ACL_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "read-secret-name"},
					Key:                  "read-only-pass",
					Optional:             &f,
				},
			},
		},
		{
			Name:      "SOLR_ZK_CREDS_AND_ACLS",
			Value:     "-DzkACLProvider=org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider -DzkCredentialsProvider=org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider -DzkDigestUsername=$(ZK_ALL_ACL_USERNAME) -DzkDigestPassword=$(ZK_ALL_ACL_PASSWORD) -DzkDigestReadonlyUsername=$(ZK_READ_ACL_USERNAME) -DzkDigestReadonlyPassword=$(ZK_READ_ACL_PASSWORD)",
			ValueFrom: nil,
		},
	}
	assert.Equal(t, 8, len(foundEnv), "Incorrect number of Env Vars found")
	assert.Equal(t, zkAclEnvVars, foundEnv[0:5], "ZK ACL Env Vars are not correct")
	assert.Equal(t, extraVars, foundEnv[5:len(foundEnv)-1], "Extra Env Vars are not the same as the ones provided in podOptions")
	testMetricsPodEnvVariables(t, expectedEnvVars, foundEnv[len(foundEnv)-1:])
	assert.Empty(t, deployment.Spec.Template.Spec.ServiceAccountName, "No custom serviceAccountName specified, so the field should be empty.")

	expectedServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "metrics"})
	expectedServiceAnnotations := map[string]string{"prometheus.io/path": "/metrics", "prometheus.io/port": "80", "prometheus.io/scheme": "http", "prometheus.io/scrape": "true"}
	service := expectService(t, g, requests, expectedMetricsRequest, metricsSKey, expectedDeploymentLabels)
	assert.Equal(t, "true", service.Annotations["prometheus.io/scrape"], "Metrics Service Prometheus scraping is not enabled.")
	testMapsEqual(t, "service labels", expectedServiceLabels, service.Labels)
	testMapsEqual(t, "service annotations", expectedServiceAnnotations, service.Annotations)
}

func TestMetricsReconcileWithUserProvidedConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrPrometheusExporterReconciler := &SolrPrometheusExporterReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrPrometheusExporter"),
	}
	newRec, requests := SetupTestReconcile(solrPrometheusExporterReconciler)
	g.Expect(solrPrometheusExporterReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, expectedMetricsRequest.Namespace)

	// configure the exporter to pull config from a user-provided ConfigMap instead of the default
	withUserProvidedConfigMapName := "custom-exporter-config"
	instance := &solr.SolrPrometheusExporter{
		ObjectMeta: metav1.ObjectMeta{Name: expectedMetricsRequest.Name, Namespace: expectedMetricsRequest.Namespace},
		Spec: solr.SolrPrometheusExporterSpec{
			CustomKubeOptions: solr.CustomExporterKubeOptions{
				ConfigMapOptions: &solr.ConfigMapOptions{
					ProvidedConfigMap: withUserProvidedConfigMapName,
				},
			},
		},
	}

	// Create the SolrPrometheusExporter object and expect the Reconcile and Deployment to be created
	err = testClient.Create(context.TODO(), instance)
	// The instance object may not be a valid object because it might be missing some required fields.
	// Please modify the instance object by adding required fields and then remove the following if statement.
	if apierrors.IsInvalid(err) {
		t.Logf("failed to create object, got an invalid object error: %v", err)
		return
	}
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)

	// reconcile is happening but it can't proceed b/c the user-provided configmap doesn't exist
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	// create the user-provided ConfigMap but w/o the expected key
	userProvidedConfigXml := "<config/>"
	userProvidedConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      withUserProvidedConfigMapName,
			Namespace: expectedMetricsRequest.Namespace,
		},
		Data: map[string]string{
			"foo": userProvidedConfigXml,
		},
	}
	err = testClient.Create(context.TODO(), userProvidedConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	userProvidedConfigMapNN := types.NamespacedName{Name: userProvidedConfigMap.Name, Namespace: userProvidedConfigMap.Namespace}

	// can't proceed b/c the user-provided ConfigMap is invalid
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	// update the config to fix the error
	updateUserProvidedConfigMap(testClient, g, userProvidedConfigMapNN, map[string]string{util.PrometheusExporterConfigMapKey: userProvidedConfigXml})

	// reconcile should pass now
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	deployment := expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, userProvidedConfigMap.Name)
	expectedAnnotations := map[string]string{
		util.PrometheusExporterConfigXmlMd5Annotation: fmt.Sprintf("%x", md5.Sum([]byte(userProvidedConfigXml))),
	}
	testMapsEqual(t, "pod annotations", expectedAnnotations, deployment.Spec.Template.ObjectMeta.Annotations)

	// update the user-provided ConfigMap to trigger reconcile on the deployment
	updatedConfigXml := "<config>updated by user</config>"
	updateUserProvidedConfigMap(testClient, g, userProvidedConfigMapNN, map[string]string{util.PrometheusExporterConfigMapKey: updatedConfigXml})

	// reconcile should happen again 3x
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))
	time.Sleep(time.Millisecond * 250)

	deployment = expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, userProvidedConfigMap.Name)
	expectedAnnotations = map[string]string{
		util.PrometheusExporterConfigXmlMd5Annotation: fmt.Sprintf("%x", md5.Sum([]byte(updatedConfigXml))),
	}
	testMapsEqual(t, "pod annotations", expectedAnnotations, deployment.Spec.Template.ObjectMeta.Annotations)
}

func updateUserProvidedConfigMap(testClient client.Client, g *gomega.GomegaWithT, userProvidedConfigMapNN types.NamespacedName, dataMap map[string]string) {
	foundConfigMap := &corev1.ConfigMap{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), userProvidedConfigMapNN, foundConfigMap) }, timeout).Should(gomega.Succeed())
	foundConfigMap.Data = dataMap
	err := testClient.Update(context.TODO(), foundConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())
}

func TestMetricsReconcileWithTLSConfig(t *testing.T) {
	testReconcileWithTLS(t, "tls-cert-secret-from-user", false, false, false, false)
}

func TestMetricsReconcileWithTLSAndPkcs12Conversion(t *testing.T) {
	testReconcileWithTLS(t, "tls-cert-secret-from-user-no-pkcs12", true, false, false, false)
}

func TestMetricsReconcileWithTLSSecretUpdate(t *testing.T) {
	testReconcileWithTLS(t, "tls-cert-secret-update", false, true, false, false)
}

func TestMetricsReconcileWithTLSConfigAndBasicAuth(t *testing.T) {
	testReconcileWithTLS(t, "tls-cert-secret-with-auth", false, true, true, false)
}

func TestMetricsReconcileWithTLSConfigAndBasicAuthSecretUpdate(t *testing.T) {
	testReconcileWithTLS(t, "tls-cert-secret-with-auth-update", false, true, true, true)
}

func testReconcileWithTLS(t *testing.T, tlsSecretName string, needsPkcs12InitContainer bool, restartOnTLSSecretUpdate bool, testWithBasicAuthEnabled bool, updateAuthSecret bool) {
	ctx := context.TODO()

	g := gomega.NewGomegaWithT(t)
	instance := &solr.SolrPrometheusExporter{
		ObjectMeta: metav1.ObjectMeta{Name: expectedMetricsRequest.Name, Namespace: expectedMetricsRequest.Namespace},
		Spec:       solr.SolrPrometheusExporterSpec{},
	}

	keystorePassKey := "keystore-passwords-are-important"

	instance.Spec.SolrReference.SolrTLS = createTLSOptions(tlsSecretName, keystorePassKey, restartOnTLSSecretUpdate)
	verifyUserSuppliedTLSConfig(t, instance.Spec.SolrReference.SolrTLS, tlsSecretName, keystorePassKey, tlsSecretName, needsPkcs12InitContainer)

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrPrometheusExporterReconciler := &SolrPrometheusExporterReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrPrometheusExporter"),
	}
	newRec, requests := SetupTestReconcile(solrPrometheusExporterReconciler)
	g.Expect(solrPrometheusExporterReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, expectedMetricsRequest.Namespace)

	// create the TLS and keystore secrets needed for reconciling TLS options
	tlsKey := "keystore.p12"
	if needsPkcs12InitContainer {
		tlsKey = "tls.key" // to trigger the initContainer creation, don't want keystore.p12 in the secret
	}
	mockSecret, err := createMockTLSSecret(ctx, testClient, tlsSecretName, tlsKey, instance.Namespace, keystorePassKey)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(ctx, &mockSecret)

	basicAuthMd5 := ""
	if testWithBasicAuthEnabled {
		secretName := tlsSecretName + "-basic-auth"
		basicAuthSecret := createBasicAuthSecret(secretName, solr.DefaultBasicAuthUsername, expectedMetricsRequest.Namespace)
		err := testClient.Create(ctx, basicAuthSecret)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer testClient.Delete(ctx, basicAuthSecret)
		creds := fmt.Sprintf("%s:%s", basicAuthSecret.Data[corev1.BasicAuthUsernameKey], basicAuthSecret.Data[corev1.BasicAuthPasswordKey])
		basicAuthMd5 = fmt.Sprintf("%x", md5.Sum([]byte(creds)))

		// this would come from the user in a real app
		instance.Spec.SolrReference.BasicAuthSecret = secretName
	}

	// Create the SolrPrometheusExporter object and expect the Reconcile and Deployment to be created
	err = testClient.Create(ctx, instance)
	// The instance object may not be a valid object because it might be missing some required fields.
	// Please modify the instance object by adding required fields and then remove the following if statement.
	if apierrors.IsInvalid(err) {
		t.Logf("failed to create object, got an invalid object error: %v", err)
		return
	}
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(ctx, instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	deployment := expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, "")
	mainContainer := expectTLSConfigOnPodTemplate(t, instance.Spec.SolrReference.SolrTLS, &deployment.Spec.Template, needsPkcs12InitContainer)

	// make sure JAVA_OPTS is set correctly with the TLS related sys props
	envVars := filterVarsByName(mainContainer.Env, func(n string) bool {
		return strings.HasPrefix(n, "JAVA_OPTS")
	})
	assert.Equal(t, 1, len(envVars))
	assert.True(t, strings.Contains(envVars[0].Value, "-Dsolr.ssl.checkPeerName=$(SOLR_SSL_CHECK_PEER_NAME)"))

	if testWithBasicAuthEnabled {
		lookupAuthSecret := &corev1.Secret{}
		err = testClient.Get(ctx, types.NamespacedName{Name: instance.Spec.SolrReference.BasicAuthSecret, Namespace: instance.Namespace}, lookupAuthSecret)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		expectBasicAuthEnvVars(t, mainContainer.Env, lookupAuthSecret)
	}

	if !restartOnTLSSecretUpdate && instance.Spec.SolrReference.BasicAuthSecret == "" {
		// shouldn't be any annotations on the podTemplateSpec if we're not tracking updates to the TLS secret
		assert.Nil(t, deployment.Spec.Template.ObjectMeta.Annotations)
		return
	}

	// let's trigger an update to the TLS secret to simulate the cert getting renewed and the pods getting restarted
	expectedAnnotations := map[string]string{
		util.SolrTlsCertMd5Annotation: fmt.Sprintf("%x", md5.Sum(mockSecret.Data[util.TLSCertKey])),
	}
	if testWithBasicAuthEnabled {
		// if auth enabled, then annotations also include an md5 for the password so exporters get restarted when it changes
		expectedAnnotations[util.BasicAuthMd5Annotation] = basicAuthMd5
	}

	testMapsEqual(t, "pod annotations", expectedAnnotations, deployment.Spec.Template.ObjectMeta.Annotations)

	foundTLSSecret := &corev1.Secret{}
	err = testClient.Get(ctx, types.NamespacedName{Name: instance.Spec.SolrReference.SolrTLS.PKCS12Secret.Name, Namespace: instance.Namespace}, foundTLSSecret)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// change the tls.crt which should trigger a rolling restart
	updatedTlsCertData := "certificate renewed"
	foundTLSSecret.Data[util.TLSCertKey] = []byte(updatedTlsCertData)
	err = testClient.Update(context.TODO(), foundTLSSecret)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// capture all reconcile requests
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

	// Check the annotation on the pod template to make sure a rolling restart will take place
	time.Sleep(time.Millisecond * 250)
	deployment = expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, "")
	expectedAnnotations = map[string]string{
		util.SolrTlsCertMd5Annotation: fmt.Sprintf("%x", md5.Sum(foundTLSSecret.Data[util.TLSCertKey])),
	}
	if testWithBasicAuthEnabled {
		// if auth enabled, then annotations also include an md5 for the password so exporters get restarted when it changes
		expectedAnnotations[util.BasicAuthMd5Annotation] = basicAuthMd5
	}
	testMapsEqual(t, "pod annotations", expectedAnnotations, deployment.Spec.Template.ObjectMeta.Annotations)

	if testWithBasicAuthEnabled && updateAuthSecret {
		// verify the pods would get restarted if the basic auth secret changes
		secretName := instance.Spec.SolrReference.BasicAuthSecret
		lookupBasicAuthSecret := &corev1.Secret{}
		err = testClient.Get(ctx, types.NamespacedName{Name: secretName, Namespace: instance.Namespace}, lookupBasicAuthSecret)
		g.Expect(err).NotTo(gomega.HaveOccurred())

		// change the tls.crt which should trigger a rolling restart
		updatedPassword := "updated-password"
		lookupBasicAuthSecret.Data[corev1.BasicAuthUsernameKey] = []byte(updatedPassword)
		err = testClient.Update(context.TODO(), lookupBasicAuthSecret)
		g.Expect(err).NotTo(gomega.HaveOccurred())

		g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))
		g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))
		g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedMetricsRequest)))

		// Check the annotation on the pod template to make sure a rolling restart will take place
		time.Sleep(time.Millisecond * 250)
		deployment = expectDeployment(t, g, requests, expectedMetricsRequest, metricsDKey, "")

		creds := string(lookupBasicAuthSecret.Data[corev1.BasicAuthUsernameKey]) + ":" + string(lookupBasicAuthSecret.Data[corev1.BasicAuthPasswordKey])
		expectedAnnotations = map[string]string{
			util.SolrTlsCertMd5Annotation: fmt.Sprintf("%x", md5.Sum(foundTLSSecret.Data[util.TLSCertKey])),
			util.BasicAuthMd5Annotation:   fmt.Sprintf("%x", md5.Sum([]byte(creds))),
		}
		testMapsEqual(t, "pod annotations", expectedAnnotations, deployment.Spec.Template.ObjectMeta.Annotations)
	}
}

func expectBasicAuthEnvVars(t *testing.T, envVars []corev1.EnvVar, basicAuthSecret *corev1.Secret) {
	assert.NotNil(t, envVars)
	envVars = filterVarsByName(envVars, func(n string) bool {
		return n == "BASIC_AUTH_PASS" || n == "BASIC_AUTH_USER" || n == "JAVA_OPTS"
	})
	assert.True(t, len(envVars) == 3)

	for _, envVar := range envVars {
		if envVar.Name == "JAVA_OPTS" {
			assert.True(t, strings.Contains(envVar.Value, "-Dbasicauth=$(BASIC_AUTH_USER):$(BASIC_AUTH_PASS)"), "Expected basic auth creds in JAVA_OPTS")
		}

		if envVar.Name == "BASIC_AUTH_PASS" {
			assert.NotNil(t, envVar.ValueFrom)
			assert.NotNil(t, envVar.ValueFrom.SecretKeyRef)
			assert.Equal(t, basicAuthSecret.Name, envVar.ValueFrom.SecretKeyRef.Name)
			assert.Equal(t, corev1.BasicAuthPasswordKey, envVar.ValueFrom.SecretKeyRef.Key)
		}

		if envVar.Name == "BASIC_AUTH_USER" {
			assert.NotNil(t, envVar.ValueFrom)
			assert.NotNil(t, envVar.ValueFrom.SecretKeyRef)
			assert.Equal(t, basicAuthSecret.Name, envVar.ValueFrom.SecretKeyRef.Name)
			assert.Equal(t, corev1.BasicAuthUsernameKey, envVar.ValueFrom.SecretKeyRef.Key)
		}

	}
}
