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
	"strconv"
	"time"

	solr "github.com/apache/solr-operator/api/v1beta1"
	"github.com/apache/solr-operator/controllers/util"
	"github.com/onsi/gomega"
	zookeeperv1beta1 "github.com/pravega/zookeeper-operator/pkg/apis/zookeeper/v1beta1"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
	"testing"
)

var _ reconcile.Reconciler = &SolrCloudReconciler{}

var (
	expectedCloudRequest = reconcile.Request{NamespacedName: types.NamespacedName{Name: "foo-clo", Namespace: "default"}}
	cloudSsKey           = types.NamespacedName{Name: "foo-clo-solrcloud", Namespace: "default"}
	cloudCsKey           = types.NamespacedName{Name: "foo-clo-solrcloud-common", Namespace: "default"}
	cloudHsKey           = types.NamespacedName{Name: "foo-clo-solrcloud-headless", Namespace: "default"}
	cloudIKey            = types.NamespacedName{Name: "foo-clo-solrcloud-common", Namespace: "default"}
	cloudCMKey           = types.NamespacedName{Name: "foo-clo-solrcloud-configmap", Namespace: "default"}
	cloudZkKey           = types.NamespacedName{Name: "foo-clo-solrcloud-zookeeper", Namespace: "default"}
)

func TestCloudReconcile(t *testing.T) {
	UseZkCRD(true)
	g := gomega.NewGomegaWithT(t)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
				},
			},
			SolrJavaMem:  "-Xmx4G",
			SolrOpts:     "extra-opts",
			SolrLogLevel: "DEBUG",
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				PodOptions: &solr.PodOptions{
					EnvVariables:       extraVars,
					PodSecurityContext: &podSecurityContext,
					Volumes:            extraVolumes,
					Affinity:           affinity,
					Resources:          resources,
					SidecarContainers:  extraContainers1,
					InitContainers:     extraContainers2,
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	newRec, requests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrCloud object and expect the Reconcile and StatefulSet to be created
	err = testClient.Create(context.TODO(), instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Update the status of the SolrCloud
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	// Check extra containers
	assert.Equal(t, 3, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires the solr container plus the desired sidecars.")
	assert.EqualValues(t, extraContainers1, statefulSet.Spec.Template.Spec.Containers[1:])

	assert.Equal(t, 3, len(statefulSet.Spec.Template.Spec.InitContainers), "Solr StatefulSet requires a default initContainer plus the additional specified.")
	assert.EqualValues(t, extraContainers2, statefulSet.Spec.Template.Spec.InitContainers[1:])

	// Check the update strategy
	assert.EqualValues(t, appsv1.OnDeleteStatefulSetStrategyType, statefulSet.Spec.UpdateStrategy.Type, "Incorrect statefulset update strategy")
	assert.EqualValues(t, appsv1.ParallelPodManagement, statefulSet.Spec.PodManagementPolicy, "Incorrect statefulset pod management policy")

	// Host Alias Tests
	assert.Nil(t, statefulSet.Spec.Template.Spec.HostAliases, "There is no need for host aliases because traffic is going directly to pods.")

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "host:7271/",
		"SOLR_HOST":      "$(POD_HOSTNAME)." + instance.HeadlessServiceName() + "." + instance.Namespace,
		"SOLR_JAVA_MEM":  "-Xmx4G",
		"SOLR_PORT":      "8983",
		"SOLR_NODE_PORT": "8983",
		"SOLR_LOG_LEVEL": "DEBUG",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT) extra-opts",
	}
	foundEnv := statefulSet.Spec.Template.Spec.Containers[0].Env
	assert.Equal(t, extraVars, foundEnv[len(foundEnv)-3:len(foundEnv)-1], "Extra Env Vars are not the same as the ones provided in podOptions")
	// Note that this check changes the variable foundEnv, so the values are no longer valid afterwards.
	// TODO: Make this not invalidate foundEnv
	testPodEnvVariables(t, expectedEnvVars, append(foundEnv[:len(foundEnv)-3], foundEnv[len(foundEnv)-1]))

	// Other Pod Options Checks
	assert.Equal(t, podSecurityContext, *statefulSet.Spec.Template.Spec.SecurityContext, "PodSecurityContext is not the same as the one provided in podOptions")
	assert.Equal(t, affinity, statefulSet.Spec.Template.Spec.Affinity, "Affinity is not the same as the one provided in podOptions")
	assert.Equal(t, resources.Limits, statefulSet.Spec.Template.Spec.Containers[0].Resources.Limits, "Resources.Limits is not the same as the one provided in podOptions")
	assert.Equal(t, resources.Requests, statefulSet.Spec.Template.Spec.Containers[0].Resources.Requests, "Resources.Requests is not the same as the one provided in podOptions")
	extraVolumes[0].DefaultContainerMount.Name = extraVolumes[0].Name
	assert.Equal(t, len(extraVolumes)+1, len(statefulSet.Spec.Template.Spec.Containers[0].VolumeMounts), "Container has wrong number of volumeMounts")
	assert.Equal(t, *extraVolumes[0].DefaultContainerMount, statefulSet.Spec.Template.Spec.Containers[0].VolumeMounts[1], "Additional Volume from podOptions not mounted into container properly.")
	assert.Equal(t, len(extraVolumes)+2, len(statefulSet.Spec.Template.Spec.Volumes), "Pod has wrong number of volumes")
	assert.Equal(t, extraVolumes[0].Name, statefulSet.Spec.Template.Spec.Volumes[2].Name, "Additional Volume from podOptions not loaded into pod properly.")
	assert.Equal(t, extraVolumes[0].Source, statefulSet.Spec.Template.Spec.Volumes[2].VolumeSource, "Additional Volume from podOptions not loaded into pod properly.")

	// Check the client Service
	clientService := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Template.Labels)
	expetedLabels, _ := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: clientService.Spec.Selector})
	assert.EqualValues(t, expetedLabels.String(), instance.Status.PodSelector, "Incorrect pod selector object in ")

	// Check the headless Service
	expectService(t, g, requests, expectedCloudRequest, cloudHsKey, statefulSet.Spec.Template.Labels)

	// Check the ingress
	expectNoIngress(g, cloudIKey)
}

func TestCustomKubeOptionsCloudReconcile(t *testing.T) {
	UseZkCRD(true)
	g := gomega.NewGomegaWithT(t)
	replicas := int32(4)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{
			Name:      expectedCloudRequest.Name,
			Namespace: expectedCloudRequest.Namespace,
			Labels:    map[string]string{"base": "here"},
		},
		Spec: solr.SolrCloudSpec{
			SolrImage: &solr.ContainerImage{
				ImagePullSecret: testImagePullSecretName,
			},
			Replicas: &replicas,
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
				},
			},
			UpdateStrategy: solr.SolrUpdateStrategy{
				Method:          solr.StatefulSetUpdate,
				RestartSchedule: "@every 30m",
			},
			SolrGCTune: "gc Options",
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				PodOptions: &solr.PodOptions{
					Annotations:                   testPodAnnotations,
					Labels:                        testPodLabels,
					Tolerations:                   testTolerations,
					NodeSelector:                  testNodeSelectors,
					LivenessProbe:                 testProbeLivenessNonDefaults,
					ReadinessProbe:                testProbeReadinessNonDefaults,
					StartupProbe:                  testProbeStartup,
					PriorityClassName:             testPriorityClass,
					ImagePullSecrets:              testAdditionalImagePullSecrets,
					TerminationGracePeriodSeconds: &testTerminationGracePeriodSeconds,
					ServiceAccountName:            testServiceAccountName,
				},
				StatefulSetOptions: &solr.StatefulSetOptions{
					Annotations:         testSSAnnotations,
					Labels:              testSSLabels,
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
				},
				CommonServiceOptions: &solr.ServiceOptions{
					Annotations: testCommonServiceAnnotations,
					Labels:      testCommonServiceLabels,
				},
				HeadlessServiceOptions: &solr.ServiceOptions{
					Annotations: testHeadlessServiceAnnotations,
					Labels:      testHeadlessServiceLabels,
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

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	newRec, requests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrCloud object and expect the Reconcile and StatefulSet to be created
	err = testClient.Create(context.TODO(), instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	// Add an additional check for reconcile, so that the services will have IP addresses for the hostAliases to use
	// Otherwise the reconciler will have 'blockReconciliationOfStatefulSet' set to true, and the stateful set will not be created
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the configMap
	configMap := expectConfigMap(t, g, requests, expectedCloudRequest, cloudCMKey, map[string]string{})
	testMapsEqual(t, "configMap labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testConfigMapLabels), configMap.Labels)
	testMapsEqual(t, "configMap annotations", testConfigMapAnnotations, configMap.Annotations)

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)
	assert.EqualValues(t, replicas, *statefulSet.Spec.Replicas, "Solr StatefulSet has incorrect number of replicas.")

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "host:7271/",
		"SOLR_HOST":      "$(POD_HOSTNAME).foo-clo-solrcloud-headless.default",
		"SOLR_PORT":      "8983",
		"SOLR_NODE_PORT": "8983",
		"GC_TUNE":        "gc Options",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT)",
		"SOLR_STOP_WAIT": strconv.FormatInt(testTerminationGracePeriodSeconds-5, 10),
	}
	expectedStatefulSetLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"technology": "solr-cloud"})
	expectedStatefulSetAnnotations := map[string]string{util.SolrZKConnectionStringAnnotation: "host:7271/"}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	testMapsEqual(t, "statefulSet labels", util.MergeLabelsOrAnnotations(expectedStatefulSetLabels, testSSLabels), statefulSet.Labels)
	testMapsEqual(t, "statefulSet annotations", util.MergeLabelsOrAnnotations(expectedStatefulSetAnnotations, testSSAnnotations), statefulSet.Annotations)
	testMapsEqual(t, "pod labels", util.MergeLabelsOrAnnotations(expectedStatefulSetLabels, testPodLabels), statefulSet.Spec.Template.ObjectMeta.Labels)
	if assert.True(t, metav1.HasAnnotation(statefulSet.Spec.Template.ObjectMeta, util.SolrScheduledRestartAnnotation), "Pod Template does not have scheduled restart annotation when it should") {
		// Remove the annotation when we know that it exists, we don't know the exact value so we can't check it below.
		delete(statefulSet.Spec.Template.Annotations, util.SolrScheduledRestartAnnotation)
	}
	testMapsEqual(t, "pod annotations", util.MergeLabelsOrAnnotations(map[string]string{"solr.apache.org/solrXmlMd5": fmt.Sprintf("%x", md5.Sum([]byte(configMap.Data["solr.xml"])))}, testPodAnnotations), statefulSet.Spec.Template.Annotations)
	testMapsEqual(t, "pod node selectors", testNodeSelectors, statefulSet.Spec.Template.Spec.NodeSelector)
	testPodProbe(t, testProbeLivenessNonDefaults, statefulSet.Spec.Template.Spec.Containers[0].LivenessProbe, "liveness")
	testPodProbe(t, testProbeReadinessNonDefaults, statefulSet.Spec.Template.Spec.Containers[0].ReadinessProbe, "readiness")
	testPodProbe(t, testProbeStartup, statefulSet.Spec.Template.Spec.Containers[0].StartupProbe, "startup")
	assert.Equal(t, []string{"solr", "stop", "-p", "8983"}, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "Incorrect pre-stop command")
	testPodTolerations(t, testTolerations, statefulSet.Spec.Template.Spec.Tolerations)
	assert.EqualValues(t, testPriorityClass, statefulSet.Spec.Template.Spec.PriorityClassName, "Incorrect Priority class name for Pod Spec")
	assert.ElementsMatch(t, append(testAdditionalImagePullSecrets, corev1.LocalObjectReference{Name: testImagePullSecretName}), statefulSet.Spec.Template.Spec.ImagePullSecrets, "Incorrect imagePullSecrets")
	assert.EqualValues(t, &testTerminationGracePeriodSeconds, statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds, "Incorrect terminationGracePeriodSeconds")
	assert.EqualValues(t, testServiceAccountName, statefulSet.Spec.Template.Spec.ServiceAccountName, "Incorrect serviceAccountName")

	// Check the update strategy
	assert.EqualValues(t, appsv1.RollingUpdateStatefulSetStrategyType, statefulSet.Spec.UpdateStrategy.Type, "Incorrect statefulset update strategy")
	assert.EqualValues(t, appsv1.OrderedReadyPodManagement, statefulSet.Spec.PodManagementPolicy, "Incorrect statefulset pod management policy")

	// Check the client Service
	service := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Selector.MatchLabels)
	expectedCommonServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "common"})
	testMapsEqual(t, "common service labels", util.MergeLabelsOrAnnotations(expectedCommonServiceLabels, testCommonServiceLabels), service.Labels)
	testMapsEqual(t, "common service annotations", testCommonServiceAnnotations, service.Annotations)

	// Check that the headless Service does not exist
	headlessService := expectService(t, g, requests, expectedCloudRequest, cloudHsKey, statefulSet.Spec.Selector.MatchLabels)
	expectedHeadlessServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "headless"})
	testMapsEqual(t, "common service labels", util.MergeLabelsOrAnnotations(expectedHeadlessServiceLabels, testHeadlessServiceLabels), headlessService.Labels)
	testMapsEqual(t, "common service annotations", testHeadlessServiceAnnotations, headlessService.Annotations)
}

func TestCloudWithExternalZookeeperChroot(t *testing.T) {
	UseZkCRD(true)
	g := gomega.NewGomegaWithT(t)
	connString := "host:7271,host2:7271"
	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					ChRoot:                   "a-ch/root",
					ExternalConnectionString: &connString,
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	newRec, requests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrCloud object and expect the Reconcile and StatefulSet to be created
	err = testClient.Create(context.TODO(), instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	// Add an additional check for reconcile, so that the zkCluster will have been created
	// Otherwise the reconciler will have 'blockReconciliationOfStatefulSet' set to true, and the stateful set will not be created
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())

	// Check that the ZkConnectionInformation is correct
	assert.Equal(t, "host:7271,host2:7271", instance.Status.ZookeeperConnectionInfo.InternalConnectionString, "Wrong internal zkConnectionString in status")
	assert.Equal(t, "host:7271,host2:7271", *instance.Status.ZookeeperConnectionInfo.ExternalConnectionString, "Wrong external zkConnectionString in status")
	assert.Equal(t, "/a-ch/root", instance.Status.ZookeeperConnectionInfo.ChRoot, "Wrong zk chRoot in status")
	assert.Equal(t, "host:7271,host2:7271/a-ch/root", instance.Status.ZookeeperConnectionInfo.ZkConnectionString(), "Wrong zkConnectionString())")

	// Check that the statefulSet has been created, using the given chRoot
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")
	expectedZKHost := "host:7271,host2:7271/a-ch/root"
	expectedEnvVars := map[string]string{
		"ZK_HOST":   expectedZKHost,
		"ZK_SERVER": "host:7271,host2:7271",
		"ZK_CHROOT": "/a-ch/root",
		"SOLR_HOST": "$(POD_HOSTNAME)." + cloudHsKey.Name + "." + cloudHsKey.Namespace,
		"SOLR_PORT": "8983",
		"GC_TUNE":   "",
	}
	expectedStatefulSetAnnotations := map[string]string{util.SolrZKConnectionStringAnnotation: expectedZKHost}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	testMapsEqual(t, "statefulSet annotations", expectedStatefulSetAnnotations, statefulSet.Annotations)
	assert.EqualValues(t, []string{"sh", "-c", "solr zk ls ${ZK_CHROOT} -z ${ZK_SERVER} || solr zk mkroot ${ZK_CHROOT} -z ${ZK_SERVER}"}, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PostStart.Exec.Command, "Incorrect post-start command")
	assert.Empty(t, statefulSet.Spec.Template.Spec.ServiceAccountName, "No custom serviceAccountName specified, so the field should be empty.")
}

func TestDefaults(t *testing.T) {
	UseZkCRD(true)
	g := gomega.NewGomegaWithT(t)
	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			ZookeeperRef: &solr.ZookeeperRef{
				ProvidedZookeeper: &solr.ZookeeperSpec{
					Persistence: &zookeeperv1beta1.Persistence{},
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	newRec, requests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrCloud object and expect the Reconcile and StatefulSet to be created
	err = testClient.Create(context.TODO(), instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())

	// Solr defaults
	assert.EqualValues(t, solr.DefaultSolrReplicas, *instance.Spec.Replicas, "Bad Default - Spec.Replicas")
	assert.NotNil(t, instance.Spec.SolrImage, "Bad Default - instance.Spec.SolrImage")
	assert.Equal(t, solr.DefaultSolrRepo, instance.Spec.SolrImage.Repository, "Bad Default - instance.Spec.SolrImage.Repository")
	assert.Equal(t, solr.DefaultSolrVersion, instance.Spec.SolrImage.Tag, "Bad Default - instance.Spec.SolrImage.Tag")
	assert.NotNil(t, instance.Spec.BusyBoxImage, "Bad Default - instance.Spec.BusyBoxImage")
	assert.Equal(t, solr.DefaultBusyBoxImageRepo, instance.Spec.BusyBoxImage.Repository, "Bad Default - instance.Spec.BusyBoxImage.Repository")
	assert.Equal(t, solr.DefaultBusyBoxImageVersion, instance.Spec.BusyBoxImage.Tag, "Bad Default - instance.Spec.BusyBoxImage.Tag")
	assert.Equal(t, 80, instance.Spec.SolrAddressability.CommonServicePort, "Bad Default - instance.Spec.SolrAddressability.CommonServicePort")
	assert.Equal(t, 8983, instance.Spec.SolrAddressability.PodPort, "Bad Default - instance.Spec.SolrAddressability.PodPort")
	assert.Nil(t, instance.Spec.SolrAddressability.External, "Bad Default - instance.Spec.SolrAddressability.External")

	// Check the default Zookeeper
	assert.Equal(t, solr.DefaultZkReplicas, *instance.Spec.ZookeeperRef.ProvidedZookeeper.Replicas, "Bad Default - Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Replicas")
	assert.NotNil(t, instance.Spec.ZookeeperRef.ProvidedZookeeper.Image, "Bad Default - instance.Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Image")
	assert.Equal(t, solr.DefaultZkRepo, instance.Spec.ZookeeperRef.ProvidedZookeeper.Image.Repository, "Bad Default - instance.Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Image.Repository")
	assert.Equal(t, solr.DefaultZkVersion, instance.Spec.ZookeeperRef.ProvidedZookeeper.Image.Tag, "Bad Default - instance.Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Image.Tag")
	assert.NotNil(t, instance.Spec.ZookeeperRef.ProvidedZookeeper.Persistence, "Bad Default - instance.Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Persistence")
	assert.Equal(t, solr.DefaultZkVolumeReclaimPolicy, instance.Spec.ZookeeperRef.ProvidedZookeeper.Persistence.VolumeReclaimPolicy, "Bad Default - instance.Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Persistence.VolumeReclaimPolicy")
	assert.Equal(t, solr.DefaultZkVolumeReclaimPolicy, instance.Spec.ZookeeperRef.ProvidedZookeeper.Persistence.VolumeReclaimPolicy, "Bad Default - instance.Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Persistence.VolumeReclaimPolicy")
	assert.NotNil(t, instance.Spec.ZookeeperRef.ProvidedZookeeper.Persistence.PersistentVolumeClaimSpec, "Bad Default - instance.Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Persistence.PersistentVolumeClaimSpec")
	assert.Equal(t, 1, len(instance.Spec.ZookeeperRef.ProvidedZookeeper.Persistence.PersistentVolumeClaimSpec.Resources.Requests), "Bad Default - Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Persistence.PersistentVolumeClaimSpec.Resources length")
	assert.Equal(t, 1, len(instance.Spec.ZookeeperRef.ProvidedZookeeper.Persistence.PersistentVolumeClaimSpec.AccessModes), "Bad Default - Spec.ZookeeperRef.ProvidedZookeeper.Zookeeper.Persistence.PersistentVolumeClaimSpec.AccesModes length")

	// verify the ConfigMap created and the annotation with the checksum is set
	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), cloudSsKey, stateful) }, timeout).Should(gomega.Succeed())

	assert.NotNil(t, stateful.Spec.Template.Spec.Volumes)
	var solrXmlVol *corev1.Volume = nil
	for _, vol := range stateful.Spec.Template.Spec.Volumes {
		if vol.Name == "solr-xml" {
			solrXmlVol = &vol
			break
		}
	}
	assert.True(t, solrXmlVol != nil, "Didn't find custom solr.xml volume in sts config!")
	assert.True(t, solrXmlVol.VolumeSource.ConfigMap != nil, "solr-xml Volume should have a ConfigMap source")
	assert.Equal(t, solrXmlVol.VolumeSource.ConfigMap.Name, instance.ConfigMapName(), "solr-xml Volume should have a ConfigMap source")

	foundConfigMap := &corev1.ConfigMap{}
	err = testClient.Get(context.TODO(), types.NamespacedName{Name: instance.ConfigMapName(), Namespace: instance.Namespace}, foundConfigMap)
	solrXmlMd5 := fmt.Sprintf("%x", md5.Sum([]byte(foundConfigMap.Data[util.SolrXmlFile])))
	assert.True(t, stateful.Spec.Template.Annotations[util.SolrXmlMd5Annotation] == solrXmlMd5,
		"solr.xml MD5 annotation should be set on the pod template!")
}

func TestExternalKubeDomainCloudReconcile(t *testing.T) {
	UseZkCRD(false)
	g := gomega.NewGomegaWithT(t)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
				},
			},
			SolrAddressability: solr.SolrAddressabilityOptions{
				PodPort:           2000,
				CommonServicePort: 5000,
				KubeDomain:        testKubeDomain,
			},
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				CommonServiceOptions: &solr.ServiceOptions{
					Annotations: testCommonServiceAnnotations,
					Labels:      testCommonServiceLabels,
				},
				HeadlessServiceOptions: &solr.ServiceOptions{
					Annotations: testHeadlessServiceAnnotations,
					Labels:      testHeadlessServiceLabels,
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	newRec, requests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrCloud object and expect the Reconcile and StatefulSet to be created
	err = testClient.Create(context.TODO(), instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "host:7271/",
		"SOLR_HOST":      "$(POD_HOSTNAME)." + cloudHsKey.Name + "." + instance.Namespace + ".svc." + testKubeDomain,
		"SOLR_PORT":      "2000",
		"SOLR_NODE_PORT": "2000",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT)",
	}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	assert.Nil(t, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PostStart, "Post-start command should be nil since there is no chRoot to ensure exists.")

	// Check the client Service
	service := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "common service annotations", testCommonServiceAnnotations, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 5000, service.Spec.Ports[0].Port, "Wrong port on common Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on common Service")

	// Check the headless Service
	service = expectService(t, g, requests, expectedCloudRequest, cloudHsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "headless service annotations", testHeadlessServiceAnnotations, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 2000, service.Spec.Ports[0].Port, "Wrong port on headless Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on headless Service")

	// Make sure individual Node services don't exist
	nodeNames := instance.GetAllSolrNodeNames()
	for _, nodeName := range nodeNames {
		nodeSKey := types.NamespacedName{Name: nodeName, Namespace: "default"}
		expectNoService(g, nodeSKey, "Node service shouldn't exist, but it does.")
	}

	// Check the ingress
	expectNoIngress(g, cloudIKey)

	// Check that the Addresses in the status are correct
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	assert.Equal(t, "http://"+cloudCsKey.Name+"."+instance.Namespace+".svc."+testKubeDomain+":5000", instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	assert.Nil(t, instance.Status.ExternalCommonAddress, "External common address in status should be nil")
}

func TestCloudWithCustomSolrXmlConfigMapReconcile(t *testing.T) {
	UseZkCRD(true)
	g := gomega.NewGomegaWithT(t)

	testCustomSolrXmlConfigMap := "my-custom-solr-xml"

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				ConfigMapOptions: &solr.ConfigMapOptions{
					ProvidedConfigMap: testCustomSolrXmlConfigMap,
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	newRec, requests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrCloud object and expect the Reconcile and StatefulSet to be created
	err = testClient.Create(context.TODO(), instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)

	// reconcile will have failed b/c the provided ConfigMap doesn't exist ...
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// start with an invalid provided ConfigMap
	invalidConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCustomSolrXmlConfigMap,
			Namespace: instance.Namespace,
		},
		Data: map[string]string{
			"foo": "bar",
		},
	}
	err = testClient.Create(context.TODO(), invalidConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	invalidConfigMap.Data[util.SolrXmlFile] = "Invalid XML"
	err = testClient.Update(context.TODO(), invalidConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// update with a valid custom config map so reconcile can proceed
	providedConfigMap := util.GenerateConfigMap(instance)
	providedConfigMap.Name = testCustomSolrXmlConfigMap
	err = testClient.Update(context.TODO(), providedConfigMap)

	defer testClient.Delete(context.TODO(), providedConfigMap)

	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	emptyRequests(requests)

	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), cloudSsKey, stateful) }, timeout).Should(gomega.Succeed())

	assert.NotEmpty(t, stateful.Spec.Template.Annotations[util.SolrXmlMd5Annotation], "Custom solr.xml MD5 annotation should be set on the pod template!")

	assert.NotNil(t, stateful.Spec.Template.Spec.Volumes)
	var solrXmlVol *corev1.Volume = nil
	for _, vol := range stateful.Spec.Template.Spec.Volumes {
		if vol.Name == "solr-xml" {
			solrXmlVol = &vol
			break
		}
	}
	assert.NotNil(t, solrXmlVol, "Didn't find custom solr.xml volume in sts config!")
	assert.NotNil(t, solrXmlVol.VolumeSource.ConfigMap, "solr-xml Volume should have a ConfigMap source")
	assert.Equal(t, solrXmlVol.VolumeSource.ConfigMap.Name, testCustomSolrXmlConfigMap, "solr-xml Volume should have a ConfigMap source")

	notExistName := fmt.Sprintf("%s-solrcloud-configmap", instance.GetName())
	foundConfigMap := &corev1.ConfigMap{}
	err = testClient.Get(context.TODO(), types.NamespacedName{Name: notExistName, Namespace: instance.Namespace}, foundConfigMap)
	g.Expect(err).To(gomega.HaveOccurred(), "Built-in solr.xml ConfigMap should not exist! "+notExistName)

	emptyRequests(requests)
	// update the embedded solr.xml to trigger a pod rolling restart
	foundConfigMap = &corev1.ConfigMap{}
	err = testClient.Get(context.TODO(), types.NamespacedName{Name: testCustomSolrXmlConfigMap, Namespace: instance.Namespace}, foundConfigMap)
	updateSolrXml := foundConfigMap.Data[util.SolrXmlFile]
	foundConfigMap.Data[util.SolrXmlFile] = strings.Replace(updateSolrXml, "${zkClientTimeout:30000}", "${zkClientTimeout:15000}", 1)
	err = testClient.Update(context.TODO(), foundConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the annotation on the pod template to make sure a rolling restart will take place
	updateSolrXml = foundConfigMap.Data[util.SolrXmlFile]
	updateSolrXmlMd5 := fmt.Sprintf("%x", md5.Sum([]byte(updateSolrXml)))
	time.Sleep(time.Millisecond * 250)
	err = testClient.Get(context.TODO(), cloudSsKey, stateful)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	assert.Equal(t, updateSolrXmlMd5, stateful.Spec.Template.Annotations[util.SolrXmlMd5Annotation], "Custom solr.xml MD5 annotation should be updated on the pod template.")
}

func TestCloudWithUserProvidedLogConfigMapReconcile(t *testing.T) {
	UseZkCRD(true)
	g := gomega.NewGomegaWithT(t)

	testCustomLogXmlConfigMap := "my-custom-log4j2-xml"

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				ConfigMapOptions: &solr.ConfigMapOptions{
					ProvidedConfigMap: testCustomLogXmlConfigMap,
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	newRec, requests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the SolrCloud object and expect the Reconcile and StatefulSet to be created
	err = testClient.Create(context.TODO(), instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)

	// reconcile will have failed b/c the provided ConfigMap doesn't exist ...
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	userProvidedConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCustomLogXmlConfigMap,
			Namespace: instance.Namespace,
		},
		Data: map[string]string{
			util.LogXmlFile: "<Configuration/>",
		},
	}
	err = testClient.Create(context.TODO(), userProvidedConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), userProvidedConfigMap)

	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	emptyRequests(requests)

	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), cloudSsKey, stateful) }, timeout).Should(gomega.Succeed())
	assert.NotEmpty(t, stateful.Spec.Template.Annotations[util.LogXmlMd5Annotation], "Custom log XML MD5 annotation should be set on the pod template!")

	assert.NotNil(t, stateful.Spec.Template.Spec.Volumes)
	var logXmlVol *corev1.Volume = nil
	for _, vol := range stateful.Spec.Template.Spec.Volumes {
		if vol.Name == "log4j2-xml" {
			logXmlVol = &vol
			break
		}
	}
	assert.NotNil(t, logXmlVol, "Didn't find custom log4j2-xml volume in sts config!")
	assert.NotNil(t, logXmlVol.VolumeSource.ConfigMap, "log4j2-xml Volume should have a ConfigMap source")
	assert.Equal(t, logXmlVol.VolumeSource.ConfigMap.Name, testCustomLogXmlConfigMap, "log4j2-xml Volume should have a ConfigMap source")

	var logXmlVolMount *corev1.VolumeMount = nil
	for _, mount := range stateful.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == "log4j2-xml" {
			logXmlVolMount = &mount
			break
		}
	}
	assert.NotNil(t, logXmlVolMount, "Didn't find the log4j2-xml Volume mount")
	expectedMountPath := fmt.Sprintf("/var/solr/%s", testCustomLogXmlConfigMap)
	assert.Equal(t, expectedMountPath, logXmlVolMount.MountPath, "Custom log4j2.xml mount path is incorrect.")

	solrXmlConfigName := fmt.Sprintf("%s-solrcloud-configmap", instance.GetName())
	foundConfigMap := &corev1.ConfigMap{}
	err = testClient.Get(context.TODO(), types.NamespacedName{Name: solrXmlConfigName, Namespace: instance.Namespace}, foundConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "Built-in solr.xml ConfigMap should still exist! ")

	emptyRequests(requests)
	// update the user-provided log XML to trigger a pod rolling restart
	updatedXml := "<Configuration>Updated!</Configuration>"
	foundConfigMap = &corev1.ConfigMap{}
	err = testClient.Get(context.TODO(), types.NamespacedName{Name: testCustomLogXmlConfigMap, Namespace: instance.Namespace}, foundConfigMap)
	foundConfigMap.Data[util.LogXmlFile] = updatedXml
	err = testClient.Update(context.TODO(), foundConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	// capture all reconcile requests
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the annotation on the pod template to make sure a rolling restart will take place
	updateLogXmlMd5 := fmt.Sprintf("%x", md5.Sum([]byte(updatedXml)))
	time.Sleep(time.Millisecond * 250)
	err = testClient.Get(context.TODO(), cloudSsKey, stateful)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	assert.Equal(t, updateLogXmlMd5, stateful.Spec.Template.Annotations[util.LogXmlMd5Annotation], "Custom log XML MD5 annotation should be updated on the pod template.")
	expectedEnvVars := map[string]string{"LOG4J_PROPS": fmt.Sprintf("%s/%s", expectedMountPath, util.LogXmlFile)}
	testPodEnvVariables(t, expectedEnvVars, stateful.Spec.Template.Spec.Containers[0].Env)

	// switch the log4j config over to using monitorInterval, so no more rolling restart on change to config
	emptyRequests(requests)
	// update the user-provided log XML to trigger a pod rolling restart
	updatedXml = "<Configuration monitorInterval=\"30\">Updated!</Configuration>"
	foundConfigMap = &corev1.ConfigMap{}
	err = testClient.Get(context.TODO(), types.NamespacedName{Name: testCustomLogXmlConfigMap, Namespace: instance.Namespace}, foundConfigMap)
	foundConfigMap.Data[util.LogXmlFile] = updatedXml
	err = testClient.Update(context.TODO(), foundConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	time.Sleep(time.Millisecond * 250)
	err = testClient.Get(context.TODO(), cloudSsKey, stateful)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	// log xml md5 annotation should no longer be on the pod template
	assert.Equal(t, "", stateful.Spec.Template.Annotations[util.LogXmlMd5Annotation], "Custom log XML MD5 annotation should not be set on the pod template.")
}

func TestCloudWithUserProvidedSolrXmlAndLogConfigReconcile(t *testing.T) {
	UseZkCRD(true)
	g := gomega.NewGomegaWithT(t)

	testCustomConfigMap := "my-custom-config-xml"

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				ConfigMapOptions: &solr.ConfigMapOptions{
					ProvidedConfigMap: testCustomConfigMap,
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(testCfg, manager.Options{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	testClient = mgr.GetClient()

	solrCloudReconciler := &SolrCloudReconciler{
		Client: testClient,
		Log:    ctrl.Log.WithName("controllers").WithName("SolrCloud"),
	}
	newRec, requests := SetupTestReconcile(solrCloudReconciler)
	g.Expect(solrCloudReconciler.SetupWithManagerAndReconciler(mgr, newRec)).NotTo(gomega.HaveOccurred())

	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	cleanupTest(g, instance.Namespace)

	// Create the user-provided ConfigMap first to streamline reconcile,
	// other tests cover creating it after the reconcile loop starts on the SolrCloud
	userProvidedConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCustomConfigMap,
			Namespace: instance.Namespace,
		},
		Data: map[string]string{
			util.LogXmlFile:  "<Configuration/>",
			util.SolrXmlFile: "<solr> ${hostPort: </solr>", // the controller checks for ${hostPort: in the solr.xml
		},
	}
	err = testClient.Create(context.TODO(), userProvidedConfigMap)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), userProvidedConfigMap)

	// Create the SolrCloud object and expect the Reconcile and StatefulSet to be created
	err = testClient.Create(context.TODO(), instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(context.TODO(), instance)

	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	emptyRequests(requests)

	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), cloudSsKey, stateful) }, timeout).Should(gomega.Succeed())
	assert.NotEmpty(t, stateful.Spec.Template.Annotations[util.LogXmlMd5Annotation], "Custom log XML MD5 annotation should be set on the pod template!")

	assert.NotNil(t, stateful.Spec.Template.Spec.Volumes)
	var customConfigVol *corev1.Volume = nil
	for _, vol := range stateful.Spec.Template.Spec.Volumes {
		if vol.Name == "solr-xml" {
			customConfigVol = &vol
			break
		}
	}
	assert.NotNil(t, customConfigVol, "Didn't find custom solr-xml volume in sts config!")
	assert.NotNil(t, customConfigVol.VolumeSource.ConfigMap, "solr-xml Volume should have a ConfigMap source")
	assert.Equal(t, customConfigVol.VolumeSource.ConfigMap.Name, testCustomConfigMap, "solr-xml Volume should have a ConfigMap source")

	var logXmlVolMount *corev1.VolumeMount = nil
	for _, mount := range stateful.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == "solr-xml" {
			logXmlVolMount = &mount
			break
		}
	}
	assert.NotNil(t, logXmlVolMount, "Didn't find the log4j2-xml Volume mount")
	expectedMountPath := fmt.Sprintf("/var/solr/%s", testCustomConfigMap)
	assert.Equal(t, expectedMountPath, logXmlVolMount.MountPath)

	solrXmlConfigName := fmt.Sprintf("%s-solrcloud-configmap", instance.GetName())
	foundConfigMap := &corev1.ConfigMap{}
	err = testClient.Get(context.TODO(), types.NamespacedName{Name: solrXmlConfigName, Namespace: instance.Namespace}, foundConfigMap)
	g.Expect(err).To(gomega.HaveOccurred(), "Built-in solr.xml ConfigMap should NOT exist! ")

	emptyRequests(requests)

	customSolrXml := userProvidedConfigMap.Data[util.SolrXmlFile]
	expectedSolrXmlMd5 := fmt.Sprintf("%x", md5.Sum([]byte(customSolrXml)))
	assert.Equal(t, expectedSolrXmlMd5, stateful.Spec.Template.Annotations[util.SolrXmlMd5Annotation], "Custom solr.xml MD5 annotation should be set on the pod template.")

	customLogXml := userProvidedConfigMap.Data[util.LogXmlFile]
	expectedLogXmlMd5 := fmt.Sprintf("%x", md5.Sum([]byte(customLogXml)))
	assert.Equal(t, expectedLogXmlMd5, stateful.Spec.Template.Annotations[util.LogXmlMd5Annotation], "Custom log4j2.xml MD5 annotation should be set on the pod template.")
	expectedEnvVars := map[string]string{"LOG4J_PROPS": fmt.Sprintf("%s/%s", expectedMountPath, util.LogXmlFile)}
	testPodEnvVariables(t, expectedEnvVars, stateful.Spec.Template.Spec.Containers[0].Env)
}
