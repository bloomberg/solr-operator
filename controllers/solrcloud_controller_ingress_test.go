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
	extv1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"strconv"
	"testing"

	"github.com/apache/solr-operator/controllers/util"
	"github.com/stretchr/testify/assert"

	solr "github.com/apache/solr-operator/api/v1beta1"
	"github.com/onsi/gomega"
	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ reconcile.Reconciler = &SolrCloudReconciler{}

func TestIngressCloudReconcile(t *testing.T) {
	UseZkCRD(false)
	g := gomega.NewGomegaWithT(t)

	replicas := int32(4)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			Replicas: &replicas,
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
				},
			},
			SolrAddressability: solr.SolrAddressabilityOptions{
				External: &solr.ExternalAddressability{
					Method:             solr.Ingress,
					UseExternalAddress: true,
					DomainName:         testDomain,
					NodePortOverride:   100,
				},
				PodPort:           3000,
				CommonServicePort: 4000,
			},
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				CommonServiceOptions: &solr.ServiceOptions{
					Annotations: testCommonServiceAnnotations,
					Labels:      testCommonServiceLabels,
				},
				IngressOptions: &solr.IngressOptions{
					Annotations: testIngressAnnotations,
					Labels:      testIngressLabels,
				},
				NodeServiceOptions: &solr.ServiceOptions{
					Annotations: testNodeServiceAnnotations,
					Labels:      testNodeServiceLabels,
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
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")

	// Host Alias Tests
	assert.EqualValues(t, replicas, len(statefulSet.Spec.Template.Spec.HostAliases), "Since external address is used for advertising, host aliases should be used for every node.")
	for i, hostAlias := range statefulSet.Spec.Template.Spec.HostAliases {
		assert.EqualValues(t, cloudSsKey.Namespace+"-"+statefulSet.Name+"-"+strconv.Itoa(i)+"."+testDomain, hostAlias.Hostnames[0], "Since external address is used for advertising, host aliases should be used for every node.")
	}

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "host:7271/",
		"SOLR_HOST":      instance.Namespace + "-$(POD_HOSTNAME)." + testDomain,
		"SOLR_PORT":      "3000",
		"SOLR_NODE_PORT": "100",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT)",
	}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, []string{"solr", "stop", "-p", "3000"}, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "Incorrect pre-stop command")

	// Check the client Service
	service := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "common service annotations", testCommonServiceAnnotations, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 4000, service.Spec.Ports[0].Port, "Wrong port on common Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on common Service")

	// Check that the headless Service does not exist
	expectNoService(g, cloudHsKey, "Headless service shouldn't exist, but it does.")

	// Check on individual node services
	nodeNames := instance.GetAllSolrNodeNames()
	assert.EqualValues(t, replicas, len(nodeNames), "SolrCloud has incorrect number of nodeNames.")
	for _, nodeName := range nodeNames {
		nodeSKey := types.NamespacedName{Name: nodeName, Namespace: "default"}
		service := expectService(t, g, requests, expectedCloudRequest, nodeSKey, util.MergeLabelsOrAnnotations(statefulSet.Spec.Selector.MatchLabels, map[string]string{"statefulset.kubernetes.io/pod-name": nodeName}))
		expectedNodeServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "external"})
		testMapsEqual(t, "node '"+nodeName+"' service labels", util.MergeLabelsOrAnnotations(expectedNodeServiceLabels, testNodeServiceLabels), service.Labels)
		testMapsEqual(t, "node '"+nodeName+"' service annotations", testNodeServiceAnnotations, service.Annotations)
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
		assert.EqualValues(t, 100, service.Spec.Ports[0].Port, "Wrong port on node Service")
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on node Service")
	}

	// Check the ingress
	ingress := expectIngress(g, requests, expectedCloudRequest, cloudIKey)
	testMapsEqual(t, "ingress labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testIngressLabels), ingress.Labels)
	testMapsEqual(t, "ingress annotations", testIngressAnnotations, ingress.Annotations)
	testIngressRules(t, ingress, true, int(replicas), []string{testDomain}, 4000, 100)

	// Check that the Addresses in the status are correct
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	assert.Equal(t, "http://"+cloudCsKey.Name+"."+instance.Namespace+":4000", instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	assert.NotNil(t, instance.Status.ExternalCommonAddress, "External common address in Status should not be nil.")
	assert.EqualValues(t, "http://"+instance.Namespace+"-"+instance.Name+"-solrcloud"+"."+testDomain, *instance.Status.ExternalCommonAddress, "Wrong external common address in status")
}

func TestIngressNoNodesCloudReconcile(t *testing.T) {
	UseZkCRD(false)
	g := gomega.NewGomegaWithT(t)

	replicas := int32(4)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			Replicas: &replicas,
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
				},
			},
			SolrAddressability: solr.SolrAddressabilityOptions{
				External: &solr.ExternalAddressability{
					Method:             solr.Ingress,
					UseExternalAddress: true,
					HideNodes:          true,
					DomainName:         testDomain,
					NodePortOverride:   100,
				},
				PodPort:           3000,
				CommonServicePort: 4000,
			},
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				CommonServiceOptions: &solr.ServiceOptions{
					Annotations: testCommonServiceAnnotations,
					Labels:      testCommonServiceLabels,
				},
				IngressOptions: &solr.IngressOptions{
					Annotations: testIngressAnnotations,
					Labels:      testIngressLabels,
				},
				NodeServiceOptions: &solr.ServiceOptions{
					Annotations: testNodeServiceAnnotations,
					Labels:      testNodeServiceLabels,
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
	// The default value of UseExternalAddress needs to be set to False
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")

	// Host Alias Tests
	assert.Nil(t, statefulSet.Spec.Template.Spec.HostAliases, "Since external address is not used, there is no need for host aliases.")

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "host:7271/",
		"SOLR_HOST":      "$(POD_HOSTNAME)." + cloudHsKey.Name + "." + cloudHsKey.Namespace,
		"SOLR_PORT":      "3000",
		"SOLR_NODE_PORT": "3000",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT)",
	}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, []string{"solr", "stop", "-p", "3000"}, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "Incorrect pre-stop command")

	// Check the client Service
	service := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "common service annotations", testCommonServiceAnnotations, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 4000, service.Spec.Ports[0].Port, "Wrong port on common Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on common Service")

	// Check the headless Service
	service = expectService(t, g, requests, expectedCloudRequest, cloudHsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "headless service annotations", nil, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 3000, service.Spec.Ports[0].Port, "Wrong port on headless Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on headless Service")

	// Make sure individual Node services don't exist
	nodeNames := instance.GetAllSolrNodeNames()
	assert.EqualValues(t, replicas, len(nodeNames), "SolrCloud has incorrect number of nodeNames.")
	for _, nodeName := range nodeNames {
		nodeSKey := types.NamespacedName{Name: nodeName, Namespace: "default"}
		expectNoService(g, nodeSKey, "Node service shouldn't exist, but it does.")
	}

	// Check the ingress
	ingress := expectIngress(g, requests, expectedCloudRequest, cloudIKey)
	testMapsEqual(t, "ingress labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testIngressLabels), ingress.Labels)
	testMapsEqual(t, "ingress annotations", testIngressAnnotations, ingress.Annotations)
	testIngressRules(t, ingress, true, 0, []string{testDomain}, 4000, 100)

	// Check that the Addresses in the status are correct
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	assert.Equal(t, "http://"+cloudCsKey.Name+"."+instance.Namespace+":4000", instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	assert.NotNil(t, instance.Status.ExternalCommonAddress, "External common address in Status should not be nil.")
	assert.EqualValues(t, "http://"+instance.Namespace+"-"+instance.Name+"-solrcloud"+"."+testDomain, *instance.Status.ExternalCommonAddress, "Wrong external common address in status")
}

func TestIngressNoCommonCloudReconcile(t *testing.T) {
	UseZkCRD(false)
	g := gomega.NewGomegaWithT(t)

	replicas := int32(4)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			Replicas: &replicas,
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
				},
			},
			SolrAddressability: solr.SolrAddressabilityOptions{
				External: &solr.ExternalAddressability{
					Method:             solr.Ingress,
					UseExternalAddress: true,
					HideCommon:         true,
					DomainName:         testDomain,
					NodePortOverride:   100,
				},
				PodPort:           3000,
				CommonServicePort: 4000,
			},
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				CommonServiceOptions: &solr.ServiceOptions{
					Annotations: testCommonServiceAnnotations,
					Labels:      testCommonServiceLabels,
				},
				IngressOptions: &solr.IngressOptions{
					Annotations: testIngressAnnotations,
					Labels:      testIngressLabels,
				},
				NodeServiceOptions: &solr.ServiceOptions{
					Annotations: testNodeServiceAnnotations,
					Labels:      testNodeServiceLabels,
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
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")

	// Host Alias Tests
	assert.EqualValues(t, replicas, len(statefulSet.Spec.Template.Spec.HostAliases), "Since external address is used for advertising, host aliases should be used for every node.")
	for i, hostAlias := range statefulSet.Spec.Template.Spec.HostAliases {
		assert.EqualValues(t, cloudSsKey.Namespace+"-"+statefulSet.Name+"-"+strconv.Itoa(i)+"."+testDomain, hostAlias.Hostnames[0], "Since external address is used for advertising, host aliases should be used for every node.")
	}

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "host:7271/",
		"SOLR_HOST":      instance.Namespace + "-$(POD_HOSTNAME)." + testDomain,
		"SOLR_PORT":      "3000",
		"SOLR_NODE_PORT": "100",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT)",
	}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, []string{"solr", "stop", "-p", "3000"}, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "Incorrect pre-stop command")

	// Check the client Service
	service := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "common service annotations", testCommonServiceAnnotations, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 4000, service.Spec.Ports[0].Port, "Wrong port on common Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on common Service")

	// Check that the headless Service does not exist
	expectNoService(g, cloudHsKey, "Headless service shouldn't exist, but it does.")

	// Check on individual node services
	nodeNames := instance.GetAllSolrNodeNames()
	assert.EqualValues(t, replicas, len(nodeNames), "SolrCloud has incorrect number of nodeNames.")
	for _, nodeName := range nodeNames {
		nodeSKey := types.NamespacedName{Name: nodeName, Namespace: "default"}
		service := expectService(t, g, requests, expectedCloudRequest, nodeSKey, util.MergeLabelsOrAnnotations(statefulSet.Spec.Selector.MatchLabels, map[string]string{"statefulset.kubernetes.io/pod-name": nodeName}))
		expectedNodeServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "external"})
		testMapsEqual(t, "node '"+nodeName+"' service labels", util.MergeLabelsOrAnnotations(expectedNodeServiceLabels, testNodeServiceLabels), service.Labels)
		testMapsEqual(t, "node '"+nodeName+"' service annotations", testNodeServiceAnnotations, service.Annotations)
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
		assert.EqualValues(t, 100, service.Spec.Ports[0].Port, "Wrong port on node Service")
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on node Service")
	}

	// Check the ingress
	ingress := expectIngress(g, requests, expectedCloudRequest, cloudIKey)
	testMapsEqual(t, "ingress labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testIngressLabels), ingress.Labels)
	testMapsEqual(t, "ingress annotations", testIngressAnnotations, ingress.Annotations)
	testIngressRules(t, ingress, false, int(replicas), []string{testDomain}, 4000, 100)

	// Check that the Addresses in the status are correct
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	assert.Equal(t, "http://"+cloudCsKey.Name+"."+instance.Namespace+":4000", instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	assert.Nil(t, instance.Status.ExternalCommonAddress, "External common address in Status should be nil.")
}

func TestIngressUseInternalAddressCloudReconcile(t *testing.T) {
	UseZkCRD(false)
	g := gomega.NewGomegaWithT(t)

	replicas := int32(4)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			Replicas: &replicas,
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
				},
			},
			SolrAddressability: solr.SolrAddressabilityOptions{
				External: &solr.ExternalAddressability{
					Method:             solr.Ingress,
					UseExternalAddress: false,
					DomainName:         testDomain,
					NodePortOverride:   100,
				},
				PodPort:           3000,
				CommonServicePort: 4000,
			},
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				CommonServiceOptions: &solr.ServiceOptions{
					Annotations: testCommonServiceAnnotations,
					Labels:      testCommonServiceLabels,
				},
				IngressOptions: &solr.IngressOptions{
					Annotations: testIngressAnnotations,
					Labels:      testIngressLabels,
				},
				NodeServiceOptions: &solr.ServiceOptions{
					Annotations: testNodeServiceAnnotations,
					Labels:      testNodeServiceLabels,
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
	// Additional reconcile for defaulting of Spec
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")

	// Host Alias Tests
	assert.Nil(t, statefulSet.Spec.Template.Spec.HostAliases, "Since external address is not used, there is no need for host aliases.")

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "host:7271/",
		"SOLR_HOST":      "$(POD_HOSTNAME)." + expectedCloudRequest.Namespace,
		"SOLR_PORT":      "3000",
		"SOLR_NODE_PORT": "100",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT)",
	}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, []string{"solr", "stop", "-p", "3000"}, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "Incorrect pre-stop command")

	// Check the client Service
	service := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "common service annotations", testCommonServiceAnnotations, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 4000, service.Spec.Ports[0].Port, "Wrong port on common Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on common Service")

	// Check that the headless Service does not exist
	expectNoService(g, cloudHsKey, "Headless service shouldn't exist, but it does.")

	// Check on individual node services
	nodeNames := instance.GetAllSolrNodeNames()
	assert.EqualValues(t, replicas, len(nodeNames), "SolrCloud has incorrect number of nodeNames.")
	for _, nodeName := range nodeNames {
		nodeSKey := types.NamespacedName{Name: nodeName, Namespace: "default"}
		service := expectService(t, g, requests, expectedCloudRequest, nodeSKey, util.MergeLabelsOrAnnotations(statefulSet.Spec.Selector.MatchLabels, map[string]string{"statefulset.kubernetes.io/pod-name": nodeName}))
		expectedNodeServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "external"})
		testMapsEqual(t, "node '"+nodeName+"' service labels", util.MergeLabelsOrAnnotations(expectedNodeServiceLabels, testNodeServiceLabels), service.Labels)
		testMapsEqual(t, "node '"+nodeName+"' service annotations", testNodeServiceAnnotations, service.Annotations)
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
		assert.EqualValues(t, 100, service.Spec.Ports[0].Port, "Wrong port on node Service")
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on node Service")
	}

	// Check the ingress
	ingress := expectIngress(g, requests, expectedCloudRequest, cloudIKey)
	testMapsEqual(t, "ingress labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testIngressLabels), ingress.Labels)
	testMapsEqual(t, "ingress annotations", testIngressAnnotations, ingress.Annotations)
	testIngressRules(t, ingress, true, int(replicas), []string{testDomain}, 4000, 100)

	// Check that the Addresses in the status are correct
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	assert.Equal(t, "http://"+cloudCsKey.Name+"."+instance.Namespace+":4000", instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	assert.NotNil(t, instance.Status.ExternalCommonAddress, "External common address in Status should not be nil.")
	assert.EqualValues(t, "http://"+instance.Namespace+"-"+instance.Name+"-solrcloud"+"."+testDomain, *instance.Status.ExternalCommonAddress, "Wrong external common address in status")
}

func TestIngressExtraDomainsCloudReconcile(t *testing.T) {
	UseZkCRD(false)
	g := gomega.NewGomegaWithT(t)

	replicas := int32(4)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			Replicas: &replicas,
			ZookeeperRef: &solr.ZookeeperRef{
				ConnectionInfo: &solr.ZookeeperConnectionInfo{
					InternalConnectionString: "host:7271",
				},
			},
			SolrAddressability: solr.SolrAddressabilityOptions{
				External: &solr.ExternalAddressability{
					Method:                solr.Ingress,
					UseExternalAddress:    true,
					DomainName:            testDomain,
					AdditionalDomainNames: testAdditionalDomains,
					NodePortOverride:      100,
				},
				PodPort:           3000,
				CommonServicePort: 4000,
			},
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				CommonServiceOptions: &solr.ServiceOptions{
					Annotations: testCommonServiceAnnotations,
					Labels:      testCommonServiceLabels,
				},
				IngressOptions: &solr.IngressOptions{
					Annotations: testIngressAnnotations,
					Labels:      testIngressLabels,
				},
				NodeServiceOptions: &solr.ServiceOptions{
					Annotations: testNodeServiceAnnotations,
					Labels:      testNodeServiceLabels,
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
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")

	// Host Alias Tests
	assert.EqualValues(t, replicas, len(statefulSet.Spec.Template.Spec.HostAliases), "Since external address is used for advertising, host aliases should be used for every node.")
	for i, hostAlias := range statefulSet.Spec.Template.Spec.HostAliases {
		assert.EqualValues(t, cloudSsKey.Namespace+"-"+statefulSet.Name+"-"+strconv.Itoa(i)+"."+testDomain, hostAlias.Hostnames[0], "Since external address is used for advertising, host aliases should be used for every node.")
	}

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "host:7271/",
		"SOLR_HOST":      instance.Namespace + "-$(POD_HOSTNAME)." + testDomain,
		"SOLR_PORT":      "3000",
		"SOLR_NODE_PORT": "100",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT)",
	}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, []string{"solr", "stop", "-p", "3000"}, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "Incorrect pre-stop command")

	// Check the client Service
	service := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "common service annotations", testCommonServiceAnnotations, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 4000, service.Spec.Ports[0].Port, "Wrong port on common Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on common Service")

	// Check that the headless Service does not exist
	expectNoService(g, cloudHsKey, "Headless service shouldn't exist, but it does.")

	// Check on individual node services
	nodeNames := instance.GetAllSolrNodeNames()
	assert.EqualValues(t, replicas, len(nodeNames), "SolrCloud has incorrect number of nodeNames.")
	for _, nodeName := range nodeNames {
		nodeSKey := types.NamespacedName{Name: nodeName, Namespace: "default"}
		service := expectService(t, g, requests, expectedCloudRequest, nodeSKey, util.MergeLabelsOrAnnotations(statefulSet.Spec.Selector.MatchLabels, map[string]string{"statefulset.kubernetes.io/pod-name": nodeName}))
		expectedNodeServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "external"})
		testMapsEqual(t, "node '"+nodeName+"' service labels", util.MergeLabelsOrAnnotations(expectedNodeServiceLabels, testNodeServiceLabels), service.Labels)
		testMapsEqual(t, "node '"+nodeName+"' service annotations", testNodeServiceAnnotations, service.Annotations)
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
		assert.EqualValues(t, 100, service.Spec.Ports[0].Port, "Wrong port on node Service")
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on node Service")
	}

	// Check the ingress
	ingress := expectIngress(g, requests, expectedCloudRequest, cloudIKey)
	testMapsEqual(t, "ingress labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testIngressLabels), ingress.Labels)
	testMapsEqual(t, "ingress annotations", testIngressAnnotations, ingress.Annotations)
	testIngressRules(t, ingress, true, int(replicas), append([]string{testDomain}, testAdditionalDomains...), 4000, 100)

	// Check that the Addresses in the status are correct
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	assert.Equal(t, "http://"+cloudCsKey.Name+"."+instance.Namespace+":4000", instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	assert.NotNil(t, instance.Status.ExternalCommonAddress, "External common address in Status should not be nil.")
	assert.EqualValues(t, "http://"+instance.Namespace+"-"+instance.Name+"-solrcloud"+"."+testDomain, *instance.Status.ExternalCommonAddress, "Wrong external common address in status")
}

func TestIngressKubeDomainCloudReconcile(t *testing.T) {
	UseZkCRD(true)
	g := gomega.NewGomegaWithT(t)

	replicas := int32(4)

	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudRequest.Name, Namespace: expectedCloudRequest.Namespace},
		Spec: solr.SolrCloudSpec{
			Replicas: &replicas,
			SolrAddressability: solr.SolrAddressabilityOptions{
				External: &solr.ExternalAddressability{
					Method:             solr.Ingress,
					UseExternalAddress: false,
					DomainName:         testDomain,
					NodePortOverride:   100,
				},
				PodPort:    3000,
				KubeDomain: testKubeDomain,
			},
			CustomSolrKubeOptions: solr.CustomSolrKubeOptions{
				CommonServiceOptions: &solr.ServiceOptions{
					Annotations: testCommonServiceAnnotations,
					Labels:      testCommonServiceLabels,
				},
				IngressOptions: &solr.IngressOptions{
					Annotations: testIngressAnnotations,
					Labels:      testIngressLabels,
				},
				NodeServiceOptions: &solr.ServiceOptions{
					Annotations: testNodeServiceAnnotations,
					Labels:      testNodeServiceLabels,
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
	// Additional reconcile for defaulting of Spec
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudRequest)))

	// Check the statefulSet
	statefulSet := expectStatefulSet(t, g, requests, expectedCloudRequest, cloudSsKey)

	assert.Equal(t, 1, len(statefulSet.Spec.Template.Spec.Containers), "Solr StatefulSet requires a container.")

	// Host Alias Tests
	assert.Nil(t, statefulSet.Spec.Template.Spec.HostAliases, "Since external address is not used, there is no need for host aliases.")

	// Env Variable Tests
	expectedEnvVars := map[string]string{
		"ZK_HOST":        "foo-clo-solrcloud-zookeeper-0.foo-clo-solrcloud-zookeeper-headless.default.svc." + testKubeDomain + ":2181,foo-clo-solrcloud-zookeeper-1.foo-clo-solrcloud-zookeeper-headless.default.svc." + testKubeDomain + ":2181,foo-clo-solrcloud-zookeeper-2.foo-clo-solrcloud-zookeeper-headless.default.svc." + testKubeDomain + ":2181/",
		"SOLR_HOST":      "$(POD_HOSTNAME)." + expectedCloudRequest.Namespace + ".svc." + testKubeDomain,
		"SOLR_PORT":      "3000",
		"SOLR_NODE_PORT": "100",
		"SOLR_OPTS":      "-DhostPort=$(SOLR_NODE_PORT)",
	}
	testPodEnvVariables(t, expectedEnvVars, statefulSet.Spec.Template.Spec.Containers[0].Env)
	assert.Equal(t, []string{"solr", "stop", "-p", "3000"}, statefulSet.Spec.Template.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "Incorrect pre-stop command")

	// Check the client Service
	service := expectService(t, g, requests, expectedCloudRequest, cloudCsKey, statefulSet.Spec.Template.Labels)
	testMapsEqual(t, "common service annotations", testCommonServiceAnnotations, service.Annotations)
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
	assert.EqualValues(t, 80, service.Spec.Ports[0].Port, "Wrong port on common Service")
	assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on common Service")

	// Check that the headless Service does not exist
	expectNoService(g, cloudHsKey, "Headless service shouldn't exist, but it does.")

	// Check on individual node services
	nodeNames := instance.GetAllSolrNodeNames()
	assert.EqualValues(t, replicas, len(nodeNames), "SolrCloud has incorrect number of nodeNames.")
	for _, nodeName := range nodeNames {
		nodeSKey := types.NamespacedName{Name: nodeName, Namespace: "default"}
		service := expectService(t, g, requests, expectedCloudRequest, nodeSKey, util.MergeLabelsOrAnnotations(statefulSet.Spec.Selector.MatchLabels, map[string]string{"statefulset.kubernetes.io/pod-name": nodeName}))
		expectedNodeServiceLabels := util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), map[string]string{"service-type": "external"})
		testMapsEqual(t, "node '"+nodeName+"' service labels", util.MergeLabelsOrAnnotations(expectedNodeServiceLabels, testNodeServiceLabels), service.Labels)
		testMapsEqual(t, "node '"+nodeName+"' service annotations", testNodeServiceAnnotations, service.Annotations)
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].Name, "Wrong port name on common Service")
		assert.EqualValues(t, 100, service.Spec.Ports[0].Port, "Wrong port on node Service")
		assert.EqualValues(t, "solr-client", service.Spec.Ports[0].TargetPort.StrVal, "Wrong podPort name on node Service")
	}

	// Check the ingress
	ingress := expectIngress(g, requests, expectedCloudRequest, cloudIKey)
	testMapsEqual(t, "ingress labels", util.MergeLabelsOrAnnotations(instance.SharedLabelsWith(instance.Labels), testIngressLabels), ingress.Labels)
	testMapsEqual(t, "ingress annotations", testIngressAnnotations, ingress.Annotations)
	testIngressRules(t, ingress, true, int(replicas), []string{testDomain}, 80, 100)

	// Check that the Addresses in the status are correct
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedCloudRequest.NamespacedName, instance) }, timeout).Should(gomega.Succeed())
	assert.Equal(t, "http://"+cloudCsKey.Name+"."+instance.Namespace+".svc."+testKubeDomain, instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	assert.NotNil(t, instance.Status.ExternalCommonAddress, "External common address in Status should not be nil.")
	assert.EqualValues(t, "http://"+instance.Namespace+"-"+instance.Name+"-solrcloud"+"."+testDomain, *instance.Status.ExternalCommonAddress, "Wrong external common address in status")
}

func testIngressRules(t *testing.T, ingress *extv1.Ingress, withCommon bool, withNodes int, domainNames []string, commonPort int, nodePort int) {
	expected := 0
	if withCommon {
		expected += 1
	}
	if withNodes > 0 {
		expected += withNodes
	}
	perDomain := expected
	numDomains := len(domainNames)
	expected *= numDomains
	assert.EqualValues(t, expected, len(ingress.Spec.Rules), "Wrong number of ingress rules.")
	for i := 0; i < perDomain; i++ {
		// Common Rules
		ruleName := "common"
		hostAppend := ""
		port := commonPort
		serviceSuffix := "common"

		// Node Rules
		if i > 0 || !withCommon {
			nodeNum := strconv.Itoa(i - 1)
			if !withCommon {
				nodeNum = strconv.Itoa(i)
			}
			ruleName = "node-" + nodeNum
			hostAppend = "-" + nodeNum
			serviceSuffix = nodeNum
			port = nodePort
		}
		for j, domainName := range domainNames {
			ruleIndex := j + i*numDomains
			rule := ingress.Spec.Rules[ruleIndex]
			expectedHost := expectedCloudRequest.Namespace + "-" + expectedCloudRequest.Name + "-solrcloud" + hostAppend + "." + domainName
			assert.EqualValues(t, expectedHost, rule.Host, "Wrong host for ingress rule: "+ruleName)
			assert.EqualValues(t, 1, len(rule.HTTP.Paths), "Wrong number of path rules in ingress host: "+ruleName)
			path := rule.HTTP.Paths[0]
			assert.EqualValues(t, "", path.Path, "There should be no path value for ingress rule: "+ruleName)
			assert.EqualValues(t, expectedCloudRequest.Name+"-solrcloud-"+serviceSuffix, path.Backend.ServiceName, "Wrong service name for ingress rule: "+ruleName)
			assert.EqualValues(t, port, path.Backend.ServicePort.IntVal, "Wrong port name for ingress rule: "+ruleName)
		}
	}
}
