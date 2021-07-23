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
	b64 "encoding/base64"
	"fmt"
	"github.com/apache/solr-operator/controllers/util"
	zkv1beta1 "github.com/pravega/zookeeper-operator/pkg/apis/zookeeper/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"testing"

	solr "github.com/apache/solr-operator/api/v1beta1"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	extv1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func emptyRequests(requests chan reconcile.Request) {
	for len(requests) > 0 {
		<-requests
	}
}

func expectStatefulSet(t *testing.T, g *gomega.GomegaWithT, requests chan reconcile.Request, expectedRequest reconcile.Request, statefulSetKey types.NamespacedName) *appsv1.StatefulSet {
	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), statefulSetKey, stateful) }, timeout).
		Should(gomega.Succeed())

	// Verify the statefulSet Specs
	testMapContainsOther(t, "StatefulSet pod template selector", stateful.Spec.Template.Labels, stateful.Spec.Selector.MatchLabels)
	assert.GreaterOrEqual(t, len(stateful.Spec.Selector.MatchLabels), 1, "StatefulSet pod template selector must have at least 1 label")

	// Delete the StatefulSet and expect Reconcile to be called for StatefulSet deletion
	g.Expect(testClient.Delete(context.TODO(), stateful)).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))
	g.Eventually(func() error { return testClient.Get(context.TODO(), statefulSetKey, stateful) }, timeout).
		Should(gomega.Succeed())

	// Manually delete StatefulSet since GC isn't enabled in the test control plane
	g.Eventually(func() error { return testClient.Delete(context.TODO(), stateful) }, timeout).
		Should(gomega.MatchError("statefulsets.apps \"" + statefulSetKey.Name + "\" not found"))

	return stateful
}

func expectNoStatefulSet(g *gomega.GomegaWithT, statefulSetKey types.NamespacedName) {
	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), statefulSetKey, stateful) }, timeout).
		Should(gomega.MatchError("StatefulSet.apps \"" + statefulSetKey.Name + "\" not found"))
}

func expectService(t *testing.T, g *gomega.GomegaWithT, requests chan reconcile.Request, expectedRequest reconcile.Request, serviceKey types.NamespacedName, selectorLables map[string]string) *corev1.Service {
	service := &corev1.Service{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), serviceKey, service) }, timeout).
		Should(gomega.Succeed())

	assert.Equal(t, selectorLables, service.Spec.Selector, "Service is not pointing to the correct Pods.")

	// Delete the Service and expect Reconcile to be called for Service deletion
	g.Expect(testClient.Delete(context.TODO(), service)).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))
	g.Eventually(func() error { return testClient.Get(context.TODO(), serviceKey, service) }, timeout).
		Should(gomega.Succeed())

	// Manually delete Service since GC isn't enabled in the test control plane
	g.Eventually(func() error { return testClient.Delete(context.TODO(), service) }, timeout).
		Should(gomega.MatchError("services \"" + serviceKey.Name + "\" not found"))

	return service
}

func expectNoService(g *gomega.GomegaWithT, serviceKey types.NamespacedName, message string) {
	service := &corev1.Service{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), serviceKey, service) }, timeout).
		Should(gomega.MatchError("Service \""+serviceKey.Name+"\" not found"), message)
}

func expectIngress(g *gomega.GomegaWithT, requests chan reconcile.Request, expectedRequest reconcile.Request, ingressKey types.NamespacedName) *extv1.Ingress {
	ingress := &extv1.Ingress{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), ingressKey, ingress) }, timeout).
		Should(gomega.Succeed())

	// Delete the Ingress and expect Reconcile to be called for Ingress deletion
	g.Expect(testClient.Delete(context.TODO(), ingress)).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))
	g.Eventually(func() error { return testClient.Get(context.TODO(), ingressKey, ingress) }, timeout).
		Should(gomega.Succeed())

	// Manually delete Ingress since GC isn't enabled in the test control plane
	g.Eventually(func() error { return testClient.Delete(context.TODO(), ingress) }, timeout).
		Should(gomega.MatchError("ingresses.extensions \"" + ingressKey.Name + "\" not found"))

	return ingress
}

func expectNoIngress(g *gomega.GomegaWithT, ingressKey types.NamespacedName) {
	ingress := &extv1.Ingress{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), ingressKey, ingress) }, timeout).
		Should(gomega.MatchError("Ingress.extensions \"" + ingressKey.Name + "\" not found"))
}

func expectConfigMap(t *testing.T, g *gomega.GomegaWithT, requests chan reconcile.Request, expectedRequest reconcile.Request, configMapKey types.NamespacedName, configMapData map[string]string) *corev1.ConfigMap {
	configMap := &corev1.ConfigMap{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), configMapKey, configMap) }, timeout).
		Should(gomega.Succeed())

	// Verify the ConfigMap Specs
	for k, d := range configMapData {
		assert.Equal(t, configMap.Data[k], d, "ConfigMap does not have the correct data.")
	}

	// Delete the ConfigMap and expect Reconcile to be called for Deployment deletion
	g.Expect(testClient.Delete(context.TODO(), configMap)).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))
	g.Eventually(func() error { return testClient.Get(context.TODO(), configMapKey, configMap) }, timeout).
		Should(gomega.Succeed())

	// Manually delete ConfigMap since GC isn't enabled in the test control plane
	g.Eventually(func() error { return testClient.Delete(context.TODO(), configMap) }, timeout).
		Should(gomega.MatchError("configmaps \"" + configMapKey.Name + "\" not found"))

	return configMap
}

func expectNoConfigMap(g *gomega.GomegaWithT, configMapKey types.NamespacedName) {
	configMap := &corev1.ConfigMap{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), configMapKey, configMap) }, timeout).
		Should(gomega.MatchError("ConfigMap \"" + configMapKey.Name + "\" not found"))
}

func expectDeployment(t *testing.T, g *gomega.GomegaWithT, requests chan reconcile.Request, expectedRequest reconcile.Request, deploymentKey types.NamespacedName, configMapName string) *appsv1.Deployment {
	deploy := &appsv1.Deployment{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), deploymentKey, deploy) }, timeout).
		Should(gomega.Succeed())

	// Verify the deployment Specs
	testMapContainsOther(t, "Deployment pod template selector", deploy.Spec.Template.Labels, deploy.Spec.Selector.MatchLabels)
	assert.GreaterOrEqual(t, len(deploy.Spec.Selector.MatchLabels), 1, "Deployment pod template selector must have at least 1 label")

	if configMapName != "" {
		if assert.LessOrEqual(t, 1, len(deploy.Spec.Template.Spec.Volumes), "Deployment should have at least 1 volume, the configMap. Less were found.") {
			assert.Equal(t, configMapName, deploy.Spec.Template.Spec.Volumes[0].ConfigMap.Name, "Deployment configMap volume has the wrong name.")
		}
	}

	// Delete the Deployment and expect Reconcile to be called for Deployment deletion
	g.Expect(testClient.Delete(context.TODO(), deploy)).NotTo(gomega.HaveOccurred())
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))
	g.Eventually(func() error { return testClient.Get(context.TODO(), deploymentKey, deploy) }, timeout).
		Should(gomega.Succeed())

	// Manually delete Deployment since GC isn't enabled in the test control plane
	g.Eventually(func() error { return testClient.Delete(context.TODO(), deploy) }, timeout).
		Should(gomega.MatchError("deployments.apps \"" + deploymentKey.Name + "\" not found"))

	return deploy
}

func verifyUserSuppliedTLSConfig(t *testing.T, tls *solr.SolrTLSOptions, expectedKeystorePasswordSecretName string, expectedKeystorePasswordSecretKey string, expectedTlsSecretName string, needsPkcs12InitContainer bool) {
	assert.NotNil(t, tls)
	assert.Equal(t, expectedKeystorePasswordSecretName, tls.KeyStorePasswordSecret.Name)
	assert.Equal(t, expectedKeystorePasswordSecretKey, tls.KeyStorePasswordSecret.Key)
	assert.Equal(t, expectedTlsSecretName, tls.PKCS12Secret.Name)
	assert.Equal(t, "keystore.p12", tls.PKCS12Secret.Key)

	// is there a separate truststore?
	expectedTrustStorePath := ""
	if tls.TrustStoreSecret != nil {
		expectedTrustStorePath = util.DefaultTrustStorePath + "/" + tls.TrustStoreSecret.Key
	}

	expectTLSEnvVars(t, util.TLSEnvVars(tls, needsPkcs12InitContainer), expectedKeystorePasswordSecretName, expectedKeystorePasswordSecretKey, needsPkcs12InitContainer, expectedTrustStorePath)
}

func createTLSOptions(tlsSecretName string, keystorePassKey string, restartOnTLSSecretUpdate bool) *solr.SolrTLSOptions {
	return &solr.SolrTLSOptions{
		KeyStorePasswordSecret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: tlsSecretName},
			Key:                  keystorePassKey,
		},
		PKCS12Secret: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: tlsSecretName},
			Key:                  util.Pkcs12KeystoreFile,
		},
		RestartOnTLSSecretUpdate: restartOnTLSSecretUpdate,
	}
}

func createMockTLSSecret(ctx context.Context, apiClient client.Client, secretName string, secretKey string, ns string, keystorePasswordKey string) (corev1.Secret, error) {
	secretData := map[string][]byte{}
	secretData[secretKey] = []byte(b64.StdEncoding.EncodeToString([]byte("mock keystore")))
	secretData[util.TLSCertKey] = []byte(b64.StdEncoding.EncodeToString([]byte("mock tls.crt")))

	if keystorePasswordKey != "" {
		secretData[keystorePasswordKey] = []byte(b64.StdEncoding.EncodeToString([]byte("mock keystore password")))
	}

	mockTLSSecret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: ns},
		Data:       secretData,
		Type:       corev1.SecretTypeOpaque,
	}
	err := apiClient.Create(ctx, &mockTLSSecret)
	return mockTLSSecret, err
}

func createBasicAuthSecret(name string, key string, ns string) *corev1.Secret {
	secretData := map[string][]byte{corev1.BasicAuthUsernameKey: []byte(key), corev1.BasicAuthPasswordKey: []byte("secret password")}
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Data: secretData, Type: corev1.SecretTypeBasicAuth}
}

// Ensures all the TLS env vars, volume mounts and initContainers are setup for the PodTemplateSpec
func expectTLSConfigOnPodTemplate(t *testing.T, tls *solr.SolrTLSOptions, podTemplate *corev1.PodTemplateSpec, needsPkcs12InitContainer bool) *corev1.Container {
	assert.NotNil(t, podTemplate.Spec.Volumes)
	var keystoreVol *corev1.Volume = nil
	for _, vol := range podTemplate.Spec.Volumes {
		if vol.Name == "keystore" {
			keystoreVol = &vol
			break
		}
	}
	assert.NotNil(t, keystoreVol, fmt.Sprintf("keystore volume not found in pod template; volumes: %v", podTemplate.Spec.Volumes))
	assert.NotNil(t, keystoreVol.VolumeSource.Secret, "Didn't find TLS keystore volume in sts config!")
	assert.Equal(t, tls.PKCS12Secret.Name, keystoreVol.VolumeSource.Secret.SecretName)

	// check the SOLR_SSL_ related env vars on the sts
	assert.NotNil(t, podTemplate.Spec.Containers)
	assert.True(t, len(podTemplate.Spec.Containers) > 0)
	mainContainer := podTemplate.Spec.Containers[0]
	assert.NotNil(t, mainContainer, "Didn't find the main solrcloud-node container in the sts!")
	assert.NotNil(t, mainContainer.Env, "Didn't find the main solrcloud-node container in the sts!")

	// is there a separate truststore?
	expectedTrustStorePath := ""
	if tls.TrustStoreSecret != nil {
		expectedTrustStorePath = util.DefaultTrustStorePath + "/" + tls.TrustStoreSecret.Key
	}

	expectTLSEnvVars(t, mainContainer.Env, tls.KeyStorePasswordSecret.Name, tls.KeyStorePasswordSecret.Key, needsPkcs12InitContainer, expectedTrustStorePath)

	// different trust store?
	if tls.TrustStoreSecret != nil {
		var truststoreVol *corev1.Volume = nil
		for _, vol := range podTemplate.Spec.Volumes {
			if vol.Name == "truststore" {
				truststoreVol = &vol
				break
			}
		}
		assert.NotNil(t, truststoreVol, fmt.Sprintf("truststore volume not found in pod template; volumes: %v", podTemplate.Spec.Volumes))
		assert.NotNil(t, truststoreVol.VolumeSource.Secret, "Didn't find TLS truststore volume in sts config!")
		assert.Equal(t, tls.TrustStoreSecret.Name, truststoreVol.VolumeSource.Secret.SecretName)
	}

	// initContainers
	if needsPkcs12InitContainer {
		var pkcs12Vol *corev1.Volume = nil
		for _, vol := range podTemplate.Spec.Volumes {
			if vol.Name == "pkcs12" {
				pkcs12Vol = &vol
				break
			}
		}

		assert.NotNil(t, pkcs12Vol, "Didn't find TLS keystore volume in sts config!")
		assert.NotNil(t, pkcs12Vol.EmptyDir, "pkcs12 vol should by an emptyDir")

		assert.NotNil(t, podTemplate.Spec.InitContainers)
		var expInitContainer *corev1.Container = nil
		for _, cnt := range podTemplate.Spec.InitContainers {
			if cnt.Name == "gen-pkcs12-keystore" {
				expInitContainer = &cnt
				break
			}
		}
		expCmd := "openssl pkcs12 -export -in /var/solr/tls/tls.crt -in /var/solr/tls/ca.crt -inkey /var/solr/tls/tls.key -out /var/solr/tls/pkcs12/keystore.p12 -passout pass:${SOLR_SSL_KEY_STORE_PASSWORD}"
		assert.NotNil(t, expInitContainer, "Didn't find the gen-pkcs12-keystore InitContainer in the sts!")
		assert.Equal(t, expCmd, expInitContainer.Command[2])
	}

	if tls.ClientAuth == solr.Need {
		// verify the probes use a command with SSL opts
		tlsProps := "-Djavax.net.ssl.keyStore=$SOLR_SSL_KEY_STORE -Djavax.net.ssl.keyStorePassword=$SOLR_SSL_KEY_STORE_PASSWORD " +
			"-Djavax.net.ssl.trustStore=$SOLR_SSL_TRUST_STORE -Djavax.net.ssl.trustStorePassword=$SOLR_SSL_TRUST_STORE_PASSWORD"
		assert.NotNil(t, mainContainer.LivenessProbe, "main container should have a liveness probe defined")
		assert.NotNil(t, mainContainer.LivenessProbe.Exec, "liveness probe should have an exec when auth is enabled")
		assert.True(t, strings.Contains(mainContainer.LivenessProbe.Exec.Command[2], tlsProps), "liveness probe should invoke java with SSL opts")
		assert.NotNil(t, mainContainer.ReadinessProbe, "main container should have a readiness probe defined")
		assert.NotNil(t, mainContainer.ReadinessProbe.Exec, "readiness probe should have an exec when auth is enabled")
		assert.True(t, strings.Contains(mainContainer.ReadinessProbe.Exec.Command[2], tlsProps), "readiness probe should invoke java with SSL opts")
	}

	return &mainContainer // return as a convenience in case tests want to do more checking on the main container
}

// ensure the TLS related env vars are set for the Solr pod
func expectTLSEnvVars(t *testing.T, envVars []corev1.EnvVar, expectedKeystorePasswordSecretName string, expectedKeystorePasswordSecretKey string, needsPkcs12InitContainer bool, expectedTruststorePath string) {
	assert.NotNil(t, envVars)
	envVars = filterVarsByName(envVars, func(n string) bool {
		return strings.HasPrefix(n, "SOLR_SSL_")
	})
	assert.True(t, len(envVars) == 9)

	expectedKeystorePath := util.DefaultKeyStorePath + "/keystore.p12"
	if needsPkcs12InitContainer {
		expectedKeystorePath = util.DefaultWritableKeyStorePath + "/keystore.p12"
	}

	if expectedTruststorePath == "" {
		expectedTruststorePath = expectedKeystorePath
	}

	for _, envVar := range envVars {
		if envVar.Name == "SOLR_SSL_ENABLED" {
			assert.Equal(t, "true", envVar.Value)
		}

		if envVar.Name == "SOLR_SSL_KEY_STORE" {
			assert.Equal(t, expectedKeystorePath, envVar.Value)
		}

		if envVar.Name == "SOLR_SSL_TRUST_STORE" {
			assert.Equal(t, expectedTruststorePath, envVar.Value)
		}

		if envVar.Name == "SOLR_SSL_KEY_STORE_PASSWORD" {
			assert.NotNil(t, envVar.ValueFrom)
			assert.NotNil(t, envVar.ValueFrom.SecretKeyRef)
			assert.Equal(t, expectedKeystorePasswordSecretName, envVar.ValueFrom.SecretKeyRef.Name)
			assert.Equal(t, expectedKeystorePasswordSecretKey, envVar.ValueFrom.SecretKeyRef.Key)
		}
	}
}

// filter env vars by name using a supplied match function
func filterVarsByName(envVars []corev1.EnvVar, f func(string) bool) []corev1.EnvVar {
	filtered := make([]corev1.EnvVar, 0)
	for _, v := range envVars {
		if f(v.Name) {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

func testPodEnvVariables(t *testing.T, expectedEnvVars map[string]string, foundEnvVars []corev1.EnvVar) {
	testGenericPodEnvVariables(t, expectedEnvVars, foundEnvVars, "SOLR_OPTS")
}

func testMetricsPodEnvVariables(t *testing.T, expectedEnvVars map[string]string, foundEnvVars []corev1.EnvVar) {
	testGenericPodEnvVariables(t, expectedEnvVars, foundEnvVars, "JAVA_OPTS")
}

func testGenericPodEnvVariables(t *testing.T, expectedEnvVars map[string]string, foundEnvVars []corev1.EnvVar, lastVarName string) {
	matchCount := 0
	for _, envVar := range foundEnvVars {
		if expectedVal, match := expectedEnvVars[envVar.Name]; match {
			matchCount += 1
			assert.Equal(t, expectedVal, envVar.Value, "Wrong value for env variable '%s' in podSpec", envVar.Name)
		}
	}
	assert.Equal(t, len(expectedEnvVars), matchCount, "Not all expected env variables found in podSpec")
	assert.Equal(t, lastVarName, foundEnvVars[len(foundEnvVars)-1].Name, lastVarName+" must be the last envVar set, as it uses other envVars.")
}

func testPodTolerations(t *testing.T, expectedTolerations []corev1.Toleration, foundTolerations []corev1.Toleration) {
	assert.EqualValues(t, expectedTolerations, foundTolerations, "Expected tolerations and found tolerations don't match")
}

func testPodProbe(t *testing.T, expectedProbe *corev1.Probe, foundProbe *corev1.Probe, probeType string) {
	assert.EqualValuesf(t, expectedProbe, foundProbe, "Incorrect default container %s probe", probeType)
}

func testMapsEqual(t *testing.T, mapName string, expected map[string]string, found map[string]string) {
	assert.Equal(t, expected, found, "Expected and found %s are not the same", mapName)
}

func testMapContainsOther(t *testing.T, mapName string, base map[string]string, other map[string]string) {
	for k, v := range other {
		foundV, foundExists := base[k]
		assert.True(t, foundExists, "Expected key '%s' does not exist in found %s", k, mapName)
		if foundExists {
			assert.Equal(t, v, foundV, "Wrong value for %s key '%s'", mapName, k)
		}
	}
}

func testACLEnvVars(t *testing.T, actualEnvVars []corev1.EnvVar) {
	/*
		This test verifies ACL related env vars are set correctly and in the correct order, but expects a very specific config to be used in your test SolrCloud config:

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

	*/
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
	assert.Equal(t, zkAclEnvVars, actualEnvVars, "ZK ACL Env Vars are not correct")
}

func cleanupTest(g *gomega.GomegaWithT, namespace string) {
	deleteOpts := []client.DeleteAllOfOption{
		client.InNamespace(namespace),
	}

	cleanupObjects := []runtime.Object{
		// Solr Operator CRDs, modify this list whenever CRDs are added/deleted
		&solr.SolrCloud{}, &solr.SolrBackup{}, &solr.SolrPrometheusExporter{},

		// Dependency CRDs
		&zkv1beta1.ZookeeperCluster{},

		// All dependent Kubernetes types, in order of dependence (deployment then replicaSet then pod)
		&corev1.ConfigMap{}, &batchv1.Job{}, &extv1.Ingress{},
		&corev1.PersistentVolumeClaim{}, &corev1.PersistentVolume{},
		&appsv1.StatefulSet{}, &appsv1.Deployment{}, &appsv1.ReplicaSet{}, &corev1.Pod{}, &corev1.PersistentVolumeClaim{},
		&corev1.Secret{},
	}
	cleanupTestObjects(g, namespace, deleteOpts, cleanupObjects)

	// Delete all Services separately (https://github.com/kubernetes/kubernetes/pull/85802#issuecomment-561239845)
	opts := []client.ListOption{
		client.InNamespace(namespace),
	}
	services := &corev1.ServiceList{}
	g.Eventually(func() error { return testClient.List(context.TODO(), services, opts...) }, timeout).Should(gomega.Succeed())

	for _, item := range services.Items {
		g.Eventually(func() error { return testClient.Delete(context.TODO(), &item) }, timeout).Should(gomega.Succeed())
	}
}

func cleanupTestObjects(g *gomega.GomegaWithT, namespace string, deleteOpts []client.DeleteAllOfOption, objects []runtime.Object) {
	// Delete all SolrClouds
	for _, obj := range objects {
		g.Eventually(func() error { return testClient.DeleteAllOf(context.TODO(), obj, deleteOpts...) }, timeout).Should(gomega.Succeed())
	}
}

var (
	testKubeDomain        = "kube.domain.com"
	testDomain            = "test.domain.com"
	testAdditionalDomains = []string{
		"test1.domain.com",
		"test2.domain.com",
	}
	testPodAnnotations = map[string]string{
		"testP1": "valueP1",
		"testP2": "valueP2",
	}
	testPodLabels = map[string]string{
		"testP3": "valueP3",
		"testP4": "valueP4",
	}
	testSSAnnotations = map[string]string{
		"testSS1": "valueSS1",
		"testSS2": "valueSS2",
	}
	testSSLabels = map[string]string{
		"testSS3": "valueSS3",
		"testSS4": "valueSS4",
	}
	testIngressLabels = map[string]string{
		"testI1": "valueI1",
		"testI2": "valueI2",
	}
	testIngressAnnotations = map[string]string{
		"testI3": "valueI3",
		"testI4": "valueI4",
	}
	testCommonServiceLabels = map[string]string{
		"testCS1": "valueCS1",
		"testCS2": "valueCS2",
	}
	testCommonServiceAnnotations = map[string]string{
		"testCS3": "valueCS3",
		"testCS4": "valueCS4",
	}
	testHeadlessServiceLabels = map[string]string{
		"testHS1": "valueHS1",
		"testHS2": "valueHS2",
	}
	testHeadlessServiceAnnotations = map[string]string{
		"testHS3": "valueHS3",
		"testHS4": "valueHS4",
	}
	testNodeServiceLabels = map[string]string{
		"testNS1": "valueNS1",
		"testNS2": "valueNS2",
	}
	testNodeServiceAnnotations = map[string]string{
		"testNS3": "valueNS3",
		"testNS4": "valueNS4",
	}
	testConfigMapLabels = map[string]string{
		"testCM1": "valueCM1",
		"testCM2": "valueCM2",
	}
	testConfigMapAnnotations = map[string]string{
		"testCM3": "valueCM3",
		"testCM4": "valueCM4",
	}
	testDeploymentAnnotations = map[string]string{
		"testD1": "valueD1",
		"testD2": "valueD2",
	}
	testDeploymentLabels = map[string]string{
		"testD3": "valueD3",
		"testD4": "valueD4",
	}
	testMetricsServiceLabels = map[string]string{
		"testS1": "valueS1",
		"testS2": "valueS2",
	}
	testMetricsServiceAnnotations = map[string]string{
		"testS3": "valueS3",
		"testS4": "valueS4",
	}
	testNodeSelectors = map[string]string{
		"beta.kubernetes.io/arch": "amd64",
		"beta.kubernetes.io/os":   "linux",
		"solrclouds":              "true",
	}
	testProbeLivenessNonDefaults = &corev1.Probe{
		InitialDelaySeconds: 20,
		TimeoutSeconds:      1,
		SuccessThreshold:    1,
		FailureThreshold:    3,
		PeriodSeconds:       10,
		Handler: corev1.Handler{
			HTTPGet: &corev1.HTTPGetAction{
				Scheme: corev1.URISchemeHTTP,
				Path:   "/solr/admin/info/system",
				Port:   intstr.FromInt(8983),
			},
		},
	}
	testProbeReadinessNonDefaults = &corev1.Probe{
		InitialDelaySeconds: 15,
		TimeoutSeconds:      1,
		SuccessThreshold:    1,
		FailureThreshold:    3,
		PeriodSeconds:       5,
		Handler: corev1.Handler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt(8983),
			},
		},
	}
	testProbeStartup = &corev1.Probe{
		InitialDelaySeconds: 1,
		TimeoutSeconds:      1,
		SuccessThreshold:    1,
		FailureThreshold:    5,
		PeriodSeconds:       5,
		Handler: corev1.Handler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"ls",
				},
			},
		},
	}
	testTolerations = []corev1.Toleration{
		{
			Effect:   "NoSchedule",
			Key:      "node-restriction.kubernetes.io/dedicated",
			Value:    "solrclouds",
			Operator: "Exists",
		},
	}
	testTolerationsPromExporter = []corev1.Toleration{
		{
			Effect:   "NoSchedule",
			Operator: "Exists",
		},
	}
	testPriorityClass              = "p4"
	testImagePullSecretName        = "MAIN_SECRET"
	testAdditionalImagePullSecrets = []corev1.LocalObjectReference{
		{Name: "ADDITIONAL_SECRET_1"},
		{Name: "ADDITIONAL_SECRET_2"},
	}
	testTerminationGracePeriodSeconds = int64(50)
	extraVars                         = []corev1.EnvVar{
		{
			Name:  "VAR_1",
			Value: "VAL_1",
		},
		{
			Name: "VAR_2",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.name",
				},
			},
		},
	}
	one                = int64(1)
	two                = int64(2)
	four               = int32(4)
	five               = int32(5)
	podSecurityContext = corev1.PodSecurityContext{
		RunAsUser:  &one,
		RunAsGroup: &two,
	}
	extraVolumes = []solr.AdditionalVolume{
		{
			Name: "vol1",
			Source: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
			DefaultContainerMount: &corev1.VolumeMount{
				Name:      "ignore",
				ReadOnly:  false,
				MountPath: "/test/mount/path",
				SubPath:   "sub/",
			},
		},
	}
	affinity = &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					TopologyKey: "testKey",
				},
			},
			PreferredDuringSchedulingIgnoredDuringExecution: nil,
		},
	}
	resources = corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU: *resource.NewMilliQuantity(5300, resource.DecimalSI),
		},
		Requests: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceEphemeralStorage: resource.MustParse("5Gi"),
		},
	}
	extraContainers1 = []corev1.Container{
		{
			Name:                     "container1",
			Image:                    "image1",
			TerminationMessagePolicy: "File",
			TerminationMessagePath:   "/dev/termination-log",
			ImagePullPolicy:          "Always",
		},
		{
			Name:                     "container2",
			Image:                    "image2",
			TerminationMessagePolicy: "File",
			TerminationMessagePath:   "/dev/termination-log",
			ImagePullPolicy:          "Always",
		},
	}
	extraContainers2 = []corev1.Container{
		{
			Name:                     "container3",
			Image:                    "image3",
			TerminationMessagePolicy: "File",
			TerminationMessagePath:   "/dev/termination-log",
			ImagePullPolicy:          "Always",
		},
		{
			Name:                     "container4",
			Image:                    "image4",
			TerminationMessagePolicy: "File",
			TerminationMessagePath:   "/dev/termination-log",
			ImagePullPolicy:          "Always",
		},
	}
	testServiceAccountName = "test-service-account"
)
