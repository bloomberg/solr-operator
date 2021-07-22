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
	b64 "encoding/base64"
	"fmt"
	solr "github.com/apache/solr-operator/api/v1beta1"
	"github.com/apache/solr-operator/controllers/util"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
	"sync"
	"testing"
	"time"
)

var _ reconcile.Reconciler = &SolrCloudReconciler{}

var (
	expectedCloudWithTLSRequest = reconcile.Request{NamespacedName: types.NamespacedName{Name: "foo-tls", Namespace: "default"}}
	expectedIngressWithTLS      = types.NamespacedName{Name: "foo-tls-solrcloud-common", Namespace: "default"}
	expectedStatefulSetName     = types.NamespacedName{Name: "foo-tls-solrcloud", Namespace: "default"}
)

func TestBasicAuthBootstrapSecurityJson(t *testing.T) {
	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic, ProbesRequireAuth: true}
	verifyReconcileWithSecurity(t, instance, false)
}

func TestBasicAuthBootstrapSecurityJsonWithZkACLs(t *testing.T) {
	UseZkCRD(true)
	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic, ProbesRequireAuth: true}
	zkReplicas := int32(1)
	instance.Spec.ZookeeperRef = &solr.ZookeeperRef{
		ProvidedZookeeper: &solr.ZookeeperSpec{
			Replicas: &zkReplicas,
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
	}
	verifyReconcileWithSecurity(t, instance, false)
}

func TestBasicAuthBootstrapSecurityJsonDeleteSecret(t *testing.T) {
	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic}
	verifyReconcileWithSecurity(t, instance, true)
}

func TestBasicAuthWithUserProvidedCreds(t *testing.T) {
	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{
		AuthenticationType: solr.Basic,
		BasicAuthSecret:    "my-basic-auth-secret",
	}
	verifyReconcileWithSecurity(t, instance, false)
}

func TestBasicAuthBootstrapWithTLS(t *testing.T) {
	instance := buildTestSolrCloud()

	// custom probe endpoint too
	instance.Spec.CustomSolrKubeOptions.PodOptions = &solr.PodOptions{
		ReadinessProbe: &corev1.Probe{
			Handler: corev1.Handler{
				HTTPGet: &corev1.HTTPGetAction{
					Scheme: corev1.URISchemeHTTPS,
					Path:   "/solr/admin/info/health",
					Port:   intstr.FromInt(8983),
				},
			},
		},
	}
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic}

	probePaths := util.GetCustomProbePaths(instance)
	assert.EqualValues(t, []string{"/solr/admin/info/health"}, probePaths)

	tlsSecretName := "tls-cert-secret-from-user"
	keystorePassKey := "some-password-key-thingy"
	instance.Spec.SolrTLS = createTLSOptions(tlsSecretName, keystorePassKey, false)
	verifyUserSuppliedTLSConfig(t, instance.Spec.SolrTLS, tlsSecretName, keystorePassKey, tlsSecretName, false)
	verifyReconcileUserSuppliedTLS(t, instance, false, false)
}

// For TLS, all we really need is a secret holding the keystore password and a secret holding the pkcs12 keystore,
// which can come from anywhere really, so this method tests handling of user-supplied secrets
func TestUserSuppliedTLSSecretWithPkcs12Keystore(t *testing.T) {
	tlsSecretName := "tls-cert-secret-from-user"
	keystorePassKey := "some-password-key-thingy"
	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic} // with basic-auth too
	instance.Spec.SolrTLS = createTLSOptions(tlsSecretName, keystorePassKey, false)
	verifyUserSuppliedTLSConfig(t, instance.Spec.SolrTLS, tlsSecretName, keystorePassKey, tlsSecretName, false)
	verifyReconcileUserSuppliedTLS(t, instance, false, false)
}

// User wants a different trust store than the keystore
func TestUserSuppliedTLSSecretWithSeparateTrustStore(t *testing.T) {
	tlsSecretName := "tls-cert-secret-from-user"
	keystorePassKey := "some-password-key-thingy"
	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic} // with basic-auth too
	instance.Spec.SolrTLS = createTLSOptions(tlsSecretName, keystorePassKey, false)

	trustStoreSecretName := "custom-truststore-secret"
	trustStoreFile := "truststore.p12"
	instance.Spec.SolrTLS.TrustStoreSecret = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: trustStoreSecretName},
		Key:                  trustStoreFile,
	}

	instance.Spec.SolrTLS.TrustStorePasswordSecret = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: trustStoreSecretName},
		Key:                  "truststore-pass",
	}

	instance.Spec.SolrTLS.ClientAuth = solr.Need // require client auth too (mTLS between the pods)

	verifyUserSuppliedTLSConfig(t, instance.Spec.SolrTLS, tlsSecretName, keystorePassKey, tlsSecretName, false)
	verifyReconcileUserSuppliedTLS(t, instance, false, false)
}

// Test upgrade from non-TLS cluster to TLS enabled cluster
func TestEnableTLSOnExistingCluster(t *testing.T) {

	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic}

	changed := instance.WithDefaults()
	assert.True(t, changed, "WithDefaults should have changed the test SolrCloud instance")

	g := gomega.NewGomegaWithT(t)
	helper := NewTLSTestHelper(g)
	defer func() {
		helper.StopTest()
	}()

	ctx := context.TODO()
	helper.ReconcileSolrCloud(ctx, instance, 2)
	defer testClient.Delete(ctx, instance)

	// now, update the config to enable TLS
	tlsSecretName := "tls-cert-secret-from-user"
	keystorePassKey := "some-password-key-thingy"

	err := testClient.Get(ctx, types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	instance.Spec.SolrTLS = createTLSOptions(tlsSecretName, keystorePassKey, true)
	foundTLSSecret := &corev1.Secret{}
	lookupErr := testClient.Get(ctx, types.NamespacedName{Name: instance.Spec.SolrTLS.PKCS12Secret.Name, Namespace: instance.Namespace}, foundTLSSecret)
	// TLS secret should not exist
	assert.True(t, errors.IsNotFound(lookupErr))

	// apply the update to trigger the upgrade to https
	err = testClient.Update(ctx, instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a mock secret in the background so the isCert ready function returns
	var mockTLSSecret *corev1.Secret
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		secret, _ := createMockTLSSecret(ctx, testClient, instance.Spec.SolrTLS.PKCS12Secret.Name, "keystore.p12", instance.Namespace, instance.Spec.SolrTLS.KeyStorePasswordSecret.Key)
		mockTLSSecret = &secret
		wg.Done()
	}()
	helper.WaitForReconcile(3)
	wg.Wait()

	expectStatefulSetTLSConfig(t, g, instance, false)
	expectPassthroughIngressTLSConfig(t, g, tlsSecretName)

	defer testClient.Delete(ctx, mockTLSSecret)
}

func TestUserSuppliedTLSSecretWithPkcs12Conversion(t *testing.T) {
	tlsSecretName := "tls-cert-secret-from-user-no-pkcs12"
	keystorePassKey := "some-password-key-thingy"
	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic}
	instance.Spec.SolrTLS = createTLSOptions(tlsSecretName, keystorePassKey, false)
	verifyUserSuppliedTLSConfig(t, instance.Spec.SolrTLS, tlsSecretName, keystorePassKey, tlsSecretName, true)
	verifyReconcileUserSuppliedTLS(t, instance, true, false)
}

func TestTLSSecretUpdate(t *testing.T) {
	tlsSecretName := "tls-cert-secret-update"
	keystorePassKey := "some-password-key-thingy"
	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic}
	instance.Spec.SolrTLS = createTLSOptions(tlsSecretName, keystorePassKey, true)
	instance.Spec.SolrTLS.ClientAuth = solr.Need
	verifyUserSuppliedTLSConfig(t, instance.Spec.SolrTLS, tlsSecretName, keystorePassKey, tlsSecretName, false)
	verifyReconcileUserSuppliedTLS(t, instance, false, true)
}

func verifyReconcileUserSuppliedTLS(t *testing.T, instance *solr.SolrCloud, needsPkcs12InitContainer bool, restartOnTLSSecretUpdate bool) {
	g := gomega.NewGomegaWithT(t)
	ctx := context.TODO()

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

	// Custom truststore?
	if instance.Spec.SolrTLS.TrustStoreSecret != nil {
		// create the mock truststore secret
		secretData := map[string][]byte{}
		secretData[instance.Spec.SolrTLS.TrustStoreSecret.Key] = []byte(b64.StdEncoding.EncodeToString([]byte("mock truststore")))
		secretData[instance.Spec.SolrTLS.TrustStorePasswordSecret.Key] = []byte(b64.StdEncoding.EncodeToString([]byte("mock truststore password")))
		trustStoreSecret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: instance.Spec.SolrTLS.TrustStoreSecret.Name, Namespace: instance.Namespace},
			Data:       secretData,
			Type:       corev1.SecretTypeOpaque,
		}
		err := testClient.Create(ctx, &trustStoreSecret)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer testClient.Delete(ctx, &trustStoreSecret)
	}

	// create the secret required for reconcile, it has both keys ...
	tlsKey := "keystore.p12"
	if needsPkcs12InitContainer {
		tlsKey = "tls.key" // to trigger the initContainer creation, don't want keystore.p12 in the secret
	}

	secret, err := createMockTLSSecret(ctx, testClient, instance.Spec.SolrTLS.PKCS12Secret.Name, tlsKey, instance.Namespace, instance.Spec.SolrTLS.KeyStorePasswordSecret.Key)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(ctx, &secret)

	// now try to reconcile
	err = testClient.Create(ctx, instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(ctx, instance)

	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))

	sts := expectStatefulSetTLSConfig(t, g, instance, needsPkcs12InitContainer)

	if !restartOnTLSSecretUpdate {
		assert.Empty(t, sts.Spec.Template.ObjectMeta.Annotations[util.SolrTlsCertMd5Annotation],
			"Shouldn't have a cert MD5 as we're not tracking updates to the secret")
		return
	}

	// let's trigger an update to the TLS secret to simulate the cert getting renewed and the pods getting restarted
	foundTLSSecret := &corev1.Secret{}
	err = testClient.Get(ctx, types.NamespacedName{Name: instance.Spec.SolrTLS.PKCS12Secret.Name, Namespace: instance.Namespace}, foundTLSSecret)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	expectedCertMd5 := fmt.Sprintf("%x", md5.Sum(foundTLSSecret.Data[util.TLSCertKey]))
	assert.Equal(t, expectedCertMd5, sts.Spec.Template.ObjectMeta.Annotations[util.SolrTlsCertMd5Annotation],
		"TLS cert MD5 annotation on STS does not match the secret")

	// change the tls.crt which should trigger a rolling restart
	updatedTlsCertData := "certificate renewed"
	foundTLSSecret.Data[util.TLSCertKey] = []byte(updatedTlsCertData)
	err = testClient.Update(context.TODO(), foundTLSSecret)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// capture all reconcile requests
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))

	// Check the annotation on the pod template to make sure a rolling restart will take place
	time.Sleep(time.Millisecond * 250)
	sts = expectStatefulSetTLSConfig(t, g, instance, needsPkcs12InitContainer)
	expectedCertMd5 = fmt.Sprintf("%x", md5.Sum(foundTLSSecret.Data[util.TLSCertKey]))
	assert.Equal(t, expectedCertMd5, sts.Spec.Template.ObjectMeta.Annotations[util.SolrTlsCertMd5Annotation],
		"TLS cert MD5 annotation on STS does not match the UPDATED secret")

	// does basic-auth work with TLS? That's the most common so we test both here
	if instance.Spec.SolrSecurity != nil {
		expectBootstrapSecret := instance.Spec.SolrSecurity.BasicAuthSecret == ""
		expectStatefulSetBasicAuthConfig(t, g, instance, expectBootstrapSecret)
	}
}

func TestTLSCommonIngressTermination(t *testing.T) {
	// now, update the config to enable TLS
	tlsSecretName := "tls-cert-secret-from-user"

	instance := buildTestSolrCloud()
	instance.Spec.SolrSecurity = &solr.SolrSecurityOptions{AuthenticationType: solr.Basic}
	instance.Spec.SolrAddressability.External.IngressTLSTerminationSecret = tlsSecretName

	changed := instance.WithDefaults()
	assert.True(t, changed, "WithDefaults should have changed the test SolrCloud instance")

	g := gomega.NewGomegaWithT(t)
	helper := NewTLSTestHelper(g)
	defer func() {
		helper.StopTest()
	}()

	ctx := context.TODO()
	helper.ReconcileSolrCloud(ctx, instance, 1)

	expectTerminateIngressTLSConfig(t, g, tlsSecretName)

	// Check that the Addresses in the status are correct
	g.Eventually(func() error {
		return testClient.Get(context.TODO(), types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, instance)
	}, timeout).Should(gomega.Succeed())
	assert.Equal(t, "http://"+instance.Name+"-solrcloud-common."+instance.Namespace, instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	if assert.NotNil(t, instance.Status.ExternalCommonAddress, "External common address in Status should not be nil.") {
		assert.EqualValues(t, "https://"+instance.Namespace+"-"+instance.Name+"-solrcloud."+testDomain, *instance.Status.ExternalCommonAddress, "Wrong external common address in status")
	}
	assert.Equal(t, "http://"+instance.Name+"-solrcloud-common."+instance.Namespace, instance.Status.InternalCommonAddress, "Wrong internal common address in status")
	if assert.NotNil(t, instance.Status.ExternalCommonAddress, "External common address in Status should not be nil.") {
		assert.EqualValues(t, "https://"+instance.Namespace+"-"+instance.Name+"-solrcloud."+testDomain, *instance.Status.ExternalCommonAddress, "Wrong external common address in status")
	}
	defer testClient.Delete(ctx, instance)
}

// ensures the TLS settings are applied correctly to the STS
func expectStatefulSetTLSConfig(t *testing.T, g *gomega.GomegaWithT, sc *solr.SolrCloud, needsPkcs12InitContainer bool) *appsv1.StatefulSet {
	ctx := context.TODO()
	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(ctx, expectedStatefulSetName, stateful) }, timeout).Should(gomega.Succeed())
	podTemplate := &stateful.Spec.Template
	expectTLSConfigOnPodTemplate(t, sc.Spec.SolrTLS, podTemplate, needsPkcs12InitContainer)

	// Check HTTPS cluster prop setup container
	assert.NotNil(t, podTemplate.Spec.InitContainers)
	var zkSetupInitContainer *corev1.Container = nil
	for _, cnt := range podTemplate.Spec.InitContainers {
		if cnt.Name == "setup-zk" {
			zkSetupInitContainer = &cnt
			break
		}
	}
	expCmd := "/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost ${ZK_HOST} -cmd clusterprop -name urlScheme -val https"
	if sc.Spec.SolrTLS != nil {
		assert.NotNil(t, zkSetupInitContainer, "Didn't find the zk-setup InitContainer in the sts!")
		if zkSetupInitContainer != nil {
			assert.Equal(t, stateful.Spec.Template.Spec.Containers[0].Image, zkSetupInitContainer.Image, "The zk-setup init container should use the same image as the Solr container")
			assert.Equal(t, 3, len(zkSetupInitContainer.Command), "Wrong command length for zk-setup init container")
			assert.Contains(t, zkSetupInitContainer.Command[2], expCmd, "ZK Setup command does not set urlScheme")
			expNumVars := 3
			if sc.Spec.SolrSecurity != nil && sc.Spec.SolrSecurity.BasicAuthSecret == "" {
				expNumVars = 4 // one more for SECURITY_JSON
			}
			assert.Equal(t, expNumVars, len(zkSetupInitContainer.Env), "Wrong number of envVars for zk-setup init container")
		}
	} else {
		assert.Nil(t, zkSetupInitContainer, "Shouldn't find the zk-setup InitContainer in the sts, when not using https!")
	}
	return stateful
}

func verifyReconcileWithSecurity(t *testing.T, instance *solr.SolrCloud, deleteBootstrapSecret bool) {
	g := gomega.NewGomegaWithT(t)
	ctx := context.TODO()

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

	// is there a user-provided secret for basic auth creds?
	if instance.Spec.SolrSecurity != nil && instance.Spec.SolrSecurity.BasicAuthSecret != "" {
		basicAuthSecret := createBasicAuthSecret(instance.Spec.SolrSecurity.BasicAuthSecret, solr.DefaultBasicAuthUsername, instance.Namespace)
		err := testClient.Create(ctx, basicAuthSecret)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		defer testClient.Delete(ctx, basicAuthSecret)
	}

	// now try to reconcile
	err = testClient.Create(ctx, instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer testClient.Delete(ctx, instance)

	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))

	expectBootstrapSecret := instance.Spec.SolrSecurity.BasicAuthSecret == ""
	expectStatefulSetBasicAuthConfig(t, g, instance, expectBootstrapSecret)

	if deleteBootstrapSecret {
		bootstrapSecret := &corev1.Secret{}
		err := testClient.Get(ctx, types.NamespacedName{Name: instance.SecurityBootstrapSecretName(), Namespace: instance.Namespace}, bootstrapSecret)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		// this should trigger a reconcile ...
		testClient.Delete(ctx, bootstrapSecret)
		g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))
		g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))
		g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))

		expectStatefulSetBasicAuthConfig(t, g, instance, false /* bootstrap secret not found */)
	}
}

func expectStatefulSetBasicAuthConfig(t *testing.T, g *gomega.GomegaWithT, sc *solr.SolrCloud, expectBootstrapSecret bool) *appsv1.StatefulSet {
	assert.NotNil(t, sc.Spec.SolrSecurity, "solrSecurity is not configured for this SolrCloud instance!")

	ctx := context.TODO()
	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(ctx, expectedStatefulSetName, stateful) }, timeout).Should(gomega.Succeed())
	podTemplate := &stateful.Spec.Template
	expectBasicAuthConfigOnPodTemplate(t, sc, podTemplate, expectBootstrapSecret)

	basicAuthSecret := &corev1.Secret{}
	err := testClient.Get(ctx, types.NamespacedName{Name: sc.BasicAuthSecretName(), Namespace: sc.Namespace}, basicAuthSecret)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	assert.Equal(t, solr.DefaultBasicAuthUsername, string(basicAuthSecret.Data[corev1.BasicAuthUsernameKey]), "password should be set for k8s-oper in the basic auth secret")
	assert.True(t, basicAuthSecret.Data[corev1.BasicAuthPasswordKey] != nil && len(basicAuthSecret.Data[corev1.BasicAuthPasswordKey]) > 0, "password should be set for k8s-oper in the basic auth secret")

	// verify a security.json gets bootstrapped if not using a user-provided secret
	if sc.Spec.SolrSecurity.BasicAuthSecret == "" && expectBootstrapSecret {
		bootstrapSecret := &corev1.Secret{}
		err := testClient.Get(ctx, types.NamespacedName{Name: sc.SecurityBootstrapSecretName(), Namespace: sc.Namespace}, bootstrapSecret)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		assert.True(t, bootstrapSecret.Data["admin"] != nil && len(bootstrapSecret.Data["admin"]) > 0, "password should be set for admin in the bootstrap security secret")
		assert.True(t, bootstrapSecret.Data["solr"] != nil && len(bootstrapSecret.Data["solr"]) > 0, "password should be set for solr in the bootstrap security secret")
		assert.True(t, bootstrapSecret.Data["security.json"] != nil && len(bootstrapSecret.Data["security.json"]) > 0, "security.json not found in the bootstrap security secret")

		if sc.Spec.CustomSolrKubeOptions.PodOptions != nil {
			probePaths := util.GetCustomProbePaths(sc)
			if len(probePaths) > 0 {
				securityJson := string(bootstrapSecret.Data["security.json"])
				assert.True(t, strings.Contains(securityJson, util.DefaultProbePath), "bootstrapped security.json should have an authz rule for "+util.DefaultProbePath)
				for _, p := range probePaths {
					p = p[len("/solr"):] // drop the /solr part on the path
					assert.True(t, strings.Contains(securityJson, p), "bootstrapped security.json should have an authz rule for "+p)
				}
			}
		}
	}

	return stateful
}

func expectPassthroughIngressTLSConfig(t *testing.T, g *gomega.GomegaWithT, expectedTLSSecretName string) {
	ingress := &netv1.Ingress{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedIngressWithTLS, ingress) }, timeout).Should(gomega.Succeed())
	assert.True(t, ingress.Spec.TLS != nil && len(ingress.Spec.TLS) == 1, "Wrong number of TLS Secrets for ingress")
	assert.Equal(t, expectedTLSSecretName, ingress.Spec.TLS[0].SecretName, "Wrong secretName for ingress TLS")
	assert.Equal(t, "HTTPS", ingress.ObjectMeta.Annotations["nginx.ingress.kubernetes.io/backend-protocol"], "Ingress Backend Protocol annotation incorrect")
}

func expectTerminateIngressTLSConfig(t *testing.T, g *gomega.GomegaWithT, expectedTLSSecretName string) {
	ingress := &netv1.Ingress{}
	g.Eventually(func() error { return testClient.Get(context.TODO(), expectedIngressWithTLS, ingress) }, timeout).Should(gomega.Succeed())
	assert.Equal(t, 1, len(ingress.Spec.TLS), "Wrong number of TLS Secrets for ingress")
	assert.Equal(t, expectedTLSSecretName, ingress.Spec.TLS[0].SecretName, "Wrong secretName for ingress TLS")
	assert.NotContains(t, ingress.ObjectMeta.Annotations, "nginx.ingress.kubernetes.io/backend-protocol", "Ingress Backend Protocol annotation should not exist for TLS termination")
	assert.Equal(t, 2, len(ingress.Spec.TLS[0].Hosts), "Wrong number of hosts for Ingress TLS termination")
	assert.Equal(t, "default-foo-tls-solrcloud."+testDomain, ingress.Spec.TLS[0].Hosts[0], "Wrong common-host name for Ingress TLS termination")
	assert.Equal(t, "default-foo-tls-solrcloud-0."+testDomain, ingress.Spec.TLS[0].Hosts[1], "Wrong common-host name for Ingress TLS termination")
}

// Ensures config is setup for basic-auth enabled Solr pods
func expectBasicAuthConfigOnPodTemplate(t *testing.T, instance *solr.SolrCloud, podTemplate *corev1.PodTemplateSpec, expectBootstrapSecret bool) *corev1.Container {

	// check the env vars needed for the probes to work with auth
	assert.NotNil(t, podTemplate.Spec.Containers)
	assert.True(t, len(podTemplate.Spec.Containers) > 0)
	mainContainer := podTemplate.Spec.Containers[0]
	assert.NotNil(t, mainContainer, "Didn't find the main solrcloud-node container in the sts!")
	assert.NotNil(t, mainContainer.Env, "Didn't find the main solrcloud-node container in the sts!")

	// probes with auth
	if instance.Spec.SolrSecurity.ProbesRequireAuth {
		assert.NotNil(t, podTemplate.Spec.Volumes)
		secretName := instance.BasicAuthSecretName()
		var basicAuthSecretVol *corev1.Volume = nil
		for _, vol := range podTemplate.Spec.Volumes {
			if vol.Name == secretName {
				basicAuthSecretVol = &vol
				break
			}
		}
		assert.NotNil(t, basicAuthSecretVol)
		assert.NotNil(t, basicAuthSecretVol.VolumeSource.Secret, "Didn't find the basic auth secret volume in sts config!")
		assert.Equal(t, instance.BasicAuthSecretName(), basicAuthSecretVol.VolumeSource.Secret.SecretName)

		var basicAuthSecretVolMount *corev1.VolumeMount = nil
		for _, m := range podTemplate.Spec.Containers[0].VolumeMounts {
			if m.Name == secretName {
				basicAuthSecretVolMount = &m
				break
			}
		}
		assert.NotNil(t, basicAuthSecretVolMount)
		assert.Equal(t, "/etc/secrets/"+secretName, basicAuthSecretVolMount.MountPath)

		expProbeCmd := "JAVA_TOOL_OPTIONS=\"-Dbasicauth=$(cat /etc/secrets/foo-tls-solrcloud-basic-auth/username):$(cat /etc/secrets/foo-tls-solrcloud-basic-auth/password)\" java -Dsolr.ssl.checkPeerName=false " +
			"-Dsolr.httpclient.builder.factory=org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory " +
			"-Dsolr.install.dir=\"/opt/solr\" -Dlog4j.configurationFile=\"/opt/solr/server/resources/log4j2-console.xml\" " +
			"-classpath \"/opt/solr/server/solr-webapp/webapp/WEB-INF/lib/*:/opt/solr/server/lib/ext/*:/opt/solr/server/lib/*\" " +
			"org.apache.solr.util.SolrCLI api -get http://localhost:8983/solr/admin/info/system"
		assert.NotNil(t, mainContainer.LivenessProbe, "main container should have a liveness probe defined")
		assert.NotNil(t, mainContainer.LivenessProbe.Exec, "liveness probe should have an exec when auth is enabled")
		assert.Equal(t, expProbeCmd, mainContainer.LivenessProbe.Exec.Command[2], "liveness probe should invoke java with auth opts")
		assert.NotNil(t, mainContainer.ReadinessProbe, "main container should have a readiness probe defined")
		assert.NotNil(t, mainContainer.ReadinessProbe.Exec, "readiness probe should have an exec when auth is enabled")
		assert.Equal(t, expProbeCmd, mainContainer.ReadinessProbe.Exec.Command[2], "readiness probe should invoke java with auth opts")
	}

	// if no user-provided auth secret, then check that security.json gets bootstrapped correctly
	if instance.Spec.SolrSecurity.BasicAuthSecret == "" {
		// initContainers
		assert.NotNil(t, podTemplate.Spec.InitContainers)
		var expInitContainer *corev1.Container = nil
		for _, cnt := range podTemplate.Spec.InitContainers {
			if cnt.Name == "setup-zk" {
				expInitContainer = &cnt
				break
			}
		}

		if expectBootstrapSecret {
			// if the zookeeperRef has ACLs set, verify the env vars were set correctly for this initContainer
			allACL, _ := instance.Spec.ZookeeperRef.GetACLs()
			if allACL != nil {
				assert.Equal(t, 10, len(expInitContainer.Env))
				assert.Equal(t, "SOLR_OPTS", expInitContainer.Env[len(expInitContainer.Env)-2].Name)
				assert.Equal(t, "SECURITY_JSON", expInitContainer.Env[len(expInitContainer.Env)-1].Name)
				testACLEnvVars(t, expInitContainer.Env[3:len(expInitContainer.Env)-2])
			} // else this ref not using ACLs

			assert.NotNil(t, expInitContainer, "Didn't find the setup-zk InitContainer in the sts!")
			expCmd := "ZK_SECURITY_JSON=$(/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost ${ZK_HOST} -cmd get /security.json); " +
				"if [ ${#ZK_SECURITY_JSON} -lt 3 ]; then " +
				"echo $SECURITY_JSON > /tmp/security.json; " +
				"/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost ${ZK_HOST} -cmd putfile /security.json /tmp/security.json; echo \"put security.json in ZK\"; fi"
			assert.True(t, strings.Contains(expInitContainer.Command[2], expCmd), "setup-zk initContainer not configured to bootstrap security.json!")
		} else {
			assert.True(t, expInitContainer == nil || !strings.Contains(expInitContainer.Command[2], "SECURITY_JSON"),
				"setup-zk initContainer not reconciled after bootstrap secret deleted")
		}

	}

	return &mainContainer // return as a convenience in case tests want to do more checking on the main container
}

func buildTestSolrCloud() *solr.SolrCloud {
	replicas := int32(1)
	instance := &solr.SolrCloud{
		ObjectMeta: metav1.ObjectMeta{Name: expectedCloudWithTLSRequest.Name, Namespace: expectedCloudWithTLSRequest.Namespace},
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
				},
			},
		},
	}
	return instance
}

// Consolidate common reconcile TLS test setup and behavior
type TLSTestHelper struct {
	g          *gomega.GomegaWithT
	mgr        manager.Manager
	requests   chan reconcile.Request
	stopMgr    chan struct{}
	mgrStopped *sync.WaitGroup
}

func NewTLSTestHelper(g *gomega.GomegaWithT) *TLSTestHelper {
	UseZkCRD(false)

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

	return &TLSTestHelper{
		g:          g,
		mgr:        mgr,
		requests:   requests,
		stopMgr:    stopMgr,
		mgrStopped: mgrStopped,
	}
}

func (helper *TLSTestHelper) StopTest() {
	close(helper.stopMgr)
	helper.mgrStopped.Wait()
}

func (helper *TLSTestHelper) WaitForReconcile(expectedRequests int) {
	g := helper.g
	requests := helper.requests
	for r := 0; r < expectedRequests; r++ {
		g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedCloudWithTLSRequest)))
	}
	// clear so we can wait for update reconcile to occur
	emptyRequests(requests)
}

func (helper *TLSTestHelper) ReconcileSolrCloud(ctx context.Context, instance *solr.SolrCloud, expectedRequests int) {
	g := helper.g
	cleanupTest(g, instance.Namespace)

	// trigger the reconcile process and then wait for reconcile to finish
	// expectedRequests gives the expected number of requests created during the reconcile process
	err := testClient.Create(ctx, instance)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a mock secret in the background so the isCert ready function returns
	var wg sync.WaitGroup
	if instance.Spec.SolrTLS != nil {
		wg.Add(1)
		go func() {
			createMockTLSSecret(ctx, testClient, instance.Spec.SolrTLS.PKCS12Secret.Name, instance.Spec.SolrTLS.PKCS12Secret.Key, instance.Namespace, instance.Spec.SolrTLS.KeyStorePasswordSecret.Key)
			wg.Done()
		}()
	}
	helper.WaitForReconcile(expectedRequests)
	wg.Wait()

	stateful := &appsv1.StatefulSet{}
	g.Eventually(func() error { return testClient.Get(ctx, expectedStatefulSetName, stateful) }, timeout).Should(gomega.Succeed())
}
