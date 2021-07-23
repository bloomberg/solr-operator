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
	"context"
	"crypto/md5"
	"fmt"
	solrv1beta1 "github.com/apache/solr-operator/api/v1beta1"
	"github.com/apache/solr-operator/controllers/util"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// SolrPrometheusExporterReconciler reconciles a SolrPrometheusExporter object
type SolrPrometheusExporterReconciler struct {
	client.Client
	Log    logr.Logger
	scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=,resources=configmaps/status,verbs=get
// +kubebuilder:rbac:groups=,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=,resources=services/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups=solr.apache.org,resources=solrclouds,verbs=get;list;watch
// +kubebuilder:rbac:groups=solr.apache.org,resources=solrclouds/status,verbs=get
// +kubebuilder:rbac:groups=solr.apache.org,resources=solrprometheusexporters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=solr.apache.org,resources=solrprometheusexporters/status,verbs=get;update;patch

func (r *SolrPrometheusExporterReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	_ = context.Background()

	logger := r.Log.WithValues("namespace", req.Namespace, "solrPrometheusExporter", req.Name)

	// Fetch the SolrPrometheusExporter instance
	prometheusExporter := &solrv1beta1.SolrPrometheusExporter{}
	err := r.Get(context.TODO(), req.NamespacedName, prometheusExporter)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the req.
		return ctrl.Result{}, err
	}

	changed := prometheusExporter.WithDefaults()
	if changed {
		logger.Info("Setting default settings for Solr PrometheusExporter")
		if err := r.Update(context.TODO(), prometheusExporter); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	configMapKey := util.PrometheusExporterConfigMapKey
	configXmlMd5 := ""
	if prometheusExporter.Spec.Config == "" && prometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions != nil && prometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap != "" {
		foundConfigMap := &corev1.ConfigMap{}
		err = r.Get(context.TODO(), types.NamespacedName{Name: prometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap, Namespace: prometheusExporter.Namespace}, foundConfigMap)
		if err != nil {
			return ctrl.Result{}, err
		}

		if foundConfigMap.Data != nil {
			configXml, ok := foundConfigMap.Data[configMapKey]
			if ok {
				configXmlMd5 = fmt.Sprintf("%x", md5.Sum([]byte(configXml)))
			} else {
				return ctrl.Result{}, fmt.Errorf("required '%s' key not found in provided ConfigMap %s",
					configMapKey, prometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap)
			}
		} else {
			return ctrl.Result{}, fmt.Errorf("provided ConfigMap %s has no data",
				prometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap)
		}
	}

	if prometheusExporter.Spec.Config != "" {
		// Generate ConfigMap
		configMap := util.GenerateMetricsConfigMap(prometheusExporter)

		// capture the MD5 for the default config XML, otherwise we already computed it above
		if configXmlMd5 == "" {
			configXmlMd5 = fmt.Sprintf("%x", md5.Sum([]byte(configMap.Data[configMapKey])))
		}

		// Check if the ConfigMap already exists
		configMapLogger := logger.WithValues("configMap", configMap.Name)
		foundConfigMap := &corev1.ConfigMap{}
		err = r.Get(context.TODO(), types.NamespacedName{Name: configMap.Name, Namespace: configMap.Namespace}, foundConfigMap)
		if err != nil && errors.IsNotFound(err) {
			configMapLogger.Info("Creating ConfigMap")
			if err = controllerutil.SetControllerReference(prometheusExporter, configMap, r.scheme); err == nil {
				err = r.Create(context.TODO(), configMap)
			}
		} else if err == nil {
			var needsUpdate bool
			needsUpdate, err = util.OvertakeControllerRef(prometheusExporter, foundConfigMap, r.scheme)
			needsUpdate = util.CopyConfigMapFields(configMap, foundConfigMap, configMapLogger) || needsUpdate

			// Update the found ConfigMap and write the result back if there are any changes
			if needsUpdate && err == nil {
				configMapLogger.Info("Updating ConfigMap")
				err = r.Update(context.TODO(), foundConfigMap)
			}
		}
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// Generate Metrics Service
	metricsService := util.GenerateSolrMetricsService(prometheusExporter)

	// Check if the Metrics Service already exists
	serviceLogger := logger.WithValues("service", metricsService.Name)
	foundMetricsService := &corev1.Service{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: metricsService.Name, Namespace: metricsService.Namespace}, foundMetricsService)
	if err != nil && errors.IsNotFound(err) {
		serviceLogger.Info("Creating Service")
		if err = controllerutil.SetControllerReference(prometheusExporter, metricsService, r.scheme); err == nil {
			err = r.Create(context.TODO(), metricsService)
		}
	} else if err == nil {
		var needsUpdate bool
		needsUpdate, err = util.OvertakeControllerRef(prometheusExporter, foundMetricsService, r.scheme)
		needsUpdate = util.CopyServiceFields(metricsService, foundMetricsService, serviceLogger) || needsUpdate

		// Update the found Metrics Service and write the result back if there are any changes
		if needsUpdate && err == nil {
			serviceLogger.Info("Updating Service")
			err = r.Update(context.TODO(), foundMetricsService)
		}
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// Get the ZkConnectionString to connect to
	solrConnectionInfo := util.SolrConnectionInfo{}
	if solrConnectionInfo, err = getSolrConnectionInfo(r, prometheusExporter); err != nil {
		return ctrl.Result{}, err
	}

	// Make sure the TLS config is in order
	var tlsClientOptions *util.TLSClientOptions = nil
	if prometheusExporter.Spec.SolrReference.SolrTLS != nil && prometheusExporter.Spec.SolrReference.SolrTLS.PKCS12Secret != nil {
		requeueOrNot := reconcile.Result{}
		ctx := context.TODO()
		foundTLSSecret := &corev1.Secret{}
		lookupErr := r.Get(ctx, types.NamespacedName{Name: prometheusExporter.Spec.SolrReference.SolrTLS.PKCS12Secret.Name, Namespace: prometheusExporter.Namespace}, foundTLSSecret)
		if lookupErr != nil {
			return requeueOrNot, lookupErr
		} else {
			// Make sure the secret containing the keystore password exists as well
			keyStorePasswordSecret := &corev1.Secret{}
			err := r.Get(ctx, types.NamespacedName{Name: prometheusExporter.Spec.SolrReference.SolrTLS.KeyStorePasswordSecret.Name, Namespace: foundTLSSecret.Namespace}, keyStorePasswordSecret)
			if err != nil {
				return requeueOrNot, lookupErr
			}
			// we found the keystore secret, but does it have the key we expect?
			if _, ok := keyStorePasswordSecret.Data[prometheusExporter.Spec.SolrReference.SolrTLS.KeyStorePasswordSecret.Key]; !ok {
				return requeueOrNot, fmt.Errorf("%s key not found in keystore password secret %s",
					prometheusExporter.Spec.SolrReference.SolrTLS.KeyStorePasswordSecret.Key, keyStorePasswordSecret.Name)
			}

			tlsClientOptions = &util.TLSClientOptions{}
			tlsClientOptions.TLSOptions = prometheusExporter.Spec.SolrReference.SolrTLS

			if _, ok := foundTLSSecret.Data[prometheusExporter.Spec.SolrReference.SolrTLS.PKCS12Secret.Key]; !ok {
				// the keystore.p12 key is not in the TLS secret, indicating we need to create it using an initContainer
				tlsClientOptions.NeedsPkcs12InitContainer = true
			}

			// We have a watch on secrets, so will get notified when the secret changes (such as after cert renewal)
			// capture the hash of the secret and stash in an annotation so that pods get restarted if the cert changes
			if prometheusExporter.Spec.SolrReference.SolrTLS.RestartOnTLSSecretUpdate {
				if tlsCertBytes, ok := foundTLSSecret.Data[util.TLSCertKey]; ok {
					tlsClientOptions.TLSCertMd5 = fmt.Sprintf("%x", md5.Sum(tlsCertBytes))
				} else {
					return requeueOrNot, fmt.Errorf("%s key not found in TLS secret %s, cannot watch for updates to the cert without this data but 'solrTLS.restartOnTLSSecretUpdate' is enabled",
						util.TLSCertKey, foundTLSSecret.Name)
				}
			}
		}
	} else if prometheusExporter.Spec.SolrReference.SolrTLS != nil && prometheusExporter.Spec.SolrReference.SolrTLS.MountedTLSDir != nil {
		tlsClientOptions = &util.TLSClientOptions{}
		tlsClientOptions.TLSOptions = prometheusExporter.Spec.SolrReference.SolrTLS
		// TODO: How to get this from the SolrCloud reference?
		tlsClientOptions.BusyBoxImage = &solrv1beta1.ContainerImage{Repository: solrv1beta1.DefaultBusyBoxImageRepo, Tag: solrv1beta1.DefaultBusyBoxImageVersion}
	}

	basicAuthMd5 := ""
	if prometheusExporter.Spec.SolrReference.BasicAuthSecret != "" {
		basicAuthSecret := &corev1.Secret{}
		err := r.Get(context.TODO(), types.NamespacedName{Name: prometheusExporter.Spec.SolrReference.BasicAuthSecret, Namespace: prometheusExporter.Namespace}, basicAuthSecret)
		if err != nil {
			return reconcile.Result{}, err
		}

		err = util.ValidateBasicAuthSecret(basicAuthSecret)
		if err != nil {
			return reconcile.Result{}, err
		}
		creds := fmt.Sprintf("%s:%s", basicAuthSecret.Data[corev1.BasicAuthUsernameKey], basicAuthSecret.Data[corev1.BasicAuthPasswordKey])
		basicAuthMd5 = fmt.Sprintf("%x", md5.Sum([]byte(creds)))
	}

	deploy := util.GenerateSolrPrometheusExporterDeployment(prometheusExporter, solrConnectionInfo, configXmlMd5, tlsClientOptions, basicAuthMd5)

	ready := false
	// Check if the Metrics Deployment already exists
	deploymentLogger := logger.WithValues("deployment", deploy.Name)
	foundDeploy := &appsv1.Deployment{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: deploy.Name, Namespace: deploy.Namespace}, foundDeploy)
	if err != nil && errors.IsNotFound(err) {
		deploymentLogger.Info("Creating Deployment")
		if err = controllerutil.SetControllerReference(prometheusExporter, deploy, r.scheme); err == nil {
			err = r.Create(context.TODO(), deploy)
		}
	} else if err == nil {
		var needsUpdate bool
		needsUpdate, err = util.OvertakeControllerRef(prometheusExporter, foundDeploy, r.scheme)
		needsUpdate = util.CopyDeploymentFields(deploy, foundDeploy, deploymentLogger) || needsUpdate

		// Update the found Metrics Service and write the result back if there are any changes
		if needsUpdate && err == nil {
			deploymentLogger.Info("Updating Deployment")
			err = r.Update(context.TODO(), foundDeploy)
		}
		ready = foundDeploy.Status.ReadyReplicas > 0
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if ready != prometheusExporter.Status.Ready {
		prometheusExporter.Status.Ready = ready
		logger.Info("Updating status for solr-prometheus-exporter")
		err = r.Status().Update(context.TODO(), prometheusExporter)
	}

	return ctrl.Result{}, err
}

func getSolrConnectionInfo(r *SolrPrometheusExporterReconciler, prometheusExporter *solrv1beta1.SolrPrometheusExporter) (solrConnectionInfo util.SolrConnectionInfo, err error) {
	solrConnectionInfo = util.SolrConnectionInfo{}

	if prometheusExporter.Spec.SolrReference.Standalone != nil {
		solrConnectionInfo.StandaloneAddress = prometheusExporter.Spec.SolrReference.Standalone.Address
	}
	if prometheusExporter.Spec.SolrReference.Cloud != nil {
		cloudRef := prometheusExporter.Spec.SolrReference.Cloud
		if cloudRef.ZookeeperConnectionInfo != nil {
			solrConnectionInfo.CloudZkConnnectionInfo = cloudRef.ZookeeperConnectionInfo
		} else if cloudRef.Name != "" {
			solrCloud := &solrv1beta1.SolrCloud{}
			err = r.Get(context.TODO(), types.NamespacedName{Name: prometheusExporter.Spec.SolrReference.Cloud.Name, Namespace: prometheusExporter.Spec.SolrReference.Cloud.Namespace}, solrCloud)
			if err == nil {
				solrConnectionInfo.CloudZkConnnectionInfo = &solrCloud.Status.ZookeeperConnectionInfo
			}
		}
	}
	return solrConnectionInfo, err
}

func (r *SolrPrometheusExporterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.SetupWithManagerAndReconciler(mgr, r)
}

func (r *SolrPrometheusExporterReconciler) SetupWithManagerAndReconciler(mgr ctrl.Manager, reconciler reconcile.Reconciler) error {
	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		For(&solrv1beta1.SolrPrometheusExporter{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.Deployment{})

	var err error
	ctrlBuilder, err = r.indexAndWatchForProvidedConfigMaps(mgr, ctrlBuilder)
	if err != nil {
		return err
	}

	// Get notified when the TLS secret updates (such as when the cert gets renewed)
	ctrlBuilder, err = r.indexAndWatchForTLSSecret(mgr, ctrlBuilder)
	if err != nil {
		return err
	}

	// Get notified when the basic auth secret updates; exporter pods must be restarted if the basic auth password
	// changes b/c the credentials are loaded from a Java system property at startup and not watched for changes.
	ctrlBuilder, err = r.indexAndWatchForBasicAuthSecret(mgr, ctrlBuilder)
	if err != nil {
		return err
	}

	r.scheme = mgr.GetScheme()
	return ctrlBuilder.Complete(reconciler)
}

func (r *SolrPrometheusExporterReconciler) indexAndWatchForProvidedConfigMaps(mgr ctrl.Manager, ctrlBuilder *builder.Builder) (*builder.Builder, error) {
	providedConfigMapField := ".spec.customKubeOptions.configMapOptions.providedConfigMap"

	if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &solrv1beta1.SolrPrometheusExporter{}, providedConfigMapField, func(rawObj runtime.Object) []string {
		// grab the SolrCloud object, extract the used configMap...
		exporter := rawObj.(*solrv1beta1.SolrPrometheusExporter)
		if exporter.Spec.CustomKubeOptions.ConfigMapOptions == nil {
			return nil
		}
		if exporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap == "" {
			return nil
		}
		// ...and if so, return it
		return []string{exporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap}
	}); err != nil {
		return ctrlBuilder, err
	}

	return ctrlBuilder.Watches(
		&source.Kind{Type: &corev1.ConfigMap{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(func(a handler.MapObject) []reconcile.Request {
				foundExporters := &solrv1beta1.SolrPrometheusExporterList{}
				listOps := &client.ListOptions{
					FieldSelector: fields.OneTermEqualSelector(providedConfigMapField, a.Meta.GetName()),
					Namespace:     a.Meta.GetNamespace(),
				}
				err := r.List(context.TODO(), foundExporters, listOps)
				if err != nil {
					// if no exporters found, just no-op this
					return []reconcile.Request{}
				}

				requests := make([]reconcile.Request, len(foundExporters.Items))
				for i, item := range foundExporters.Items {
					requests[i] = reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      item.GetName(),
							Namespace: item.GetNamespace(),
						},
					}
				}
				return requests
			}),
		},
		builder.WithPredicates(predicate.ResourceVersionChangedPredicate{})), nil
}

func (r *SolrPrometheusExporterReconciler) indexAndWatchForTLSSecret(mgr ctrl.Manager, ctrlBuilder *builder.Builder) (*builder.Builder, error) {
	tlsSecretField := ".spec.solrReference.solrTLS.pkcs12Secret"

	if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &solrv1beta1.SolrPrometheusExporter{}, tlsSecretField, func(rawObj runtime.Object) []string {
		// grab the SolrCloud object, extract the referenced TLS secret...
		exporter := rawObj.(*solrv1beta1.SolrPrometheusExporter)
		if exporter.Spec.SolrReference.SolrTLS == nil || exporter.Spec.SolrReference.SolrTLS.PKCS12Secret == nil {
			return nil
		}
		// ...and if so, return it
		return []string{exporter.Spec.SolrReference.SolrTLS.PKCS12Secret.Name}
	}); err != nil {
		return ctrlBuilder, err
	}

	return r.buildSecretWatch(tlsSecretField, ctrlBuilder)
}

func (r *SolrPrometheusExporterReconciler) indexAndWatchForBasicAuthSecret(mgr ctrl.Manager, ctrlBuilder *builder.Builder) (*builder.Builder, error) {
	secretField := ".spec.solrReference.basicAuthSecret"

	if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &solrv1beta1.SolrPrometheusExporter{}, secretField, func(rawObj runtime.Object) []string {
		// grab the SolrCloud object, extract the referenced TLS secret...
		exporter := rawObj.(*solrv1beta1.SolrPrometheusExporter)
		if exporter.Spec.SolrReference.BasicAuthSecret == "" {
			return nil
		}
		// ...and if so, return it
		return []string{exporter.Spec.SolrReference.BasicAuthSecret}
	}); err != nil {
		return ctrlBuilder, err
	}

	return r.buildSecretWatch(secretField, ctrlBuilder)
}

func (r *SolrPrometheusExporterReconciler) buildSecretWatch(secretField string, ctrlBuilder *builder.Builder) (*builder.Builder, error) {
	return ctrlBuilder.Watches(
		&source.Kind{Type: &corev1.Secret{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(func(a handler.MapObject) []reconcile.Request {
				foundExporters := &solrv1beta1.SolrPrometheusExporterList{}
				listOps := &client.ListOptions{
					FieldSelector: fields.OneTermEqualSelector(secretField, a.Meta.GetName()),
					Namespace:     a.Meta.GetNamespace(),
				}
				err := r.List(context.TODO(), foundExporters, listOps)
				if err != nil {
					// if no exporters found, just no-op this
					return []reconcile.Request{}
				}

				requests := make([]reconcile.Request, len(foundExporters.Items))
				for i, item := range foundExporters.Items {
					requests[i] = reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      item.GetName(),
							Namespace: item.GetNamespace(),
						},
					}
				}
				return requests
			}),
		},
		builder.WithPredicates(predicate.ResourceVersionChangedPredicate{})), nil
}
