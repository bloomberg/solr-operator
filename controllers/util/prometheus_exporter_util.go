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

package util

import (
	solr "github.com/apache/solr-operator/api/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"strconv"
	"strings"
)

const (
	SolrMetricsPort     = 8080
	SolrMetricsPortName = "solr-metrics"
	ExtSolrMetricsPort  = 80

	DefaultPrometheusExporterEntrypoint      = "/opt/solr/contrib/prometheus-exporter/bin/solr-exporter"
	PrometheusExporterConfigMapKey           = "solr-prometheus-exporter.xml"
	PrometheusExporterConfigXmlMd5Annotation = "solr.apache.org/exporterConfigXmlMd5"
)

// SolrConnectionInfo defines how to connect to a cloud or standalone solr instance.
// One, and only one, of Cloud or Standalone must be provided.
type SolrConnectionInfo struct {
	CloudZkConnnectionInfo *solr.ZookeeperConnectionInfo
	StandaloneAddress      string
}

// Used internally to capture config needed to provided Solr client apps like the exporter
// with config needed to call TLS enabled Solr pods
type TLSClientOptions struct {
	TLSOptions               *solr.SolrTLSOptions
	NeedsPkcs12InitContainer bool
	TLSCertMd5               string
}

// GenerateSolrPrometheusExporterDeployment returns a new appsv1.Deployment pointer generated for the SolrCloud Prometheus Exporter instance
// solrPrometheusExporter: SolrPrometheusExporter instance
func GenerateSolrPrometheusExporterDeployment(solrPrometheusExporter *solr.SolrPrometheusExporter, solrConnectionInfo SolrConnectionInfo, configXmlMd5 string, tls *TLSClientOptions, basicAuthMd5 string) *appsv1.Deployment {
	gracePeriodTerm := int64(10)
	singleReplica := int32(1)
	fsGroup := int64(SolrMetricsPort)

	labels := solrPrometheusExporter.SharedLabelsWith(solrPrometheusExporter.GetLabels())
	var annotations map[string]string
	selectorLabels := solrPrometheusExporter.SharedLabels()

	labels["technology"] = solr.SolrPrometheusExporterTechnologyLabel
	selectorLabels["technology"] = solr.SolrPrometheusExporterTechnologyLabel

	podLabels := labels
	var podAnnotations map[string]string
	var imagePullSecrets []corev1.LocalObjectReference

	customDeploymentOptions := solrPrometheusExporter.Spec.CustomKubeOptions.DeploymentOptions
	if nil != customDeploymentOptions {
		labels = MergeLabelsOrAnnotations(labels, customDeploymentOptions.Labels)
		annotations = customDeploymentOptions.Annotations
	}

	customPodOptions := solrPrometheusExporter.Spec.CustomKubeOptions.PodOptions
	if nil != customPodOptions {
		podLabels = MergeLabelsOrAnnotations(podLabels, customPodOptions.Labels)
		podAnnotations = customPodOptions.Annotations
		imagePullSecrets = customPodOptions.ImagePullSecrets
	}

	var envVars []corev1.EnvVar
	var allJavaOpts []string

	var solrVolumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount
	exporterArgs := []string{
		"-p", strconv.Itoa(SolrMetricsPort),
		"-n", strconv.Itoa(int(solrPrometheusExporter.Spec.NumThreads)),
	}

	if solrPrometheusExporter.Spec.ScrapeInterval > 0 {
		exporterArgs = append(exporterArgs, "-s", strconv.Itoa(int(solrPrometheusExporter.Spec.ScrapeInterval)))
	}

	// Setup the solrConnectionInfo
	if solrConnectionInfo.CloudZkConnnectionInfo != nil {
		exporterArgs = append(exporterArgs, "-z", solrConnectionInfo.CloudZkConnnectionInfo.ZkConnectionString())

		// Add ACL information, if given, through Env Vars
		if hasACLs, aclEnvs := AddACLsToEnv(solrConnectionInfo.CloudZkConnnectionInfo.AllACL, solrConnectionInfo.CloudZkConnnectionInfo.ReadOnlyACL); hasACLs {
			envVars = append(envVars, aclEnvs...)

			// The $SOLR_ZK_CREDS_AND_ACLS parameter does not get picked up when running the Prometheus Exporter, it must be added to the JAVA_OPTS.
			allJavaOpts = append(allJavaOpts, "$(SOLR_ZK_CREDS_AND_ACLS)")
		}
	} else if solrConnectionInfo.StandaloneAddress != "" {
		exporterArgs = append(exporterArgs, "-b", solrConnectionInfo.StandaloneAddress)
	}

	// Only add the config if it is passed in from the user. Otherwise, use the default.
	if solrPrometheusExporter.Spec.Config != "" ||
		(solrPrometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions != nil && solrPrometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap != "") {
		configMapName := solrPrometheusExporter.MetricsConfigMapName()
		if solrPrometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions != nil && solrPrometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap != "" {
			configMapName = solrPrometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions.ProvidedConfigMap
		}
		solrVolumes = []corev1.Volume{{
			Name: "solr-prometheus-exporter-xml",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMapName,
					},
					Items: []corev1.KeyToPath{
						{
							Key:  PrometheusExporterConfigMapKey,
							Path: PrometheusExporterConfigMapKey,
						},
					},
				},
			},
		}}

		volumeMounts = []corev1.VolumeMount{{Name: "solr-prometheus-exporter-xml", MountPath: "/opt/solr-exporter", ReadOnly: true}}

		exporterArgs = append(exporterArgs, "-f", "/opt/solr-exporter/"+PrometheusExporterConfigMapKey)
	} else {
		exporterArgs = append(exporterArgs, "-f", "/opt/solr/contrib/prometheus-exporter/conf/solr-exporter-config.xml")
	}

	entrypoint := DefaultPrometheusExporterEntrypoint
	if solrPrometheusExporter.Spec.ExporterEntrypoint != "" {
		entrypoint = solrPrometheusExporter.Spec.ExporterEntrypoint
	}

	// Add Custom EnvironmentVariables to the solr container
	if nil != customPodOptions {
		// Add environment variables to container
		envVars = append(envVars, customPodOptions.EnvVariables...)

		// Add Custom Volumes to pod
		for _, volume := range customPodOptions.Volumes {
			// Only add the container mount if one has been provided.
			if volume.DefaultContainerMount != nil {
				volume.DefaultContainerMount.Name = volume.Name
				volumeMounts = append(volumeMounts, *volume.DefaultContainerMount)
			}

			solrVolumes = append(solrVolumes, corev1.Volume{
				Name:         volume.Name,
				VolumeSource: volume.Source,
			})
		}
	}

	if tls != nil {
		envVars = append(envVars, TLSEnvVars(tls.TLSOptions, tls.NeedsPkcs12InitContainer)...)
		volumeMounts = append(volumeMounts, tlsVolumeMounts(tls.TLSOptions, tls.NeedsPkcs12InitContainer)...)
		solrVolumes = append(solrVolumes, tlsVolumes(tls.TLSOptions, tls.NeedsPkcs12InitContainer)...)
		allJavaOpts = append(allJavaOpts, tlsJavaOpts(tls.TLSOptions)...)
	}

	// basic auth enabled?
	if solrPrometheusExporter.Spec.SolrReference.BasicAuthSecret != "" {
		lor := corev1.LocalObjectReference{Name: solrPrometheusExporter.Spec.SolrReference.BasicAuthSecret}
		usernameRef := &corev1.SecretKeySelector{LocalObjectReference: lor, Key: corev1.BasicAuthUsernameKey}
		passwordRef := &corev1.SecretKeySelector{LocalObjectReference: lor, Key: corev1.BasicAuthPasswordKey}
		envVars = append(envVars, corev1.EnvVar{Name: "BASIC_AUTH_USER", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: usernameRef}})
		envVars = append(envVars, corev1.EnvVar{Name: "BASIC_AUTH_PASS", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: passwordRef}})
		allJavaOpts = append(allJavaOpts, "-Dbasicauth=$(BASIC_AUTH_USER):$(BASIC_AUTH_PASS)")
		allJavaOpts = append(allJavaOpts, "-Dsolr.httpclient.builder.factory=org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory")
	}

	// the order of env vars in the array is important for the $(var) syntax to work
	// since JAVA_OPTS refers to $(SOLR_SSL_*) if TLS is enabled, it needs to be last
	if len(allJavaOpts) > 0 {
		envVars = append(envVars, corev1.EnvVar{Name: "JAVA_OPTS", Value: strings.Join(allJavaOpts, " ")})
	}

	containers := []corev1.Container{
		{
			Name:            "solr-prometheus-exporter",
			Image:           solrPrometheusExporter.Spec.Image.ToImageName(),
			ImagePullPolicy: solrPrometheusExporter.Spec.Image.PullPolicy,
			Ports:           []corev1.ContainerPort{{ContainerPort: SolrMetricsPort, Name: SolrMetricsPortName, Protocol: corev1.ProtocolTCP}},
			VolumeMounts:    volumeMounts,
			Command:         []string{entrypoint},
			Args:            exporterArgs,
			Env:             envVars,

			LivenessProbe: &corev1.Probe{
				Handler: corev1.Handler{
					HTTPGet: &corev1.HTTPGetAction{
						Scheme: corev1.URISchemeHTTP,
						Path:   "/metrics",
						Port:   intstr.FromInt(SolrMetricsPort),
					},
				},
				InitialDelaySeconds: 20,
				TimeoutSeconds:      1,
				PeriodSeconds:       10,
				SuccessThreshold:    1,
				FailureThreshold:    3,
			},
		},
	}

	var initContainers []corev1.Container
	if customPodOptions != nil {
		if len(customPodOptions.SidecarContainers) > 0 {
			containers = append(containers, customPodOptions.SidecarContainers...)
		}

		if len(customPodOptions.InitContainers) > 0 {
			initContainers = customPodOptions.InitContainers
		}
	}

	// if the supplied TLS secret does not have the pkcs12 keystore, use an initContainer to create its
	if tls != nil && tls.NeedsPkcs12InitContainer {
		pkcs12InitContainer := generatePkcs12InitContainer(tls.TLSOptions,
			solrPrometheusExporter.Spec.Image.ToImageName(), solrPrometheusExporter.Spec.Image.PullPolicy)
		initContainers = append(initContainers, pkcs12InitContainer)
	}

	// track the MD5 of the custom exporter config in the pod spec annotations,
	// so we get a rolling restart when the configMap changes
	if configXmlMd5 != "" {
		if podAnnotations == nil {
			podAnnotations = make(map[string]string, 1)
		}
		podAnnotations[PrometheusExporterConfigXmlMd5Annotation] = configXmlMd5
	}

	if tls != nil && tls.TLSCertMd5 != "" {
		if podAnnotations == nil {
			podAnnotations = make(map[string]string, 1)
		}
		podAnnotations[SolrTlsCertMd5Annotation] = tls.TLSCertMd5
	}

	// if the basic-auth secret changes, we want to restart the deployment pods
	if basicAuthMd5 != "" {
		if podAnnotations == nil {
			podAnnotations = make(map[string]string, 1)
		}
		podAnnotations[BasicAuthMd5Annotation] = basicAuthMd5
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrPrometheusExporter.MetricsDeploymentName(),
			Namespace:   solrPrometheusExporter.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels,
			},
			Replicas: &singleReplica,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &gracePeriodTerm,
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup: &fsGroup,
					},
					Volumes:        solrVolumes,
					InitContainers: initContainers,
					Containers:     containers,
				},
			},
		},
	}

	if solrPrometheusExporter.Spec.Image.ImagePullSecret != "" {
		imagePullSecrets = append(
			imagePullSecrets,
			corev1.LocalObjectReference{Name: solrPrometheusExporter.Spec.Image.ImagePullSecret},
		)
	}

	deployment.Spec.Template.Spec.ImagePullSecrets = imagePullSecrets

	if nil != customPodOptions {
		metricsContainer := &deployment.Spec.Template.Spec.Containers[0]
		if customPodOptions.ServiceAccountName != "" {
			deployment.Spec.Template.Spec.ServiceAccountName = customPodOptions.ServiceAccountName
		}

		if customPodOptions.Affinity != nil {
			deployment.Spec.Template.Spec.Affinity = customPodOptions.Affinity
		}

		if customPodOptions.Resources.Limits != nil || customPodOptions.Resources.Requests != nil {
			metricsContainer.Resources = customPodOptions.Resources
		}

		if customPodOptions.PodSecurityContext != nil {
			deployment.Spec.Template.Spec.SecurityContext = customPodOptions.PodSecurityContext
		}

		if customPodOptions.Tolerations != nil {
			deployment.Spec.Template.Spec.Tolerations = customPodOptions.Tolerations
		}

		if customPodOptions.NodeSelector != nil {
			deployment.Spec.Template.Spec.NodeSelector = customPodOptions.NodeSelector
		}

		if customPodOptions.PriorityClassName != "" {
			deployment.Spec.Template.Spec.PriorityClassName = customPodOptions.PriorityClassName
		}

		if customPodOptions.TerminationGracePeriodSeconds != nil {
			deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = customPodOptions.TerminationGracePeriodSeconds
		}

		if customPodOptions.ReadinessProbe != nil {
			// Default Prometheus Exporter container does not contain a readinessProbe, so copy the livenessProbe
			baseProbe := metricsContainer.LivenessProbe.DeepCopy()
			metricsContainer.ReadinessProbe = customizeProbe(baseProbe, *customPodOptions.ReadinessProbe)
		}

		if customPodOptions.StartupProbe != nil {
			// Default Prometheus Exporter container does not contain a startupProbe, so copy the livenessProbe
			baseProbe := metricsContainer.LivenessProbe.DeepCopy()
			metricsContainer.StartupProbe = customizeProbe(baseProbe, *customPodOptions.StartupProbe)
		}

		if customPodOptions.LivenessProbe != nil {
			metricsContainer.LivenessProbe = customizeProbe(metricsContainer.LivenessProbe, *customPodOptions.LivenessProbe)
		}
	}

	return deployment
}

// GenerateMetricsConfigMap returns a new corev1.ConfigMap pointer generated for the Solr Prometheus Exporter instance solr-prometheus-exporter.xml
// solrPrometheusExporter: SolrPrometheusExporter instance
func GenerateMetricsConfigMap(solrPrometheusExporter *solr.SolrPrometheusExporter) *corev1.ConfigMap {
	labels := solrPrometheusExporter.SharedLabelsWith(solrPrometheusExporter.GetLabels())
	var annotations map[string]string

	customOptions := solrPrometheusExporter.Spec.CustomKubeOptions.ConfigMapOptions
	if nil != customOptions {
		labels = MergeLabelsOrAnnotations(labels, customOptions.Labels)
		annotations = customOptions.Annotations
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrPrometheusExporter.MetricsConfigMapName(),
			Namespace:   solrPrometheusExporter.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Data: map[string]string{
			PrometheusExporterConfigMapKey: solrPrometheusExporter.Spec.Config,
		},
	}
	return configMap
}

// GenerateSolrMetricsService returns a new corev1.Service pointer generated for the SolrCloud Prometheus Exporter deployment
// Metrics will be collected on this service endpoint, as we don't want to double-tick data if multiple exporters are runnning.
// solrPrometheusExporter: solrPrometheusExporter instance
func GenerateSolrMetricsService(solrPrometheusExporter *solr.SolrPrometheusExporter) *corev1.Service {
	copyLabels := solrPrometheusExporter.GetLabels()
	if copyLabels == nil {
		copyLabels = map[string]string{}
	}
	labels := solrPrometheusExporter.SharedLabelsWith(solrPrometheusExporter.GetLabels())
	labels["service-type"] = "metrics"
	annotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/scheme": "http",
		"prometheus.io/path":   "/metrics",
		"prometheus.io/port":   strconv.Itoa(ExtSolrMetricsPort),
	}

	selectorLabels := solrPrometheusExporter.SharedLabels()
	selectorLabels["technology"] = solr.SolrPrometheusExporterTechnologyLabel

	customOptions := solrPrometheusExporter.Spec.CustomKubeOptions.ServiceOptions
	if nil != customOptions {
		labels = MergeLabelsOrAnnotations(labels, customOptions.Labels)
		annotations = MergeLabelsOrAnnotations(annotations, customOptions.Annotations)
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrPrometheusExporter.MetricsServiceName(),
			Namespace:   solrPrometheusExporter.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: SolrMetricsPortName, Port: ExtSolrMetricsPort, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt(SolrMetricsPort)},
			},
			Selector: selectorLabels,
		},
	}
	return service
}

// CreateMetricsIngressRule returns a new Ingress Rule generated for the solr metrics endpoint
// This is not currently used, as an ingress is not created for the metrics endpoint.
// solrCloud: SolrCloud instance
// nodeName: string Name of the node
// ingressBaseDomain: string base domain for the ingress controller
func CreateMetricsIngressRule(solrPrometheusExporter *solr.SolrPrometheusExporter, ingressBaseDomain string) netv1.IngressRule {
	pathType := netv1.PathTypeImplementationSpecific
	externalAddress := solrPrometheusExporter.MetricsIngressUrl(ingressBaseDomain)
	return netv1.IngressRule{
		Host: externalAddress,
		IngressRuleValue: netv1.IngressRuleValue{
			HTTP: &netv1.HTTPIngressRuleValue{
				Paths: []netv1.HTTPIngressPath{
					{
						Backend: netv1.IngressBackend{
							ServiceName: solrPrometheusExporter.MetricsServiceName(),
							ServicePort: intstr.FromInt(ExtSolrMetricsPort),
						},
						PathType: &pathType,
					},
				},
			},
		},
	}
}

func tlsJavaOpts(tlsOptions *solr.SolrTLSOptions) []string {
	javaOpts := []string{
		"-Djavax.net.ssl.keyStore=$(SOLR_SSL_KEY_STORE)",
		"-Djavax.net.ssl.keyStorePassword=$(SOLR_SSL_KEY_STORE_PASSWORD)",
		"-Djavax.net.ssl.trustStore=$(SOLR_SSL_TRUST_STORE)",
		"-Djavax.net.ssl.trustStorePassword=$(SOLR_SSL_TRUST_STORE_PASSWORD)",
		"-Djavax.net.ssl.keyStoreType=PKCS12",
		"-Djavax.net.ssl.trustStoreType=PKCS12",
		"-Dsolr.ssl.checkPeerName=$(SOLR_SSL_CHECK_PEER_NAME)",
	}

	if tlsOptions.VerifyClientHostname {
		javaOpts = append(javaOpts, "-Dsolr.jetty.ssl.verifyClientHostName=HTTPS")
	}

	return javaOpts
}
