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
	"crypto/sha256"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	solr "github.com/apache/solr-operator/api/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	SolrClientPortName  = "solr-client"
	BackupRestoreVolume = "backup-restore"

	SolrNodeContainer = "solrcloud-node"

	DefaultSolrUser  = 8983
	DefaultSolrGroup = 8983

	SolrStorageFinalizer             = "storage.finalizers.solr.apache.org"
	SolrZKConnectionStringAnnotation = "solr.apache.org/zkConnectionString"
	SolrPVCTechnologyLabel           = "solr.apache.org/technology"
	SolrCloudPVCTechnology           = "solr-cloud"
	SolrPVCStorageLabel              = "solr.apache.org/storage"
	SolrCloudPVCDataStorage          = "data"
	SolrPVCInstanceLabel             = "solr.apache.org/instance"
	SolrXmlMd5Annotation             = "solr.apache.org/solrXmlMd5"
	SolrTlsCertMd5Annotation         = "solr.apache.org/tlsCertMd5"
	SolrXmlFile                      = "solr.xml"
	LogXmlMd5Annotation              = "solr.apache.org/logXmlMd5"
	LogXmlFile                       = "log4j2.xml"
	SecurityJsonFile                 = "security.json"
	BasicAuthMd5Annotation           = "solr.apache.org/basicAuthMd5"
	DefaultProbePath                 = "/admin/info/system"

	DefaultStatefulSetPodManagementPolicy = appsv1.ParallelPodManagement

	DefaultLivenessProbeInitialDelaySeconds = 20
	DefaultLivenessProbeTimeoutSeconds      = 1
	DefaultLivenessProbeSuccessThreshold    = 1
	DefaultLivenessProbeFailureThreshold    = 3
	DefaultLivenessProbePeriodSeconds       = 10

	DefaultReadinessProbeInitialDelaySeconds = 15
	DefaultReadinessProbeTimeoutSeconds      = 1
	DefaultReadinessProbeSuccessThreshold    = 1
	DefaultReadinessProbeFailureThreshold    = 3
	DefaultReadinessProbePeriodSeconds       = 5

	DefaultStartupProbeInitialDelaySeconds = 20
	DefaultStartupProbeTimeoutSeconds      = 30
	DefaultStartupProbeSuccessThreshold    = 1
	DefaultStartupProbeFailureThreshold    = 15
	DefaultStartupProbePeriodSeconds       = 10

	DefaultKeyStorePath         = "/var/solr/tls"
	DefaultWritableKeyStorePath = "/var/solr/tls/pkcs12"
	TLSCertKey                  = "tls.crt"
	TLSKeyKey                   = "tls.key"
	DefaultTrustStorePath       = "/var/solr/tls-truststore"
	InitdbPath                  = "/docker-entrypoint-initdb.d"
)

// GenerateStatefulSet returns a new appsv1.StatefulSet pointer generated for the SolrCloud instance
// object: SolrCloud instance
// replicas: the number of replicas for the SolrCloud instance
// storage: the size of the storage for the SolrCloud instance (e.g. 100Gi)
// zkConnectionString: the connectionString of the ZK instance to connect to
func GenerateStatefulSet(solrCloud *solr.SolrCloud, solrCloudStatus *solr.SolrCloudStatus, hostNameIPs map[string]string, reconcileConfigInfo map[string]string, createPkcs12InitContainer bool, tlsCertMd5 string) *appsv1.StatefulSet {
	terminationGracePeriod := int64(60)
	solrPodPort := solrCloud.Spec.SolrAddressability.PodPort
	fsGroup := int64(DefaultSolrGroup)
	defaultMode := int32(420)

	probeScheme := corev1.URISchemeHTTP
	if solrCloud.Spec.SolrTLS != nil {
		probeScheme = corev1.URISchemeHTTPS
	}

	defaultHandler := corev1.Handler{
		HTTPGet: &corev1.HTTPGetAction{
			Scheme: probeScheme,
			Path:   "/solr" + DefaultProbePath,
			Port:   intstr.FromInt(solrPodPort),
		},
	}

	labels := solrCloud.SharedLabelsWith(solrCloud.GetLabels())
	selectorLabels := solrCloud.SharedLabels()

	labels["technology"] = solr.SolrTechnologyLabel
	selectorLabels["technology"] = solr.SolrTechnologyLabel

	annotations := map[string]string{
		SolrZKConnectionStringAnnotation: solrCloudStatus.ZkConnectionString(),
	}

	podLabels := labels

	customSSOptions := solrCloud.Spec.CustomSolrKubeOptions.StatefulSetOptions
	if nil != customSSOptions {
		labels = MergeLabelsOrAnnotations(labels, customSSOptions.Labels)
		annotations = MergeLabelsOrAnnotations(annotations, customSSOptions.Annotations)
	}

	customPodOptions := solrCloud.Spec.CustomSolrKubeOptions.PodOptions
	var podAnnotations map[string]string
	if nil != customPodOptions {
		podLabels = MergeLabelsOrAnnotations(podLabels, customPodOptions.Labels)
		podAnnotations = customPodOptions.Annotations

		if customPodOptions.TerminationGracePeriodSeconds != nil {
			terminationGracePeriod = *customPodOptions.TerminationGracePeriodSeconds
		}
	}

	// Keep track of the SolrOpts that the Solr Operator needs to set
	// These will be added to the SolrOpts given by the user.
	allSolrOpts := []string{"-DhostPort=$(SOLR_NODE_PORT)"}

	// Volumes & Mounts
	solrVolumes := []corev1.Volume{
		{
			Name: "solr-xml",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: reconcileConfigInfo[SolrXmlFile],
					},
					Items: []corev1.KeyToPath{
						{
							Key:  SolrXmlFile,
							Path: SolrXmlFile,
						},
					},
					DefaultMode: &defaultMode,
				},
			},
		},
	}

	solrDataVolumeName := "data"
	volumeMounts := []corev1.VolumeMount{{Name: solrDataVolumeName, MountPath: "/var/solr/data"}}

	if solrCloud.Spec.SolrTLS != nil && solrCloud.Spec.SolrTLS.MountedTLSDir == nil {
		solrVolumes = append(solrVolumes, tlsVolumes(solrCloud.Spec.SolrTLS, createPkcs12InitContainer)...)
		volumeMounts = append(volumeMounts, tlsVolumeMounts(solrCloud.Spec.SolrTLS, createPkcs12InitContainer)...)
	}

	var pvcs []corev1.PersistentVolumeClaim
	if solrCloud.UsesPersistentStorage() {
		pvc := solrCloud.Spec.StorageOptions.PersistentStorage.PersistentVolumeClaimTemplate.DeepCopy()

		// Set the default name of the pvc
		if pvc.ObjectMeta.Name == "" {
			pvc.ObjectMeta.Name = solrDataVolumeName
		}

		// Set some defaults in the PVC Spec
		if len(pvc.Spec.AccessModes) == 0 {
			pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			}
		}
		if pvc.Spec.VolumeMode == nil {
			temp := corev1.PersistentVolumeFilesystem
			pvc.Spec.VolumeMode = &temp
		}

		//  Add internally-used labels.
		internalLabels := map[string]string{
			SolrPVCTechnologyLabel: SolrCloudPVCTechnology,
			SolrPVCStorageLabel:    SolrCloudPVCDataStorage,
			SolrPVCInstanceLabel:   solrCloud.Name,
		}
		pvc.ObjectMeta.Labels = MergeLabelsOrAnnotations(internalLabels, pvc.ObjectMeta.Labels)

		pvcs = []corev1.PersistentVolumeClaim{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:        pvc.ObjectMeta.Name,
					Labels:      pvc.ObjectMeta.Labels,
					Annotations: pvc.ObjectMeta.Annotations,
				},
				Spec: pvc.Spec,
			},
		}
	} else {
		ephemeralVolume := corev1.Volume{
			Name:         solrDataVolumeName,
			VolumeSource: corev1.VolumeSource{},
		}
		if solrCloud.Spec.StorageOptions.EphemeralStorage != nil {
			if nil != solrCloud.Spec.StorageOptions.EphemeralStorage.HostPath {
				ephemeralVolume.VolumeSource.HostPath = solrCloud.Spec.StorageOptions.EphemeralStorage.HostPath
			} else if nil != solrCloud.Spec.StorageOptions.EphemeralStorage.EmptyDir {
				ephemeralVolume.VolumeSource.EmptyDir = solrCloud.Spec.StorageOptions.EphemeralStorage.EmptyDir
			} else {
				ephemeralVolume.VolumeSource.EmptyDir = &corev1.EmptyDirVolumeSource{}
			}
		} else {
			ephemeralVolume.VolumeSource.EmptyDir = &corev1.EmptyDirVolumeSource{}
		}
		solrVolumes = append(solrVolumes, ephemeralVolume)
	}
	// Add backup volumes
	if solrCloud.Spec.StorageOptions.BackupRestoreOptions != nil {
		solrVolumes = append(solrVolumes, corev1.Volume{
			Name:         BackupRestoreVolume,
			VolumeSource: solrCloud.Spec.StorageOptions.BackupRestoreOptions.Volume,
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      BackupRestoreVolume,
			MountPath: BaseBackupRestorePath,
			SubPath:   BackupRestoreSubPathForCloud(solrCloud.Spec.StorageOptions.BackupRestoreOptions.Directory, solrCloud.Name),
			ReadOnly:  false,
		})
	}

	if nil != customPodOptions {
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

	// Auto-TLS uses an initContainer to create a script in the initdb, so mount that if it has not already been mounted
	if solrCloud.Spec.SolrTLS != nil && solrCloud.Spec.SolrTLS.MountedTLSDir != nil {
		var initdbMount *corev1.VolumeMount
		for _, mount := range volumeMounts {
			if mount.MountPath == InitdbPath {
				initdbMount = &mount
				break
			}
		}
		if initdbMount == nil {
			solrVolumes = append(solrVolumes, corev1.Volume{Name: "initdb", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
			volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "initdb", MountPath: InitdbPath})
		}
	}

	// Host Aliases
	hostAliases := make([]corev1.HostAlias, len(hostNameIPs))
	if len(hostAliases) == 0 {
		hostAliases = nil
	} else {
		hostNames := make([]string, len(hostNameIPs))
		index := 0
		for hostName := range hostNameIPs {
			hostNames[index] = hostName
			index += 1
		}

		sort.Strings(hostNames)

		for index, hostName := range hostNames {
			hostAliases[index] = corev1.HostAlias{
				IP:        hostNameIPs[hostName],
				Hostnames: []string{hostName},
			}
			index++
		}
	}

	solrHostName := solrCloud.AdvertisedNodeHost("$(POD_HOSTNAME)")
	solrAdressingPort := solrCloud.NodePort()

	// Solr can take longer than SOLR_STOP_WAIT to run solr stop, give it a few extra seconds before forcefully killing the pod.
	solrStopWait := terminationGracePeriod - 5
	if solrStopWait < 0 {
		solrStopWait = 0
	}

	// Environment Variables
	envVars := []corev1.EnvVar{
		{
			Name:  "SOLR_JAVA_MEM",
			Value: solrCloud.Spec.SolrJavaMem,
		},
		{
			Name:  "SOLR_HOME",
			Value: "/var/solr/data",
		},
		{
			// This is the port that jetty will listen on
			Name:  "SOLR_PORT",
			Value: strconv.Itoa(solrPodPort),
		},
		{
			// This is the port that the Solr Node will advertise itself as listening on in live_nodes
			Name:  "SOLR_NODE_PORT",
			Value: strconv.Itoa(solrAdressingPort),
		},
		{
			Name: "POD_HOSTNAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath:  "metadata.name",
					APIVersion: "v1",
				},
			},
		},
		{
			Name:  "SOLR_HOST",
			Value: solrHostName,
		},
		{
			Name:  "SOLR_LOG_LEVEL",
			Value: solrCloud.Spec.SolrLogLevel,
		},
		{
			Name:  "GC_TUNE",
			Value: solrCloud.Spec.SolrGCTune,
		},
		{
			Name:  "SOLR_STOP_WAIT",
			Value: strconv.FormatInt(solrStopWait, 10),
		},
	}

	// Add all necessary information for connection to Zookeeper
	zkEnvVars, zkSolrOpt, hasChroot := createZkConnectionEnvVars(solrCloud, solrCloudStatus)
	if zkSolrOpt != "" {
		allSolrOpts = append(allSolrOpts, zkSolrOpt)
	}
	envVars = append(envVars, zkEnvVars...)

	// Only have a postStart command to create the chRoot, if it is not '/' (which does not need to be created)
	var postStart *corev1.Handler
	if hasChroot {
		postStart = &corev1.Handler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-c", "solr zk ls ${ZK_CHROOT} -z ${ZK_SERVER} || solr zk mkroot ${ZK_CHROOT} -z ${ZK_SERVER}"},
			},
		}
	}

	// Append TLS related env vars if enabled
	if solrCloud.Spec.SolrTLS != nil {
		envVars = append(envVars, TLSEnvVars(solrCloud.Spec.SolrTLS, createPkcs12InitContainer)...)
	}

	// Add Custom EnvironmentVariables to the solr container
	if nil != customPodOptions {
		envVars = append(envVars, customPodOptions.EnvVariables...)
	}

	// Did the user provide a custom log config?
	if reconcileConfigInfo[LogXmlFile] != "" {
		if reconcileConfigInfo[LogXmlMd5Annotation] != "" {
			if podAnnotations == nil {
				podAnnotations = make(map[string]string, 1)
			}
			podAnnotations[LogXmlMd5Annotation] = reconcileConfigInfo[LogXmlMd5Annotation]
		}

		// cannot use /var/solr as a mountPath, so mount the custom log config
		// in a sub-dir named after the user-provided ConfigMap
		volMount, envVar, newVolume := setupVolumeMountForUserProvidedConfigMapEntry(reconcileConfigInfo, LogXmlFile, solrVolumes, "LOG4J_PROPS")
		volumeMounts = append(volumeMounts, *volMount)
		envVars = append(envVars, *envVar)
		if newVolume != nil {
			solrVolumes = append(solrVolumes, *newVolume)
		}
	}

	if (solrCloud.Spec.SolrTLS != nil && solrCloud.Spec.SolrTLS.ClientAuth != solr.None) || (solrCloud.Spec.SolrSecurity != nil && solrCloud.Spec.SolrSecurity.ProbesRequireAuth) {
		probeCommand, vol, volMount := configureSecureProbeCommand(solrCloud, defaultHandler.HTTPGet)
		if vol != nil {
			solrVolumes = append(solrVolumes, *vol)
		}
		if volMount != nil {
			volumeMounts = append(volumeMounts, *volMount)
		}
		// reset the defaultHandler for the probes to invoke the SolrCLI api action instead of HTTP
		defaultHandler = corev1.Handler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", probeCommand}}}
	}

	// track the MD5 of the custom solr.xml in the pod spec annotations,
	// so we get a rolling restart when the configMap changes
	if reconcileConfigInfo[SolrXmlMd5Annotation] != "" {
		if podAnnotations == nil {
			podAnnotations = make(map[string]string, 1)
		}
		podAnnotations[SolrXmlMd5Annotation] = reconcileConfigInfo[SolrXmlMd5Annotation]
	}

	// track the MD5 of the TLS cert (from secret) to trigger restarts if the cert changes
	if solrCloud.Spec.SolrTLS != nil && solrCloud.Spec.SolrTLS.RestartOnTLSSecretUpdate && tlsCertMd5 != "" {
		if podAnnotations == nil {
			podAnnotations = make(map[string]string, 1)
		}
		podAnnotations[SolrTlsCertMd5Annotation] = tlsCertMd5
	}

	if solrCloud.Spec.SolrOpts != "" {
		allSolrOpts = append(allSolrOpts, solrCloud.Spec.SolrOpts)
	}

	// Add SOLR_OPTS last, so that it can use values from all of the other ENV_VARS
	envVars = append(envVars, corev1.EnvVar{
		Name:  "SOLR_OPTS",
		Value: strings.Join(allSolrOpts, " "),
	})

	initContainers := generateSolrSetupInitContainers(solrCloud, solrCloudStatus, solrDataVolumeName, reconcileConfigInfo)

	// Add user defined additional init containers
	if customPodOptions != nil && len(customPodOptions.InitContainers) > 0 {
		initContainers = append(initContainers, customPodOptions.InitContainers...)
	}

	containers := []corev1.Container{
		{
			Name:            SolrNodeContainer,
			Image:           solrCloud.Spec.SolrImage.ToImageName(),
			ImagePullPolicy: solrCloud.Spec.SolrImage.PullPolicy,
			Ports: []corev1.ContainerPort{
				{
					ContainerPort: int32(solrPodPort),
					Name:          SolrClientPortName,
					Protocol:      "TCP",
				},
			},
			LivenessProbe: &corev1.Probe{
				InitialDelaySeconds: DefaultLivenessProbeInitialDelaySeconds,
				TimeoutSeconds:      DefaultLivenessProbeTimeoutSeconds,
				SuccessThreshold:    DefaultLivenessProbeSuccessThreshold,
				FailureThreshold:    DefaultLivenessProbeFailureThreshold,
				PeriodSeconds:       DefaultLivenessProbePeriodSeconds,
				Handler:             defaultHandler,
			},
			ReadinessProbe: &corev1.Probe{
				InitialDelaySeconds: DefaultReadinessProbeInitialDelaySeconds,
				TimeoutSeconds:      DefaultReadinessProbeTimeoutSeconds,
				SuccessThreshold:    DefaultReadinessProbeSuccessThreshold,
				FailureThreshold:    DefaultReadinessProbeFailureThreshold,
				PeriodSeconds:       DefaultReadinessProbePeriodSeconds,
				Handler:             defaultHandler,
			},
			VolumeMounts: volumeMounts,
			Env:          envVars,
			Lifecycle: &corev1.Lifecycle{
				PostStart: postStart,
				PreStop: &corev1.Handler{
					Exec: &corev1.ExecAction{
						Command: []string{"solr", "stop", "-p", strconv.Itoa(solrPodPort)},
					},
				},
			},
		},
	}

	// Add user defined additional sidecar containers
	if customPodOptions != nil && len(customPodOptions.SidecarContainers) > 0 {
		containers = append(containers, customPodOptions.SidecarContainers...)
	}

	// Decide which update strategy to use
	updateStrategy := appsv1.OnDeleteStatefulSetStrategyType
	if solrCloud.Spec.UpdateStrategy.Method == solr.StatefulSetUpdate {
		// Only use the rolling update strategy if the StatefulSetUpdate method is specified.
		updateStrategy = appsv1.RollingUpdateStatefulSetStrategyType
	}

	// Determine which podManagementPolicy to use for the statefulSet
	podManagementPolicy := DefaultStatefulSetPodManagementPolicy
	if solrCloud.Spec.CustomSolrKubeOptions.StatefulSetOptions != nil && solrCloud.Spec.CustomSolrKubeOptions.StatefulSetOptions.PodManagementPolicy != "" {
		podManagementPolicy = solrCloud.Spec.CustomSolrKubeOptions.StatefulSetOptions.PodManagementPolicy
	}

	if createPkcs12InitContainer {
		pkcs12InitContainer := generatePkcs12InitContainer(solrCloud.Spec.SolrTLS,
			solrCloud.Spec.SolrImage.ToImageName(), solrCloud.Spec.SolrImage.PullPolicy)
		initContainers = append(initContainers, pkcs12InitContainer)
	}

	// Create the Stateful Set
	stateful := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrCloud.StatefulSetName(),
			Namespace:   solrCloud.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels,
			},
			ServiceName:         solrCloud.HeadlessServiceName(),
			Replicas:            solrCloud.Spec.Replicas,
			PodManagementPolicy: podManagementPolicy,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: updateStrategy,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},

				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &terminationGracePeriod,
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup: &fsGroup,
					},
					Volumes:        solrVolumes,
					InitContainers: initContainers,
					HostAliases:    hostAliases,
					Containers:     containers,
				},
			},
			VolumeClaimTemplates: pvcs,
		},
	}

	var imagePullSecrets []corev1.LocalObjectReference

	if customPodOptions != nil {
		imagePullSecrets = customPodOptions.ImagePullSecrets
	}

	if solrCloud.Spec.SolrImage.ImagePullSecret != "" {
		imagePullSecrets = append(
			imagePullSecrets,
			corev1.LocalObjectReference{Name: solrCloud.Spec.SolrImage.ImagePullSecret},
		)
	}

	stateful.Spec.Template.Spec.ImagePullSecrets = imagePullSecrets

	if nil != customPodOptions {
		if customPodOptions.ServiceAccountName != "" {
			stateful.Spec.Template.Spec.ServiceAccountName = customPodOptions.ServiceAccountName
		}

		if customPodOptions.Affinity != nil {
			stateful.Spec.Template.Spec.Affinity = customPodOptions.Affinity
		}

		if customPodOptions.Resources.Limits != nil || customPodOptions.Resources.Requests != nil {
			stateful.Spec.Template.Spec.Containers[0].Resources = customPodOptions.Resources
		}

		if customPodOptions.PodSecurityContext != nil {
			stateful.Spec.Template.Spec.SecurityContext = customPodOptions.PodSecurityContext
		}

		if customPodOptions.Tolerations != nil {
			stateful.Spec.Template.Spec.Tolerations = customPodOptions.Tolerations
		}

		if customPodOptions.NodeSelector != nil {
			stateful.Spec.Template.Spec.NodeSelector = customPodOptions.NodeSelector
		}

		if customPodOptions.LivenessProbe != nil {
			stateful.Spec.Template.Spec.Containers[0].LivenessProbe = fillProbe(*customPodOptions.LivenessProbe, DefaultLivenessProbeInitialDelaySeconds, DefaultLivenessProbeTimeoutSeconds, DefaultLivenessProbeSuccessThreshold, DefaultLivenessProbeFailureThreshold, DefaultLivenessProbePeriodSeconds, &defaultHandler)
		}

		if customPodOptions.ReadinessProbe != nil {
			stateful.Spec.Template.Spec.Containers[0].ReadinessProbe = fillProbe(*customPodOptions.ReadinessProbe, DefaultReadinessProbeInitialDelaySeconds, DefaultReadinessProbeTimeoutSeconds, DefaultReadinessProbeSuccessThreshold, DefaultReadinessProbeFailureThreshold, DefaultReadinessProbePeriodSeconds, &defaultHandler)
		}

		if customPodOptions.StartupProbe != nil {
			stateful.Spec.Template.Spec.Containers[0].StartupProbe = fillProbe(*customPodOptions.StartupProbe, DefaultStartupProbeInitialDelaySeconds, DefaultStartupProbeTimeoutSeconds, DefaultStartupProbeSuccessThreshold, DefaultStartupProbeFailureThreshold, DefaultStartupProbePeriodSeconds, &defaultHandler)
		}

		if customPodOptions.PriorityClassName != "" {
			stateful.Spec.Template.Spec.PriorityClassName = customPodOptions.PriorityClassName
		}
	}

	return stateful
}

func generateSolrSetupInitContainers(solrCloud *solr.SolrCloud, solrCloudStatus *solr.SolrCloudStatus, solrDataVolumeName string, reconcileConfigInfo map[string]string) (containers []corev1.Container) {
	// The setup of the solr.xml will always be necessary
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "solr-xml",
			MountPath: "/tmp",
		},
		{
			Name:      solrDataVolumeName,
			MountPath: "/tmp-config",
		},
	}
	setupCommands := []string{"cp /tmp/solr.xml /tmp-config/solr.xml"}

	// Add prep for backup-restore volume
	// This entails setting the correct permissions for the directory
	if solrCloud.Spec.StorageOptions.BackupRestoreOptions != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      BackupRestoreVolume,
			MountPath: "/backup-restore",
			SubPath:   BackupRestoreSubPathForCloud(solrCloud.Spec.StorageOptions.BackupRestoreOptions.Directory, solrCloud.Name),
			ReadOnly:  false,
		})

		setupCommands = append(setupCommands, fmt.Sprintf("chown -R %d:%d /backup-restore", DefaultSolrUser, DefaultSolrGroup))
	}

	volumePrepInitContainer := corev1.Container{
		Name:            "cp-solr-xml",
		Image:           solrCloud.Spec.BusyBoxImage.ToImageName(),
		ImagePullPolicy: solrCloud.Spec.BusyBoxImage.PullPolicy,
		Command:         []string{"sh", "-c", strings.Join(setupCommands, " && ")},
		VolumeMounts:    volumeMounts,
	}

	containers = append(containers, volumePrepInitContainer)

	if hasZKSetupContainer, zkSetupContainer := generateZKInteractionInitContainer(solrCloud, solrCloudStatus, reconcileConfigInfo); hasZKSetupContainer {
		containers = append(containers, zkSetupContainer)
	}

	if solrCloud.Spec.SolrTLS != nil && solrCloud.Spec.SolrTLS.MountedTLSDir != nil {
		containers = append(containers, generateTLSInitdbScriptInitContainer(solrCloud))
	}

	return containers
}

// GenerateConfigMap returns a new corev1.ConfigMap pointer generated for the SolrCloud instance solr.xml
// solrCloud: SolrCloud instance
func GenerateConfigMap(solrCloud *solr.SolrCloud) *corev1.ConfigMap {
	labels := solrCloud.SharedLabelsWith(solrCloud.GetLabels())
	var annotations map[string]string

	customOptions := solrCloud.Spec.CustomSolrKubeOptions.ConfigMapOptions
	if nil != customOptions {
		labels = MergeLabelsOrAnnotations(labels, customOptions.Labels)
		annotations = MergeLabelsOrAnnotations(annotations, customOptions.Annotations)
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrCloud.ConfigMapName(),
			Namespace:   solrCloud.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Data: map[string]string{
			"solr.xml": `<?xml version="1.0" encoding="UTF-8" ?>
<solr>
  <solrcloud>
    <str name="host">${host:}</str>
    <int name="hostPort">${hostPort:80}</int>
    <str name="hostContext">${hostContext:solr}</str>
    <bool name="genericCoreNodeNames">${genericCoreNodeNames:true}</bool>
    <int name="zkClientTimeout">${zkClientTimeout:30000}</int>
    <int name="distribUpdateSoTimeout">${distribUpdateSoTimeout:600000}</int>
    <int name="distribUpdateConnTimeout">${distribUpdateConnTimeout:60000}</int>
    <str name="zkCredentialsProvider">${zkCredentialsProvider:org.apache.solr.common.cloud.DefaultZkCredentialsProvider}</str>
    <str name="zkACLProvider">${zkACLProvider:org.apache.solr.common.cloud.DefaultZkACLProvider}</str>
  </solrcloud>
  <shardHandlerFactory name="shardHandlerFactory"
    class="HttpShardHandlerFactory">
    <int name="socketTimeout">${socketTimeout:600000}</int>
    <int name="connTimeout">${connTimeout:60000}</int>
  </shardHandlerFactory>
</solr>
`,
		},
	}

	return configMap
}

// fillProbe builds the probe logic used for pod liveness, readiness, startup checks
func fillProbe(customProbe corev1.Probe, defaultInitialDelaySeconds int32, defaultTimeoutSeconds int32, defaultSuccessThreshold int32, defaultFailureThreshold int32, defaultPeriodSeconds int32, defaultHandler *corev1.Handler) *corev1.Probe {
	probe := &corev1.Probe{
		InitialDelaySeconds: defaultInitialDelaySeconds,
		TimeoutSeconds:      defaultTimeoutSeconds,
		SuccessThreshold:    defaultSuccessThreshold,
		FailureThreshold:    defaultFailureThreshold,
		PeriodSeconds:       defaultPeriodSeconds,
		Handler:             *defaultHandler,
	}

	if customProbe.InitialDelaySeconds != 0 {
		probe.InitialDelaySeconds = customProbe.InitialDelaySeconds
	}

	if customProbe.TimeoutSeconds != 0 {
		probe.TimeoutSeconds = customProbe.TimeoutSeconds
	}

	if customProbe.SuccessThreshold != 0 {
		probe.SuccessThreshold = customProbe.SuccessThreshold
	}

	if customProbe.FailureThreshold != 0 {
		probe.FailureThreshold = customProbe.FailureThreshold
	}

	if customProbe.PeriodSeconds != 0 {
		probe.PeriodSeconds = customProbe.PeriodSeconds
	}

	if customProbe.Handler.Exec != nil || customProbe.Handler.HTTPGet != nil || customProbe.Handler.TCPSocket != nil {
		probe.Handler = customProbe.Handler
	}

	return probe
}

// GenerateCommonService returns a new corev1.Service pointer generated for the entire SolrCloud instance
// solrCloud: SolrCloud instance
func GenerateCommonService(solrCloud *solr.SolrCloud) *corev1.Service {
	labels := solrCloud.SharedLabelsWith(solrCloud.GetLabels())
	labels["service-type"] = "common"

	selectorLabels := solrCloud.SharedLabels()
	selectorLabels["technology"] = solr.SolrTechnologyLabel

	var annotations map[string]string

	// Add externalDNS annotation if necessary
	extOpts := solrCloud.Spec.SolrAddressability.External
	if extOpts != nil && extOpts.Method == solr.ExternalDNS && !extOpts.HideCommon {
		annotations = make(map[string]string, 1)
		urls := []string{solrCloud.ExternalDnsDomain(extOpts.DomainName)}
		for _, domain := range extOpts.AdditionalDomainNames {
			urls = append(urls, solrCloud.ExternalDnsDomain(domain))
		}
		annotations["external-dns.alpha.kubernetes.io/hostname"] = strings.Join(urls, ",")
	}

	customOptions := solrCloud.Spec.CustomSolrKubeOptions.CommonServiceOptions
	if nil != customOptions {
		labels = MergeLabelsOrAnnotations(labels, customOptions.Labels)
		annotations = MergeLabelsOrAnnotations(annotations, customOptions.Annotations)
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrCloud.CommonServiceName(),
			Namespace:   solrCloud.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: SolrClientPortName, Port: int32(solrCloud.Spec.SolrAddressability.CommonServicePort), Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString(SolrClientPortName)},
			},
			Selector: selectorLabels,
		},
	}
	return service
}

// GenerateHeadlessService returns a new Headless corev1.Service pointer generated for the SolrCloud instance
// The PublishNotReadyAddresses option is set as true, because we want each pod to be reachable no matter the readiness of the pod.
// solrCloud: SolrCloud instance
func GenerateHeadlessService(solrCloud *solr.SolrCloud) *corev1.Service {
	labels := solrCloud.SharedLabelsWith(solrCloud.GetLabels())
	labels["service-type"] = "headless"

	selectorLabels := solrCloud.SharedLabels()
	selectorLabels["technology"] = solr.SolrTechnologyLabel

	var annotations map[string]string

	// Add externalDNS annotation if necessary
	extOpts := solrCloud.Spec.SolrAddressability.External
	if extOpts != nil && extOpts.Method == solr.ExternalDNS && !extOpts.HideNodes {
		annotations = make(map[string]string, 1)
		urls := []string{solrCloud.ExternalDnsDomain(extOpts.DomainName)}
		for _, domain := range extOpts.AdditionalDomainNames {
			urls = append(urls, solrCloud.ExternalDnsDomain(domain))
		}
		annotations["external-dns.alpha.kubernetes.io/hostname"] = strings.Join(urls, ",")
	}

	customOptions := solrCloud.Spec.CustomSolrKubeOptions.HeadlessServiceOptions
	if nil != customOptions {
		labels = MergeLabelsOrAnnotations(labels, customOptions.Labels)
		annotations = MergeLabelsOrAnnotations(annotations, customOptions.Annotations)
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrCloud.HeadlessServiceName(),
			Namespace:   solrCloud.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: SolrClientPortName, Port: int32(solrCloud.NodePort()), Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString(SolrClientPortName)},
			},
			Selector:                 selectorLabels,
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
		},
	}
	return service
}

// GenerateNodeService returns a new External corev1.Service pointer generated for the given Solr Node.
// The PublishNotReadyAddresses option is set as true, because we want each pod to be reachable no matter the readiness of the pod.
// solrCloud: SolrCloud instance
// nodeName: string node
func GenerateNodeService(solrCloud *solr.SolrCloud, nodeName string) *corev1.Service {
	labels := solrCloud.SharedLabelsWith(solrCloud.GetLabels())
	labels["service-type"] = "external"

	selectorLabels := solrCloud.SharedLabels()
	selectorLabels["technology"] = solr.SolrTechnologyLabel
	selectorLabels["statefulset.kubernetes.io/pod-name"] = nodeName

	var annotations map[string]string

	customOptions := solrCloud.Spec.CustomSolrKubeOptions.NodeServiceOptions
	if nil != customOptions {
		labels = MergeLabelsOrAnnotations(labels, customOptions.Labels)
		annotations = MergeLabelsOrAnnotations(annotations, customOptions.Annotations)
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        nodeName,
			Namespace:   solrCloud.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Selector: selectorLabels,
			Ports: []corev1.ServicePort{
				{Name: SolrClientPortName, Port: int32(solrCloud.NodePort()), Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString(SolrClientPortName)},
			},
			PublishNotReadyAddresses: true,
		},
	}
	return service
}

// GenerateIngress returns a new Ingress pointer generated for the entire SolrCloud, pointing to all instances
// solrCloud: SolrCloud instance
// nodeStatuses: []SolrNodeStatus the nodeStatuses
func GenerateIngress(solrCloud *solr.SolrCloud, nodeNames []string) (ingress *netv1.Ingress) {
	labels := solrCloud.SharedLabelsWith(solrCloud.GetLabels())
	var annotations map[string]string

	customOptions := solrCloud.Spec.CustomSolrKubeOptions.IngressOptions
	if nil != customOptions {
		labels = MergeLabelsOrAnnotations(labels, customOptions.Labels)
		annotations = MergeLabelsOrAnnotations(annotations, customOptions.Annotations)
	}

	extOpts := solrCloud.Spec.SolrAddressability.External

	// Create advertised domain name and possible additional domain names
	rules := CreateSolrIngressRules(solrCloud, nodeNames, append([]string{extOpts.DomainName}, extOpts.AdditionalDomainNames...))

	var ingressTLS []netv1.IngressTLS
	if solrCloud.Spec.SolrTLS != nil {
		if annotations == nil {
			annotations = make(map[string]string, 1)
		}
		_, ok := annotations["nginx.ingress.kubernetes.io/backend-protocol"]
		if !ok {
			annotations["nginx.ingress.kubernetes.io/backend-protocol"] = "HTTPS"
		}
		ingressTLS = append(ingressTLS, netv1.IngressTLS{SecretName: solrCloud.Spec.SolrTLS.PKCS12Secret.Name})
	}

	ingress = &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrCloud.CommonIngressName(),
			Namespace:   solrCloud.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: netv1.IngressSpec{
			Rules: rules,
			TLS:   ingressTLS,
		},
	}
	return ingress
}

// CreateSolrIngressRules returns all applicable ingress rules for a cloud.
// solrCloud: SolrCloud instance
// nodeNames: the names for each of the solr pods
// domainName: string Domain for the ingress rule to use
func CreateSolrIngressRules(solrCloud *solr.SolrCloud, nodeNames []string, domainNames []string) []netv1.IngressRule {
	var ingressRules []netv1.IngressRule
	if !solrCloud.Spec.SolrAddressability.External.HideCommon {
		for _, domainName := range domainNames {
			ingressRules = append(ingressRules, CreateCommonIngressRule(solrCloud, domainName))
		}
	}
	if !solrCloud.Spec.SolrAddressability.External.HideNodes {
		for _, nodeName := range nodeNames {
			for _, domainName := range domainNames {
				ingressRules = append(ingressRules, CreateNodeIngressRule(solrCloud, nodeName, domainName))
			}
		}
	}
	return ingressRules
}

// CreateCommonIngressRule returns a new Ingress Rule generated for a SolrCloud under the given domainName
// solrCloud: SolrCloud instance
// domainName: string Domain for the ingress rule to use
func CreateCommonIngressRule(solrCloud *solr.SolrCloud, domainName string) (ingressRule netv1.IngressRule) {
	pathType := netv1.PathTypeImplementationSpecific
	ingressRule = netv1.IngressRule{
		Host: solrCloud.ExternalCommonUrl(domainName, false),
		IngressRuleValue: netv1.IngressRuleValue{
			HTTP: &netv1.HTTPIngressRuleValue{
				Paths: []netv1.HTTPIngressPath{
					{
						Backend: netv1.IngressBackend{
							ServiceName: solrCloud.CommonServiceName(),
							ServicePort: intstr.FromInt(solrCloud.Spec.SolrAddressability.CommonServicePort),
						},
						PathType: &pathType,
					},
				},
			},
		},
	}
	return ingressRule
}

// CreateNodeIngressRule returns a new Ingress Rule generated for a specific Solr Node under the given domainName
// solrCloud: SolrCloud instance
// nodeName: string Name of the node
// domainName: string Domain for the ingress rule to use
func CreateNodeIngressRule(solrCloud *solr.SolrCloud, nodeName string, domainName string) (ingressRule netv1.IngressRule) {
	pathType := netv1.PathTypeImplementationSpecific
	ingressRule = netv1.IngressRule{
		Host: solrCloud.ExternalNodeUrl(nodeName, domainName, false),
		IngressRuleValue: netv1.IngressRuleValue{
			HTTP: &netv1.HTTPIngressRuleValue{
				Paths: []netv1.HTTPIngressPath{
					{
						Backend: netv1.IngressBackend{
							ServiceName: nodeName,
							ServicePort: intstr.FromInt(solrCloud.NodePort()),
						},
						PathType: &pathType,
					},
				},
			},
		},
	}
	return ingressRule
}

func generateTLSInitdbScriptInitContainer(solrCloud *solr.SolrCloud) corev1.Container {
	// run an initContainer that creates a script in the initdb that exports the
	// keystore secret from a file to the env before Solr is started
	shCmd := fmt.Sprintf("echo -e \"#!/bin/bash\\nexport SOLR_SSL_KEY_STORE_PASSWORD=\\`cat %s\\`\\nexport SOLR_SSL_TRUST_STORE_PASSWORD=\\`cat %s\\`\" > /docker-entrypoint-initdb.d/export-tls-vars.sh",
		solrCloud.Spec.SolrTLS.MountedTLSKeystorePasswordPath(), solrCloud.Spec.SolrTLS.MountedTLSTruststorePasswordPath())

	/*
	   Init container creates a script like:

	      #!/bin/bash
	      export SOLR_SSL_KEY_STORE_PASSWORD=`cat $MOUNTED_TLS_DIR/keystore-password`
	      export SOLR_SSL_TRUST_STORE_PASSWORD=`cat $MOUNTED_TLS_DIR/keystore-password`

	*/

	return corev1.Container{
		Name:            "export-tls-password",
		Image:           solrCloud.Spec.BusyBoxImage.ToImageName(),
		ImagePullPolicy: solrCloud.Spec.BusyBoxImage.PullPolicy,
		Command:         []string{"sh", "-c", shCmd},
		VolumeMounts:    []corev1.VolumeMount{{Name: "initdb", MountPath: InitdbPath}},
	}
}

// TODO: Have this replace the postStart hook for creating the chroot
func generateZKInteractionInitContainer(solrCloud *solr.SolrCloud, solrCloudStatus *solr.SolrCloudStatus, reconcileConfigInfo map[string]string) (bool, corev1.Container) {
	allSolrOpts := make([]string, 0)

	// Add all necessary ZK Info
	envVars, zkSolrOpt, _ := createZkConnectionEnvVars(solrCloud, solrCloudStatus)
	if zkSolrOpt != "" {
		allSolrOpts = append(allSolrOpts, zkSolrOpt)
	}

	if solrCloud.Spec.SolrOpts != "" {
		allSolrOpts = append(allSolrOpts, solrCloud.Spec.SolrOpts)
	}

	// Add SOLR_OPTS last, so that it can use values from all of the other ENV_VARS
	if len(allSolrOpts) > 0 {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "SOLR_OPTS",
			Value: strings.Join(allSolrOpts, " "),
		})
	}

	cmd := ""

	if solrCloud.Spec.SolrTLS != nil {
		cmd = "solr zk ls ${ZK_CHROOT} -z ${ZK_SERVER} || solr zk mkroot ${ZK_CHROOT} -z ${ZK_SERVER}" +
			"; /opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost ${ZK_HOST} -cmd clusterprop -name urlScheme -val https" +
			"; /opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost ${ZK_HOST} -cmd get /clusterprops.json;"
	}

	if reconcileConfigInfo[SecurityJsonFile] != "" {
		envVars = append(envVars, corev1.EnvVar{Name: "SECURITY_JSON", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: solrCloud.SecurityBootstrapSecretName()},
				Key:                  SecurityJsonFile}}})

		if cmd == "" {
			cmd += "solr zk ls ${ZK_CHROOT} -z ${ZK_SERVER} || solr zk mkroot ${ZK_CHROOT} -z ${ZK_SERVER}; "
		}
		cmd += "ZK_SECURITY_JSON=$(/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost ${ZK_HOST} -cmd get /security.json); "
		cmd += "if [ ${#ZK_SECURITY_JSON} -lt 3 ]; then echo $SECURITY_JSON > /tmp/security.json; /opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost ${ZK_HOST} -cmd putfile /security.json /tmp/security.json; echo \"put security.json in ZK\"; fi"
	}

	if cmd != "" {
		return true, corev1.Container{
			Name:                     "setup-zk",
			Image:                    solrCloud.Spec.SolrImage.ToImageName(),
			ImagePullPolicy:          solrCloud.Spec.SolrImage.PullPolicy,
			TerminationMessagePath:   "/dev/termination-log",
			TerminationMessagePolicy: "File",
			Command:                  []string{"sh", "-c", cmd},
			Env:                      envVars,
		}
	}

	return false, corev1.Container{}
}

func TLSEnvVars(opts *solr.SolrTLSOptions, createPkcs12InitContainer bool) []corev1.EnvVar {

	// Determine the correct values for the SOLR_SSL_WANT_CLIENT_AUTH and SOLR_SSL_NEED_CLIENT_AUTH vars
	wantClientAuth := "false"
	needClientAuth := "false"
	if opts.ClientAuth == solr.Need {
		needClientAuth = "true"
	} else if opts.ClientAuth == solr.Want {
		wantClientAuth = "true"
	}

	var keystoreFile string
	var passwordValueFrom *corev1.EnvVarSource
	var truststoreFile string
	var truststorePassFrom *corev1.EnvVarSource
	if opts.MountedTLSDir != nil {
		// TLS files are mounted by some external agent
		keystoreFile = opts.MountedTLSKeystorePath()
		truststoreFile = opts.MountedTLSTruststorePath()
	} else {
		// the keystore path depends on whether we're just loading it from the secret or whether
		// our initContainer has to generate it from the TLS secret using openssl
		// this complexity is due to the secret mount directory not being writable
		var keystorePath string
		if createPkcs12InitContainer {
			keystorePath = DefaultWritableKeyStorePath
		} else {
			keystorePath = DefaultKeyStorePath
		}

		keystoreFile = keystorePath + "/" + solr.DefaultPkcs12KeystoreFile
		passwordValueFrom = &corev1.EnvVarSource{SecretKeyRef: opts.KeyStorePasswordSecret}

		// If using a truststore that is different from the keystore
		truststoreFile = keystoreFile
		truststorePassFrom = passwordValueFrom
		if opts.TrustStoreSecret != nil {
			if opts.TrustStoreSecret.Name != opts.PKCS12Secret.Name {
				// trust store is in a different secret, so will be mounted in a different dir
				truststoreFile = DefaultTrustStorePath
			} else {
				// trust store is a different key in the same secret as the keystore
				truststoreFile = DefaultKeyStorePath
			}
			truststoreFile += "/" + opts.TrustStoreSecret.Key
			if opts.TrustStorePasswordSecret != nil {
				truststorePassFrom = &corev1.EnvVarSource{SecretKeyRef: opts.TrustStorePasswordSecret}
			}
		}
	}

	envVars := []corev1.EnvVar{
		{
			Name:  "SOLR_SSL_ENABLED",
			Value: "true",
		},
		{
			Name:  "SOLR_SSL_KEY_STORE",
			Value: keystoreFile,
		},
		{
			Name:  "SOLR_SSL_TRUST_STORE",
			Value: truststoreFile,
		},
		{
			Name:  "SOLR_SSL_WANT_CLIENT_AUTH",
			Value: wantClientAuth,
		},
		{
			Name:  "SOLR_SSL_NEED_CLIENT_AUTH",
			Value: needClientAuth,
		},
		{
			Name:  "SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION",
			Value: strconv.FormatBool(opts.VerifyClientHostname),
		},
		{
			Name:  "SOLR_SSL_CHECK_PEER_NAME",
			Value: strconv.FormatBool(opts.CheckPeerName),
		},
	}

	if passwordValueFrom != nil {
		envVars = append(envVars, corev1.EnvVar{Name: "SOLR_SSL_KEY_STORE_PASSWORD", ValueFrom: passwordValueFrom})
	}

	if truststorePassFrom != nil {
		envVars = append(envVars, corev1.EnvVar{Name: "SOLR_SSL_TRUST_STORE_PASSWORD", ValueFrom: truststorePassFrom})
	}

	return envVars
}

func tlsVolumeMounts(opts *solr.SolrTLSOptions, createPkcs12InitContainer bool) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{
			Name:      "keystore",
			ReadOnly:  true,
			MountPath: DefaultKeyStorePath,
		},
	}

	if opts.TrustStoreSecret != nil && opts.TrustStoreSecret.Name != opts.PKCS12Secret.Name {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "truststore",
			ReadOnly:  true,
			MountPath: DefaultTrustStorePath,
		})
	}

	// We need an initContainer to convert a TLS cert into the pkcs12 format Java wants (using openssl)
	// but openssl cannot write to the /var/solr/tls directory because of the way secret mounts work
	// so we need to mount an empty directory to write pkcs12 keystore into
	if createPkcs12InitContainer {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "pkcs12",
			ReadOnly:  false,
			MountPath: DefaultWritableKeyStorePath,
		})
	}

	return mounts
}

func tlsVolumes(opts *solr.SolrTLSOptions, createPkcs12InitContainer bool) []corev1.Volume {
	optional := false
	defaultMode := int32(0664)
	vols := []corev1.Volume{
		{
			Name: "keystore",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  opts.PKCS12Secret.Name,
					DefaultMode: &defaultMode,
					Optional:    &optional,
				},
			},
		},
	}

	// if they're using a different truststore other than the keystore, but don't mount an additional volume
	// if it's just pointing at the same secret
	if opts.TrustStoreSecret != nil && opts.TrustStoreSecret.Name != opts.PKCS12Secret.Name {
		vols = append(vols, corev1.Volume{
			Name: "truststore",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  opts.TrustStoreSecret.Name,
					DefaultMode: &defaultMode,
					Optional:    &optional,
				},
			},
		})
	}

	if createPkcs12InitContainer {
		vols = append(vols, corev1.Volume{
			Name: "pkcs12",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
	}

	return vols
}

func generatePkcs12InitContainer(opts *solr.SolrTLSOptions, imageName string, imagePullPolicy corev1.PullPolicy) corev1.Container {
	// get the keystore password from the env for generating the keystore using openssl
	passwordValueFrom := &corev1.EnvVarSource{SecretKeyRef: opts.KeyStorePasswordSecret}
	envVars := []corev1.EnvVar{
		{
			Name:      "SOLR_SSL_KEY_STORE_PASSWORD",
			ValueFrom: passwordValueFrom,
		},
	}

	cmd := "openssl pkcs12 -export -in " + DefaultKeyStorePath + "/" + TLSCertKey + " -in " + DefaultKeyStorePath +
		"/ca.crt -inkey " + DefaultKeyStorePath + "/tls.key -out " + DefaultKeyStorePath +
		"/pkcs12/" + solr.DefaultPkcs12KeystoreFile + " -passout pass:${SOLR_SSL_KEY_STORE_PASSWORD}"
	return corev1.Container{
		Name:                     "gen-pkcs12-keystore",
		Image:                    imageName,
		ImagePullPolicy:          imagePullPolicy,
		TerminationMessagePath:   "/dev/termination-log",
		TerminationMessagePolicy: "File",
		Command:                  []string{"sh", "-c", cmd},
		VolumeMounts:             tlsVolumeMounts(opts, true),
		Env:                      envVars,
	}
}

func createZkConnectionEnvVars(solrCloud *solr.SolrCloud, solrCloudStatus *solr.SolrCloudStatus) (envVars []corev1.EnvVar, solrOpt string, hasChroot bool) {
	zkConnectionStr, zkServer, zkChroot := solrCloudStatus.DissectZkInfo()
	envVars = []corev1.EnvVar{
		{
			Name:  "ZK_HOST",
			Value: zkConnectionStr,
		},
		{
			Name:  "ZK_CHROOT",
			Value: zkChroot,
		},
		{
			Name:  "ZK_SERVER",
			Value: zkServer,
		},
	}

	// Add ACL information, if given, through Env Vars
	allACL, readOnlyACL := solrCloud.Spec.ZookeeperRef.GetACLs()
	if hasACLs, aclEnvs := AddACLsToEnv(allACL, readOnlyACL); hasACLs {
		envVars = append(envVars, aclEnvs...)

		// The $SOLR_ZK_CREDS_AND_ACLS parameter does not get picked up when running solr, it must be added to the SOLR_OPTS.
		solrOpt = "$(SOLR_ZK_CREDS_AND_ACLS)"
	}

	return envVars, solrOpt, len(zkChroot) > 1
}

func setupVolumeMountForUserProvidedConfigMapEntry(reconcileConfigInfo map[string]string, fileKey string, solrVolumes []corev1.Volume, envVar string) (*corev1.VolumeMount, *corev1.EnvVar, *corev1.Volume) {
	volName := strings.ReplaceAll(fileKey, ".", "-")
	mountPath := fmt.Sprintf("/var/solr/%s", reconcileConfigInfo[fileKey])
	appendedToExisting := false
	if reconcileConfigInfo[fileKey] == reconcileConfigInfo[SolrXmlFile] {
		// the user provided a custom log4j2.xml and solr.xml, append to the volume for solr.xml created above
		for _, vol := range solrVolumes {
			if vol.Name == "solr-xml" {
				vol.ConfigMap.Items = append(vol.ConfigMap.Items, corev1.KeyToPath{Key: fileKey, Path: fileKey})
				appendedToExisting = true
				volName = vol.Name
				break
			}
		}
	}

	var vol *corev1.Volume = nil
	if !appendedToExisting {
		defaultMode := int32(420)
		vol = &corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: reconcileConfigInfo[fileKey]},
					Items:                []corev1.KeyToPath{{Key: fileKey, Path: fileKey}},
					DefaultMode:          &defaultMode,
				},
			},
		}
	}
	pathToFile := fmt.Sprintf("%s/%s", mountPath, fileKey)

	return &corev1.VolumeMount{Name: volName, MountPath: mountPath}, &corev1.EnvVar{Name: envVar, Value: pathToFile}, vol
}

func BasicAuthHeader(basicAuthSecret *corev1.Secret) string {
	creds := fmt.Sprintf("%s:%s", basicAuthSecret.Data[corev1.BasicAuthUsernameKey], basicAuthSecret.Data[corev1.BasicAuthPasswordKey])
	return "Basic " + b64.StdEncoding.EncodeToString([]byte(creds))
}

func ValidateBasicAuthSecret(basicAuthSecret *corev1.Secret) error {
	if basicAuthSecret.Type != corev1.SecretTypeBasicAuth {
		return fmt.Errorf("invalid secret type %v; user-provided secret %s must be of type: %v",
			basicAuthSecret.Type, basicAuthSecret.Name, corev1.SecretTypeBasicAuth)
	}

	if _, ok := basicAuthSecret.Data[corev1.BasicAuthUsernameKey]; !ok {
		return fmt.Errorf("%s key not found in user-provided basic-auth secret %s",
			corev1.BasicAuthUsernameKey, basicAuthSecret.Name)
	}

	if _, ok := basicAuthSecret.Data[corev1.BasicAuthPasswordKey]; !ok {
		return fmt.Errorf("%s key not found in user-provided basic-auth secret %s",
			corev1.BasicAuthPasswordKey, basicAuthSecret.Name)
	}

	return nil
}

func GenerateBasicAuthSecretWithBootstrap(solrCloud *solr.SolrCloud) (*corev1.Secret, *corev1.Secret) {

	securityBootstrapInfo := generateSecurityJson(solrCloud)

	labels := solrCloud.SharedLabelsWith(solrCloud.GetLabels())
	var annotations map[string]string
	basicAuthSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrCloud.BasicAuthSecretName(),
			Namespace:   solrCloud.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Data: map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte(solr.DefaultBasicAuthUsername),
			corev1.BasicAuthPasswordKey: securityBootstrapInfo[solr.DefaultBasicAuthUsername],
		},
		Type: corev1.SecretTypeBasicAuth,
	}

	// this secret holds the admin and solr user credentials and the security.json needed to bootstrap Solr security
	// once the security.json is created using the setup-zk initContainer, it is not updated by the operator
	boostrapSecuritySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        solrCloud.SecurityBootstrapSecretName(),
			Namespace:   solrCloud.GetNamespace(),
			Labels:      labels,
			Annotations: annotations,
		},
		Data: map[string][]byte{
			"admin":          securityBootstrapInfo["admin"],
			"solr":           securityBootstrapInfo["solr"],
			SecurityJsonFile: securityBootstrapInfo[SecurityJsonFile],
		},
		Type: corev1.SecretTypeOpaque,
	}

	return basicAuthSecret, boostrapSecuritySecret
}

func generateSecurityJson(solrCloud *solr.SolrCloud) map[string][]byte {
	blockUnknown := true

	probeRole := "\"k8s\"" // probe endpoints are secures
	if !solrCloud.Spec.SolrSecurity.ProbesRequireAuth {
		blockUnknown = false
		probeRole = "null" // a JSON null value here to allow open access
	}

	probePaths := getProbePaths(solrCloud)
	probeAuthz := ""
	for i, p := range probePaths {
		if i > 0 {
			probeAuthz += ", "
		}
		if strings.HasPrefix(p, "/solr") {
			p = p[len("/solr"):]
		}
		probeAuthz += fmt.Sprintf("{ \"name\": \"k8s-probe-%d\", \"role\":%s, \"collection\": null, \"path\":\"%s\" }", i, probeRole, p)
	}

	// Create the user accounts for security.json with random passwords
	// hashed with random salt, just as Solr's hashing works
	username := solr.DefaultBasicAuthUsername
	users := []string{"admin", username, "solr"}
	secretData := make(map[string][]byte, len(users))
	credentials := make(map[string]string, len(users))
	for _, u := range users {
		secretData[u] = randomPassword()
		credentials[u] = solrPasswordHash(secretData[u])
	}
	credentialsJson, _ := json.Marshal(credentials)

	securityJson := fmt.Sprintf(`{
      "authentication":{
        "blockUnknown": %t,
        "class":"solr.BasicAuthPlugin",
        "credentials": %s,
        "realm":"Solr Basic Auth",
        "forwardCredentials": false
      },
      "authorization": {
        "class": "solr.RuleBasedAuthorizationPlugin",
        "user-role": {
          "admin": ["admin", "k8s"],
          "%s": ["k8s"],
          "solr": ["users", "k8s"]
        },
        "permissions": [
          %s,
          { "name": "k8s-status", "role":"k8s", "collection": null, "path":"/admin/collections" },
          { "name": "k8s-metrics", "role":"k8s", "collection": null, "path":"/admin/metrics" },
          { "name": "k8s-ping", "role":"k8s", "collection": "*", "path":"/admin/ping" },
          { "name": "all", "role":["admin","users"] },
          { "name": "read", "role":["admin","users"] },
          { "name": "update", "role":["admin"] },
          { "name": "security-read", "role": "admin"},
          { "name": "security-edit", "role": "admin"}
        ]
      }
    }`, blockUnknown, credentialsJson, username, probeAuthz)

	// we need to store the security.json in the secret, otherwise we'd recompute it for every reconcile loop
	// but that doesn't work for randomized passwords ...
	secretData[SecurityJsonFile] = []byte(securityJson)

	return secretData
}

func GetCustomProbePaths(solrCloud *solr.SolrCloud) []string {
	probePaths := []string{}

	podOptions := solrCloud.Spec.CustomSolrKubeOptions.PodOptions
	if podOptions == nil {
		return probePaths
	}

	// include any custom paths
	if podOptions.ReadinessProbe != nil && podOptions.ReadinessProbe.HTTPGet != nil {
		probePaths = append(probePaths, podOptions.ReadinessProbe.HTTPGet.Path)
	}

	if podOptions.LivenessProbe != nil && podOptions.LivenessProbe.HTTPGet != nil {
		probePaths = append(probePaths, podOptions.LivenessProbe.HTTPGet.Path)
	}

	if podOptions.StartupProbe != nil && podOptions.StartupProbe.HTTPGet != nil {
		probePaths = append(probePaths, podOptions.StartupProbe.HTTPGet.Path)
	}

	return probePaths
}

// Gets a list of probe paths we need to setup authz for
func getProbePaths(solrCloud *solr.SolrCloud) []string {
	probePaths := []string{DefaultProbePath}
	probePaths = append(probePaths, GetCustomProbePaths(solrCloud)...)
	return uniqueProbePaths(probePaths)
}

func randomPassword() []byte {
	rand.Seed(time.Now().UnixNano())
	lower := "abcdefghijklmnpqrstuvwxyz" // no 'o'
	upper := strings.ToUpper(lower)
	digits := "0123456789"
	chars := lower + upper + digits + "()[]%#@-()[]%#@-"
	pass := make([]byte, 16)
	// start with a lower char and end with an upper
	pass[0] = lower[rand.Intn(len(lower))]
	pass[len(pass)-1] = upper[rand.Intn(len(upper))]
	perm := rand.Perm(len(chars))
	for i := 1; i < len(pass)-1; i++ {
		pass[i] = chars[perm[i]]
	}
	return pass
}

func randomSaltHash() []byte {
	b := make([]byte, 32)
	rand.Read(b)
	salt := sha256.Sum256(b)
	return salt[:]
}

// this mimics the password hash generation approach used by Solr
func solrPasswordHash(passBytes []byte) string {
	// combine password with salt to create the hash
	salt := randomSaltHash()
	passHashBytes := sha256.Sum256(append(salt[:], passBytes...))
	passHashBytes = sha256.Sum256(passHashBytes[:])
	passHash := b64.StdEncoding.EncodeToString(passHashBytes[:])
	return fmt.Sprintf("%s %s", passHash, b64.StdEncoding.EncodeToString(salt))
}

func uniqueProbePaths(paths []string) []string {
	keys := make(map[string]bool)
	var set []string
	for _, name := range paths {
		if _, exists := keys[name]; !exists {
			keys[name] = true
			set = append(set, name)
		}
	}
	return set
}

// When running with TLS and clientAuth=Need or if the probe endpoints require auth, we need to use a command instead of HTTP Get
// This function builds the custom probe command and returns any associated volume / mounts needed for the auth secrets
func configureSecureProbeCommand(solrCloud *solr.SolrCloud, defaultProbeGetAction *corev1.HTTPGetAction) (string, *corev1.Volume, *corev1.VolumeMount) {
	// mount the secret in a file so it gets updated; env vars do not see:
	// https://kubernetes.io/docs/concepts/configuration/secret/#environment-variables-are-not-updated-after-a-secret-update
	basicAuthOption := ""
	enableBasicAuth := ""
	var volMount *corev1.VolumeMount
	var vol *corev1.Volume
	if solrCloud.Spec.SolrSecurity != nil {
		secretName := solrCloud.BasicAuthSecretName()
		defaultMode := int32(420)
		vol = &corev1.Volume{
			Name: strings.ReplaceAll(secretName, ".", "-"),
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  secretName,
					DefaultMode: &defaultMode,
				},
			},
		}
		mountPath := fmt.Sprintf("/etc/secrets/%s", vol.Name)
		volMount = &corev1.VolumeMount{Name: vol.Name, MountPath: mountPath}
		usernameFile := fmt.Sprintf("%s/%s", mountPath, corev1.BasicAuthUsernameKey)
		passwordFile := fmt.Sprintf("%s/%s", mountPath, corev1.BasicAuthPasswordKey)
		basicAuthOption = fmt.Sprintf("-Dbasicauth=$(cat %s):$(cat %s)", usernameFile, passwordFile)
		enableBasicAuth = " -Dsolr.httpclient.builder.factory=org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory "
	}

	// Is TLS enabled? If so we need some additional SSL related props
	tlsProps := ""
	if solrCloud.Spec.SolrTLS != nil {
		if solrCloud.Spec.SolrTLS.MountedTLSDir != nil {
			keystorePasswordFile := solrCloud.Spec.SolrTLS.MountedTLSKeystorePasswordPath()
			truststorePasswordFile := solrCloud.Spec.SolrTLS.MountedTLSTruststorePasswordPath()
			tlsProps = "-Djavax.net.ssl.keyStore=$SOLR_SSL_KEY_STORE -Djavax.net.ssl.trustStore=$SOLR_SSL_TRUST_STORE " +
				"-Djavax.net.ssl.keyStorePassword=$(cat " + keystorePasswordFile + ") -Djavax.net.ssl.trustStorePassword=$(cat " + truststorePasswordFile + ")"
		} else {
			tlsProps = "-Djavax.net.ssl.keyStore=$SOLR_SSL_KEY_STORE -Djavax.net.ssl.keyStorePassword=$SOLR_SSL_KEY_STORE_PASSWORD " +
				"-Djavax.net.ssl.trustStore=$SOLR_SSL_TRUST_STORE -Djavax.net.ssl.trustStorePassword=$SOLR_SSL_TRUST_STORE_PASSWORD"
		}
	}

	javaToolOptions := strings.TrimSpace(basicAuthOption + " " + tlsProps)

	// construct the probe command to invoke the SolrCLI "api" action
	//
	// and yes, this is ugly, but bin/solr doesn't expose the "api" action (as of 8.8.0) so we have to invoke java directly
	// taking some liberties on the /opt/solr path based on the official Docker image as there is no ENV var set for that path
	probeCommand := fmt.Sprintf("JAVA_TOOL_OPTIONS=\"%s\" java -Dsolr.ssl.checkPeerName=false %s "+
		"-Dsolr.install.dir=\"/opt/solr\" -Dlog4j.configurationFile=\"/opt/solr/server/resources/log4j2-console.xml\" "+
		"-classpath \"/opt/solr/server/solr-webapp/webapp/WEB-INF/lib/*:/opt/solr/server/lib/ext/*:/opt/solr/server/lib/*\" "+
		"org.apache.solr.util.SolrCLI api -get %s://localhost:%d%s",
		javaToolOptions, enableBasicAuth, solrCloud.UrlScheme(), defaultProbeGetAction.Port.IntVal, defaultProbeGetAction.Path)
	probeCommand = regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(probeCommand), " ")

	return probeCommand, vol, volMount
}
