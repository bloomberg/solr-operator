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
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/pointer"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strconv"
	"strings"
)

// CopyLabelsAndAnnotations copies the labels and annotations from one object to another.
// Additional Labels and Annotations in the 'to' object will not be removed.
// Returns true if there are updates required to the object.
func CopyLabelsAndAnnotations(from, to *metav1.ObjectMeta, logger logr.Logger) (requireUpdate bool) {
	if len(to.Labels) == 0 && len(from.Labels) > 0 {
		to.Labels = make(map[string]string, len(from.Labels))
	}
	for k, v := range from.Labels {
		if to.Labels[k] != v {
			requireUpdate = true
			logger.Info("Update Label", "label", k, "newValue", v, "oldValue", to.Labels[k])
			to.Labels[k] = v
		}
	}

	if len(to.Annotations) == 0 && len(from.Annotations) > 0 {
		to.Annotations = make(map[string]string, len(from.Annotations))
	}
	for k, v := range from.Annotations {
		if to.Annotations[k] != v {
			requireUpdate = true
			logger.Info("Update Annotation", "annotation", k, "newValue", v, "oldValue", to.Annotations[k])
			to.Annotations[k] = v
		}
	}

	return requireUpdate
}

func DuplicateLabelsOrAnnotations(from map[string]string) map[string]string {
	to := make(map[string]string, len(from))
	for k, v := range from {
		to[k] = v
	}
	return to
}

func MergeLabelsOrAnnotations(base, additional map[string]string) map[string]string {
	merged := DuplicateLabelsOrAnnotations(base)
	for k, v := range additional {
		if _, alreadyExists := merged[k]; !alreadyExists {
			merged[k] = v
		}
	}
	return merged
}

// DeepEqualWithNils returns a deepEquals call that treats nil and zero-length maps, arrays and slices as the same.
func DeepEqualWithNils(x, y interface{}) bool {
	if (x == nil) != (y == nil) {
		// Make sure that x is not the nil value
		if x == nil {
			x = y
		}
		v := reflect.ValueOf(x)
		switch v.Kind() {
		case reflect.Array:
		case reflect.Map:
		case reflect.Slice:
			return v.Len() == 0
		}
	}
	return reflect.DeepEqual(x, y)
}

// ContainsString helper function to test string contains
func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// RemoveString helper function to remove string
func RemoveString(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return
}

// IsPVCOrphan determines whether the given name represents a PVC that is an orphan, or no longer has a pod associated with it.
func IsPVCOrphan(pvcName string, replicas int32) bool {
	index := strings.LastIndexAny(pvcName, "-")
	if index == -1 {
		return false
	}

	ordinal, err := strconv.Atoi(pvcName[index+1:])
	if err != nil {
		return false
	}

	return int32(ordinal) >= replicas
}

// customizeProbe builds the probe logic used for pod liveness, readiness, startup checks
func customizeProbe(initialProbe *corev1.Probe, customProbe corev1.Probe) *corev1.Probe {
	if customProbe.InitialDelaySeconds != 0 {
		initialProbe.InitialDelaySeconds = customProbe.InitialDelaySeconds
	}

	if customProbe.TimeoutSeconds != 0 {
		initialProbe.TimeoutSeconds = customProbe.TimeoutSeconds
	}

	if customProbe.SuccessThreshold != 0 {
		initialProbe.SuccessThreshold = customProbe.SuccessThreshold
	}

	if customProbe.FailureThreshold != 0 {
		initialProbe.FailureThreshold = customProbe.FailureThreshold
	}

	if customProbe.PeriodSeconds != 0 {
		initialProbe.PeriodSeconds = customProbe.PeriodSeconds
	}

	if customProbe.Handler.Exec != nil || customProbe.Handler.HTTPGet != nil || customProbe.Handler.TCPSocket != nil {
		initialProbe.Handler = customProbe.Handler
	}

	return initialProbe
}

// CopyConfigMapFields copies the owned fields from one ConfigMap to another
func CopyConfigMapFields(from, to *corev1.ConfigMap, logger logr.Logger) bool {
	logger = logger.WithValues("kind", "configMap")
	requireUpdate := CopyLabelsAndAnnotations(&from.ObjectMeta, &to.ObjectMeta, logger)

	// Don't copy the entire Spec, because we can't overwrite the clusterIp field

	if !DeepEqualWithNils(to.Data, from.Data) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Data", "from", to.Data, "to", from.Data)
	}
	to.Data = from.Data

	return requireUpdate
}

// CopyServiceFields copies the owned fields from one Service to another
func CopyServiceFields(from, to *corev1.Service, logger logr.Logger) bool {
	logger = logger.WithValues("kind", "service")
	requireUpdate := CopyLabelsAndAnnotations(&from.ObjectMeta, &to.ObjectMeta, logger)

	// Don't copy the entire Spec, because we can't overwrite the clusterIp field

	if !DeepEqualWithNils(to.Spec.Selector, from.Spec.Selector) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.Selector", "from", to.Spec.Selector, "to", from.Spec.Selector)
	}
	to.Spec.Selector = from.Spec.Selector

	if !DeepEqualWithNils(to.Spec.Ports, from.Spec.Ports) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.Ports", "from", to.Spec.Ports, "to", from.Spec.Ports)
	}
	to.Spec.Ports = from.Spec.Ports

	if !DeepEqualWithNils(to.Spec.ExternalName, from.Spec.ExternalName) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.ExternalName", "from", to.Spec.ExternalName, "to", from.Spec.ExternalName)
	}
	to.Spec.ExternalName = from.Spec.ExternalName

	if !DeepEqualWithNils(to.Spec.PublishNotReadyAddresses, from.Spec.PublishNotReadyAddresses) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.PublishNotReadyAddresses", "from", to.Spec.PublishNotReadyAddresses, "to", from.Spec.PublishNotReadyAddresses)
	}
	to.Spec.PublishNotReadyAddresses = from.Spec.PublishNotReadyAddresses

	return requireUpdate
}

// CopyIngressFields copies the owned fields from one Ingress to another
func CopyIngressFields(from, to *netv1.Ingress, logger logr.Logger) bool {
	logger = logger.WithValues("kind", "ingress")
	requireUpdate := CopyLabelsAndAnnotations(&from.ObjectMeta, &to.ObjectMeta, logger)

	if len(to.Spec.Rules) != len(from.Spec.Rules) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.Rules", "from", to.Spec.Rules, "to", from.Spec.Rules)
		to.Spec.Rules = from.Spec.Rules
	} else {
		for i := range from.Spec.Rules {
			ruleBase := "Spec.Rules[" + strconv.Itoa(i) + "]."
			fromRule := &from.Spec.Rules[i]
			toRule := &to.Spec.Rules[i]

			if !DeepEqualWithNils(toRule.Host, fromRule.Host) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", ruleBase+"Host", "from", toRule.Host, "to", fromRule.Host)
				toRule.Host = fromRule.Host
			}

			if fromRule.HTTP == nil || toRule.HTTP == nil {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", ruleBase+"HTTP", "from", toRule.HTTP, "to", fromRule.HTTP)
				toRule.HTTP = fromRule.HTTP
			} else if len(fromRule.HTTP.Paths) != len(toRule.HTTP.Paths) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", ruleBase+"HTTP.Paths", "from", toRule.HTTP.Paths, "to", fromRule.HTTP.Paths)
				toRule.HTTP.Paths = fromRule.HTTP.Paths
			} else {
				for j := range fromRule.HTTP.Paths {
					pathBase := ruleBase + "HTTP.Paths[" + strconv.Itoa(j) + "]."
					fromPath := &fromRule.HTTP.Paths[j]
					toPath := &toRule.HTTP.Paths[j]

					if toPath.PathType != nil && !DeepEqualWithNils(toPath.PathType, fromPath.PathType) {
						requireUpdate = true
						logger.Info("Update required because field changed", "field", pathBase+"PathType", "from", toPath.PathType, "to", fromPath.PathType)
						toPath.PathType = fromPath.PathType
					}

					if !DeepEqualWithNils(toPath.Path, fromPath.Path) {
						requireUpdate = true
						logger.Info("Update required because field changed", "field", pathBase+"Path", "from", toPath.Path, "to", fromPath.Path)
						toPath.Path = fromPath.Path
					}

					if !DeepEqualWithNils(toPath.Backend.ServiceName, fromPath.Backend.ServiceName) {
						requireUpdate = true
						logger.Info("Update required because field changed", "field", pathBase+"Backend.ServiceName", "from", toPath.Backend.ServiceName, "to", fromPath.Backend.ServiceName)
						toPath.Backend.ServiceName = fromPath.Backend.ServiceName
					}

					if !DeepEqualWithNils(toPath.Backend.ServicePort, fromPath.Backend.ServicePort) {
						requireUpdate = true
						logger.Info("Update required because field changed", "field", pathBase+"Backend.ServicePort", "from", toPath.Backend.ServicePort, "to", fromPath.Backend.ServicePort)
						toPath.Backend.ServicePort = fromPath.Backend.ServicePort
					}

					if !DeepEqualWithNils(toPath.Backend.Resource, fromPath.Backend.Resource) {
						requireUpdate = true
						logger.Info("Update required because field changed", "field", pathBase+"Backend.Resource", "from", toPath.Backend.Resource, "to", fromPath.Backend.Resource)
						toPath.Backend.Resource = fromPath.Backend.Resource
					}
				}
			}
		}
	}

	if !DeepEqualWithNils(to.Spec.TLS, from.Spec.TLS) {
		requireUpdate = true
		to.Spec.TLS = from.Spec.TLS
	}

	return requireUpdate
}

// CopyStatefulSetFields copies the owned fields from one StatefulSet to another
// Returns true if the fields copied from don't match to.
func CopyStatefulSetFields(from, to *appsv1.StatefulSet, logger logr.Logger) bool {
	logger = logger.WithValues("kind", "statefulSet")
	requireUpdate := CopyLabelsAndAnnotations(&from.ObjectMeta, &to.ObjectMeta, logger)

	if !DeepEqualWithNils(to.Spec.Replicas, from.Spec.Replicas) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.Replicas", "from", to.Spec.Replicas, "to", from.Spec.Replicas)
		to.Spec.Replicas = from.Spec.Replicas
	}

	if !DeepEqualWithNils(to.Spec.UpdateStrategy, from.Spec.UpdateStrategy) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.UpdateStrategy", "from", to.Spec.UpdateStrategy, "to", from.Spec.UpdateStrategy)
		to.Spec.UpdateStrategy = from.Spec.UpdateStrategy
	}

	/*
			Kubernetes does not currently support updates to these fields: Selector and PodManagementPolicy

		if !DeepEqualWithNils(to.Spec.Selector, from.Spec.Selector) {
			requireUpdate = true
			logger.Info("Update required because field changed", "field", "Spec.Selector", "from", to.Spec.Selector, "to", from.Spec.Selector)
			to.Spec.Selector = from.Spec.Selector
		}

		if !DeepEqualWithNils(to.Spec.PodManagementPolicy, from.Spec.PodManagementPolicy) {
			requireUpdate = true
			logger.Info("Update required because field changed", "field", "Spec.PodManagementPolicy", "from", to.Spec.PodManagementPolicy, "to", from.Spec.PodManagementPolicy)
			to.Spec.PodManagementPolicy = from.Spec.PodManagementPolicy
		}
	*/

	/*
			Kubernetes does not support modification of VolumeClaimTemplates currently. See:
		    https://github.com/kubernetes/enhancements/issues/661

		if len(from.Spec.VolumeClaimTemplates) > len(to.Spec.VolumeClaimTemplates) {
			requireUpdate = true
			logger.Info("Update required because field changed", "field", "Spec.VolumeClaimTemplates", "from", to.Spec.VolumeClaimTemplates, "to", from.Spec.VolumeClaimTemplates)
			to.Spec.VolumeClaimTemplates = from.Spec.VolumeClaimTemplates
		}
		for i := range from.Spec.VolumeClaimTemplates {
			vctBase := "Spec.VolumeClaimTemplates["+strconv.Itoa(i)+"]."
			fromVct := &from.Spec.VolumeClaimTemplates[i]
			toVct := &to.Spec.VolumeClaimTemplates[i]
			if !DeepEqualWithNils(to.Spec.VolumeClaimTemplates[i].Name, fromVct.Name) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", vctBase+"Name", "from", toVct.Name, "to", fromVct.Name)
				toVct.Name = fromVct.Name
			}
			if !DeepEqualWithNils(to.Spec.VolumeClaimTemplates[i].Labels, fromVct.Labels) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", vctBase+"Labels", "from", toVct.Labels, "to", fromVct.Labels)
				toVct.Labels = fromVct.Labels
			}
			if !DeepEqualWithNils(to.Spec.VolumeClaimTemplates[i].Annotations, fromVct.Annotations) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", vctBase+"Annotations", "from", toVct.Annotations, "to", fromVct.Annotations)
				toVct.Annotations = fromVct.Annotations
			}
			if !DeepEqualWithNils(to.Spec.VolumeClaimTemplates[i].Spec, fromVct.Spec) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", vctBase+"Spec", "from", toVct.Spec, "to", fromVct.Spec)
				toVct.Spec = fromVct.Spec
			}
		}
	*/

	requireUpdate = CopyPodTemplates(&from.Spec.Template, &to.Spec.Template, "Spec.Template.", logger) || requireUpdate

	return requireUpdate
}

// CopyDeploymentFields copies the owned fields from one Deployment to another
// Returns true if the fields copied from don't match to.
func CopyDeploymentFields(from, to *appsv1.Deployment, logger logr.Logger) bool {
	logger = logger.WithValues("kind", "deployment")
	requireUpdate := CopyLabelsAndAnnotations(&from.ObjectMeta, &to.ObjectMeta, logger)

	if !DeepEqualWithNils(to.Spec.Replicas, from.Spec.Replicas) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.Replicas", "from", to.Spec.Replicas, "to", from.Spec.Replicas)
		to.Spec.Replicas = from.Spec.Replicas
	}

	if !DeepEqualWithNils(to.Spec.Selector, from.Spec.Selector) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", "Spec.Selector", "from", to.Spec.Selector, "to", from.Spec.Selector)
		to.Spec.Selector = from.Spec.Selector
	}

	requireUpdate = CopyPodTemplates(&from.Spec.Template, &to.Spec.Template, "Spec.Template.", logger) || requireUpdate

	return requireUpdate
}

func CopyPodTemplates(from, to *corev1.PodTemplateSpec, basePath string, logger logr.Logger) (requireUpdate bool) {
	if basePath == "" {
		logger = logger.WithValues("kind", "pod")
	}
	if !DeepEqualWithNils(to.Labels, from.Labels) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Labels", "from", to.Labels, "to", from.Labels)
		to.Labels = from.Labels
	}

	if !DeepEqualWithNils(to.Annotations, from.Annotations) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Annotations", "from", to.Annotations, "to", from.Annotations)
		to.Annotations = from.Annotations
	}

	requireUpdate = CopyPodContainers(&from.Spec.Containers, &to.Spec.Containers, basePath+"Spec.Containers", logger) || requireUpdate

	requireUpdate = CopyPodContainers(&from.Spec.InitContainers, &to.Spec.InitContainers, basePath+"Spec.InitContainers", logger) || requireUpdate

	if !DeepEqualWithNils(to.Spec.HostAliases, from.Spec.HostAliases) {
		requireUpdate = true
		to.Spec.HostAliases = from.Spec.HostAliases
		logger.Info("Update required because field changed", "field", basePath+"Spec.HostAliases", "from", to.Spec.HostAliases, "to", from.Spec.HostAliases)
	}

	if !DeepEqualWithNils(to.Spec.Volumes, from.Spec.Volumes) {
		requireUpdate = true
		to.Spec.Volumes = from.Spec.Volumes
		logger.Info("Update required because field changed", "field", basePath+"Spec.Volumes", "from", to.Spec.Volumes, "to", from.Spec.Volumes)
	}

	if !DeepEqualWithNils(to.Spec.ImagePullSecrets, from.Spec.ImagePullSecrets) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Spec.ImagePullSecrets", "from", to.Spec.ImagePullSecrets, "to", from.Spec.ImagePullSecrets)
		to.Spec.ImagePullSecrets = from.Spec.ImagePullSecrets
	}

	if !DeepEqualWithNils(to.Spec.Affinity, from.Spec.Affinity) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Spec.Affinity", "from", to.Spec.Affinity, "to", from.Spec.Affinity)
		to.Spec.Affinity = from.Spec.Affinity
	}

	if !DeepEqualWithNils(to.Spec.SecurityContext, from.Spec.SecurityContext) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Spec.SecurityContext", "from", to.Spec.SecurityContext, "to", from.Spec.SecurityContext)
		to.Spec.SecurityContext = from.Spec.SecurityContext
	}

	if !DeepEqualWithNils(to.Spec.NodeSelector, from.Spec.NodeSelector) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Spec.NodeSelector", "from", to.Spec.NodeSelector, "to", from.Spec.NodeSelector)
		to.Spec.NodeSelector = from.Spec.NodeSelector
	}

	if !DeepEqualWithNils(to.Spec.Tolerations, from.Spec.Tolerations) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Spec.Tolerations", "from", to.Spec.Tolerations, "to", from.Spec.Tolerations)
		to.Spec.Tolerations = from.Spec.Tolerations
	}

	if !DeepEqualWithNils(to.Spec.PriorityClassName, from.Spec.PriorityClassName) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Spec.PriorityClassName", "from", to.Spec.PriorityClassName, "to", from.Spec.PriorityClassName)
		to.Spec.PriorityClassName = from.Spec.PriorityClassName
	}

	if !DeepEqualWithNils(to.Spec.TerminationGracePeriodSeconds, from.Spec.TerminationGracePeriodSeconds) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Spec.TerminationGracePeriodSeconds", "from", to.Spec.TerminationGracePeriodSeconds, "to", from.Spec.TerminationGracePeriodSeconds)
		to.Spec.TerminationGracePeriodSeconds = from.Spec.TerminationGracePeriodSeconds
	}

	if !DeepEqualWithNils(to.Spec.ServiceAccountName, from.Spec.ServiceAccountName) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Spec.ServiceAccountName", "from", to.Spec.ServiceAccountName, "to", from.Spec.ServiceAccountName)
		to.Spec.ServiceAccountName = from.Spec.ServiceAccountName
	}

	return requireUpdate
}

func CopyPodContainers(fromPtr, toPtr *[]corev1.Container, basePath string, logger logr.Logger) (requireUpdate bool) {
	to := *toPtr
	from := *fromPtr
	if len(to) != len(from) {
		requireUpdate = true
		logger.Info("Update required because field changed", "field", basePath+"Length", "from", len(to), "to", len(from))
		*toPtr = from
	} else {
		for i := 0; i < len(from); i++ {
			containerBasePath := basePath + "[" + strconv.Itoa(i) + "]."
			if !DeepEqualWithNils(to[i].Name, from[i].Name) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"Name", "from", to[i].Name, "to", from[i].Name)
				to[i].Name = from[i].Name
			}

			if !DeepEqualWithNils(to[i].Image, from[i].Image) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"Image", "from", to[i].Image, "to", from[i].Image)
				to[i].Image = from[i].Image
			}

			if from[i].ImagePullPolicy != "" && !DeepEqualWithNils(to[i].ImagePullPolicy, from[i].ImagePullPolicy) {
				// Only request an update if the requestedPullPolicy is not empty
				// Otherwise kubernetes will specify a defaultPollPolicy and the operator will endlessly recurse, trying to unset the default policy.
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"ImagePullPolicy", "from", to[i].ImagePullPolicy, "to", from[i].ImagePullPolicy)
			}
			to[i].ImagePullPolicy = from[i].ImagePullPolicy

			if !DeepEqualWithNils(to[i].Command, from[i].Command) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"Command", "from", to[i].Command, "to", from[i].Command)
				to[i].Command = from[i].Command
			}

			if !DeepEqualWithNils(to[i].Args, from[i].Args) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"Args", "from", to[i].Args, "to", from[i].Args)
				to[i].Args = from[i].Args
			}

			if !DeepEqualWithNils(to[i].Env, from[i].Env) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"Env", "from", to[i].Env, "to", from[i].Env)
				to[i].Env = from[i].Env
			}

			if !DeepEqualWithNils(to[i].Resources, from[i].Resources) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"Resources", "from", to[i].Resources, "to", from[i].Resources)
				to[i].Resources = from[i].Resources
			}

			if !DeepEqualWithNils(to[i].VolumeMounts, from[i].VolumeMounts) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"VolumeMounts", "from", to[i].VolumeMounts, "to", from[i].VolumeMounts)
				to[i].VolumeMounts = from[i].VolumeMounts
			}

			if !DeepEqualWithNils(to[i].Ports, from[i].Ports) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"Ports", "from", to[i].Ports, "to", from[i].Ports)
				to[i].Ports = from[i].Ports
			}

			if !DeepEqualWithNils(to[i].Lifecycle, from[i].Lifecycle) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"Lifecycle", "from", to[i].Lifecycle, "to", from[i].Lifecycle)
				to[i].Lifecycle = from[i].Lifecycle
			}

			if !DeepEqualWithNils(to[i].LivenessProbe, from[i].LivenessProbe) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"LivenessProbe", "from", to[i].LivenessProbe, "to", from[i].LivenessProbe)
				to[i].LivenessProbe = from[i].LivenessProbe
			}

			if !DeepEqualWithNils(to[i].ReadinessProbe, from[i].ReadinessProbe) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"ReadinessProbe", "from", to[i].ReadinessProbe, "to", from[i].ReadinessProbe)
				to[i].ReadinessProbe = from[i].ReadinessProbe
			}

			if !DeepEqualWithNils(to[i].StartupProbe, from[i].StartupProbe) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"StartupProbe", "from", to[i].StartupProbe, "to", from[i].StartupProbe)
				to[i].StartupProbe = from[i].StartupProbe
			}

			if from[i].TerminationMessagePath != "" && !DeepEqualWithNils(to[i].TerminationMessagePath, from[i].TerminationMessagePath) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"TerminationMessagePath", "from", to[i].TerminationMessagePath, "to", from[i].TerminationMessagePath)
				to[i].TerminationMessagePath = from[i].TerminationMessagePath
			}

			if from[i].TerminationMessagePolicy != "" && !DeepEqualWithNils(to[i].TerminationMessagePolicy, from[i].TerminationMessagePolicy) {
				requireUpdate = true
				logger.Info("Update required because field changed", "field", containerBasePath+"TerminationMessagePolicy", "from", to[i].TerminationMessagePolicy, "to", from[i].TerminationMessagePolicy)
				to[i].TerminationMessagePolicy = from[i].TerminationMessagePolicy
			}
		}
	}
	return requireUpdate
}

// OvertakeControllerRef makes sure that the controlled object has the owner as the controller ref.
// If the object has a different controller, then that ref will be downgraded to an "owner" and the new controller ref will be added
func OvertakeControllerRef(owner metav1.Object, controlled metav1.Object, scheme *runtime.Scheme) (needsUpdate bool, err error) {
	if !metav1.IsControlledBy(controlled, owner) {
		if otherController := metav1.GetControllerOfNoCopy(controlled); otherController != nil {
			otherController.Controller = pointer.BoolPtr(false)
			otherController.BlockOwnerDeletion = pointer.BoolPtr(false)
		}
		err = controllerutil.SetControllerReference(owner, controlled, scheme)
		needsUpdate = true
	}
	return needsUpdate, err
}
