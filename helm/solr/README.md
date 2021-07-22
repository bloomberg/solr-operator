Apache Solr
=============

Apache Solr is the popular, blazing-fast, open source enterprise search platform built on Apache Lucene™.

Solr is highly reliable, scalable and fault tolerant, providing distributed indexing, replication and load-balanced querying, automated failover and recovery, centralized configuration and more.
Solr powers the search and navigation features of many of the world's largest internet sites.

Documentation around using Apache Solr can be found on it's [official site](https://solr.apache.org).

## Tutorials

Check out the following tutorials on getting started with Solr.

- [Solr on Kubernetes Tutorials](https://solr.apache.org/operator/resources.html#tutorials)
- [Using Solr Tutorials](https://solr.apache.org/resources.html#tutorials)

## Dependencies

This Helm chart relies on the [Solr Operator](https://solr.apache.org/operator) being installed.
You can install the Solr Operator via it's [Helm chart](https://artifacthub.io/packages/helm/apache-solr/solr-operator).

If you are using a provided Zookeeper instance for your Solr Cloud, then you must also install the [Zookeeper Operator](https://github.com/pravega/zookeeper-operator).
However, it is [installed by default as a dependency](https://artifacthub.io/packages/helm/apache-solr/solr-operator#installing-the-zookeeper-operator) when using the Solr Operator Helm chart.

Please make sure that the same version of the Solr Operator chart and Solr chart are installed.
The Solr version can be any supported version of Solr you wish to run, but the _chart version_ must match the chart version of the Solr Operator that you have installed.

## Upgrade Notes

Before upgrading your Solr Operator and Solr Helm chart version, **please refer to the [Upgrade Notes](https://apache.github.io/solr-operator/docs/upgrade-notes.html)**.
There may be breaking changes between the version you are using and the version you want to upgrade to.

## Using the Helm Chart

### Installing the Chart

To install the Solr Operator for the first time in your cluster, you can use the latest version or a specific version, run with the following commands:

```bash
helm install example apache-solr/solr --version 0.4.0-prerelease --set image.tag=8.8.0
```

The command deploys a SolrCloud object on the Kubernetes cluster with the default configuration.
The [Solr Operator](https://solr.apache.org/operator) is then in charge of creating the necessary Kubernetes resources to run that Solr Cloud.
The [configuration](#chart-values) section lists the parameters that can be configured during installation.

_Note that the Helm chart version does not contain a `v` prefix, which the Solr Operator release versions do._

### Upgrading Solr

If you are upgrading your SolrCloud deployment, you should always use a specific version of the chart and upgrade **after [upgrading the Solr Operator](https://artifacthub.io/packages/helm/apache-solr/solr-operator#upgrading-the-solr-operator) to the same version**:

```bash
helm upgrade example apache-solr/solr --version 0.4.0-prerelease --reuse-values --set image.tag=8.9.0
```

The upgrade will be done according to the `upgradeStrategy.method` chosen in the values.
Be sure to select the [update strategy](https://apache.github.io/solr-operator/docs/solr-cloud/solr-cloud-crd.html#update-strategy) that best fits your use case.
However, the `Managed` strategy is highly recommended.

### Uninstalling the Chart

To uninstall/delete the Solr Cloud, run:

```bash
helm uninstall example
```

The command removes the SolrCloud resource, and then Kubernetes will garbage collect the resources owned by that SolrCloud, including the StatefulSet, Services, ConfigMap, etc.

## Chart Values

### Running Solr

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| fullnameOverride | string | `""` | A custom name for the Solr Operator Deployment |
| nameOverride | string | `""` |  |
| replicas | int | `3` | The number of Solr pods to run in the Solr Cloud. If you want to use autoScaling, do not set this field. |
| image.repository | string | `"apache/solr"` | The repository of the Solr image |
| image.tag | string | `"8.9"` | The tag/version of Solr to run |
| image.pullPolicy | string |  | PullPolicy for the Solr image, defaults to the empty Pod behavior |
| image.imagePullSecret | string |  | PullSecret for the Solr image |
| busyBoxImage.repository | string | `"busybox"` | The repository of the BusyBox image |
| busyBoxImage.tag | string | `"1.28.0-glibc"` | The tag/version of BusyBox to run |
| busyBoxImage.pullPolicy | string |  | PullPolicy for the BusyBox image, defaults to the empty Pod behavior |
| busyBoxImage.imagePullSecret | string |  | PullSecret for the BusyBox image |
| solrOptions.javaMemory | string | `"-Xms1g -Xmx2g"` | PullSecret for the BusyBox image |
| solrOptions.javaOpts | string | `""` | Additional java arguments to pass via the command line |
| solrOptions.logLevel | string | `"INFO"` | Log level to run Solr under |
| solrOptions.gcTune | string | `""` | GC Tuning parameters for Solr |
| solrOptions.security.authenticationType | string | `""` | Type of authentication to use for Solr |
| solrOptions.security.basicAuthSecret | string | `""` | Name of Secret in the same namespace that stores the basicAuth information for the Solr user |
| solrOptions.security.probesRequireAuth | boolean | | Whether the probes for the SolrCloud pod require auth |
| updateStrategy.method | string | `"Managed"` | The method for conducting updates of Solr pods. Either `Managed`, `StatefulSet` or `Manual`. See the [docs](https://apache.github.io/solr-operator/docs/solr-cloud/solr-cloud-crd.html#update-strategy) for more information |
| updateStrategy.managedUpdate.maxPodsUnavailable | int-or-string | `"25%"` | The number of Solr pods in a Solr Cloud that are allowed to be unavailable during the rolling restart. Either a static number, or a percentage representing the percentage of total pods requested for the statefulSet. |
| updateStrategy.managedUpdate.maxShardReplicasUnavailable | int-or-string | `1` | The number of replicas for each shard allowed to be unavailable during the restart. Either a static number, or a percentage representing the percentage of the number of replicas for a shard. |
| updateStrategy.restartSchedule | [string (CRON)](https://pkg.go.dev/github.com/robfig/cron/v3?utm_source=godoc#hdr-CRON_Expression_Format) | | A CRON schedule for automatically restarting the Solr Cloud. [Refer here](https://pkg.go.dev/github.com/robfig/cron/v3?utm_source=godoc#hdr-CRON_Expression_Format) for all possible CRON syntaxes accepted. |
| serviceAccount.create | boolean | `false` | Create a serviceAccount to be used for all pods being deployed (Solr & ZK). If `serviceAccount.name` is not specified, the full name of the deployment will be used. |
| serviceAccount.name | string |  | The optional default service account used for Solr and ZK unless overridden below. If `serviceAccount.create` is set to `false`, this serviceAccount must exist in the target namespace. |

### Data Storage Options

See the [documentation](https://apache.github.io/solr-operator/docs/solr-cloud/solr-cloud-crd.html#data-storage) for more information.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| dataStorage.type | string | `"ephemeral"` | Whether your Solr data storage should be `ephemeral` or `persistent`. The `storage.<ephemeral,persistent>.*` options are used only if their storage type is selected here. |
| dataStorage.capacity | string | `"20Gi"` | Capacity for your data storage, ephemeral or persistent |
| dataStorage.ephemeral.emptyDir | object | | Specify options for and ephemeral emptyDir volume to store Solr data. |
| dataStorage.ephemeral.hostPath | object | | Specify options for and ephemeral hostPath volume to store Solr data. Is not used when `emptyDir` is specified. |
| dataStorage.persistent.reclaimPolicy | string | `"Retain"` | Determines whether to delete or keep the PVCs when Solr is deleted or scaled down. Either `Retain` or `Delete`. |
| dataStorage.persistent.pvc.name | string | | Override the default Solr data PVC base-name |
| dataStorage.persistent.pvc.annotations | map[string]string | | Set the annotations for your Solr data PVCs |
| dataStorage.persistent.pvc.labels | map[string]string | | Set the labels for your Solr data PVCs |
| dataStorage.persistent.pvc.storageClassName | string | | Override the default storageClass for your Solr data PVCs |
| dataStorage.backupRestoreOptions.volume | object | | A read-write-many volume that can be attached to all Solr pods, for the purpose of storing backup data. This is required when using the SolrBackup CRD. |
| dataStorage.backupRestoreOptions.directory | string | | Override the default backup-restore volume location in the Solr container |

### Addressability Options

See the [documentation](https://apache.github.io/solr-operator/docs/solr-cloud/solr-cloud-crd.html#addressability) for more information.

If providing external addressability, then `method` and `domainName` must be provided.
External addressability is disabled by default.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| addressability.podPort | int | `8983` | The port that Solr should listen on within the pod. |
| addressability.commonServicePort | int | `` | The port that Solr's load-balancing common service should listen on. |
| addressability.kubeDomain | string | | The cluster domain the Kubernetes is addressed under. Overrides the `global.clusterDomain` option. |
| addressability.external.method | string | | The method by which Solr should be made addressable outside of the Kubernetes cluster. Either `Ingress` or `ExternalDNS` |
| addressability.external.domainName | string | | The base domain name that Solr nodes should be addressed under. |
| addressability.external.additionalDomainNames | []string | | Additional base domain names that Solr nodes should be addressed under. These are not used to advertise Solr locations, just the `domainName` is. |
| addressability.external.hideNodes | boolean | `false` | Do not make the individual Solr nodes addressable outside of the Kubernetes cluster. |
| addressability.external.hideCommon | boolean | `false` | Do not make the load-balanced common Solr endpoint addressable outside of the Kubernetes cluster. |
| addressability.external.nodePortOverride | int | | Override the port of individual Solr nodes when using the `Ingress` method. This will default to `80` if using an Ingress without TLS and `443` when using an Ingress with Solr TLS enabled (not TLS Termination described below). |
| addressability.external.ingressTLSTerminationSecret | int | | Name of Kubernetes Secret to terminate TLS when using the `Ingress` method. This cannot be used with `SolrTLS`, and will be ignored if `SolrTLS` is enabled. |

### ZK Options

Specify what ZooKeeper cluster your SolrCloud should connect to.
If you specify `zk.address` or `zk.externalAddress`, then no ZooKeeper cluster will be created for you.
If neither of those options are specified, then a new ZooKeeper cluster will be created by the [Zookeeper Operator](https://github.com/pravega/zookeeper-operator), with the settings provided under `zk.provided.*`.

Currently the Zookeeper Operator does not support ACLs, so do not use the provided ZK cluster in conjunction with them.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| zk.chroot | string | | A ZooKeeper Node to host all the information for this SolrCloud under |
| zk.uniqueChroot | boolean | `false` | If true, this will add the `/<namespace>/<name>` to the end of the provided chroot, if any is provided. This will let you deploy multiple Solr Clouds without having to manage the specific chroots yourself. |
| zk.address | string | | An existing ZooKeeper cluster address to use for Solr. Must be reachable within the Kubernetes cluster. |
| zk.externalAddress | string | | An existing ZooKeeper cluster address to use for Solr. Must be reachable within and outside the Kubernetes cluster. |
| zk.provided.replicas | int | `3` | The number of replicas to run for your ZooKeeper cluster. |
| zk.provided.image.repository | string | `"pravega/zookeeper"` | The repository of the Solr image |
| zk.provided.image.tag | string | | The tag/version of Zookeeper to run. Generally leave this blank, so that the Zookeeper Operator can manage it. |
| zk.provided.image.pullPolicy | string | `"IfNotPresent"` | PullPolicy for the ZooKeeper image |
| zk.provided.image.imagePullSecret | string |  | PullSecret for the ZooKeeper image |
| zk.provided.storageType | string |  | Explicitly set the Zookeeper storage type, not necessary if providing a "persistence" or "ephemeral" option below, or if you want to use same option that you use for Solr. |
| zk.provided.persistence.reclaimPolicy | string | `"Retain"` | Determines whether to delete or keep the PVCs when ZooKeeper is deleted or scaled down. Either `Retain` or `Delete`. |
| zk.provided.persistence.spec | object | | A PVC Spec for the ZooKeeper PVC(s) |
| zk.provided.ephemeral.emptydirvolumesource | object | | An emptyDir volume source for the ZooKeeper Storage on each pod. |
| zk.provided.zookeeperPodPolicy.serviceAccountName | string |  | Optional serviceAccount to run the ZK Pod under |
| zk.provided.zookeeperPodPolicy.affinity | string |  | PullSecret for the ZooKeeper image |
| zk.provided.zookeeperPodPolicy.resources.limits | map[string]string |  | Provide Resource limits for the ZooKeeper containers |
| zk.provided.zookeeperPodPolicy.resources.requests | map[string]string |  | Provide Resource requests for the ZooKeeper containers |
| zk.provided.zookeeperPodPolicy.nodeSelector | map[string]string |  | Add a node selector for the ZooKeeper pod, to specify where it can be scheduled |
| zk.provided.zookeeperPodPolicy.affinity | object |  | Add Kubernetes affinity information for the ZooKeeper pod |
| zk.provided.zookeeperPodPolicy.tolerations | []object |  | Specify a list of Kubernetes tolerations for the ZooKeeper pod |
| zk.provided.zookeeperPodPolicy.envVars | []object |  | List of additional environment variables for the ZooKeeper container |
| zk.acl.secret | string |  | Name of a secret in the same namespace as the Solr cloud that stores the ZK admin ACL information |
| zk.acl.usernameKey | string |  | Key in the Admin ACL Secret that stores the ACL username |
| zk.acl.passwordKey | string |  | Key in the Admin ACL Secret that stores the ACL password |
| zk.readOnlyAcl.secret | string |  | Name of a secret in the same namespace as the Solr cloud that stores the ZK read-only ACL information |
| zk.readOnlyAcl.usernameKey | string |  | Key in the read-only ACL Secret that stores the ACL username |
| zk.readOnlyAcl.passwordKey | string |  | Key in the read-only ACL Secret that stores the ACL password |

### TLS Options

See [documentation](https://apache.github.io/solr-operator/docs/solr-cloud/solr-cloud-crd.html#enable-tls-between-solr-pods) for more information.

Solr TLS is disabled by default. Provide any of the following to enable it.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| solrTLS.clientAuth | string | `"None"` | Whether clientAuth is Needed, Wanted or None in Solr |
| solrTLS.verifyClientHostname | boolean | `false` | Whether Solr should verify client hostnames |
| solrTLS.checkPeerName | boolean | `false` | Whether Solr should check peer names |
| solrTLS.restartOnTLSSecretUpdate | boolean | `false` | Whether Solr pods should auto-restart when the TLS Secrets are updated |
| solrTLS.pkcs12Secret.name | string |  | Name of the Secret that stores the Solr TLS pkcs12 file |
| solrTLS.pkcs12Secret.key | string |  | Key in the Secret that stores the Solr TLS pkcs12 file |
| solrTLS.keyStorePasswordSecret.name | string |  | Name of the Secret that stores the Solr TLS keystore |
| solrTLS.keyStorePasswordSecret.key | string |  | Key in the Secret that stores the Solr TLS keystore |
| solrTLS.trustStoreSecret.name | string |  | Name of the Secret that stores the Solr TLS truststore |
| solrTLS.trustStoreSecret.key | string |  | Key in the Secret that stores the Solr TLS truststore |
| solrTLS.trustStorePasswordSecret.name | string |  | Name of the Secret that stores the Solr TLS truststore password |
| solrTLS.trustStorePasswordSecret.key | string |  | Key in the Secret that stores the Solr TLS truststore password |
| solrTLS.trustStorePasswordSecret.key | string |  | Key in the Secret that stores the Solr TLS truststore password |

### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| global.imagePullSecrets | []object |  | The list of imagePullSecrets to include in pods |
| global.clusterDomain | string |  | The cluster domain the Kubernetes is addressed under. |

### Custom Kubernetes Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| podOptions.annotations | map[string]string |  | Custom annotations to add to the Solr pod |
| podOptions.labels | map[string]string |  | Custom labels to add to the Solr pod |
| podOptions.resources.limits | map[string]string |  | Provide Resource limits for the Solr container |
| podOptions.resources.requests | map[string]string |  | Provide Resource requests for the Solr container |
| podOptions.nodeSelector | map[string]string |  | Add a node selector for the Solr pod, to specify where it can be scheduled |
| podOptions.affinity | object |  | Add Kubernetes affinity information for the Solr pod |
| podOptions.tolerations | []object |  | Specify a list of Kubernetes tolerations for the Solr pod |
| podOptions.serviceAccountName | string |  | Optional serviceAccount to run the Solr pods under |
| podOptions.priorityClassName | string | | Optional priorityClassName for the Solr pod |
| podOptions.sidecarContainers | []object |  | An optional list of additional containers to run along side the Solr in its pod |
| podOptions.initContainers | []object |  | An optional list of additional initContainers to run before the Solr container starts |
| podOptions.envVars | []object |  | List of additional environment variables for the Solr container |
| podOptions.podSecurityContext | object |  | Security context for the Solr pod |
| podOptions.terminationGracePeriodSeconds | int |  | Optional amount of time to wait for Solr to stop on its own, before manually killing it |
| podOptions.livenessProbe | object |  | Custom liveness probe for the Solr container |
| podOptions.readinessProbe | object |  | Custom readiness probe for the Solr container |
| podOptions.startupProbe | object |  | Custom startup probe for the Solr container |
| podOptions.imagePullSecrets | []object |  | List of image pull secrets to inject into the Solr pod, in addition to `global.imagePullSecrets` |
| podOptions.volumes | []object |  | List of additional volumes to attach to the Solr pod, and optionally how to mount them to the Solr container |
| statefulSetOptions.annotations | map[string]string |  | Custom annotations to add to the Solr statefulSet |
| statefulSetOptions.labels | map[string]string |  | Custom labels to add to the Solr statefulSet |
| statefulSetOptions.podManagementPolicy | string | `"Parallel"` | Policy for how Solr pods should be managed in the statefulSet, ["OrderedReady" or "Parallel"](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-management-policies) |
| commonServiceOptions.annotations | map[string]string |  | Custom annotations to add to the Solr common service |
| commonServiceOptions.labels | map[string]string |  | Custom labels to add to the Solr common service |
| headlessServiceOptions.annotations | map[string]string |  | Custom annotations to add to the Solr headless service |
| headlessServiceOptions.labels | map[string]string |  | Custom labels to add to the Solr headless service |
| nodeServiceOptions.annotations | map[string]string |  | Custom annotations to add to the Solr node service(s) |
| nodeServiceOptions.labels | map[string]string |  | Custom labels to add to the Solr node service(s) |
| ingressOptions.annotations | map[string]string |  | Custom annotations to add to the Solr ingress, if it exists |
| ingressOptions.labels | map[string]string |  | Custom labels to add to the Solr ingress, if it exists |
| configMapOptions.annotations | map[string]string |  | Custom annotations to add to the Solr configMap |
| configMapOptions.labels | map[string]string |  | Custom labels to add to the Solr configMap |
| configMapOptions.providedConfigMap | string |  | Provide an existing configMap for the Solr XML and/or Solr log4j files. *ADVANCED* |
