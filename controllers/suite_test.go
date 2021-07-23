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
	"fmt"
	stdlog "log"
	"os"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	solrv1beta1 "github.com/apache/solr-operator/api/v1beta1"
	"github.com/onsi/gomega"
	zkOp "github.com/pravega/zookeeper-operator/pkg/apis"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var testCfg *rest.Config
var testClient client.Client

const timeout = time.Second * 5

var additionalLables = map[string]string{
	"additional": "label",
	"another":    "test",
}

func TestMain(m *testing.M) {
	// TODO: We can probably remove this once we upgrade our minimum supported version of Kubernetes
	customApiServerFlags := []string{
		"--feature-gates=StartupProbe=true",
	}

	apiServerFlags := append([]string(nil), envtest.DefaultKubeAPIServerFlags...)
	apiServerFlags = append(apiServerFlags, customApiServerFlags...)

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	t := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "config", "crd", "bases"),
			filepath.Join("..", "config", "dependencies"),
		},
		AttachControlPlaneOutput: false, // set to true to get more logging from the control plane
		KubeAPIServerFlags:       apiServerFlags,
	}
	solrv1beta1.AddToScheme(scheme.Scheme)
	zkOp.AddToScheme(scheme.Scheme)

	var err error
	if testCfg, err = t.Start(); err != nil {
		stdlog.Fatal(err)
	}

	code := m.Run()
	t.Stop()
	os.Exit(code)
}

// SetupTestReconcile returns a reconcile.Reconcile implementation that delegates to inner and
// writes the request to requests after Reconcile is finished.
func SetupTestReconcile(inner reconcile.Reconciler) (reconcile.Reconciler, chan reconcile.Request) {
	requests := make(chan reconcile.Request)
	fn := reconcile.Func(func(req reconcile.Request) (reconcile.Result, error) {
		result, err := inner.Reconcile(req)
		if err != nil {
			fmt.Printf("\n\nReconcile Error: %s\n\n", err)
		}
		requests <- req
		return result, err
	})
	return fn, requests
}

// StartTestManager adds recFn
func StartTestManager(mgr manager.Manager, g *gomega.GomegaWithT) (chan struct{}, *sync.WaitGroup) {
	stop := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.Expect(mgr.Start(stop)).NotTo(gomega.HaveOccurred())
	}()
	return stop, wg
}
