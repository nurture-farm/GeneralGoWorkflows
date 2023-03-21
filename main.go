/*
 *  Copyright 2023 NURTURE AGTECH PVT LTD
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package main

import (
	"flag"
	"fmt"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	ggw "github.com/nurture-farm/Contracts/Workflows/GeneralGoWorkflows/Gen/GoGeneralGoWorkflows"
	"github.com/nurture-farm/GeneralGoWorkflows/lifecycle"
	"github.com/nurture-farm/WorkflowCoreCommon/constants/wfnames"
	"github.com/nurture-farm/WorkflowCoreCommon/utils"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"net/http"
)

const (
	WorkerName                      = "worker_name"
	ExecuteCreatePartitionsWorkflow = "CreatePartitionsWorkflow"
)

func runMonitoring(grpcServer *grpc.Server) {
	// register prometheus
	grpc_prometheus.Register(grpcServer)
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(fmt.Sprintf("%s:%d", "0.0.0.0", 7005), nil)
	if err != nil {
		panic(err)
	}
}

func registerAsWorker() client.Client {

	utils.LOG.Info("Workflow config", zap.Any("config", utils.WorkflowConfig))
	taskQueue := utils.WorkflowConfig[WorkerName]

	w := worker.New(utils.WorkflowClient, taskQueue, worker.Options{})

	utils.LOG.Info("Starting worker on taskQueue", zap.String("taskQueue", taskQueue), zap.Any("worker", w))

	w.RegisterWorkflowWithOptions(lifecycle.OTPDeliveryWorkflow,
		workflow.RegisterOptions{Name: wfnames.OTPDeliveryWorkflow})
	w.RegisterWorkflowWithOptions(lifecycle.TemplateWarningWorkflow,
		workflow.RegisterOptions{Name: wfnames.TemplateWarningWorkflowName})
	w.RegisterWorkflowWithOptions(lifecycle.ExecuteCampaignWorkflow,
		workflow.RegisterOptions{Name: wfnames.ExecuteCampaignWorkflow})
	w.RegisterWorkflowWithOptions(lifecycle.ExecuteUserJourneyCampaignWorkflow,
		workflow.RegisterOptions{Name: wfnames.ExecuteUserJourneyCampaignWorkflow})
	w.RegisterWorkflowWithOptions(lifecycle.CreatePartitionsWorkflow,
		workflow.RegisterOptions{Name: ExecuteCreatePartitionsWorkflow})

	utils.LOG.Info("REGISTERED workflows:", zap.Any("workflows", []string{
		wfnames.OTPDeliveryWorkflow,
		wfnames.ExecuteCampaignWorkflow,
		wfnames.ExecuteUserJourneyCampaignWorkflow,
		wfnames.TemplateWarningWorkflowName,
		ExecuteCreatePartitionsWorkflow,
	}))

	workerErr := w.Run(worker.InterruptCh())
	if workerErr != nil {
		utils.LOG.Panic("Unable to start worker", zap.Error(workerErr))
	}
	utils.LOG.Info("Started worker", zap.Any("client", utils.WorkflowClient))
	return utils.WorkflowClient
}

func main() {

	utils.LOG.Info("Start GeneralGo Workflows")
	port := flag.Int("port", 4000, "Port for GRPC server to listen")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		utils.LOG.Panic("Unable to listen on port", zap.Int("port", *port), zap.Error(err))
	}

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor))
	ggw.RegisterGeneralGoWorkflowsServer(grpcServer, GeneralGoWorkflowServer)
	utils.LOG.Info("Registered server", zap.Any("grpcServer", grpcServer), zap.Any("listener", lis), zap.Int("port", *port))

	go runMonitoring(grpcServer)
	utils.LOG.Info("Started prometheus monitoring(through go routine)")

	// register as worker
	go func() {
		c := registerAsWorker()
		defer c.Close()
	}()

	// Start server
	err = grpcServer.Serve(lis)
	if err != nil {
		utils.LOG.Panic("Unable to listen on service", zap.Int("port", *port), zap.Error(err))
	}

}
