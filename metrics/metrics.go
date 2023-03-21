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

package metrics

import (
	"github.com/nurture-farm/go-common/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	SERVICE_NAME = "GENERAL_GO_WORKFLOWS"
	DATABASE     = "database"
)

var Metrics metrics.MetricWrapper

var (
	ExecuteCampaignWorkflow_Metrics = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "NF_GGW_ExecuteCampaignWorkflow",
		Help:       "Summary metrics for ExecuteCampaignWorkflow",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"nservice", "nmethod", "ncode"})
)

var (
	ExecuteCampaignWorkflow_CampaignError_Metrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "NF_GGW_ExecuteCampaignWorkflow_CampaignError",
		Help: "Counter metrics for ExecuteCampaignWorkflow CampaignError",
	}, []string{"nservice", "ntype", "nerror"})
)
var (
	ExecuteCampaignWorkflow_SendBulkCommunicationError_Metrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "NF_GGW_ExecuteCampaignWorkflow_SendBulkCommunicationError",
		Help: "Counter metrics for ExecuteCampaignWorkflow SendBulkCommunicationError",
	}, []string{"nservice", "ntype", "nerror"})
)

var (
	ExecuteUserJourneyCampaignWorkflow_Metrics = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "NF_GGW_ExecuteUserJourneyCampaignWorkflow",
		Help:       "Summary metrics for ExecuteUserJourneyCampaignWorkflow",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"nservice", "nmethod", "ncode"})
)

var (
	ExecuteUserJourneyCampaignWorkflow_CampaignError_Metrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "NF_GGW_ExecuteUserJourneyCampaignWorkflow_CampaignError",
		Help: "Counter metrics for ExecuteUserJourneyCampaignWorkflow CampaignError",
	}, []string{"nservice", "ntype", "nerror"})
)

var (
	CreatePartitions_Metrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "NF_GGW_CreatePartitions",
		Help: "Counter metrics for CreatePartitions",
	}, []string{"nservice", "nmethod", "ncode"})
)

func init() {
	Metrics = &metrics.Helper{
		SERVICE_NAME: SERVICE_NAME,
		DATABASE:     DATABASE,
	}
	prometheus.MustRegister(ExecuteCampaignWorkflow_Metrics)
	prometheus.MustRegister(ExecuteCampaignWorkflow_CampaignError_Metrics)
	prometheus.MustRegister(ExecuteCampaignWorkflow_SendBulkCommunicationError_Metrics)
	prometheus.MustRegister(ExecuteUserJourneyCampaignWorkflow_Metrics)
	prometheus.MustRegister(ExecuteUserJourneyCampaignWorkflow_CampaignError_Metrics)
	prometheus.MustRegister(CreatePartitions_Metrics)
}
