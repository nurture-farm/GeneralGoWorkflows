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
	"context"
	"fmt"
	Common "github.com/nurture-farm/Contracts/Common/Gen/GoCommon"
	ggw "github.com/nurture-farm/Contracts/Workflows/GeneralGoWorkflows/Gen/GoGeneralGoWorkflows"
	"github.com/nurture-farm/WorkflowCoreCommon/constants/wfnames"
	"github.com/nurture-farm/WorkflowCoreCommon/utils"
	"github.com/spf13/viper"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
	"time"
)

func ExecuteCampaign(ctx context.Context, request *ggw.ExecuteCampaignRequest) *ggw.ExecuteCampaignResponse {

	response := &ggw.ExecuteCampaignResponse{}
	workflowId := fmt.Sprintf("%s_%d", "CRON_ExecuteCampaignWorkflow", request.CampaignId)
	we, err := utils.WorkflowClient.ExecuteWorkflow(ctx, utils.GetProdWorkflowOptions(workflowId, "GeneralGoWorker"), wfnames.ExecuteCampaignWorkflow, request)
	if err != nil {
		utils.LOG.Error("Start ExecuteCampaignWorkflow FAILED: ",
			zap.Error(err), zap.Any("workflowRun", we))
		return response
	}
	utils.LOG.Info("ExecuteCampaignWorkflow started", zap.String("workflowId", workflowId))

	err = we.Get(context.Background(), &response)
	if err != nil {
		utils.LOG.Error("ExecuteCampaignWorkflow response error: ",
			zap.Error(err), zap.String("workflowId", workflowId))
		return response
	}

	utils.LOG.Info("ExecuteCampaignWorkflow completed", zap.String("workflowId", workflowId))

	return response
}

func ExecuteUserJourneyCampaign(ctx context.Context, request *ggw.ExecuteUserJourneyCampaignRequest) *ggw.ExecuteUserJourneyCampaignResponse {

	response := &ggw.ExecuteUserJourneyCampaignResponse{}
	workflowId := fmt.Sprintf("%s_%d", "CRON_ExecuteUserJourneyCampaignWorkflow", request.CampaignId)
	we, err := utils.WorkflowClient.ExecuteWorkflow(ctx, utils.GetProdWorkflowOptions(workflowId, "GeneralGoWorker"), wfnames.ExecuteUserJourneyCampaignWorkflow, request)
	if err != nil {
		utils.LOG.Error("Start ExecuteUserJourneyCampaignWorkflow FAILED: ",
			zap.Error(err), zap.Any("workflowRun", we))
		return response
	}
	utils.LOG.Info("ExecuteUserJourneyCampaignWorkflow started", zap.String("workflowId", workflowId))

	err = we.Get(context.Background(), &response)
	if err != nil {
		utils.LOG.Error("ExecuteUserJourneyCampaignWorkflow response error: ",
			zap.Error(err), zap.String("workflowId", workflowId))
		return response
	}

	utils.LOG.Info("ExecuteUserJourneyCampaignWorkflow completed", zap.String("workflowId", workflowId))

	return response
}

func CreatePartitions(ctx context.Context, request *ggw.CreatePartitionsRequest) (*ggw.CreatePartitionsResponse, error) {
	workflowOptions := client.StartWorkflowOptions{
		ID:                  "CREATE_PARTITIONS_CRON",
		TaskQueue:           "GeneralGoWorker",
		CronSchedule:        viper.GetString("create_cron_partition_schedule"),
		WorkflowTaskTimeout: 5 * time.Minute,
	}

	we, err := utils.WorkflowClient.ExecuteWorkflow(ctx, workflowOptions, ExecuteCreatePartitionsWorkflow, request)
	if err != nil {
		utils.LOG.Error("Error in Create Partitions cron", zap.Error(err))
		return &ggw.CreatePartitionsResponse{
			Status: Common.ResponseStatus_ERROR,
		}, nil
	}
	utils.LOG.Info("Started Create Partitions cron workflow",
		zap.Any("WorkflowID", we.GetID()), zap.Any("RunID", we.GetRunID()))

	return &ggw.CreatePartitionsResponse{
		Status: Common.ResponseStatus_SUCCESSFUL,
	}, nil
}
