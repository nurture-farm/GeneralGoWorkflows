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

package lifecycle

import (
	campaign "github.com/nurture-farm/Contracts/CampaignService/Gen/GoCampaignService"
	common "github.com/nurture-farm/Contracts/Common/Gen/GoCommon"
	ce "github.com/nurture-farm/Contracts/CommunicationEngine/Gen/GoCommunicationEngine"
	"github.com/nurture-farm/WorkflowCoreCommon/activity_calls"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"time"
)

const (
	QUERY_UPDATE_CAMPAIGN = "SELECT id FROM campaignservicedb.campaigns where name = "
)

type ServiceBillingMetaData struct {
	PerAcreRate     int     `json:"perAcreRate,omitempty"`
	ConversionRate  float64 `json:"conversionRate,omitempty"`
	MaxRedeemPoints int     `json:"maxRedeemPoints,omitempty"`
	MaxRedeemCost   float64 `json:"maxRedeemCost,omitempty"`
	ServiceCost     float64 `json:"serviceCost,omitempty"`
	OptIn           bool    `json:"optIn,omitempty"`
}

type TemplateWarningData struct {
	CampaignName     string
	TemplateName     string
	OwnerEmail       string
	FailedPercentage float64
	LanguageId       int
}

const (
	CONST_DATE         = "date"
	CONST_X_PERCENTAGE = "x_percentage"
)

type Content struct {
	ContentId int64  `json:"content_id"`
	Tags      []Tags `json:"tags"`
}

type Tags struct {
	Tag     string   `json:"tag"`
	Values  []string `json:"values"`
	Visible bool     `json:"visible"`
}

func getIndexOfParameter(row []interface{}, parameter string) int {
	for index, value := range row {
		if value == parameter {
			return index
		}
	}
	return -1
}
func getValue(firstRow []interface{}, curRow []interface{}, parameter string) string {
	return cast.ToString(curRow[getIndexOfParameter(firstRow, parameter)])
}
func makeEvent(data TemplateWarningData) *ce.CommunicationEvent {

	emailEvent := &ce.CommunicationEvent{
		TemplateName: viper.GetString("email_template_name"),
		BusinessFlow: "TemplateWarningWorkflow",
		ReceiverActorDetails: &ce.ActorDetails{
			EmailId:      data.OwnerEmail,
			LanguageCode: common.LanguageCode_EN_US,
		},
		Channel: []common.CommunicationChannel{
			common.CommunicationChannel_EMAIL,
		},
		Placeholder: []*ce.Placeholder{
			{
				Key:   CONST_DATE,
				Value: (time.Now().AddDate(0, 0, -1)).Format("2006-01-02"),
			},
			{
				Key:   CONST_CAMPAIGN_NAME,
				Value: data.CampaignName,
			},
			{
				Key:   CONST_X_PERCENTAGE,
				Value: cast.ToString(data.FailedPercentage),
			},
		},
	}
	return emailEvent
}

func makeCampaignHalt(ctx workflow.Context, campaignName string, database string) error {
	query := QUERY_UPDATE_CAMPAIGN + "'" + campaignName + "'"
	athenaQueryResult, err := runAthenaQuery(query, database)
	if err != nil {
		workflow.GetLogger(ctx).Error("TemplateWarningWorkflow:::FAILED:Error in executing athena query", zap.Error(err))
		return err
	}
	for i, row := range athenaQueryResult {
		if i == 0 {
			continue
		}
		campaignId := cast.ToInt64(row[0])
		request := &campaign.UpdateCampaignRequest{
			Id: campaignId,
			UpdatedByActor: &common.ActorID{
				ActorId:   413428,
				ActorType: common.ActorType_FARMER,
			},
			AddCampaignRequest: &campaign.AddCampaignRequest{
				Status: common.CampaignStatus_HALTED,
			},
		}
		_, err := activity_calls.CMPSUpdateCampaign(ctx, CONST_UPDATE_CAMPAIGN_WORKFLOW, request)
		if err != nil {
			workflow.GetLogger(ctx).Error("TemplateWarningWorkflow:::FAILED:Error in Updating the campaign", zap.Any(string(campaignId), campaignId), zap.Error(err))
			return err
		}
	}
	return nil
}
