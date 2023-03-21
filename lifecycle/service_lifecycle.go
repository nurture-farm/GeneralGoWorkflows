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
	"github.com/aws/aws-sdk-go/service/athena"
	common "github.com/nurture-farm/Contracts/Common/Gen/GoCommon"
	ce "github.com/nurture-farm/Contracts/CommunicationEngine/Gen/GoCommunicationEngine"
	"github.com/spf13/viper"
	"strings"

	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	cmps "github.com/nurture-farm/Contracts/CampaignService/Gen/GoCampaignService"
	ggw "github.com/nurture-farm/Contracts/Workflows/GeneralGoWorkflows/Gen/GoGeneralGoWorkflows"
	"github.com/nurture-farm/GeneralGoWorkflows/metrics"
	"github.com/nurture-farm/WorkflowCoreCommon/activity_calls"
	//cm "github.com/nurture-farm/Contracts/CropModel/Gen/GoCropModel"
	//fs "github.com/nurture-farm/Contracts/FarmService/Gen/GoFarmService"
	db_driver "github.com/nurture-farm/go-database-driver"
	db_driver_config "github.com/nurture-farm/go-database-driver/driverConfig"
	"github.com/spf13/cast"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

const (
	CONST_BOOKING_SYSTEM                                     = "BOOKING_SYSTEM"
	CONST_FARM_SERVICE_SYSTEM                                = "FARM_SERVICE_SYSTEM"
	CONST_REWARDS_GATEWAY                                    = "REWARDS_GATEWAY"
	CONST_AFS_FARMER_APP                                     = "AFS_FARMER_APP"
	CONST_RETAILER_SYSTEM                                    = "RETAILER_SYSTEM"
	CONST_OFFER_SERVICE                                      = "OFFER_SERVICE"
	CONST_CAMPAIGN_WORKFLOW                                  = "CAMPAIGN_WORKFLOW"
	CONST_USER_JOURNEY_CAMPAIGN_WORKFLOW                     = "USER_JOURNEY_CAMPAIGN_WORKFLOW"
	CONST_UPDATE_CAMPAIGN_WORKFLOW                           = "UPDATE_CAMPAIGN_WORKFLOW"
	CONST_NOTITFY_TECHNICIAN_PREVENTIVE_MAINTENANCE_WORKFLOW = "NOTITFY_TECHNICIAN_PREVENTIVE_MAINTENANCE_WORKFLOW"
	CONST_SMS_FARMER_APP_OTP_TEMPLATE                        = "farmer_app_otp"
	CONST_WHATSAPP_FARMER_APP_OTP_TEMPLATE                   = "farmer_app_otp_wa"
	CONST_STATE_KARNATAKA                                    = "KARNATAKA"
	CONST_STATE_TANMIL_NADU                                  = "TAMIL NADU"
	CONST_STATE_KERALA                                       = "KERALA"
	CONST_STATE_ARUNACHAL_PRADESH                            = "ARUNACHAL PRADESH"
	CONST_STATE_ASSAM                                        = "ASSAM"
	CONST_STATE_MANIPUR                                      = "MANIPUR"
	CONST_STATE_MEGHALAYA                                    = "MEGHALAYA"
	CONST_STATE_MIZORAM                                      = "MIZORAM"
	CONST_STATE_NAGALAND                                     = "NAGALAND"
	CONST_STATE_TRIPURA                                      = "TRIPURA"
	CONST_STATE_SIKKIM                                       = "SIKKIM"
	CONST_STATE_GOA                                          = "GOA"
	CONST_AWS_REGION                                         = "ap-south-1"
	CONST_PARTITION_PATH_NUMERIC_VALUE_FORMATE               = "%02d"
	CONST_FAILED_PERCENTAGE                                  = "failed_percentage"
	CONST_CAMPAIGN_NAME                                      = "campaign_name"
	CONST_TEMPLATE_NAME                                      = "template_name"
	CONST_LANGUAGE_ID                                        = "language_id"
	CONST_OWNER_EMAIL                                        = "owner_email"
	CONST_CAMPAIGN_SERVICE_DB                                = "campaignservicedb"
	CONST_COMMUNICATION_ENGINE_DB                            = "communication_engine"
	CONST_NIL                                                = "NIL"
	QUERY_TEMPLATE_WARNING                                   = "select table3.campaign_name, table3.template_name, table3.language_id, table3.failed_percentage, table4.owner_email from ((select table1.campaign_name,table1.template_name, table1.language_id, ((table1.customer_undelivered*1.0)/(table2.total_sent*1.0)*100.0) as  failed_percentage from ((select campaign_name, template_name, language_id, count(*) as customer_undelivered from communication_engine.message_acknowledgements where date(cast(year as varchar(255)) || '-' || cast(month as varchar(255)) || '-' || cast(day as varchar(255))) = current_date - interval '1' day\n     and (state='CUSTOMER_UNDELIVERED' or state='VENDOR_UNDELIVERED') group by template_name, language_id, campaign_name) table1 join \n     \n(select campaign_name, template_name, language_id, count(*) as total_sent from communication_engine.message_acknowledgements where date(cast(year as varchar(255)) || '-' || cast(month as varchar(255)) || '-' || cast(day as varchar(255))) = current_date - interval '1' day\n     group by template_name, language_id, campaign_name) table2 on table1.template_name=table2.template_name and table1.language_id=table2.language_id and table1.campaign_name=table2.campaign_name) order by  failed_percentage desc) table3 join (select name, language_id, owner_email from communication_engine.templates ) table4 on table3.template_name=table4.name and table3.language_id=table4.language_id) order by  table3. failed_percentage desc"
)

var sourceMap = map[string]common.SourceSystem{
	CONST_BOOKING_SYSTEM:      common.SourceSystem_BOOKING_SYSTEM,
	CONST_FARM_SERVICE_SYSTEM: common.SourceSystem_FARM_SERVICE_SYSTEM,
	CONST_REWARDS_GATEWAY:     common.SourceSystem_REWARDS_GATEWAY,
	CONST_AFS_FARMER_APP:      common.SourceSystem_AFS_FARMER_APP,
	CONST_RETAILER_SYSTEM:     common.SourceSystem_RETAILER_SYSTEM,
	CONST_OFFER_SERVICE:       common.SourceSystem_OFFER_SERVICE,
}

var languageToLanguageCodeMap = map[common.Language]common.LanguageCode{
	common.Language_NO_LANGUAGE: common.LanguageCode_HI_IN, //default Hindi
	common.Language_ENGLISH:     common.LanguageCode_EN_US,
	common.Language_HINDI:       common.LanguageCode_HI_IN,
	common.Language_GUJARATI:    common.LanguageCode_GU,
	common.Language_PUNJABI:     common.LanguageCode_PA,
	common.Language_KANNADA:     common.LanguageCode_KN,
	common.Language_BENGALI:     common.LanguageCode_BN,
	common.Language_MARATHI:     common.LanguageCode_MR,
	common.Language_MALAYALAM:   common.LanguageCode_ML,
	common.Language_TAMIL:       common.LanguageCode_ML,
	common.Language_TELUGU:      common.LanguageCode_TE,
}

var smsTemplateToWhatsappTemplateMap = map[string]string{
	CONST_SMS_FARMER_APP_OTP_TEMPLATE: CONST_WHATSAPP_FARMER_APP_OTP_TEMPLATE,
}

var contentRegionMap = map[string]string{
	CONST_STATE_KARNATAKA:         "KARNATAKA",
	CONST_STATE_TANMIL_NADU:       "TAMIL NADU",
	CONST_STATE_KERALA:            "KERALA",
	CONST_STATE_ARUNACHAL_PRADESH: "ARUNACHAL PRADESH",
	CONST_STATE_ASSAM:             "ASSAM",
	CONST_STATE_MANIPUR:           "MANIPUR",
	CONST_STATE_MEGHALAYA:         "MEGHALAYA",
	CONST_STATE_MIZORAM:           "MIZORAM",
	CONST_STATE_NAGALAND:          "NAGALAND",
	CONST_STATE_TRIPURA:           "TRIPURA",
	CONST_STATE_SIKKIM:            "SIKKIM",
	CONST_STATE_GOA:               "GOA",
}

func OTPDeliveryWorkflow(ctx workflow.Context) (*ggw.HandoverOtpResponse, error) {

	startTime := time.Now().Add(-90 * time.Second)
	workflow.GetLogger(ctx).Info("OTPDeliveryWorkflow:::STARTED::", zap.Any("startTime", startTime))
	response := &ggw.HandoverOtpResponse{}
	startTimestamppb := timestamppb.New(startTime)
	endTime := time.Now().Add(-30 * time.Second)
	endTimestamppb := timestamppb.New(endTime)
	request := &ce.MessageAcknowledgementRequest{
		StartTime: startTimestamppb,
		EndTime:   endTimestamppb,
		Channels: []common.CommunicationChannel{
			common.CommunicationChannel_SMS,
		},
		TemplateNameLike: "%otp%",
	}
	workflow.GetLogger(ctx).Info("OTPDeliveryWorkflow calling CEPSearchMessageAcknowledgements")
	cepResponse, err := activity_calls.CEPSearchMessageAcknowledgements(ctx, CONST_NOTITFY_TECHNICIAN_PREVENTIVE_MAINTENANCE_WORKFLOW, request)
	if err != nil {
		workflow.GetLogger(ctx).Error("OTPDeliveryWorkflow:::FAILED::CEPSearchMessageAcknowledgements", zap.Error(err))
		return nil, err
	}

	if cepResponse.Status != common.ResponseStatus_SUCCESSFUL {
		workflow.GetLogger(ctx).Error("OTPDeliveryWorkflow:::FAILED::CEPSearchMessageAcknowledgements")
		return nil, fmt.Errorf("%s", "OTPDeliveryWorkflow:::FAILED::CEPSearchMessageAcknowledgements")
	}

	failedOTPSMSMap := make(map[string]*ce.MessageAcknowledgement)
	for _, messageAck := range cepResponse.MessageAcknowledgements {
		if messageAck.TemplateName == CONST_SMS_FARMER_APP_OTP_TEMPLATE {
			if messageAck.State == common.CommunicationState_CUSTOMER_DELIVERED {
				if _, ok := failedOTPSMSMap[messageAck.ReferenceId]; ok {
					delete(failedOTPSMSMap, messageAck.ReferenceId)
				}
			} else {
				if _, ok := failedOTPSMSMap[messageAck.ReferenceId]; !ok {
					failedOTPSMSMap[messageAck.ReferenceId] = messageAck
				}
			}
		}
	}

	whatsappEvents := &ce.BulkCommunicationEvent{
		CommunicationEvents: []*ce.CommunicationEvent{},
	}
	for _, messageAck := range failedOTPSMSMap {
		var placeholder *ce.Placeholder
		for i := range messageAck.Placeholders {
			if messageAck.Placeholders[i].Key == "OTP" {
				placeholder = &ce.Placeholder{
					Key:   "1",
					Value: messageAck.Placeholders[i].Value,
				}
				break
			}
		}
		workflow.GetLogger(ctx).Info("OTPDeliveryWorkflow, value for placeholder", zap.Any("messageAckPlaceholder", messageAck.Placeholders),
			zap.Any("OTPPlaceholder", placeholder))
		whatsappEvent := &ce.CommunicationEvent{
			TemplateName:  smsTemplateToWhatsappTemplateMap[messageAck.TemplateName],
			ReceiverActor: messageAck.Actor,
			BusinessFlow:  "OTPDeliveryWorkflow",
			ReceiverActorDetails: &ce.ActorDetails{
				MobileNumber: messageAck.MobileNumber,
				LanguageCode: common.LanguageCode_EN_US,
			},
			Channel: []common.CommunicationChannel{
				common.CommunicationChannel_WHATSAPP,
			},
			ReferenceId: messageAck.ReferenceId,
			Placeholder: []*ce.Placeholder{placeholder},
		}
		whatsappEvents.CommunicationEvents = append(whatsappEvents.CommunicationEvents, whatsappEvent)
	}

	if len(whatsappEvents.CommunicationEvents) > 0 {
		workflow.GetLogger(ctx).Info("OTPDeliveryWorkflow calling CEPSearchMessageAcknowledgements")
		err := activity_calls.CSSendBulkCommunication(ctx, "OTPDeliveryWorkflow", whatsappEvents)
		if err != nil {
			workflow.GetLogger(ctx).Error("OTPDeliveryWorkflow:::FAILED::CEPSendWhatsappMessage", zap.Error(err))
			return nil, err
		}
	} else {
		workflow.GetLogger(ctx).Info("OTPDeliveryWorkflow, no OTP message found in the interval", zap.Any("startTime", startTimestamppb),
			zap.Any("endTime", endTimestamppb))
	}

	workflow.GetLogger(ctx).Info("OTPDeliveryWorkflow:::COMPLETED", zap.Any("whatsappEvents", whatsappEvents))
	response.Status = common.ResponseStatus_SUCCESSFUL
	return response, nil
}

func TemplateWarningWorkflow(ctx workflow.Context) (*ggw.HandoverOtpResponse, error) {
	workflow.GetLogger(ctx).Info("TemplateWarningWorkflow started")
	query := QUERY_TEMPLATE_WARNING
	database := CONST_COMMUNICATION_ENGINE_DB
	athenaQueryResult, err := runAthenaQuery(query, database)
	if err != nil {
		workflow.GetLogger(ctx).Error("TemplateWarningWorkflow:::FAILED:Error in executing athena query", zap.Error(err))
		return nil, err
	}
	emailEvents := &ce.BulkCommunicationEvent{
		CommunicationEvents: []*ce.CommunicationEvent{},
	}
	response := &ggw.HandoverOtpResponse{}
	thresholdPercentage := cast.ToFloat64(viper.GetString("template_threshold_percentage"))
	for i, row := range athenaQueryResult {
		if i == 0 {
			continue
		}
		templateWarningData := TemplateWarningData{CampaignName: getValue(athenaQueryResult[0], row, CONST_CAMPAIGN_NAME), TemplateName: getValue(athenaQueryResult[0], row, CONST_TEMPLATE_NAME), OwnerEmail: getValue(athenaQueryResult[0], row, CONST_OWNER_EMAIL), FailedPercentage: cast.ToFloat64(getValue(athenaQueryResult[0], row, CONST_FAILED_PERCENTAGE)), LanguageId: cast.ToInt(getValue(athenaQueryResult[0], row, CONST_LANGUAGE_ID))}
		if templateWarningData.FailedPercentage >= thresholdPercentage {
			if templateWarningData.OwnerEmail == CONST_NIL {
				templateWarningData.OwnerEmail = viper.GetString("default_email_for_template_warning")
			}
			if templateWarningData.CampaignName == "" {
				templateWarningData.CampaignName = templateWarningData.TemplateName
			} else {
				makeCampaignHalt(ctx, templateWarningData.CampaignName, CONST_CAMPAIGN_SERVICE_DB)
			}
			emailEvents.CommunicationEvents = append(emailEvents.CommunicationEvents, makeEvent(templateWarningData))
		}
	}
	if len(emailEvents.CommunicationEvents) > 0 {
		workflow.GetLogger(ctx).Info("TemplateWarningWorkflow calling CEPSearchMessageAcknowledgements")
		err := activity_calls.CSSendBulkCommunication(ctx, "TemplateWarningWorkflow", emailEvents)
		if err != nil {
			//workflow.GetLogger(ctx).Error("TemplateWarningWorkflow:::FAILED::CEPSendWhatsappMessage", zap.Error(err))
			return nil, err
		}
	} else {
		workflow.GetLogger(ctx).Info("TemplateWarningWorkflow, no template or campaign is failing")
	}

	workflow.GetLogger(ctx).Info("TemplateWarningWorkflow:::COMPLETED", zap.Any("emailEvents", emailEvents))
	response.Status = common.ResponseStatus_SUCCESSFUL
	return response, nil
}
func mapCommunicationVendor(vendor string) common.CommunicationVendor {

	var res common.CommunicationVendor
	vendor = strings.ToUpper(vendor)
	if vendor == "GUPSHUP" {
		res = common.CommunicationVendor_KARIX
	} else if vendor == "KARIX" {
		res = common.CommunicationVendor_GUPSHUP
	} else { //by default
		res = common.CommunicationVendor_GUPSHUP
	}
	return res
}

func ExecuteCampaignWorkflow(wctx workflow.Context, request *ggw.ExecuteCampaignRequest) (*ggw.ExecuteCampaignResponse, error) {

	ctx := context.Background()
	workflow.GetLogger(wctx).Info("ExecuteCampaignWorkflow:::STARTED", zap.Any("request", request))

	var err error
	defer metrics.Metrics.PushToSummarytMetrics()(metrics.ExecuteCampaignWorkflow_Metrics, "ExecuteCampaignWorkflow", &err, ctx)

	campaignRequest := &cmps.CampaignRequest{
		CampaignId: request.CampaignId,
	}

	_, err = activity_calls.CMPSCampaign(wctx, CONST_CAMPAIGN_WORKFLOW, campaignRequest)
	if err != nil {
		workflow.GetLogger(wctx).Error("ExecuteCampaignWorkflow:::FAILED::CMPSCampaign", zap.Error(err), zap.Any("request", request))
		metrics.Metrics.PushToErrorCounterMetrics()(metrics.ExecuteCampaignWorkflow_CampaignError_Metrics, fmt.Errorf("ERROR_CAMPAIGN"), ctx)
		return nil, err
	}
	selector := workflow.NewSelector(wctx)
	done := false
	counter := 2
	if request.CampaignScheduleType == common.CampaignScheduleType_INACTION_OVER_TIME {
		var temp func(f workflow.Future)
		temp = func(f workflow.Future) {
			counter--
			duration, _ := ptypes.Duration(request.InactionDuration)
			timer := workflow.NewTimer(wctx, duration)
			selector.AddFuture(timer, temp)
			if counter == 0 {
				workflow.GetLogger(wctx).Info("ExecuteCampaignWorkflow:::TimerSignal::Retriggering Campaign activity", zap.Any("request", request))
				_, err = activity_calls.CMPSCampaign(wctx, CONST_CAMPAIGN_WORKFLOW, campaignRequest)
				if err != nil {
					workflow.GetLogger(wctx).Error("ExecuteCampaignWorkflow:::TimerSignal::Retriggering Campaign activity failled", zap.Error(err), zap.Any("request", request))
					metrics.Metrics.PushToErrorCounterMetrics()(metrics.ExecuteCampaignWorkflow_CampaignError_Metrics, fmt.Errorf("ERROR_CAMPAIGN"), ctx)
					return
				}
				workflow.GetLogger(wctx).Info("TimerSignal::Retriggering Campaign activity success", zap.Any("request", request))
				done = true
			}
		}

		temp(nil)

		for !done {
			selector.Select(wctx)
		}
	}

	response := &ggw.ExecuteCampaignResponse{
		Status: common.ResponseStatus_SUCCESSFUL,
	}
	workflow.GetLogger(wctx).Info("ExecuteCampaignWorkflow:::COMPLETED", zap.Any("request", request))
	return response, nil
}

func ExecuteUserJourneyCampaignWorkflow(wctx workflow.Context, request *ggw.ExecuteUserJourneyCampaignRequest) (*ggw.ExecuteUserJourneyCampaignResponse, error) {

	ctx := context.Background()
	workflow.GetLogger(wctx).Info("ExecuteUserJourneyCampaignWorkflow:::STARTED", zap.Any("request", request))

	workflow.Sleep(wctx, request.WaitDuration.AsDuration())
	var err error
	defer metrics.Metrics.PushToSummarytMetrics()(metrics.ExecuteUserJourneyCampaignWorkflow_Metrics, "ExecuteUserJourneyCampaignWorkflow", &err, ctx)

	userJourneyCampaignRequest := &cmps.UserJourneyCampaignRequest{
		CampaignId:         request.CampaignId,
		EngagementVertexId: request.EngagementVertexId,
		ReferenceId:        request.ReferenceId,
	}

	_, err = activity_calls.CMPSUserJourneyCampaign(wctx, CONST_USER_JOURNEY_CAMPAIGN_WORKFLOW, userJourneyCampaignRequest)
	if err != nil {
		workflow.GetLogger(wctx).Error("ExecuteUserJourneyCampaignWorkflow:::FAILED::CMPSCampaign", zap.Error(err), zap.Any("request", request))
		metrics.Metrics.PushToErrorCounterMetrics()(metrics.ExecuteUserJourneyCampaignWorkflow_CampaignError_Metrics, fmt.Errorf("ERROR_USER_JOURNEY_CAMPAIGN"), ctx)
		return nil, err
	}

	response := &ggw.ExecuteUserJourneyCampaignResponse{
		Status: common.ResponseStatus_SUCCESSFUL,
	}
	workflow.GetLogger(wctx).Info("ExecuteUserJourneyCampaignWorkflow:::COMPLETED", zap.Any("request", request))
	return response, nil
}
func CreatePartitionsWorkflow(wctx workflow.Context, request *ggw.CreatePartitionsRequest) (*ggw.CreatePartitionsResponse, error) {

	workflow.GetLogger(wctx).Info("CreatePartitionsWorkflow:::STARTED")
	currentTime := time.Now()

	var err error

	start := currentTime.AddDate(0, 0, 20)
	end := start.AddDate(0, 0, int(request.NumberOfDays-1))

	ptr := start

	for ptr.Before(end) || ptr.Equal(end) {
		err = createPartitionForDate(wctx, ptr, request.GetTableName(), request.GetDatabaseName(), request.GetBucket())
		if err != nil {
			metrics.CreatePartitions_Metrics.WithLabelValues(metrics.SERVICE_NAME, "CreatePartitions", "KO").Add(1)
			workflow.GetLogger(wctx).Info("CreatePartitionsWorkflow::FAILED::createPartitionForDate", zap.Error(err), zap.Any("request", request))
		}

		ptr = ptr.AddDate(0, 0, 1)
	}
	workflow.GetLogger(wctx).Info("CreatePartitionsWorkflow:::COMPLETED")
	metrics.CreatePartitions_Metrics.WithLabelValues(metrics.SERVICE_NAME, "CreatePartitions", "OK").Add(1)
	return &ggw.CreatePartitionsResponse{
		Status: common.ResponseStatus_SUCCESSFUL,
	}, nil
}

func createPartitionForDate(wctx workflow.Context, date time.Time, table string, db string, bucket string) error {
	yearStr := fmt.Sprintf(CONST_PARTITION_PATH_NUMERIC_VALUE_FORMATE, date.Year())
	monthStr := fmt.Sprintf(CONST_PARTITION_PATH_NUMERIC_VALUE_FORMATE, int(date.Month()))
	dayStr := fmt.Sprintf(CONST_PARTITION_PATH_NUMERIC_VALUE_FORMATE, date.Day())
	for i := 0; i <= 23; i++ {
		hourStr := fmt.Sprintf("%02d", i)
		query := "ALTER TABLE " + db + "." + table + " ADD PARTITION (year=" + yearStr + ", month=" + monthStr + ", day=" + dayStr + ", hour=" + hourStr + ")  location '" + bucket + yearStr + "/" + monthStr + "/" + dayStr + "/" + hourStr + "'"
		workflow.GetLogger(wctx).Info("CreatePartitionsWorkflow Athena query generation successful for iteration", zap.Any("iteration", i),
			zap.String("AthenaQuery", query))
		_, err := runAthenaQuery(query, db)
		if err != nil {
			workflow.GetLogger(wctx).Info("CreatePartitionsWorkflow Athena query execution failed", zap.Any("iteration", i),
				zap.String("AthenaQuery", query))
			return err
		}
	}

	return nil
}

func runAthenaQuery(query string, db string) ([][]interface{}, error) {
	var s athena.StartQueryExecutionInput
	s.QueryString = &query

	var q athena.QueryExecutionContext
	q.SetDatabase(db)
	s.SetQueryExecutionContext(&q)

	athenaConfig := &db_driver_config.AthenaConfig{
		AwsAccessKey: viper.GetString("AWS_ACCESS_KEY"),
		AwsSecretKey: viper.GetString("AWS_SECRET_KEY"),
		AwsRegion:    CONST_AWS_REGION,
		Database:     db,
	}

	err := db_driver.InitializeDrivers(db_driver.ATHENA, athenaConfig) // Driver is not singleton because the driver will be used very rarely ~ once a week only
	if err != nil {
		return nil, err
	}

	_, queryResult, _, err := db_driver.ExecuteQuery(query, db_driver.ATHENA)

	if err != nil {
		return nil, err
	}

	return queryResult, nil
}
