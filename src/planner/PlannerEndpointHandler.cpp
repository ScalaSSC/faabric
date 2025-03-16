#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/planner/Planner.h>
#include <faabric/planner/PlannerEndpointHandler.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/util/ExecGraph.h>
#include <faabric/util/batch.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>

namespace faabric::planner {

using header = beast::http::field;

void PlannerEndpointHandler::onRequest(
  faabric::endpoint::HttpRequestContext&& ctx,
  faabric::util::BeastHttpRequest&& request)
{
    SPDLOG_TRACE("Faabric planner received request");

    // Very permissive CORS
    faabric::util::BeastHttpResponse response;
    response.keep_alive(request.keep_alive());
    response.set(header::server, "Planner endpoint");
    response.set(header::access_control_allow_origin, "*");
    response.set(header::access_control_allow_methods, "GET,POST,PUT,OPTIONS");
    response.set(header::access_control_allow_headers,
                 "User-Agent,Content-Type");

    // Text response type
    response.set(header::content_type, "text/plain");

    // Request body contains a string that is formatted as a JSON
    std::string requestStr = request.body();

    // Handle JSON
    if (requestStr.empty()) {
        SPDLOG_ERROR("Planner handler received empty request");
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Empty request");
        return ctx.sendFunction(std::move(response));
    }

    faabric::planner::HttpMessage msg;
    try {
        faabric::util::jsonToMessage(requestStr, &msg);
    } catch (faabric::util::JsonSerialisationException e) {
        response.result(beast::http::status::bad_request);
        response.body() = std::string("Bad JSON in request body");
        return ctx.sendFunction(std::move(response));
    }

    switch (msg.type()) {
        case faabric::planner::HttpMessage_Type_RESET: {
            SPDLOG_DEBUG("Planner received RESET request");
            bool success = faabric::planner::getPlanner().reset();
            if (success) {
                response.result(beast::http::status::ok);
                response.body() = std::string("Planner fully reset!");
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed to reset planner");
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_FLUSH_AVAILABLE_HOSTS: {
            SPDLOG_DEBUG("Planner received FLUSH_AVAILABLE_HOSTS request");

            bool success = faabric::planner::getPlanner().flush(
              faabric::planner::FlushType::Hosts);

            if (success) {
                response.result(beast::http::status::ok);
                response.body() = std::string("Flushed available hosts!");
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() =
                  std::string("Failed flushing available hosts!");
            }

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_FLUSH_EXECUTORS: {
            SPDLOG_DEBUG("Planner received FLUSH_EXECUTORS request");
            bool success = faabric::planner::getPlanner().flush(
              faabric::planner::FlushType::Executors);
            if (success) {
                response.result(beast::http::status::ok);
                response.body() = std::string("Flushed executors!");
            } else {
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed flushing executors!");
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_FLUSH_SCHEDULING_STATE: {
            SPDLOG_DEBUG("Planner received FLUSH_SCHEDULING_STATE request");

            faabric::planner::getPlanner().flush(
              faabric::planner::FlushType::SchedulingState);

            response.result(beast::http::status::ok);
            response.body() = std::string("Flushed scheduling state!");

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_AVAILABLE_HOSTS: {
            SPDLOG_DEBUG("Planner received GET_AVAILABLE_HOSTS request");

            // Get the list of available hosts
            auto availableHosts =
              faabric::planner::getPlanner().getAvailableHosts();
            faabric::planner::AvailableHostsResponse hostsResponse;
            for (auto& host : availableHosts) {
                *hostsResponse.add_hosts() = *host;
            }

            // Serialise and prepare the response
            std::string responseStr;
            try {
                responseStr = faabric::util::messageToJson(hostsResponse);
                response.result(beast::http::status::ok);
                response.body() = responseStr;
            } catch (faabric::util::JsonSerialisationException& e) {
                SPDLOG_ERROR("Error processing GET_AVAILABLE_HOSTS request");
                response.result(beast::http::status::internal_server_error);
                response.body() =
                  std::string("Failed getting available hosts!");
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_CONFIG: {
            SPDLOG_DEBUG("Planner received GET_CONFIG request");
            auto config = faabric::planner::getPlanner().getConfig();
            std::string responseStr;
            try {
                responseStr = faabric::util::messageToJson(config);
                response.result(beast::http::status::ok);
                response.body() = responseStr;
            } catch (faabric::util::JsonSerialisationException& e) {
                SPDLOG_ERROR("Error processing GET_CONFIG request");
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed getting config!");
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_EXEC_GRAPH: {
            SPDLOG_DEBUG("Planner received GET_EXEC_GRAPH request");
            faabric::Message payloadMsg;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &payloadMsg);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in request body");
                return ctx.sendFunction(std::move(response));
            }

            auto execGraph = faabric::util::getFunctionExecGraph(payloadMsg);
            // An empty exec graph has one node with all fields null-ed
            if (execGraph.rootNode.msg.id() == 0) {
                SPDLOG_ERROR("Error processing GET_EXEC_GRAPH request");
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Failed getting exec. graph!");
            } else {
                response.result(beast::http::status::ok);
                response.body() = faabric::util::execGraphToJson(execGraph);
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_IN_FLIGHT_APPS: {
            SPDLOG_DEBUG("Planner received GET_IN_FLIGHT_APPS request");

            // Get in-flight apps
            auto inFlightApps =
              faabric::planner::getPlanner().getInFlightReqs();

            // Prepare response
            faabric::planner::GetInFlightAppsResponse inFlightAppsResponse;
            for (const auto& [appId, inFlightPair] : inFlightApps) {
                auto decision = inFlightPair.second;
                auto* inFlightAppResp = inFlightAppsResponse.add_apps();
                inFlightAppResp->set_appid(appId);
                for (const auto& hostIp : decision->hosts) {
                    inFlightAppResp->add_hostips(hostIp);
                }
            }

            // Also include the total number of migrations to-date
            int numMigrations =
              faabric::planner::getPlanner().getNumMigrations();
            inFlightAppsResponse.set_nummigrations(numMigrations);

            response.result(beast::http::status::ok);
            response.body() =
              faabric::util::messageToJson(inFlightAppsResponse);
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_EXECUTE_BATCH: {
            // in: BatchExecuteRequest
            // out: BatchExecuteRequestStatus
            // Parse the message payload
            SPDLOG_DEBUG("Planner received EXECUTE_BATCH request");
            faabric::BatchExecuteRequest rawBer;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawBer);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            auto ber = std::make_shared<faabric::BatchExecuteRequest>(rawBer);

            // For Request from the user, we will return false if the waiting
            // queue is too large.
            int numInFlight =
              faabric::planner::getPlanner().getInFlightAppsSize();
            if (numInFlight >= maxInflightApps) {
                response.result(beast::http::status::internal_server_error);
                response.body() = "No available hosts";
                return ctx.sendFunction(std::move(response));
            }
            // Execute the BER
            // auto decision = getPlanner().callBatch(ber);
            getPlanner().scheduleMessages(ber, false);

            // Prepare the response
            response.result(beast::http::status::ok);
            auto berStatus = faabric::util::batchExecStatusFactory(ber);
            response.body() = faabric::util::messageToJson(*berStatus);

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_EXECUTE_BATCH_STATUS: {
            // in: BatchExecuteRequestStatus
            // out: BatchExecuteRequestStatus
            // Parse the message payload
            SPDLOG_TRACE("Planner received EXECUTE_BATCH_STATUS request");
            faabric::BatchExecuteRequestStatus berStatus;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &berStatus);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in request body");
                return ctx.sendFunction(std::move(response));
            }

            // Work-out how many message results we have for the requested BER
            auto actualBerStatus =
              faabric::planner::getPlanner().getBatchResults(berStatus.appid());

            // If the result is null, it means that the app id is not
            // registered in the results map. This is an error
            if (actualBerStatus == nullptr) {
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("App not registered in results");
                return ctx.sendFunction(std::move(response));
            }

            // Prepare the response
            response.result(beast::http::status::ok);
            response.body() = faabric::util::messageToJson(*actualBerStatus);

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_PRELOAD_SCHEDULING_DECISION: {
            // foo bar
            // in: BatchExecuteRequest
            // out: none
            SPDLOG_DEBUG(
              "Planner received PRELOAD_SCHEDULING_DECISION request");
            faabric::BatchExecuteRequest ber;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &ber);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in request body");
                return ctx.sendFunction(std::move(response));
            }

            // For this method, we build the SchedulingDecision from a specially
            // crafter BER. In particular, we only need to read the BER's
            // app ID, and the `executedHost` parameter of each message in the
            // BER.
            auto decision =
              std::make_shared<batch_scheduler::SchedulingDecision>(
                ber.appid(), ber.groupid());
            for (int i = 0; i < ber.messages_size(); i++) {
                // Setting the right group idx here is key as it is the only
                // message parameter that we can emulate in advance (i.e. we
                // can not guess message ids in advance)
                decision->addMessage(ber.messages(i).executedhost(),
                                     ber.messages(i).id(),
                                     ber.messages(i).appidx(),
                                     ber.messages(i).groupidx());
            }

            // Pre-load the scheduling decision in the planner
            faabric::planner::getPlanner().preloadSchedulingDecision(
              decision->appId, decision);

            // Prepare the response
            response.result(beast::http::status::ok);
            response.body() = std::string("Decision pre-loaded to planner");

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_GET_FUNCTION_METRICS: {
            SPDLOG_DEBUG("Planner received GET_FUNCTION_METRICS request");
            response.result(beast::http::status::ok);
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_SCALE_FUNCTION_PARALLELISM: {
            SPDLOG_DEBUG("Planner received SCALE_FUNCTION_PARALLELISM request");
            faabric::planner::FunctionScaleRequest rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            std::string user = rawReq.user();
            std::string func = rawReq.function();
            std::string userFunc = user + "_" + func;
            int newParallelism = rawReq.parallelism();
            SPDLOG_DEBUG("Scaling function {} for user {} to parallelism {}",
                         func,
                         user,
                         newParallelism);
            auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
            std::shared_ptr<faabric::batch_scheduler::StateAwareScheduler>
              stateAwareScheduler = std::dynamic_pointer_cast<
                faabric::batch_scheduler::StateAwareScheduler>(batchScheduler);
            if (!stateAwareScheduler) {
                SPDLOG_ERROR(
                  "Failed to cast BatchScheduler to StateAwareBatchScheduler");
                response.result(beast::http::status::internal_server_error);
                response.body() =
                  std::string("Failed to get StateAwareScheduler");
                return ctx.sendFunction(std::move(response));
            }
            auto parallelismMap =
              stateAwareScheduler->getFunctionParallelismMap();
            if (!parallelismMap.contains(userFunc)) {
                SPDLOG_ERROR("Function {} not found in parallelism map",
                             userFunc);
                response.result(beast::http::status::internal_server_error);
                response.body() = std::string("Function not found in map");
                return ctx.sendFunction(std::move(response));
            }
            int incParallelism = newParallelism - parallelismMap.at(userFunc);
            if (incParallelism <= 0) {
                SPDLOG_ERROR("Invalid parallelism increment {}",
                             incParallelism);
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Invalid parallelism increment");
                return ctx.sendFunction(std::move(response));
            }
            faabric::planner::getPlanner().updateFuncParallelism(
              userFunc, incParallelism);

            response.result(beast::http::status::ok);
            response.body() = std::string("Parallelism updated successfully");
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_RESET_BATCH_SIZE: {
            SPDLOG_DEBUG("Planner received RESET_BATCH_SIZE request");
            faabric::planner::BatchResetRequest rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            int32_t batchsize = rawReq.batchsize();
            faabric::planner::getPlanner().resetBatchsize(batchsize);

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_RESET_REPLICAS_LIMIT: {
            SPDLOG_DEBUG("Planner received RESET_REPLICAS_LIMIT request");
            faabric::planner::MaxReplicasRequest rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            int32_t maxReplicas = rawReq.maxnum();
            faabric::planner::getPlanner().resetMaxReplicas(maxReplicas);

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_RESET_STREAM_PARAMETER: {
            SPDLOG_DEBUG("Planner received RESET_STREAM_PARAMETER request");
            faabric::planner::ResetStreamParameterRequest rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            std::string parameter = rawReq.parameter();
            int32_t value = rawReq.value();
            SPDLOG_INFO("Planner Handler Resetting parameter {} to value {}",
                        parameter,
                        value);
            if (parameter == "max_inflight_reqs") {
                maxInflightApps = value;
            } else if (parameter == "max_executors") {
                faabric::planner::getPlanner().resetParameter(parameter, value);
            } else if (parameter == "is_repartition") {
                faabric::planner::getPlanner().resetParameter(parameter, value);
            } else if (parameter == "dispatch_period") {
                faabric::planner::getPlanner().resetParameter(
                  parameter, value, true);
            } else if (parameter == "planner_call_interval") {
                faabric::planner::getPlanner().resetParameter(parameter, value);
            } else if (parameter == "batch_size") {
                faabric::planner::getPlanner().resetParameter(parameter, value);
            } else if (parameter == "max_replicas") {
                faabric::planner::getPlanner().resetParameter(parameter, value);
            } else if (parameter == "is_outputting") {
                faabric::planner::getPlanner().resetParameter(
                  parameter, value, true);
            } else {
                SPDLOG_ERROR("Unrecognized parameter {}", parameter);
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Unrecognized parameter");
                return ctx.sendFunction(std::move(response));
            }

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_REGISTER_FUNCTION_STATE: {
            SPDLOG_DEBUG("Planner received REGISTER_FUNCTION_STATE request");
            faabric::planner::RegisterFunctionStateRequest rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            std::string function = rawReq.function();
            std::string attribute = rawReq.attribute();
            std::string stateKey = rawReq.statekey();

            std::shared_ptr<faabric::batch_scheduler::BatchScheduler>
              batchScheduler = faabric::batch_scheduler::getBatchScheduler();
            // Try to cast it to a StateAwareBatchScheduler
            std::shared_ptr<faabric::batch_scheduler::StateAwareScheduler>
              stateAwareScheduler = std::dynamic_pointer_cast<
                faabric::batch_scheduler::StateAwareScheduler>(batchScheduler);
            if (stateAwareScheduler) {
                stateAwareScheduler->registerFunctionState(
                  function, attribute, stateKey);
            } else {
                SPDLOG_ERROR(
                  "Failed to cast BatchScheduler to StateAwareBatchScheduler");
                response.result(beast::http::status::internal_server_error);
                response.body() =
                  std::string("Failed to get StateAwareScheduler");
                return ctx.sendFunction(std::move(response));
            }
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_OUTPUT_RESULT: {
            SPDLOG_DEBUG("Planner received OUTPUT_RESULT request");
            faabric::planner::EmptyRequest rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            std::string result = faabric::planner::getPlanner().outputResult();
            response.result(beast::http::status::ok);
            response.body() = result;
            response.prepare_payload();

            return ctx.sendFunction(std::move(response));
        }
        default: {
            SPDLOG_ERROR("Unrecognised message type {}", msg.type());
            response.result(beast::http::status::bad_request);
            response.body() = std::string("Unrecognised message type");
            return ctx.sendFunction(std::move(response));
        }
    }
}
}
