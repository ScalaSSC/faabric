#include <faabric/batch-scheduler/Application.h>
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
#include <faabric/util/message.h>

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
            // Prepare response
            faabric::planner::GetInFlightAppsResponse inFlightAppsResponse;
            int numInFlightApp =
              faabric::planner::getPlanner().getInFlightAppsSize();
            inFlightAppsResponse.set_numinflightapp(numInFlightApp);

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

            // For the request from the user, we will return false if the
            // waiting queue is too large.
            bool enqueued =
              faabric::planner::getPlanner().enqueueBatchRequest(ber);
            if (!enqueued) {
                response.result(beast::http::status::internal_server_error);
                response.body() = "No available hosts";
                return ctx.sendFunction(std::move(response));
            }
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
        case faabric::planner::HttpMessage_Type_CUSTOM: {
            SPDLOG_DEBUG("Planner received CUSTOM request");
            faabric::planner::CustomRequest rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            std::string key = rawReq.key();
            std::string value = rawReq.value();
            SPDLOG_DEBUG("Custom request with key {} and value {}", key, value);
            if (key == "reschedule") {
                // value looks like "0-5".
                // The first number means schedule mode,
                // value == 0 means reschedule immediately.
                // value == 1 means wait until all running messages are
                // finished.
                // The second number means the number of hosts to schedule. 0
                // means no change.
                if (value.empty()) {
                    value = "0-0";
                }

                int scheduleMode = 0;
                int numHosts = 0;
                size_t dashPos = value.find('-');
                if (dashPos != std::string::npos) {
                    try {
                        scheduleMode = std::stoi(value.substr(0, dashPos));
                        numHosts = std::stoi(value.substr(dashPos + 1));
                        SPDLOG_DEBUG("Parsed reschedule: mode={}, hosts={}",
                                     scheduleMode,
                                     numHosts);
                    } catch (const std::exception& e) {
                        SPDLOG_ERROR(
                          "Failed to parse reschedule integers from '{}': {}",
                          value,
                          e.what());
                        scheduleMode = 0;
                        numHosts = 0;
                    }
                } else {
                    SPDLOG_ERROR("Malformed reschedule value. Expected format "
                                 "'X-Y', got: {}",
                                 value);
                }

                faabric::planner::getPlanner().rescheduleApp(scheduleMode,
                                                             numHosts);
            } else {
                SPDLOG_ERROR("Unrecognized custom request key {}", key);
                response.result(beast::http::status::bad_request);
                response.body() =
                  std::string("Unrecognized custom request key");
                return ctx.sendFunction(std::move(response));
            }

            response.result(beast::http::status::ok);
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
            static const std::unordered_set<std::string> plannerParams = {
                "is_outputting",
                "num_hosts_scheduled",
                "runtime_reconfig_period",
                "max_inflight_reqs",
                "max_waiting_queue_size",
                "worker_queue_num_threshold",
                "worker_queue_time_threshold",
                "planner_queue_size_threshold",
                "planner_queue_age_threshold",
            };

            if (plannerParams.contains(parameter)) {
                faabric::planner::getPlanner().resetParameter(
                  parameter, value, true);
            } else {
                faabric::planner::getPlanner().resetParameter(parameter, value);
            }

            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_SET_PERSISTENT_STATE: {
            SPDLOG_DEBUG("Planner received SET_PERSISTENT_STATE request");
            faabric::planner::MapMessage rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }
            faabric::planner::getPlanner().setPersistentState(rawReq);
            return ctx.sendFunction(std::move(response));
        }
        case faabric::planner::HttpMessage_Type_REGISTER_APPLICATION: {
            SPDLOG_INFO("Planner received REGISTER_APPLICATION request");
            faabric::planner::RegisterApplicationRequest rawReq;
            try {
                faabric::util::jsonToMessage(msg.payloadjson(), &rawReq);
            } catch (faabric::util::JsonSerialisationException e) {
                response.result(beast::http::status::bad_request);
                response.body() = std::string("Bad JSON in body's payload");
                return ctx.sendFunction(std::move(response));
            }

            auto applicationPtr = faabric::util::parseApplicationMsg(rawReq);

            // Distribute the application to the workers.
            faabric::planner::getPlanner().registerApp(
              rawReq, std::move(applicationPtr));

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
            // Return the statistics in json format
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
