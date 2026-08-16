#include <faabric/util/message.h>

namespace faabric::util {

std::unique_ptr<batch_scheduler::Application> parseApplicationMsg(
  faabric::planner::RegisterApplicationRequest& rawReq)
{
    std::string appName = rawReq.appname();
    auto applicationPtr =
      std::make_unique<faabric::batch_scheduler::Application>(appName);

    // For each node, add it to the application.
    for (const auto& node : rawReq.nodes()) {
        batch_scheduler::NodeType type =
          static_cast<batch_scheduler::NodeType>(node.type());
        std::set<std::string> inputFeilds;
        bool isInput = node.input();

        if (node.inputfields_size() > 0) {
            for (const auto& field : node.inputfields()) {
                inputFeilds.insert(field);
            }
        }
        if (type == batch_scheduler::NodeType::STATELESS) {
            batch_scheduler::Node n(
              node.name(), type, isInput, 1, std::move(inputFeilds));
            
            applicationPtr->addNode(std::make_shared<batch_scheduler::Node>(n),
                                    isInput);
        } else {
            std::string partitionBy = "None";
            if (type == batch_scheduler::NodeType::PARTITIONED_STATEFUL) {
                partitionBy = node.partitionby();
            }
            batch_scheduler::Node n(node.name(),
                                    type,
                                    isInput,
                                    node.parallelism(),
                                    std::move(inputFeilds),
                                    partitionBy);
            applicationPtr->addNode(std::make_shared<batch_scheduler::Node>(n),
                                    isInput);
        }
        if (node.successornode_size() > 0) {
            for (const auto& successor : node.successornode()) {
                applicationPtr->addConnection(node.name(), successor);
            }
        }
    }

    return applicationPtr;
}

void serializeScheduledOperatorMap(
  const std::shared_ptr<faabric::planner::SyncStatesInfoRequest>& reqPtr,
  const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
    scheduledOperatorsMap)
{
    reqPtr->clear_scheduled_operators();

    for (auto const& [key, op] : scheduledOperatorsMap) {
        // Add a new entry
        auto* pbOp = reqPtr->add_scheduled_operators();

        // --- Node sub-message ---
        auto* pbNode = pbOp->mutable_node();
        pbNode->set_name(op.node.name);
        pbNode->set_type(toProto(op.node.type));
        pbNode->set_is_input(op.node.isInput);
        pbNode->set_parallelism(op.node.parallelism);
        for (auto const& f : op.node.inputFeilds) {
            pbNode->add_input_fields(f);
        }
        pbNode->set_partition_by(op.node.partitionBy);
        pbNode->set_processed_tuples(op.node.processedTuples);
        pbNode->set_pre_workload(op.node.preWorkload);
        pbNode->set_req_resource(op.node.reqResource);

        // --- ScheduledOperator fields ---
        pbOp->set_group_id(op.groupId);
        pbOp->set_is_collocate(op.isCollocate);
        pbOp->set_collocate_with(op.collocateWith);
        pbOp->set_parallelism(op.parallelism);

        // weightDist
        for (auto const& [ip, w] : op.weightDist) {
            (*pbOp->mutable_weight_dist())[ip] = w;
        }
        // parallelismDist
        for (auto const& [idx, ip] : op.parallelismDist) {
            (*pbOp->mutable_parallelism_dist())[idx] = ip;
        }
        // executorDist (Binpack mode 0 only; empty otherwise)
        for (auto const& [ip, n] : op.executorDist) {
            (*pbOp->mutable_executor_dist())[ip] = n;
        }

        pbOp->set_local_type(toProto(op.localType));
    }
}

std::map<std::string, faabric::batch_scheduler::ScheduledOperator>
parseScheduledOperatorMap(const faabric::planner::SyncStatesInfoRequest& req)
{
    std::map<std::string, faabric::batch_scheduler::ScheduledOperator> out;

    for (auto const& pbOp : req.scheduled_operators()) {
        // reconstruct the C++ Node
        const auto& pbNode = pbOp.node();
        std::set<std::string> inputs(pbNode.input_fields().begin(),
                                     pbNode.input_fields().end());
        faabric::batch_scheduler::Node n(
          pbNode.name(),
          batch_scheduler::fromProto(pbNode.type()),
          pbNode.is_input(),
          pbNode.parallelism(),
          std::move(inputs),
          pbNode.partition_by());
        n.processedTuples = pbNode.processed_tuples();
        n.preWorkload = pbNode.pre_workload();
        n.reqResource = pbNode.req_resource();

        // reconstruct the ScheduledOperator
        auto sop = faabric::batch_scheduler::ScheduledOperator(
          std::move(n),
          pbOp.group_id(),
          pbOp.is_collocate(),
          pbOp.collocate_with(),
          pbOp.parallelism(),
          {},
          {},
          batch_scheduler::fromProto(pbOp.local_type()));

        // copy the maps back
        for (auto const& entry : pbOp.weight_dist()) {
            sop.weightDist[entry.first] = entry.second;
        }
        for (auto const& entry : pbOp.parallelism_dist()) {
            sop.parallelismDist[entry.first] = entry.second;
        }
        for (auto const& entry : pbOp.executor_dist()) {
            sop.executorDist[entry.first] = entry.second;
        }

        // use the same key as your original map
        out.emplace(sop.node.name, std::move(sop));
    }

    return out;
}

}