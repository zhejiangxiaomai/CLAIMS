/*
 * IteratorExecutorMaster.cpp
 *
 *  Created on: Jun 21, 2013
 *      Author: wangli
 */

#include "IteratorExecutorMaster.h"
#include <assert.h>
#include <string>
#include "../Environment.h"
#include "../utility/rdtsc.h"
#include "caf/io/all.hpp"
#include "../node_manager/base_node.h"
#include "caf/all.hpp"

#include "../common/memory_handle.h"

using claims::SendPlanAtom;
IteratorExecutorMaster* IteratorExecutorMaster::_instance = 0;

IteratorExecutorMaster::IteratorExecutorMaster() { _instance = this; }

IteratorExecutorMaster::~IteratorExecutorMaster() { _instance = 0; }

IteratorExecutorMaster* IteratorExecutorMaster::getInstance() {
  if (_instance == 0) {
    return new IteratorExecutorMaster();
  } else {
    return _instance;
  }
}

bool IteratorExecutorMaster::ExecuteBlockStreamIteratorsOnSites(
    PhysicalOperatorBase* it, std::vector<std::string> ip_list) {
  assert(false);  // shouldn't be here;
  return true;
}

// send serialized plan string to target
bool IteratorExecutorMaster::ExecuteBlockStreamIteratorsOnSite(
    PhysicalOperatorBase* it, NodeID target_id, u_int64_t query_id = 0,
    u_int32_t segment_id = 0) {
  PhysicalQueryPlan* physical_plan = new PhysicalQueryPlan(
      it, target_id, query_id, segment_id,
      Environment::getInstance()->get_slave_node()->get_node_id());
  string str = PhysicalQueryPlan::TextSerializePlan(*physical_plan);
  actor_system system {*Environment::getInstance()->get_caf_config()};
  scoped_actor self{system};
  LOG(INFO)<<"!!!!!Master send Plan!!!!"<<endl;
  expected<actor> target_actor =
          Environment::getInstance()->get_master_node()->GetNodeActorFromId(
              target_id);
//  if (!target_actor) {
//           std::cerr << "unable to connect to node A: "
//           << system.render(target_actor.error()) << std::endl;
//  } else {
         self->send(*target_actor, SendPlanAtom::value, str, query_id, segment_id);
//  }
  //  try {
    //warning: remote_actor may not reach.

//  } catch (caf::bind_failure& e) {
//    LOG(ERROR)
//        << "master sending plan binds port error when connecting remote actor";
//  } catch (caf::network_error& e) {
//    LOG(ERROR) << "master sending plan connect to remote node error due to "
//                  "network error!";
//  }
  DELETE_PTR(physical_plan);
  LOG(INFO) << "master send serialized plan to target slave : " << target_id
            << " succeed!" << endl;
  return true;
}
bool IteratorExecutorMaster::Propogation(const int count, std::string target) {
  assert(false);  // shouldn't be here;
  return true;
}
