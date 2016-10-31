/*
 * ResourceManagerSlave.cpp
 *
 *  Created on: Oct 31, 2013
 *      Author: wangli
 */

#include "ResourceManagerSlave.h"

#include <glog/logging.h>

#include "../Environment.h"
#include "../node_manager/base_node.h"
#include "caf/io/all.hpp"
#include "caf/all.hpp"
using caf::after;
using claims::NodeAddr;
using claims::OkAtom;
using claims::StorageBudgetAtom;
InstanceResourceManager::InstanceResourceManager()
          :system_(*dynamic_cast<actor_system_config *>
(Environment::getInstance()->get_caf_config())) {}

InstanceResourceManager::~InstanceResourceManager() {}

void InstanceResourceManager::ReportStorageBudget(
    StorageBudgetMessage& message) {
  caf::scoped_actor self{system_};
  caf::expected<caf::actor> master_actor = system_.middleman().
        remote_actor(Environment::getInstance()->
                     get_slave_node()->GetMasterAddr().first,
                     Environment::getInstance()->
                     get_slave_node()->GetMasterAddr().second);
  self->request(*master_actor, std::chrono::seconds(30),
                StorageBudgetAtom::value, message)
      .receive(
      [=](OkAtom) {
      LOG(INFO) << "reporting storage budget is ok!" << endl; }
      ,
      [&](const error& err) {
        if (err == sec::request_timeout) {
          LOG(ERROR) << "reporting storage budget timeout!"<< endl;
        }
      LOG(ERROR) << "reporting storage budget error!"
          << system_.render(err) << endl;
      });
}

void InstanceResourceManager::setStorageBudget(unsigned long memory,
                                               unsigned long disk) {}
