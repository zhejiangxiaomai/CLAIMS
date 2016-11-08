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
InstanceResourceManager::InstanceResourceManager(){
}

InstanceResourceManager::~InstanceResourceManager() {}

void InstanceResourceManager::ReportStorageBudget(
    StorageBudgetMessage& message) {
  auto master_actor_ = Environment::getInstance()->
                    get_slave_node()->GetMasterActor();
  if (!master_actor_) {
    LOG(WARNING) << "can't connect to master in ResourceManager" << endl;
  } else {
    caf::scoped_actor self{Environment::getInstance()->get_actor_system()};
  self->request(*master_actor_, std::chrono::seconds(30),
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
          << Environment::getInstance()->get_actor_system().render(err) << endl;
      });
  }
}

void InstanceResourceManager::setStorageBudget(unsigned long memory,
                                               unsigned long disk) {}
