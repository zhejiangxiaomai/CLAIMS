/*
 * BlockManagerMaster.cpp
 *
 *  Created on: 2013-10-15
 *      Author: casa
 */

#include <sstream>

#include "BlockManagerMaster.h"

#include "../common/Message.h"
#include "../utility/print_tool.h"

#include "caf/io/all.hpp"
#include "caf/all.hpp"
#include "../node_manager/base_node.h"
using caf::after;
using claims::BindingAtom;
using claims::OkAtom;
using claims::UnBindingAtom;
using namespace claims;
BlockManagerMaster *BlockManagerMaster::master_ = 0;

BlockManagerMaster::BlockManagerMaster() {
  master_ = this;
}

BlockManagerMaster::~BlockManagerMaster() { master_ = 0; }

void BlockManagerMaster::initialize() { abi_ = AllBlockInfo::getInstance(); }

/*
 * send message to specified node to set chunk number in partition
 * whose id is partition_id.
 *
 * this function is used in two way:
 *    1) bind a projection to a node:
 *      add new partition info like chunk number in the node
 *      chunk number is not changed
 *    2) update the chunk number in a node: chunk number changed
 */
bool BlockManagerMaster::SendBindingMessage(
    const PartitionID &partition_id, const unsigned &number_of_chunks,
    const StorageLevel &desirable_storage_level, const NodeID &target) const {
  auto& target_actor =
          Environment::getInstance()->get_slave_node()->GetNodeActorFromId(
              target);
    if (!target_actor) {
      LOG(WARNING) << "can't connect to node "<< std::endl;
    } else {
      scoped_actor self{Environment::getInstance()->get_actor_system()};
      self->request(*target_actor, std::chrono::seconds(30),
                      BindingAtom::value,
                  partition_id, number_of_chunks,
                  desirable_storage_level)
                .receive(
            [](OkAtom) {
              LOG(INFO) << "sending binding message is OK!!" << endl;
            },
            [&](const error& err) {
              LOG(WARNING) << "sending binding message, but timeout 30s!!"
                  << endl;
             return false;
             });
    }

  return true;
}

/*
 * As opposed to SendBindingMessage, except this method isn't used in updating
 * chunk number
 */
bool BlockManagerMaster::SendUnbindingMessage(const PartitionID &partition_id,
                                              NodeID &target) const {
  auto &target_actor =
          Environment::getInstance()->get_master_node()->GetNodeActorFromId(
              target);
  if (!target_actor) {
    LOG(WARNING) << "can't connect to node id: " << target;
  } else {
    scoped_actor self{Environment::getInstance()->get_actor_system()};
    self->request(*target_actor, std::chrono::seconds(30),
                  UnBindingAtom::value, partition_id)
                      .receive(
        [=](OkAtom) {
          LOG(INFO) << "sending unbinding message is OK!!" << endl;
        },
        [&](const error& err) {
          LOG(WARNING) << "sending unbinding message, but timeout 30s!!"<<
              Environment::getInstance()->get_actor_system().render(err)<< endl;
          return false;
        });
  }

  return true;
}
