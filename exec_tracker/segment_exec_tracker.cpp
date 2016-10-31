/*
 * Copyright [2012-2015] DaSE@ECNU
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * /Claims/Executor/segment_exec_tracker.cpp
 *
 *  Created on: Mar 24, 2016
 *      Author: fzh
 *		   Email: fzhedu@gmail.com
 *
 * Description:
 *
 */

#include "./segment_exec_tracker.h"
#include <glog/logging.h>
#include <string>
#include "caf/all.hpp"
#include "../Environment.h"
#include "caf/io/all.hpp"

#include "./stmt_exec_status.h"
#include "../exec_tracker/segment_exec_status.h"
using caf::actor_pool;
using caf::event_based_actor;
using caf::time_unit;
using std::string;
namespace claims {


SegmentExecTracker::SegmentExecTracker():
    system_(*dynamic_cast<actor_system_config *>
(Environment::getInstance()->get_caf_config())),
    segment_exec_tracker_actor_(unsafe_actor_handle_init) {
  segment_exec_tracker_actor_ =
      system_.spawn(SegmentExecTracker::ReportAllSegStatus, this);
  LOG(INFO) << "SegmentExecTracker created" << std::endl;
}

SegmentExecTracker::~SegmentExecTracker() {
  caf::scoped_actor self{system_};
  self->send(segment_exec_tracker_actor_, ExitAtom::value);
  LOG(INFO) << "SegmentExecTracker destoryed" << std::endl;
}

behavior SegmentExecTracker::ReportAllSegStatus(
    caf::event_based_actor* self, SegmentExecTracker* seg_exec_tracker_) {
  return {
      [=](ReportSegESAtom) {
        seg_exec_tracker_->map_lock_.acquire();
        if (seg_exec_tracker_->node_segment_id_to_status_.size() > 0) {
          auto it = seg_exec_tracker_->node_segment_id_to_status_.begin();
          for (; it != seg_exec_tracker_->node_segment_id_to_status_.end();) {
            assert(it->second != NULL);
            if (it->second->stop_report_) {
              // every sending message sent before has been received, so you
              // can
              // delete it now
              if (it->second->logic_time_ == 0) {
                LOG(INFO) << it->second->node_segment_id_.first << " , "
                          << it->second->node_segment_id_.second
                          << " has been deleted from tracker";
                DELETE_PTR(it->second);
                it = seg_exec_tracker_->node_segment_id_to_status_.erase(it);
              } else {
                LOG(WARNING) << it->second->node_segment_id_.first << " , "
                             << it->second->node_segment_id_.second
                             << "segment report status out of order0!";
                ++it;
              }
            } else {
              ++it->second->logic_time_;
              self->send(self, ReportSAtom::value, (*it->second));  // bug han
              ++it;
            }
          }
        }
        seg_exec_tracker_->map_lock_.release();
        self->delayed_send(self, std::chrono::milliseconds(kReportIntervalTime),
                     ReportSegESAtom::value);
      },
      [&](ReportSAtom, SegmentExecStatus& seg_exec_status) {
        // get the status of the corresponding segment
        seg_exec_status.lock_.acquire();
        int exec_status = seg_exec_status.get_exec_status();
        string exec_info = seg_exec_status.get_exec_info();
        seg_exec_status.lock_.release();
        if (seg_exec_status.stop_report_ == true) {
          // shouldn't report
          LOG(WARNING) << seg_exec_status.node_segment_id_.first << " , "
                       << seg_exec_status.node_segment_id_.second
                       << "segment report status out of order!";
        } else {
          LOG(INFO) << seg_exec_status.node_segment_id_.first << " , "
                    << seg_exec_status.node_segment_id_.second
                    << " before send: " << exec_status << " , " << exec_info;
          // construct remote actor
          NodeAddr addr =
              Environment::getInstance()->get_slave_node()->GetNodeAddrFromId(
                  seg_exec_status.coor_node_id_);
          caf::actor_system system{*dynamic_cast<actor_system_config *>
              (Environment::getInstance()->get_caf_config())};
          expected<actor> coor_actor_ =
              system.middleman().remote_actor(addr.first, addr.second);
          self->request(*coor_actor_, std::chrono::seconds(kTimeout),
                  ReportSegESAtom::value, seg_exec_status.node_segment_id_,
                  exec_status, exec_info)
              .then(
                  // 暂时都用okAtom如果 返回 1,表示 ok 返回2 表示Cancel
                  [&](OkAtom, int i) {
                    if (i == 1) {
                      seg_exec_status.ReportErrorTimes = 0;
                      if (SegmentExecStatus::kCancelled == exec_status ||
                          SegmentExecStatus::kDone == exec_status) {
                        seg_exec_status.stop_report_ = true;
                      }
                      LOG(INFO)
                          << seg_exec_status.node_segment_id_.first << " , "
                          << seg_exec_status.node_segment_id_.second
                          << " report: " << exec_status << " , " << exec_info
                          << " successfully!";
                    } else if (i == 2) {
                      seg_exec_status.ReportErrorTimes = 0;
                      seg_exec_status.CancelSegExec();
                      LOG(INFO) << seg_exec_status.node_segment_id_.first
                                << " , "
                                << seg_exec_status.node_segment_id_.second
                                << " receive cancel signal and cancel self";
                    }
                  },
                  [&](const error& err) {
                    if (err == sec::request_timeout) {
                      ++seg_exec_status.ReportErrorTimes;
                      LOG(WARNING)
                          << seg_exec_status.node_segment_id_.first << " , "
                          << seg_exec_status.node_segment_id_.second
                          << " segment report status timeout! times= "
                          << seg_exec_status.ReportErrorTimes;
                      if (seg_exec_status.ReportErrorTimes > TryReportTimes) {
                        LOG(ERROR) << seg_exec_status.node_segment_id_.first
                                   << "  , "
                                   << seg_exec_status.node_segment_id_.second
                                   << " report status error over 20 times, "
                                   << "please check the error "
                                   << "and this segment will be cancelled!";
                        seg_exec_status.CancelSegExec();
                      }
                    } else {
                      LOG(ERROR) << seg_exec_status.node_segment_id_.first
                                 << " , "
                                 << seg_exec_status.node_segment_id_.second
                                 << " cann't connect to node  ( "
                                 << seg_exec_status.coor_node_id_
                                 << " ) when report status";
                    }
                  });
          // guarantee it's the last action!!!
          --seg_exec_status.logic_time_;
        }
      },
      [=](ExitAtom) { self->quit(); }};
}
RetCode SegmentExecTracker::CancelSegExec(NodeSegmentID node_segment_id) {}

RetCode SegmentExecTracker::RegisterSegES(NodeSegmentID node_segment_id,
                                          SegmentExecStatus* seg_exec_status) {
  map_lock_.acquire();
  if (node_segment_id_to_status_.find(node_segment_id) ==
      node_segment_id_to_status_.end()) {
    node_segment_id_to_status_.insert(
        make_pair(node_segment_id, seg_exec_status));
    LOG(INFO) << node_segment_id.first << " , " << node_segment_id.second
              << " register to segment tracker successfully!";
    map_lock_.release();
  } else {
    LOG(ERROR) << "node_segment_id < " << node_segment_id.first << " , "
               << node_segment_id.second << " >already in segment tracker";
    map_lock_.release();
    assert(false);
  }
  return rSuccess;
}

RetCode SegmentExecTracker::UnRegisterSegES(NodeSegmentID node_segment_id) {
  map_lock_.acquire();
  auto it = node_segment_id_to_status_.find(node_segment_id);
  if (it != node_segment_id_to_status_.end()) {
    node_segment_id_to_status_.erase(it);
    LOG(INFO) << node_segment_id.first << " , " << node_segment_id.second
              << " has been erased from segment tracker! then left segment= "
              << node_segment_id_to_status_.size();
    map_lock_.release();
  } else {
    LOG(ERROR) << node_segment_id.first << " , " << node_segment_id.second
               << " couldn't be found when unregister segment status";
    map_lock_.release();
    assert(false);
  }
  return rSuccess;
}
}  // namespace claims
