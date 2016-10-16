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
 * /Claims/Executor/segment_exec_tracker.h
 *
 *  Created on: Mar 24, 2016
 *      Author: fzh
 *		   Email: fzhedu@gmail.com
 *
 * Description:
 *
 */

#ifndef EXEC_TRACKER_SEGMENT_EXEC_TRACKER_H_
#define EXEC_TRACKER_SEGMENT_EXEC_TRACKER_H_
#include "../exec_tracker/segment_exec_tracker.h"

#include <boost/unordered/unordered_map.hpp>
#include <string>
#include <utility>
#include "../common/error_define.h"
#include "../utility/lock.h"
#include "caf/all.hpp"
using std::pair;
using std::string;
using caf::event_based_actor;
using caf::actor_config;
using caf::behavior;

namespace claims {
// first=query_id, second=segment_id*kMaxNodeNum + node_id
#define kReportIntervalTime 3000
typedef std::pair<u_int64_t, u_int64_t> NodeSegmentID;
//Declare for caf serialization
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, NodeSegmentID& x) {
    return f(caf::meta::type_name("NodeSegmentID"), x.first,
      x.second);
}
class SegmentExecStatus;
class SegmentExecTracker {
 public:
  friend class SegmentExecTrackerActor;
  SegmentExecTracker();
  virtual ~SegmentExecTracker();
//  behavior make_behavior() override;
  RetCode CancelSegExec(NodeSegmentID node_segment_id);
  RetCode RegisterSegES(NodeSegmentID node_segment_id,
                        SegmentExecStatus* seg_exec_status);
  RetCode UnRegisterSegES(NodeSegmentID node_segment_id);
  // report all remote_segment_status located at slave node
//  static behavior ReportAllSegStatus(caf::event_based_actor* self,
//                                 SegmentExecTracker* seg_exec_tracker);

 private:
  boost::unordered_map<NodeSegmentID, SegmentExecStatus*>
      node_segment_id_to_status_;
  Lock map_lock_;
};

}  // namespace claims

#endif  //  EXEC_TRACKER_SEGMENT_EXEC_TRACKER_H_
