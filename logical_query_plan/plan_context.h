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
 * ./LogicalPlan/plan_context.h
 *
 *  Created on: Nov 10, 2013
 *      Author: wangli, fangzhuhe
 *       Email: fzhedu@gmail.com
 *
 * Description:
 */

#ifndef LOGICAL_QUERY_PLAN_PLAN_CONTEXT_H_
#define LOGICAL_QUERY_PLAN_PLAN_CONTEXT_H_
#include <vector>
#include "../Catalog/Column.h"
#include "../Catalog/Partitioner.h"
#include "../logical_query_plan/plan_partitioner.h"
#include "../common/Schema/Schema.h"
namespace claims {
namespace logical_query_plan {

class PlanContext {
  /* describe the properties of the PlanContext*/
  friend class LogcalOperator;

 public:
  PlanContext();
  PlanContext(const PlanContext& plan_context);
  virtual ~PlanContext();
  unsigned long GetAggregatedDatasize() const;
  unsigned long GetAggregatedDataCardinality() const;
  bool IsHashPartitioned() const;
  Schema* GetSchema() const;
  unsigned GetTupleSize() const;
  Attribute GetAttribute(std::string name) const;
  Attribute GetAttribute(std::string tbname, std::string colname) const;

 public:
  std::vector<Attribute> attribute_list_;
  unsigned long commu_cost_;  // communication cost
  PlanPartitioner plan_partitioner_;
};

}  // namespace logical_query_plan
}  // namespace claims
#endif  // LOGICAL_QUERY_PLAN_PLAN_CONTEXT_H_
