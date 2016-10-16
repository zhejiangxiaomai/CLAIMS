/*
 * ids.cpp
 *
 *  Created on: Apr 4, 2014
 *      Author: wangli
 */

#include "ids.h"
#include "../Config.h"
#include "string.h"


namespace claims {
std::string PartitionID::getPathAndName() const {
  std::ostringstream str;
  std::string str1;
  //		str<<"/home/claims/data/tpc-h/4_partitions/SF-20/T"<<projection_id.table_id<<"G"<<projection_id.projection_off<<"P"<<partition_off;
  //		str<<"/home/claims/data/wangli/T"<<projection_id.table_id<<"G"<<projection_id.projection_off<<"P"<<partition_off;
  str << Config::data_dir << "T" << projection_id.table_id << "G"
      << projection_id.projection_off << "P" << partition_off;
  str1 = str.str();
  str.clear();
  return str1;
}

//Declare for caf serialization
//template <class Inspector>
//typename Inspector::result_type inspect(Inspector& f, ProjectionID& x) {
//    return f(caf::meta::type_name("ProjectionID"),&x.table_id, &x.projection_off);
//}
//
//template <class Inspector>
//typename Inspector::result_type inspect(Inspector& f, PartitionID& x) {
//    return f(caf::meta::type_name("PartitionID"),&x.projection_id, &x.partition_off);
//}
//
//template <class Inspector>
//typename Inspector::result_type inspect(Inspector& f, ExchangeID& x) {
//    return f(caf::meta::type_name("ExchangeID"), &x.exchange_id, &x.partition_offset);
//}
}
