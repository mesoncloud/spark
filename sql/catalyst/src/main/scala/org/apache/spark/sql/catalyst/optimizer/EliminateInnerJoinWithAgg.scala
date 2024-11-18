/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.internal.SQLConf

/**
 * Pushes Project operator through Limit operator.
 */
object EliminateInnerJoinWithAgg extends Rule[LogicalPlan]
  with JoinSelectionHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.ELIMINATE_INNERJOI_NWITHAGG_ENABLED)) {
      plan
    } else {
      plan.transformWithPruning(_.containsAllPatterns(
        AGGREGATE, INNER_LIKE_JOIN), ruleId) {
        case agg @ Aggregate(_, _, p @ Project(_, join @ Join(left, _, Inner, _, _)))
          if agg.groupOnly && p.outputSet.subsetOf(left.outputSet) &&
            !getBroadcastJoinBuildSide(join, conf).contains(BuildLeft) =>
              agg.copy(child = p.copy(child = join.copy(joinType = LeftSemi)))
      }
    }
  }

  def getBroadcastJoinBuildSide(join: Join, conf: SQLConf): Option[BuildSide] = {
    getBroadcastBuildSide(join.left, join.right, join.joinType,
      join.hint, hintOnly = true, conf).orElse(
      getBroadcastBuildSide(join.left, join.right, join.joinType,
        join.hint, hintOnly = false, conf))
  }
}


