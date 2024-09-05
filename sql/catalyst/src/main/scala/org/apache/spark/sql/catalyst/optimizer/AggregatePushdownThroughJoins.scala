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

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.internal.SQLConf

/**
 * Pushes Project operator through Limit operator.
 */
object AggregatePushdownThroughJoins extends Rule[LogicalPlan]
  with JoinSelectionHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.AGGREGATE_PUSHDOWN_THROUGHJOINS_ENABLED)) {
      plan
    } else {
      plan.transformWithPruning(_.containsAllPatterns(AGGREGATE, JOIN), ruleId) {
        case agg@Aggregate(_, _, Project(_, join: Join))
          if join.left.isInstanceOf[Aggregate] || join.right.isInstanceOf[Aggregate] =>
          agg

        case agg@Aggregate(_, _,
        p @ Project(_, join @ Join(left, right, _, _, _)))
          if agg.groupOnly && !canPlanAsBroadcastHashJoin(join, conf) =>
          val newJoin = join.copy(
            left = Aggregate(left.output.distinct, left.output.distinct, join.left),
            right = Aggregate(right.output.distinct, right.output.distinct, join.right))
          agg.copy(child = p.copy(child = newJoin))
      }
    }
  }
}


