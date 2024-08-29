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

//import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.internal.SQLConf

/**
 * Pushes Project operator through Limit operator.
 */
object convertInnerToSemiJoins extends Rule[LogicalPlan]
  with JoinSelectionHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.CONVERT_INNERJOIN_TO_SEMIJOINS_ENABLED)) {
      plan
    } else {
      plan.transformWithPruning(_.containsAllPatterns(
        LEFT_SEMI_OR_ANTI_JOIN, INNER_LIKE_JOIN), ruleId) {
        case js @ Join(_, p @ Project(_, ji @ Join(left, _, Inner, _, _)), LeftSemi, _, _)
          if (p.outputSet.subsetOf(left.outputSet) &&
            !getBroadcastJoinBuildSide(ji, conf).contains(BuildLeft)) =>
          js.copy(right = p.copy(child = ji.copy(joinType = LeftSemi)))
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


