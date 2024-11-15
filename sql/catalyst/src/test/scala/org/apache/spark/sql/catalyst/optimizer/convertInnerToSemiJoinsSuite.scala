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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class convertInnerToSemiJoinsSuite extends PlanTest {

  private var autoBroadcastJoinThreshold: Long = _

  protected override def beforeAll(): Unit = {
    autoBroadcastJoinThreshold = SQLConf.get.autoBroadcastJoinThreshold
    conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
  }

  protected override def afterAll(): Unit = {
    conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, autoBroadcastJoinThreshold)
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("convertInnerToSemiJoins", Once, convertInnerToSemiJoins):: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  val testRelation1 = LocalRelation($"d".int)
  val testRelation2 = LocalRelation($"e".int)

  test("Inner join to left semi join") {
    val origin = Join(testRelation,
      Project(Seq('d),
        Join(testRelation1, testRelation2, Inner, Some($"d" === $"e"), JoinHint.NONE)),
      LeftSemi, Some($"c" === $"d"), JoinHint.NONE)
    val optimized = Optimize.execute(origin.analyze)

    val correctAnswer = Join(testRelation,
      Project(Seq('d),
        Join(testRelation1, testRelation2, LeftSemi, Some($"d" === $"e"), JoinHint.NONE)),
      LeftSemi, Some($"c" === $"d"), JoinHint.NONE).analyze
    // scalastyle:off println
    println("optimized1.treeString:\n" + optimized.analyze.treeString)
    comparePlans(optimized, correctAnswer)
  }

  test("Inner join to left semi join recursively") {
    val join = Join(testRelation,
      Project(Seq('d),
        Join(testRelation1, testRelation2, Inner, Some($"d" === $"e"), JoinHint.NONE)),
      Inner, Some($"c" === $"d"), JoinHint.NONE)
    val origin = Join(testRelation2, Project(Seq('c), join),
      LeftSemi, Some($"e" === $"c"), JoinHint.NONE)
    val optimized = Optimize.execute(origin.analyze)

    val joinComparison = Join(testRelation,
      Project(Seq('d),
        Join(testRelation1, testRelation2, LeftSemi, Some($"d" === $"e"), JoinHint.NONE)),
      LeftSemi, Some($"c" === $"d"), JoinHint.NONE)
    val correctAnswer = Join(testRelation2, Project(Seq('c), joinComparison),
      LeftSemi, Some($"e" === $"c"), JoinHint.NONE).analyze
    // scalastyle:off println
    println("optimized1.treeString:\n" + optimized.analyze.treeString)
    comparePlans(optimized, correctAnswer)
  }
}
