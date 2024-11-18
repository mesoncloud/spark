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

class AggregatePushdownThroughJoinsSuite extends PlanTest {

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
      Batch("AggregatePushdownThroughJoins", Once, AggregatePushdownThroughJoins):: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  val testRelation1 = LocalRelation($"d".int)
  val testRelation2 = LocalRelation($"e".int)

  test("Aggregate pushdown through joins")  {
    val origin = Aggregate(Seq('d), Seq('d), Project(Seq('d),
        Join(testRelation1, testRelation2, Inner, Some($"d" === $"e"), JoinHint.NONE)))
    // scalastyle:off println
    println("origin.analyze:\n" + origin.analyze)
    val optimized = Optimize.execute(origin.analyze)
    println("optimized:\n" + optimized)

    val correctAnswer = Aggregate(Seq('d), Seq('d), Project(Seq('d),
      Join(Aggregate(Seq('d), Seq('d), testRelation1),
        Aggregate(Seq('e), Seq('e), testRelation2),
        Inner, Some($"d" === $"e"), JoinHint.NONE))).analyze

    comparePlans(optimized, correctAnswer)
  }

}
