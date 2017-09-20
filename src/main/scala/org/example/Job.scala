package org.example

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(1000)

    val extractSideElements = new ExtractSideElements()
    val mainStream = env
      .addSource(new MainElementsSource)
      .process(extractSideElements)
    val sideElementsStream = mainStream
      .getSideOutput(extractSideElements.unresolvedAddrTag)

    mainStream
      .coGroup(sideElementsStream)
      .where(_.uuid).equalTo(_.uuid)
      .window(GlobalWindows.create())
      .trigger(new TriggerMerge)
      .apply(new MergeElements)

    // execute program
    env.execute("GlobalWindow poc")
  }
}
