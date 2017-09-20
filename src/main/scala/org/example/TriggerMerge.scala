package org.example

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window

object TriggerMerge {
  @SerialVersionUID(1L)
  private class Sum extends ReduceFunction[Long] {
    @throws[Exception]
    override def reduce(value1: Long, value2: Long): Long = value1 + value2
  }
}

class TriggerMerge extends Trigger[TaggedUnion[MainElement, SideElement], Window] {

  val timeout: Int = 10000

  val latestDeadlineDesc = new ValueStateDescriptor[Long]("latest-deadline", classOf[Long])
  val sideElementsToWaitForDesc = new ValueStateDescriptor[Int]("side-elements-to-wait-for", classOf[Int])
  val sideElementsReceivedDesc = new ReducingStateDescriptor[Long]("side-elements-received", new TriggerMerge.Sum, classOf[Long])

  override def onElement(element: TaggedUnion[MainElement, SideElement], timestamp: Long, window: Window, ctx: TriggerContext): TriggerResult = {
    val latestDeadline = ctx.getPartitionedState(latestDeadlineDesc)
    val sideElementsToWaitFor = ctx.getPartitionedState(sideElementsToWaitForDesc)
    val sideElementsReceived = ctx.getPartitionedState(sideElementsReceivedDesc)

    // Update counters
    if (element.getOne != null) {
        sideElementsToWaitFor.update(element.getOne.elementsToWaitFor)
    }
    if (element.getTwo != null) {
      sideElementsReceived.add(1)
    }

    // Update deadline timeout
    val newDeadline = ctx.getCurrentProcessingTime + timeout
    ctx.registerProcessingTimeTimer(newDeadline)
    latestDeadline.update(newDeadline)

    // Check if everything is going as it should
    if (Option(sideElementsToWaitFor.value).nonEmpty && Option(sideElementsReceived.get).nonEmpty &&
      sideElementsToWaitFor.value == sideElementsReceived.get) {

      TriggerResult.FIRE_AND_PURGE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def clear(window: Window, ctx: TriggerContext): Unit = {
    // Cleanup state
    ctx.getPartitionedState(sideElementsToWaitForDesc).clear()
    ctx.getPartitionedState(sideElementsReceivedDesc).clear()
    ctx.getPartitionedState(latestDeadlineDesc).clear()
  }

  override def onProcessingTime(time: Long, window: Window, ctx: TriggerContext): TriggerResult =  {
    if (ctx.getPartitionedState(latestDeadlineDesc).value() != time) {
      TriggerResult.CONTINUE
    } else {
      TriggerResult.FIRE_AND_PURGE
    }
  }

  override def onEventTime(time: Long, window: Window, ctx: TriggerContext): TriggerResult = ???
}


