package org.example

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class ExtractSideElements extends ProcessFunction[MainElement, MainElement] {
  val unresolvedAddrTag: OutputTag[SideElement] = OutputTag[SideElement]("side-elements")

  override def processElement(value: MainElement, ctx: ProcessFunction[MainElement, MainElement]#Context, out: Collector[MainElement]): Unit = {
    // Collect 3 side elements
    for (_  <-  1 to 3) {
      ctx.output(unresolvedAddrTag.asInstanceOf[OutputTag[SideElement]], SideElement(value.uuid))
    }

    // Let main element pass
    out.collect(value.copy(value.uuid, 3))
  }
}
