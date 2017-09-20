package org.example

import org.apache.flink.streaming.api.functions.source.SourceFunction

class MainElementsSource extends SourceFunction[MainElement]{
  override def cancel(): Unit = ???

  override def run(ctx: SourceFunction.SourceContext[MainElement]): Unit = {
    for (_ <- 1 to 1000000) ctx.collect(MainElement(java.util.UUID.randomUUID()))
  }
}
