package org.example

import java.lang

import org.apache.flink.api.common.functions.RichCoGroupFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

class MergeElements extends RichCoGroupFunction[MainElement, SideElement, MainElement] {
  override def coGroup(first: lang.Iterable[MainElement], second: lang.Iterable[SideElement], out: Collector[MainElement]): Unit = {
    if (first.iterator().toList.length != 1)
      throw new IllegalStateException("More than one main element with the same UUID received")

    if (second.iterator().toList.length != 3)
      throw new IllegalStateException("The trigger timed out before receiving the three side elements")

    for (element <- first) {
      out.collect(element)
    }
  }
}
