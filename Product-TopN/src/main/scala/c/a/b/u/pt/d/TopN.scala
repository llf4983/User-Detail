package c.a.b.u.pt.d

import c.a.b.u.pt.bean.{UserDetail, WindowProductCount}
import c.a.b.u.pt.se.MyPro
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TopN {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val lineDS = env.readTextFile("input")
    val windowedStream = lineDS.map(ele => {
      val words = ele.split(",")
      UserDetail(words(0).trim.toLong, words(1).trim.toLong, words(2).trim.toInt, words(3).trim, words(4).trim.toLong)
    })
//    windowedStream.print()
    val winS = windowedStream.assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior=="pv")
//    winS.print()
    val keyedDS = winS.keyBy(_.itemId)
      .timeWindow(Time.minutes(60), Time.minutes(5))


    val aggedDS = keyedDS.aggregate(new AggregateFunction[UserDetail, Long, Long] {
      override def createAccumulator(): Long = 0L

      override def add(value: UserDetail, accumulator: Long): Long = accumulator + 1L

      override def getResult(accumulator: Long): Long = accumulator

      override def merge(a: Long, b: Long): Long = a + b
    }, new WindowFunction[Long, WindowProductCount, Long, TimeWindow] {
      override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[WindowProductCount]): Unit = {
        val itemId=key
        val WindowTS=window.getEnd
        val count=input.iterator.next()
        out.collect(WindowProductCount(itemId, WindowTS, count))
      }
    })
//    aggedDS.print()
    val top3 = aggedDS.keyBy(_.WindowTS).process(new MyPro(3))
    top3.print("top3")

    env.execute("testExe")
//    windowedStream
  }
}
