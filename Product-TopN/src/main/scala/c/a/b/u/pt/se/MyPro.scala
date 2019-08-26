package c.a.b.u.pt.se

import java.sql.Timestamp

import c.a.b.u.pt.bean.WindowProductCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class MyPro(n:Int) extends KeyedProcessFunction[Long,WindowProductCount,String]{
  private var stateList:ListState[WindowProductCount]=_

  override def open(parameters: Configuration): Unit = {
    stateList=getRuntimeContext.getListState(new ListStateDescriptor[WindowProductCount]("allElement",classOf[WindowProductCount]))
  }

//  override def processElement(i: WindowProductCount, context: ProcessFunction[WindowProductCount, String]#Context, collector: Collector[String]): Unit = {
//    stateList.add(i)
//
//  }
  override def processElement(i: WindowProductCount, context: KeyedProcessFunction[Long, WindowProductCount, String]#Context, collector: Collector[String]): Unit = {
    stateList.add(i)
    context.timerService().registerEventTimeTimer(i.WindowTS+100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, WindowProductCount, String]#OnTimerContext, out: Collector[String]): Unit ={

    val buffer =new ListBuffer[WindowProductCount]()
    import scala.collection.JavaConversions._
    for(i<- stateList.get()){
      buffer+=i
    }
    stateList.clear()

    val results = buffer.sortBy(_.count)(Ordering.Long.reverse).take(n)

    val builder = new StringBuilder
    builder.append("\n========================================\n")
    builder.append("时间： ").append(new Timestamp(timestamp)).append("\n")
//    for (elem <- results.indices) {
    var con=1
    for(elem<- results){
      builder.append("No_").append(con).append(":")
      con+=1
      builder.append("商品ID=").append(elem.itemId)
        .append(",浏览量=").append(elem.count)
        .append("\n")
    }
    builder.append("\n========================================\n")
    Thread.sleep(1000)
    out.collect(builder.toString())
  }

}
