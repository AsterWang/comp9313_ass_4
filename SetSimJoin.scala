package comp9313.ass4

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SetSimJoin {
  def main(args: Array[String]){
    val inputFile = args(0)
    val outputFolder = args(1)

    val threshold = args(2).toDouble
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val lines = textFile.map(_.split(" ").toList)


    val record = lines.map(t =>(t.head.toInt, t.slice(1,t.length)))

    val token_index = record.values.flatMap(t=>t)
      .map(t=>(t,1))
      .reduceByKey(_+_).sortBy(t=>t._2)
      .map(t=>t._1).zipWithIndex().map(t=>(t._1,t._2.toInt))
      .collectAsMap()

    val token_index_broadcast = sc.broadcast(token_index)
    val ordered_record = record.map(t=>(t._1,t._2.sortBy(token_index_broadcast.value(_))))

    val partitioned_record = ordered_record.map(t=>(mutable.ArrayBuffer((t._1,t._2)),t._2.take(t._2.length - Math.ceil(t._2.length * threshold).toInt+1)))
      .flatMapValues(t=>t)
      .map(t=>t.swap)
      .reduceByKey(_.union(_))
      .filter(t=>t._2.size > 1)
      .map(t=>(t._1,t._2.sortBy(_._2.length).toMap)).persist()

    def ppjoin(token:String,sim_record_id_list:Map[Int,List[String]])
    :mutable.HashMap[Int,mutable.HashSet[((Int, Int),Double)]] = {
      var S_list: mutable.HashMap[Int,mutable.HashSet[((Int, Int),Double)]] = new mutable.HashMap()
      var I_list:mutable.HashMap[Int,mutable.ArrayBuffer[(Int, Int)]] = new mutable.HashMap()
      def getField(position: Int) = I_list(position)
      for (line_id_and_simlist <- sim_record_id_list){
        val line_id = line_id_and_simlist._1
        val record_x = line_id_and_simlist._2
        val L = record_x.take(record_x.length - Math.ceil(record_x.length * threshold).toInt+1)
        var map_A:mutable.HashMap[Int, Int] = mutable.HashMap()
        var alpha:Double = 0
        val x_size = record_x.length
        for(i <- L.indices){
          val e = L(i).toInt
          var I_w = mutable.ArrayBuffer.empty[(Int,Int)]
          if (I_list.keySet.contains(e)) {
            I_w = I_list.getOrElse(e,null)
            for ((y_id,j) <- I_w) {
              if (y_id != line_id) {
                val y_size = sim_record_id_list(y_id).length
                //size filtering on |y|
                if (threshold * x_size <= y_size) {
                  alpha = math.ceil(threshold * (threshold + 1) / (x_size + y_size))
                  val ubound = 1 + math.min(x_size - i, y_size - j)
                  if (map_A.getOrElse(y_id, 0) + ubound >= alpha) {
                    if (map_A.get(y_id).isDefined) {
                      val new_value = map_A.getOrElse(y_id, 0) + 1
                      map_A(y_id) = new_value
                    } else {
                      map_A.put(y_id, 1)
                    }
                  } else {
                    map_A.put(y_id, 0)
                  }
                }
              }
            }
          }
          if (I_w.nonEmpty){
            I_w +=((line_id,i))
            I_list(e)=I_w
          } else {
            I_w += ((line_id,i))
            I_list +=(e -> I_w)
          }
        }
        //VERIFY METHOD
        for ((key, value) <- map_A) {
          val p_x = L.length
          val record_s = sim_record_id_list(key).length
          if (value > 0) {
            val p_y = record_s - math.ceil(record_s *threshold).toInt + 1
            val w_x = token_index_broadcast.value.getOrElse(L.last, 0)
            val record_y = sim_record_id_list(key)
            val w_y = token_index_broadcast.value.getOrElse(record_y(p_y - 1), 0)
            var big_O = value
            var right_part_overlap_size=0
            if (w_x < w_y) {
              val ubound = value + record_x.length - p_x
              if (ubound >= alpha) {
                val x_right_part = record_x.slice(p_x, record_x.length)
                val y_right_part = record_y.slice(value, record_y.length)
                big_O += x_right_part.intersect(y_right_part).size
              }
            } else {
              val ubound = value + record_x.length - p_y
              if (ubound >= alpha) {
                val x_right_part = record_x.slice(value, record_x.length)
                var y_right_part = record_y.slice(p_y, record_y.length)
                big_O += x_right_part.intersect(y_right_part).size
              }
            }
            if (big_O >= alpha) {
              val ratio = big_O.toDouble / record_y.toSet.union(record_x.toSet).size.toDouble
              if (ratio >= threshold) {
                var smaller_index = (0,0)
                if (line_id <= key) {
                  smaller_index = (line_id, key)
                } else {
                  smaller_index = (key, line_id)
                }
                if (S_list.get(smaller_index._1).isDefined){
                  S_list(smaller_index._1) = S_list(smaller_index._1) += ((smaller_index,ratio))
                } else {
                  var new_S_list:mutable.HashSet[((Int, Int),Double)] = mutable.HashSet()
                  new_S_list += ((smaller_index,ratio))
                  S_list += smaller_index._1 ->new_S_list
                }
              }
            }
          }
        }
      }
      S_list
    }
    val prefix_token = partitioned_record
      .map(t=>ppjoin(t._1,t._2))
      .filter(t=>t.nonEmpty)
      .flatMap(identity)
      .reduceByKey(_++=_)
      .map(t=>(t._1,t._2.toList.sortBy(_._1._2.toInt)))
      .sortBy(t=>t._1).values
      .flatMap(t=>t)
      .map(t=>"("+t._1._1+","+t._1._2+")\t"+t._2)
   prefix_token.saveAsTextFile(outputFolder)
  }
}
