import org.apache.spark.{SparkConf,SparkContext}

  object Graph{
    def main(args: Array[String]){
    

      val conf = new SparkConf().setAppName("ConnComp")
      val sc = new SparkContext(conf)
      
		
       var graph = sc.textFile(args(0)).map( line => { 	val ip = line.split(",")
                                                      	(ip(0).toLong,
                                                      	ip(0).toLong,
                                                      	ip.drop(1).toList.map(_.toLong))
                                                      })  
       var g = graph.map(m => (m._1,m))                                               
      for(i <- 1 to 5){

       graph = graph.flatMap(map => map match{ case (vid, tag, adj) => (vid, tag) :: adj.map(m => (m, tag) ) } )
				        .reduceByKey((a, b) => (if (a >= b) b else a))
				        //.join(graph.map(m => (m._1,m)))
				        .join(g)
				        .map(m => (m._2._2._2, m._2._1, m._2._2._3))
				        
			
    }
	 	val count = graph.map(m => (m._2, 1))
							.reduceByKey((m, n) => (m + n))
							.sortByKey(true,1)
							.collect()
							.foreach(println)
 }
}