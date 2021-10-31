import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val Array_vertex = Array(
    (1L,("Alice",28)),
    (2L,("Bob",27)),
    (3L,("Charlie",65)),
    (4L,("David",42)),
    (5L,("Ed",55)),
    (6L,("Fran",50)),
    (7L,("Alex",55))
)
val Array_edge = Array(
    Edge(2L,1L,7),
    Edge(2L,4L,2),
    Edge(3L,2L,4),
    Edge(3L,6L,3),
    Edge(4L,1L,1),
    Edge(5L,2L,2),
    Edge(5L,3L,8),
    Edge(5L,6L,3),
    Edge(7L,5L,3),
    Edge(7L,6L,4)
)

val vertices : RDD[(Long,(String, Int))] = sc.parallelize(Array_vertex)
val edges : RDD[Edge[Int]]=sc.parallelize(Array_edge)

val graph:Graph[(String,Int),Int] = Graph(vertices,edges)

graph.vertices.filter{ case (id,(name,age)) => age >=30}.foreach{case (id,(name,age))=>println(name)}

graph.triplets.foreach{ t=>
    println(s"${t.srcAttr._1} likes ${t.dstAttr._1}.")
}

graph.triplets.filter(t => t.attr > 5).foreach{ t=>
    println(s"${t.srcAttr._1} pretty likes ${t.dstAttr._1}.")
}

val inDegrees : VertexRDD[Int] = graph.inDegrees

case class degree(name:String,inDegree:Int,outDegree:Int,age:Int)
val graph_in = graph.mapVertices{case(id,(name,age))=>degree(name,0,0,age)}
val graph_degree_in = graph_in.outerJoinVertices(graph_in.inDegrees){
    case (id,v,inDegree) => degree(v.name,inDegree.getOrElse(0),0,v.age)
}
graph_degree_in.vertices.foreach{case(id,v)=>
                    println(s"${v.name} is liked by ${v.inDegree} people")}

val graph_degree = graph_degree_in.outerJoinVertices(graph_in.outDegrees){
    case (id,v,outDegree) => degree(v.name,v.inDegree,outDegree.getOrElse(0),v.age)
}
graph_degree.vertices.filter{
    case(id,v) =>v.inDegree==v.outDegree
}.foreach{
    case(id,v)=>println(v.name)
}

val OldestFollower:VertexRDD[(Int,String)] = graph_degree.aggregateMessages[(Int,String)](
    triplet => triplet.sendToDst(triplet.srcAttr.age, triplet.srcAttr.name),
    (a,b) => if (a._1>b._1) a else b
    )

graph_degree.vertices.leftJoin(OldestFollower){(id, v, OF)=> OF match {
    case Some((age, name)) => s"${name} is the oldest follower of ${v.name}."
    case None => s"${v.name} does not have any followers."
}
}.foreach { case (id, string) => println(string)}



