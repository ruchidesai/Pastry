package pastry

import java.security.MessageDigest
import akka.actor._
import com.typesafe.config._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

object Pastry {
  
  private val sha = MessageDigest.getInstance("SHA-1")
  
  def main(args: Array[String]){
    if ( args.isEmpty || args.length < 1) {
      println("usage: scala project2.scala <NumNodes> ")
	  return
	}
	val num_nodes = args(0).toInt
	
	//create AKKA actor system
	val system = ActorSystem("PastrySystem")
	val master = system.actorOf(Props[Master], name="master")
	master ! BuildNetwork(num_nodes)
  }
  
  sealed trait Message
  case class BuildNetwork(num_nodes: Int) extends Message
  case class Route(source: String, destination: String) extends Message
  case class Data(hashmap: Map[String, Int], list: List[String]) extends Message
  
  class Master extends Actor {
    def receive = {
      case BuildNetwork(num_nodes) =>
        var seed = 5
        var w = seed.toString
        var list = List[String]()
        //var list1 = ListBuffer[String]()
        var actorList = List[ActorRef]()
        var counter = 0
	
        //generating node ids (hashes)
        while(counter < num_nodes) {
	      var hash = hex_Digest(w)
	      list ::= hash
	      actorList::=(context.actorOf(Props(new PastryNode(num_nodes)), name=hash))
	      seed +=1
	      counter +=1	
	      w = seed.toString					  
	    }
	
	    //proximity metric = index after sorting
	    //list1=list.sorted
	    //var sortedarr:Array[String]= (list.sorted).toArray
	    val hashmap = ((list.sorted).toArray).view.zipWithIndex.toMap
	    for(actor <- actorList)
	      actor ! Data(hashmap, list)
	    
	    //context.actorSelection(list(0)) ! Route("source", list(3)) 
    }
  }
  
  class PastryNode(num_nodes: Int) extends Actor {
    
    val node_id = self.path.name
    
    var my_hashmap = Map[String, Int]()
    var my_list = List[String]()
    var my_num_nodes = num_nodes
    
    var my_leaf_set = List[String]()
    
    val row = (scala.math.log(num_nodes)/scala.math.log(16)).ceil.toInt
    val col = 15
    //var my_routing_table = Array.ofDim[String](row, col)
    var my_routing_table = Array[List[String]]()
    
    def receive = {
      case Data(hashmap, list) =>
        my_hashmap = hashmap
        my_list = list
        my_num_nodes = num_nodes
        initialize_routing_table()
      /*case Route(source, destination) =>
        if(self.path.name == destination)
          println("Message Received")
        else if(in_leaf_set(destination))
          //fwd message to node L(i) such that |L(i) - D| is minimum
          println("In leaf set")
        else {
          println("In routing table")
          route_by_routing_table(destination)
        }
      */
    }
    
    def initialize_routing_table() {
      for(node <- my_list) {
        if(node != self.path.name) {
          var common_prefix = node.zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString
          println("common_prefix.length = " + common_prefix.length)
          my_routing_table(common_prefix.length)::=node
        }        
      }
      println("Node " + self.path.name + "\'s routing table:")
      println(my_routing_table)
    }
    
    /*def in_leaf_set(destination: String): Boolean = {
      val current_index = my_hashmap(self.path.name)
      var index = current_index - 1
      var i = 0
      while (i < 8) {
        if(destination == my_list.lift(index))
          return true       
        else {
          index-=1
          i+=1
          if(index < 0)
            index+=my_num_nodes
        }
      }
      index = current_index + 1
      i = 0
      while (i < 8) {
        if(destination == my_list.lift(index))
          return true
        else {
          index+=1
          i+=1
          if(index >= my_num_nodes)
            index%=my_num_nodes
        }
      }
      return false  
    }
    
    def route_by_routing_table(destination: String) {
      //var prefix = common(self.path.name, destination)
      var prefix = destination.zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString
      var length_of_prefix = prefix.length
      var next_node = node_with_closest_prefix(length_of_prefix, destination)
      if(next_node == null)
        route_by_neighborhood_set(destination, length_of_prefix)
      else
        context.actorSelection("../"+next_node) ! TakeThis
    }
    
    def route_by_neighborhood_set(destination: String, length_of_prefix: Int) {
      val length = length_of_prefix
      //find a node id such that common(nodeid, destination).length = length_of_prefix
      //fwd message to that node
      //if such a node is not found, increment length by 1
    }*/
  }  
  
  //generates sha-1 hash
  def hex_Digest(s:String):String = {
	sha.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
}