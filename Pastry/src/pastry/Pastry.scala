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
  case object TakeThis extends Message
  
  class Master extends Actor {
    def receive = {
      case BuildNetwork(num_nodes) =>
        var seed = 5
        var w = seed.toString
        var list = ListBuffer[String]()
        var list1 = ListBuffer[String]()
        var actorList = List[ActorRef]()
        var counter = 0
	
        //generating node ids (hashes)
        while(counter< num_nodes) {
	      var hash = hex_Digest(w)
	      list += hash
	      actorList::=(context.actorOf(Props[PastryNode], name=hash))
	      seed +=1
	      counter +=1	
	      w = seed.toString					  
	    }
	
	    //proximity metric = index after sorting
	    list1=list.sorted
	    var sortedarr:Array[String]= list1.toArray
	    
	    context.actorSelection(list1(0)) ! Route("source", list1(3)) 
    }
  }
  
  class PastryNode extends Actor {
    val node_id = self.path.name
    def receive = {
      case Route(source, destination) =>
        if(in_leaf_set(destination))
          println("In leaf set")
        else {
          println("In routing table")
          route_by_routing_table(destination)
        }
        println("Sending message")
        context.actorSelection("../"+destination) ! TakeThis
      case `TakeThis` =>
        println("Message received")
    }
    
    def in_leaf_set(destination: String): Boolean = {
      val current_index = indexOf(self.path.name, hashmap)
      var index = current_index - 1
      var i = 0
      while (i < 8) {
        if(destination == sortedArray[index])
          return true       
        else {
          index-=1
          i+=1
          if(index < 0)
            index+=num_nodes
        }
      }
      index = current_index + 1
      i = 0
      while (i < 8) {
        if(destination == sortedArray[index])
          return true
        else {
          index+=1
          i+=1
          if(index >= num_nodes)
            index%=num_nodes
        }
      }
      return false  
    }
    
    def route_by_routing_table(destination: String) {
      var prefix = common(self.path.name, destination)
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
    }
  }  
  
  //generates sha-1 hash
  def hex_Digest(s:String):String = {
	sha.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
}