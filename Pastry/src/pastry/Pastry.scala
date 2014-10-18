package pastry

import java.security.MessageDigest
import akka.actor._
import com.typesafe.config._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

object Pastry {
  
  private val sha = MessageDigest.getInstance("MD5")
  
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
  case class Route(key: String, hop: Int) extends Message
  case class Data(hashmap: Map[ActorRef, Int], list: List[ActorRef]) extends Message
  case object GotIt extends Message
  
  class Master extends Actor {
    def receive = {
      case BuildNetwork(num_nodes) =>
        var seed = 1
        var w = seed.toString
        var actorList = List[ActorRef]()
        var counter = 0
        var hop = 0
	
        //generating node ids
        while(counter < num_nodes) {
	      var hash = hex_Digest(w)
	      actorList::=(context.actorOf(Props(new PastryNode(num_nodes)), name=hash))
	      seed +=1
	      counter +=1	
	      w = seed.toString					  
	    }
	
	    //array index serves as proximity metric
	    val hashmap = (actorList.toArray).view.zipWithIndex.toMap
	    for(actor <- actorList)
	      actor ! Data(hashmap, actorList.sorted)
	    actorList.head ! Route(actorList.last.path.name, hop)
	      
      case `GotIt` =>
        println("Shutting system down...")
        context.system.shutdown
    }
  }
  
  class PastryNode(num_nodes: Int) extends Actor {
    
    val node_id = self.path.name
    
    var my_hashmap = Map[ActorRef, Int]()
    var my_num_nodes = num_nodes
    var my_list = List[ActorRef]()
    
    var my_smaller_leaf_set = List[ActorRef]()
    var my_larger_leaf_set = List[ActorRef]()
    var my_neighborhood_set = List[ActorRef]()
    var my_routing_table = Array.ofDim[ListBuffer[ActorRef]](4)
    
    def receive = {
      
      case Data(hashmap, list) =>
        
        my_hashmap = hashmap
        my_num_nodes = num_nodes
        my_list = list
        
        initialize_routing_table()
        initialize_neighborhood_set()
        initialize_leaf_sets()
        
      case Route(key, hop) =>
        
        //if the current node happens to be the destination
        if(self.path.name == key) {
          println("Message Received")
          context.actorSelection("../") ! GotIt
          //send acknowledgement 
        }          
          
        //check if current node lies in the smaller leaf set
        else if(in_my_smaller_leaf_set(key)) {
          var max_common_prefix_length = ((key).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var destination = self
          for(node <- my_smaller_leaf_set) {
            var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            if (cpl > max_common_prefix_length) {
              max_common_prefix_length = cpl
              destination = node
            }              
          }
          //fwd message to destination
          destination ! Route(key, (hop + 1))
        }
        
        //check if current node lies in the larger leaf set
        else if(in_my_larger_leaf_set(key)) {
          var max_common_prefix_length = ((key).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var destination = self
          for(node <- my_larger_leaf_set) {
            var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            if (cpl >= max_common_prefix_length) {
              max_common_prefix_length = cpl
              destination = node
            }              
          }
          //fwd message to destination
          destination ! Route(key, (hop + 1))
        }
        
        //check in routing table
        else {
          var row = ((key).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var col = column_of(row, key)
          
          if (my_routing_table(row).length > col)
            my_routing_table(row)(col) ! Route(key, (hop + 1))
          
          //route to a node in L U R U M having a matching prefix with key at least as long as that of the current node
          //&
          //that is closer to key than the current node by proximity metric
          else {
            var union_list = my_smaller_leaf_set.union(my_larger_leaf_set.union(my_neighborhood_set))
            var T_list = union_list.filter(n => (((key).zip(n.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length >= row))
            var destination = self
            var min_difference = (key.compare(self.path.name)).abs
            println(min_difference)
            if (T_list.length > 0) {             
              for(T <- T_list) {
                if (key.compare(T.path.name) < min_difference) {
                  min_difference = (key.compare(T.path.name)).abs
                  destination = T
                }
              }
            }
            else {
              for(list <- my_routing_table) {
                for(node <- list) {
                  if (key.compare(node.path.name) < min_difference) {
                    min_difference = (key.compare(node.path.name)).abs
                    destination = node
                  }
                }
              }
            }
            destination ! Route(key, (hop + 1))
          }
        }
    }
    
    def initialize_routing_table() {
      var my_index = my_list.indexOf(self)
      for (i <- 0 until my_routing_table.length)
        my_routing_table(i) = ListBuffer[ActorRef]()
      for (i <- (my_index + 1) until my_list.length) {
        var node = my_list(i)
        var common_prefix = (node.path.name).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString
        if (my_routing_table(common_prefix.length).length < 15)
          my_routing_table(common_prefix.length) += node
      }
    }
    
    def initialize_neighborhood_set() {
      var index = my_hashmap(self)
      my_neighborhood_set = ((my_hashmap.filterKeys(n => ((my_hashmap(n) <= ((index + 16) % my_num_nodes)) && (my_hashmap(n) > (index % my_num_nodes))) || ((my_hashmap(n) >= ((index - 16) % my_num_nodes)) && (my_hashmap(n) < (index % my_num_nodes))))).keys).toList
    }
    
    def initialize_leaf_sets() {
      var index = my_list.indexOf(self)
      //my_smaller_leaf_set = my_list.filter(n => (((my_list.indexOf(n) % my_num_nodes) >= ((index - 8) % my_num_nodes)) && ((my_list.indexOf(n) % my_num_nodes) < (index % my_num_nodes))))
      my_smaller_leaf_set = my_list.filter(n =>(((my_list.indexOf(n) > index) && (my_list.indexOf(n) >= (index - 8 + my_num_nodes))) || ((my_list.indexOf(n) < index) && (my_list.indexOf(n) >= (index - 8)))))
      my_larger_leaf_set = my_list.filter(n => (((my_list.indexOf(n) > index) && (my_list.indexOf(n) <= (index + 8))) || ((my_list.indexOf(n) < index) && ((my_list.indexOf(n) + my_num_nodes) <= (index + 8)))))
    }
    
    def in_my_smaller_leaf_set(key: String): Boolean = {
      var min: String = (my_smaller_leaf_set.head).path.name
      var max: String = (my_smaller_leaf_set.last).path.name
      if (min <= key && key <= max)
        true
      else
        false  
    }
    
    def in_my_larger_leaf_set(key: String): Boolean = {
      var min: String = (my_larger_leaf_set.head).path.name
      var max: String = (my_larger_leaf_set.last).path.name
      if (min <= key && key <= max)
        true
      else
        false  
    }
    
    def column_of(index: Int, key: String): Int = {
      var digit = key.charAt(index)
      digit match {
        case 'a' => 10
        case 'b' => 11
        case 'c' => 12
        case 'd' => 13
        case 'e' => 14
        case 'f' => 15
        case _ => digit.toInt
      }
    }
  }  
  
  //generates sha-1 hash
  def hex_Digest(s:String):String = {
	sha.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
  def randomString(alphabet: String)(n: Int): String = {
    val random = new scala.util.Random
    Stream.continually(random.nextInt(alphabet.size)).map(alphabet).take(n).mkString
  }
  
}