package pastry

import java.security.MessageDigest
import akka.actor._
import com.typesafe.config._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Pastry3 {
  
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
  case class Data(proximity_list: List[ActorRef], list: List[ActorRef]) extends Message
  case class GotIt(hop: Int) extends Message
  case object InitializationDone extends Message
  case object StartRouting extends Message
  
  class Master extends Actor {
    var hop = 0
    var my_num_nodes = 0
    var init_done_counter = 0
    var actorList = List[ActorRef]()
    def receive = {
      case BuildNetwork(num_nodes) =>
        my_num_nodes = num_nodes
        var seed = 1
        var w = seed.toString        
        var counter = 0
	
        //generating node ids
        while(counter < num_nodes) {
	      var hash = hex_Digest(w)
	      actorList::=(context.actorOf(Props(new PastryNode(num_nodes)), name=hash))
	      seed +=1
	      counter +=1	
	      w = seed.toString					  
	    }
        
	    for(actor <- actorList) {
	      actor ! Data(actorList, actorList.sorted)
	    }
	    
      case `InitializationDone` =>
        init_done_counter += 1
        if (init_done_counter >= my_num_nodes) {
          for(actor <- actorList) {
	        actor ! StartRouting
	      }
        }        
	      
      case GotIt(h) =>
        println("No of hops = " + h)
        println("Shutting system down...")
        context.system.shutdown
    }
  }
  
  class PastryNode(num_nodes: Int) extends Actor {
    
    val node_id = self.path.name    
    
    var my_num_nodes = num_nodes
    var my_list = List[ActorRef]()
    var my_proximity_list = List[ActorRef]()
    
    //val r_t_row = ((scala.math.log(my_num_nodes)/scala.math.log(16)).ceil).toInt + 1
    //println(r_t_row)
    val r_t_row = 32
    
    var my_smaller_leaf_set = ListBuffer[ActorRef]()
    var my_larger_leaf_set = ListBuffer[ActorRef]()
    var my_neighborhood_set = ListBuffer[ActorRef]()
    //var my_routing_table = Array.ofDim[ListBuffer[ActorRef]](r_t_row)
    var my_routing_table = Array.ofDim[ActorRef](r_t_row, 16)
    
    def receive = {
      
      case Data(proximity_list, list) =>
        
        my_proximity_list = proximity_list
        my_num_nodes = num_nodes
        my_list = list
        
        initialize_routing_table()
        initialize_neighborhood_set()
        initialize_leaf_sets()
        
        context.actorSelection("../") ! InitializationDone        
        
      case `StartRouting` =>
        var rand = new Random()
	    var key_int = rand.nextInt(num_nodes)
	    var key_hash = hex_Digest(key_int.toString)
	    self ! Route(hex_Digest(key_hash), 0)
        
      case Route(key, hop) =>
        
        //if the current node happens to be the destination
        if(self.path.name == key) {
          println("Message Received")
          context.actorSelection("../") ! GotIt(hop)
        }          
          
        //check if current node lies in the smaller leaf set
        else if(in_my_smaller_leaf_set(key)) {
          var max_common_prefix_length = ((key).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
          cpl_map += self -> max_common_prefix_length
          for(node <- my_smaller_leaf_set) {
            var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            cpl_map += node -> cpl
          }
          cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
          var min_difference = (key.compare(self.path.name)).abs
          var destination = self
          for(pair <- cpl_map) {
            var difference = (key.compare(pair._1 .path.name)).abs
            if (difference < min_difference) {
              min_difference = difference
              destination = pair._1 
            }
          }
          
          if (destination == self)
            context.actorSelection("../") ! GotIt(hop)
          else
            destination ! Route(key, (hop + 1))
        }
        
        //check if current node lies in the larger leaf set
        else if(in_my_larger_leaf_set(key)) {
          var max_common_prefix_length = ((key).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
          cpl_map += self -> max_common_prefix_length
          for(node <- my_larger_leaf_set) {
            var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            cpl_map += node -> cpl
          }
          cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
          var min_difference = (key.compare(self.path.name)).abs
          var destination = self
          for(pair <- cpl_map) {
            var difference = (key.compare(pair._1 .path.name)).abs
            if (difference < min_difference) {
              min_difference = difference
              destination = pair._1 
            }
          }
          
          if (destination == self)
            context.actorSelection("../") ! GotIt(hop)
          else
            destination ! Route(key, (hop + 1))
        }
        
        //check in routing table
        else {
          var row = ((key).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var col = column_of(row, key)
          if(my_routing_table(row)(col) != null) {
            my_routing_table(row)(col) ! Route(key, (hop + 1))
          }
          else {
            //rare case - check in neighborhood set
            var max_common_prefix_length = ((key).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
            cpl_map += self -> max_common_prefix_length
            for(node <- my_neighborhood_set) {
              var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
              cpl_map += node -> cpl
            }
            cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
            var min_difference = (key.compare(self.path.name)).abs
            var destination = self
            for(pair <- cpl_map) {
              var difference = (key.compare(pair._1 .path.name)).abs
              if (difference < min_difference) {
                min_difference = difference
                destination = pair._1 
              }
            }
          
            if (destination == self)
              context.actorSelection("../") ! GotIt(hop)
            else
              destination ! Route(key, (hop + 1))
          }
        }
    }
    
    /*def initialize_routing_table() {
      var index = my_list.indexOf(self)
      for (i <- 0 until my_routing_table.length)
        my_routing_table(i) = ListBuffer[ActorRef]()
      var i = 0
      while (i < my_list.length) {
        index += 1
        if (index >= my_list.length)
          index %= my_list.length
        var node = my_list(index)
        if (node != self) {
          var common_prefix = (node.path.name).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString
          if (my_routing_table(common_prefix.length).length < 16)
            my_routing_table(common_prefix.length) += node
        }        
        i += 1
      }
    }*/
    
    def initialize_routing_table() {
      var index = my_list.indexOf(self)
      var i = 0
      while (i < my_list.length) {
        index += 1
        if (index >= my_list.length)
          index %= my_list.length
        var node = my_list(index)
        if (node != self) {
          var common_prefix = (node.path.name).zip(self.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString
          var lrow = common_prefix.length()
          var lcol = column_of(lrow, node.path.name)
          if(my_routing_table(lrow)(lcol) != null && (my_routing_table(lrow)(lcol).path.name < node.path.name)) {
            my_routing_table(lrow)(lcol) = node
          }
          else if(my_routing_table(lrow)(lcol) == null) {
            my_routing_table(lrow)(lcol) = node
          }
        }
        i += 1
      }
    }
    
    def initialize_neighborhood_set() {
      var index = my_proximity_list.indexOf(self)      
      var i = 0
      while (i < 16) {
        index += 1
        if (index >= my_num_nodes)
          index %= my_num_nodes
        my_neighborhood_set += my_proximity_list(index)                
        i += 1
      } 
      i = 0
      index = my_proximity_list.indexOf(self)
      while (i < 16) {
        index -= 1
        if (index < 0)
          index += my_num_nodes
        my_neighborhood_set += my_proximity_list(index)        
        i += 1
      }
    }
    
    def initialize_leaf_sets() {
      var index = my_list.indexOf(self)
      var i = 0
      while (i < 8) {
        index -= 1
        if (index < 0)
          index += my_num_nodes
        my_smaller_leaf_set += my_list(index)        
        i += 1
      }
      i = 0
      index = my_list.indexOf(self)
      while (i < 8) {
        index += 1
        if (index >= my_num_nodes)
          index %= my_num_nodes
        my_larger_leaf_set += my_list(index)                
        i += 1
      }
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
        case '0' => 0
        case '1' => 1
        case '2' => 2
        case '3' => 3
        case '4' => 4
        case '5' => 5
        case '6' => 6
        case '7' => 7
        case '8' => 8
        case '9' => 9
        case 'a' => 10
        case 'b' => 11
        case 'c' => 12
        case 'd' => 13
        case 'e' => 14
        case 'f' => 15
        
      }
    }
  }  
  
  //generates sha-1 hash
  def hex_Digest(s:String):String = {
	sha.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
  
}