package pastry

import java.security.MessageDigest
import akka.actor._
import com.typesafe.config._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration._
import scala.util.control.Breaks._

object Pastry6 {  
  
  def main(args: Array[String]){
    if ( args.isEmpty || args.length < 2) {
      println("usage: scala project2.scala <number of nodes> <number of requests>")
	  return
	}
	val num_nodes = args(0).toInt
	val num_requests = args(1).toInt
	
	//create AKKA actor system
	val system = ActorSystem("PastrySystem")
	val master = system.actorOf(Props[Master], name="master")
	master ! BuildNetwork(num_nodes, num_requests)
  }
  
  sealed trait Message
  case class BuildNetwork(num_nodes: Int, num_requests: Int) extends Message
  case class Route(key: String, hop: Int) extends Message
  case class Data(proximity_list: List[ActorRef], list: List[ActorRef]) extends Message
  case class GotIt(hop: Int) extends Message
  case object InitializationDone extends Message
  case object StartRouting extends Message
  case class StartJoining(proximity_list: List[ActorRef]) extends Message
  case class JoinRqstRoute(key: String, z: ActorRef, smaller_leaf_set: ListBuffer[ActorRef], larger_leaf_set: ListBuffer[ActorRef], routing_table: Array[Array[ActorRef]], node_list: List[ActorRef], hop: Int) extends Message
  case object JoinComplete extends Message
  case class UpdateYourData(node_id: String) extends Message
  case object UpdateAck extends Message
  case class NeedYourLeafSet(set_type: String, key: String, hop: Int, node: ActorRef) extends Message
  case class LeafSetAck(set_type: String, leaf_set: ListBuffer[ActorRef], key: String, hop: Int, node: ActorRef) extends Message
  case class NeedRoutingTableEntry(row: Int, col: Int, key: String, hop: Int) extends Message
  case class RoutingTableAck(row: Int, col: Int, key: String, hop: Int, node: ActorRef) extends Message
  case class NeedYourNeighbors(key: String, hop: Int, node: ActorRef) extends Message
  case class NeighborAck(key: String, hop: Int, node: ActorRef, neighborhood_set: ListBuffer[ActorRef]) extends Message
  case class Alive(msg_type: String, state: Boolean, node_type: String, key: String, hop: Int, destination: ActorRef, row: Int, col: Int) extends Message
  
  class Master extends Actor {
    var total_hops = 0
    var my_num_nodes = 0
    var my_num_requests = 0
    var init_done_counter = 0
    var join_complete_counter = 0
    var request_counter = 0
    var actorList = List[ActorRef]()
    def receive = {
      case BuildNetwork(num_nodes, num_requests) =>
        my_num_nodes = num_nodes
        my_num_requests = num_requests
        var seed = 1
        var w = seed.toString        
        var counter = 0
	
        //generating node ids
        while(counter < num_nodes) {
	      var hash = hex_Digest(w)
	      actorList::=(context.actorOf(Props(new PastryNode(num_nodes, num_requests)), name=hash))
	      seed +=1
	      counter +=1	
	      w = seed.toString					  
	    }
        
        for(actor <- actorList.take(32)) {
	      actor ! Data(actorList, actorList.take(32).sorted)
	    }
	    
      case `InitializationDone` =>
        init_done_counter += 1
        if (init_done_counter >= 32) {
          actorList(init_done_counter) ! StartJoining(actorList)
        }
          
        
      case `JoinComplete` =>
        join_complete_counter += 1
        if(join_complete_counter >= (my_num_nodes - 32)) {
          for(actor <- actorList) {
	        actor ! StartRouting
	      }
        }
        else {
          init_done_counter += 1
          actorList(init_done_counter) ! StartJoining(actorList)
        }
	      
      case GotIt(h) =>
        request_counter += 1
        total_hops += h
        if (request_counter >= (my_num_nodes * my_num_requests)) {
          var avg_hop = total_hops.toDouble / (my_num_nodes * my_num_requests)
          println("Total no of hops = " + total_hops)
          println("Total no of requests = " + request_counter)
          println("Average no of hops = " + avg_hop)
          println("Shutting system down...")
          context.system.shutdown
        }        
    }
  }
  
  class PastryNode(num_nodes: Int, num_requests: Int) extends Actor {
    
    val my_node_id = self.path.name    
    
    var my_num_nodes = num_nodes
    var my_num_requests = num_requests
    var ack_counter = 0
    var no_of_ack_nodes = 0
    var my_list = List[ActorRef]()
    var my_proximity_list = List[ActorRef]()
    var my_state = true
    var dead = List(5)
    
    //val r_t_row = ((scala.math.log(my_num_nodes)/scala.math.log(16)).ceil).toInt + 1
    //println(r_t_row)
    val r_t_row = 32
    
    var my_smaller_leaf_set = ListBuffer[ActorRef]()
    var my_larger_leaf_set = ListBuffer[ActorRef]()
    var my_neighborhood_set = ListBuffer[ActorRef]()
    //var my_routing_table = Array.ofDim[ListBuffer[ActorRef]](r_t_row)
    var my_routing_table = Array.ofDim[ActorRef](r_t_row, 16)
    
    if(my_proximity_list.indexOf(self) == 10)
      my_state = false
    
    def receive = {
      
      case Data(proximity_list, list) =>
        
        my_proximity_list = proximity_list
        my_list = list
        
        initialize_routing_table()        
        initialize_neighborhood_set()        
        initialize_leaf_sets()
        
        context.actorSelection("../") ! InitializationDone
        
      case StartJoining(proximity_list) =>
        my_proximity_list = proximity_list
        var my_index = my_proximity_list.indexOf(self)
        var closest_node_by_proximity = my_proximity_list(my_index - 1)
        var slf = ListBuffer[ActorRef]()
        var llf = ListBuffer[ActorRef]()
        var rt = Array.ofDim[ActorRef](r_t_row, 16)
        var nl = List[ActorRef]()
        closest_node_by_proximity ! JoinRqstRoute(my_node_id, null, slf, llf, rt, nl, 0)
        
      //For routing joining messages
      case JoinRqstRoute(key, z, smaller_leaf_set, larger_leaf_set, routing_table, node_list, hop) =>
        if(my_node_id == key) {
          if (z.path.name < key) {
            my_smaller_leaf_set = smaller_leaf_set.drop(1)
            my_smaller_leaf_set += z
            my_larger_leaf_set = larger_leaf_set
          }
          else {
            my_smaller_leaf_set = smaller_leaf_set
            my_larger_leaf_set = larger_leaf_set.dropRight(1)
            my_larger_leaf_set.insert(0, z)
          }          
          my_routing_table = routing_table
          initialize_neighborhood_set()
          no_of_ack_nodes = node_list.length
          for(n <- node_list)
            n ! UpdateYourData(my_node_id)          
        }
        
        else {          
          
          var rt = routing_table
          var new_hop = hop
          if(hop == 0)
          {
            var l = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            for (i <- 0 to l) {
              rt(i) = my_routing_table(i)
              new_hop += 1
            }            
          }
          else {
            var l = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            rt(l) = my_routing_table(l)
            new_hop += 1
          }            
        
          //check if current node lies in the smaller leaf set
          if(in_my_smaller_leaf_set(key)) {            
            var max_common_prefix_length = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
            cpl_map += self -> max_common_prefix_length
            for(node <- my_smaller_leaf_set) {
              var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
              cpl_map += node -> cpl
            }
            cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
            var min_difference = (key.compare(my_node_id)).abs
            var destination = self
            for(pair <- cpl_map) {
              var difference = (key.compare(pair._1 .path.name)).abs
              if (difference < min_difference) {
                min_difference = difference
                destination = pair._1 
              }
            }
          
            if (destination == self) {
              var X = my_proximity_list(my_proximity_list.indexWhere(p => p.path.name == key))
              var lnode_list = self::node_list
              X ! JoinRqstRoute(key, self, my_smaller_leaf_set, my_larger_leaf_set, rt, lnode_list, new_hop)
            }
            else {
              var lnode_list = self::node_list
              destination ! JoinRqstRoute(key, z, smaller_leaf_set, larger_leaf_set, rt, lnode_list, new_hop)
            }              
          }
        
          //check if current node lies in the larger leaf set
          else if(in_my_larger_leaf_set(key)) {            
            var max_common_prefix_length = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
            cpl_map += self -> max_common_prefix_length
            for(node <- my_larger_leaf_set) {
              var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
              cpl_map += node -> cpl
            }
            cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
            var min_difference = (key.compare(my_node_id)).abs
            var destination = self
            for(pair <- cpl_map) {
              var difference = (key.compare(pair._1 .path.name)).abs
              if (difference < min_difference) {
                min_difference = difference
                destination = pair._1 
              }
            }
          
            if (destination == self) {
              var X = my_proximity_list(my_proximity_list.indexWhere(p => p.path.name == key))
              var lnode_list = self::node_list
              X ! JoinRqstRoute(key, self, my_smaller_leaf_set, my_larger_leaf_set, rt, lnode_list, new_hop)
            }
            else {
              var lnode_list = self::node_list
              destination ! JoinRqstRoute(key, z, smaller_leaf_set, larger_leaf_set, rt, lnode_list, new_hop)
            }                  
          }
        
          //check in routing table
          else {            
            var row = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            var col = column_of(row, key)
            if(my_routing_table(row)(col) != null) {
              var lnode_list = self::node_list
              my_routing_table(row)(col) ! JoinRqstRoute(key, z, smaller_leaf_set, larger_leaf_set, rt, lnode_list, new_hop)
            }
            else {
              //rare case - check in neighborhood set
              var max_common_prefix_length = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
              var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
              cpl_map += self -> max_common_prefix_length
              for(node <- my_neighborhood_set) {
                var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
                cpl_map += node -> cpl
              }
              cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
              var min_difference = (key.compare(my_node_id)).abs
              var destination = self
              for(pair <- cpl_map) {
                var difference = (key.compare(pair._1 .path.name)).abs
                if (difference < min_difference) {
                  min_difference = difference
                  destination = pair._1 
                }
              }
              
              var X = my_proximity_list(my_proximity_list.indexWhere(p => p.path.name == key))
              var lnode_list = self::node_list
              X ! JoinRqstRoute(key, self, my_smaller_leaf_set, my_larger_leaf_set, rt, lnode_list, new_hop)
                
            }
          }
        }
        
      case UpdateYourData(node_id) =>
        update_data(node_id)
        sender ! UpdateAck
        
      case `UpdateAck` =>
        ack_counter += 1
        if(ack_counter >= no_of_ack_nodes)
          context.actorSelection("../") ! JoinComplete
      
      case `StartRouting` =>
        var rand = new Random()
        for (i <-0 until my_num_requests) {          
	      var key_int = rand.nextInt(my_num_nodes).abs
	      var key_string = key_int.toString
	      //if this is not added, program does not converge for certain values
	      import context.dispatcher
	      var key_hash = hex_Digest(key_string)
	      context.system.scheduler.scheduleOnce(1000 milliseconds, self, Route(key_hash, 0))
        }        
        
      case Route(key, hop) =>
        
        //if the current node happens to be the destination
        if(my_node_id == key) {
          context.actorSelection("../") ! GotIt(hop)
        }          
          
        //check if current node lies in the smaller leaf set
        else if(in_my_smaller_leaf_set(key)) {
          var max_common_prefix_length = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
          cpl_map += self -> max_common_prefix_length
          for(node <- my_smaller_leaf_set) {
            var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            cpl_map += node -> cpl
          }
          cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
          var min_difference = (key.compare(my_node_id)).abs
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
          else {
            //destination ! Alive("msg", false, "sls", key, (hop + 1), destination, 0, 0)
            if (isAlive(destination))
              destination ! Route(key, (hop + 1))
            else
              my_smaller_leaf_set.last ! NeedYourLeafSet("smaller", key, (hop + 1), destination)
          }            
        }
        
        //check if current node lies in the larger leaf set
        else if(in_my_larger_leaf_set(key)) {
          var max_common_prefix_length = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
          cpl_map += self -> max_common_prefix_length
          for(node <- my_larger_leaf_set) {
            var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            cpl_map += node -> cpl
          }
          cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
          var min_difference = (key.compare(my_node_id)).abs
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
          else {
            //destination ! Alive("msg", false, "lls", key, (hop + 1), destination, 0, 0)
            if (isAlive(destination))
              destination ! Route(key, (hop + 1))
            else
              my_larger_leaf_set.last ! NeedYourLeafSet("larger", key, (hop + 1), destination)
          }            
        }
        
        //check in routing table
        else {
          var row = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          var col = column_of(row, key)
          if(my_routing_table(row)(col) != null) {
            //my_routing_table(row)(col) ! Alive("msg", false, "rt", key, (hop + 1), my_routing_table(row)(col), row, col)
            if (isAlive(my_routing_table(row)(col)))
              my_routing_table(row)(col) ! Route(key, (hop + 1))
            else {
              var r = row
              var c = col + 1
              while(!isAlive(my_routing_table(r)(c)) || my_routing_table(r)(c) == null) {
                if (c >= 16) {
                  r += 1
                  c = 0
                }
                else
                  c += 1
                if (r > 31)
                 println("gone")
              }
              my_routing_table(r)(c) ! NeedRoutingTableEntry(row, col, key, (hop + 1))
            }
          }
          else {
            //rare case - check in neighborhood set
            var max_common_prefix_length = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
            cpl_map += self -> max_common_prefix_length
            for(node <- my_neighborhood_set) {
              var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
              cpl_map += node -> cpl
            }
            cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
            var min_difference = (key.compare(my_node_id)).abs
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
            else {
              //destination ! Alive("msg", false, "ns", key, (hop + 1), destination, 0, 0)
              if (isAlive(destination))
                destination ! Route(key, (hop + 1))
              else {
                if (my_neighborhood_set.indexOf(destination) > 15)
                  my_neighborhood_set.last ! NeedYourNeighbors(key, hop, destination)
                else
                  my_neighborhood_set.head ! NeedYourNeighbors(key, hop, destination)
              }
                
            }              
          }
        }
        
      case Alive(msg_type, state, node_type, key, hop, destination, row, col) =>
        msg_type match {
          case "msg" =>
            sender ! Alive("ack", my_state, node_type, key, hop, destination, row, col)
          case "ack" =>
            if (my_state)
              destination ! Route(key, hop)
            else {
              node_type match {
                case "sls" =>
                  my_smaller_leaf_set.last ! NeedYourLeafSet("smaller", key, (hop + 1), destination)
                case "lls" =>
                  my_larger_leaf_set.last ! NeedYourLeafSet("larger", key, (hop + 1), destination)
                case "rt" =>
                  if ((col + 1) < 16)
                    my_routing_table(row)(col + 1) ! NeedRoutingTableEntry(row, col, key, (hop + 1))
                  else
                    my_routing_table(row + 1)(0) ! NeedRoutingTableEntry(row, col, key, (hop + 1))
                case "ns" =>
                  if (my_neighborhood_set.indexOf(destination) > 15)
                    my_neighborhood_set.last ! NeedYourNeighbors(key, hop, destination)
                  else
                    my_neighborhood_set.head ! NeedYourNeighbors(key, hop, destination)                
              }
            }           
        }
        
      case NeedYourLeafSet(set_type, key, hop, destination) =>
        if (set_type == "smaller")
          sender ! LeafSetAck("smaller", my_smaller_leaf_set, key, hop, destination)
        else
          sender ! LeafSetAck("larger", my_larger_leaf_set, key, hop, destination)
          
      case LeafSetAck(set_type, leaf_set, key, hop, destination) =>
        var ls = leaf_set.filter(p => !dead.contains(my_proximity_list.indexOf(p)))
        if (set_type == "smaller") {
          my_smaller_leaf_set = my_smaller_leaf_set.filterNot(p => p == destination)
          my_smaller_leaf_set += ls.head
        }
        else {
          my_larger_leaf_set = my_larger_leaf_set.filterNot(p => p == destination)
          my_larger_leaf_set += ls.head
        }
        var max_common_prefix_length = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
        var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
        cpl_map += self -> max_common_prefix_length
        if (set_type == "smaller") {
          for(node <- my_smaller_leaf_set) {
            var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            cpl_map += node -> cpl
          }
        }
        else {
          for(node <- my_larger_leaf_set) {
            var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
            cpl_map += node -> cpl
          }
        }
        cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
        var min_difference = (key.compare(my_node_id)).abs
        var dest = self
        for(pair <- cpl_map) {
          var difference = (key.compare(pair._1 .path.name)).abs
          if (difference < min_difference) {
            min_difference = difference
            dest = pair._1 
          }
        }
          
        if (dest == self)
          context.actorSelection("../") ! GotIt(hop)
        else
          dest ! Route(key, hop)        
        
      case NeedRoutingTableEntry(row, col, key, hop) =>
        var r = row
        var c = col
        while (!isAlive(my_routing_table(r)(c)) || my_routing_table(r)(c) == null) {
          if (c >= 16) {
            r += 1
            c = 0
          }
          else
            c += 1
          if (r > 31)
          println("gone")
        }        
        sender ! RoutingTableAck(row, col, key, hop, my_routing_table(r)(c))
        
      case RoutingTableAck(row, col, key, hop, node) =>
        my_routing_table(row)(col) = node
        node ! Route(key, hop)
        
      case NeedYourNeighbors(key, hop, destination) =>
        sender ! NeighborAck(key, hop, destination, my_neighborhood_set)
        
      case NeighborAck(key, hop, destination, neighborhood_set) =>
        my_neighborhood_set = my_neighborhood_set.filterNot(p => p == destination)
        var ns = neighborhood_set.filter(p => !dead.contains(my_proximity_list.indexOf(p)))
        my_neighborhood_set += ns(ns.length / 2)
        
        var max_common_prefix_length = ((key).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
        var cpl_map = scala.collection.mutable.Map[ActorRef, Int]()
        cpl_map += self -> max_common_prefix_length
        for(node <- my_neighborhood_set) {
          var cpl = ((key).zip(node.path.name).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length
          cpl_map += node -> cpl
        }
        cpl_map = cpl_map.filter(p => p._2 == cpl_map.maxBy(_._2)._2)
        var min_difference = (key.compare(my_node_id)).abs
        var dest = self
        for(pair <- cpl_map) {
          var difference = (key.compare(pair._1 .path.name)).abs
          if (difference < min_difference) {
            min_difference = difference
            dest = pair._1 
          }
        }          
        if (dest == self)
          context.actorSelection("../") ! GotIt(hop)
        else
          dest ! Route(key, hop)
    }
    
    def initialize_routing_table() {
      var index = my_list.indexOf(self)
      var i = 0
      while (i < my_list.length) {
        index += 1        
        if (index >= my_list.length)
          index %= my_list.length        
        var node = my_list(index)
        if (node != self) {
          var common_prefix = (node.path.name).zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString
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
        index -= 1
        if (index < 0)
          index += my_num_nodes
        //my_neighborhood_set += my_proximity_list(index)
        my_neighborhood_set.insert(0, my_proximity_list(index))
        i += 1
      }
      i = 0
      index = my_proximity_list.indexOf(self)
      
      while (i < 16) {
        index += 1
        if (index >= my_num_nodes)
          index %= my_num_nodes        
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
          index += 32
        my_smaller_leaf_set += my_list(index)        
        i += 1
      }
      i = 0
      index = my_list.indexOf(self)
      while (i < 8) {
        index += 1
        if (index >= 32)
          index %= 32
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
    
    def update_data(key: String) {
      var key_index = my_proximity_list.indexWhere(p => p.path.name == key)
      //update smaller leaf set
      if (key >= my_smaller_leaf_set.head.path.name && key <= my_smaller_leaf_set.last.path.name) {
        var diff1 = (my_node_id.compare(key)).abs
        var pos = 0
        breakable {
          for (leaf <- my_smaller_leaf_set) {
            var diff2 = (my_node_id.compare(leaf.path.name)).abs
            if (diff1 > diff2)
              break
            else
              pos += 1
          }
        }
        my_smaller_leaf_set.insert(pos, my_proximity_list(key_index))
        my_smaller_leaf_set.drop(1)
      }
      //update larger leaf set
      else if (key >= my_larger_leaf_set.head.path.name && key <= my_larger_leaf_set.last.path.name) {
        var diff1 = (my_node_id.compare(key)).abs
        var pos = 0
        breakable {
          for (leaf <- my_larger_leaf_set) {
            var diff2 = (my_node_id.compare(leaf.path.name)).abs
            if (diff1 < diff2)
              break
            else
              pos += 1
          }
        }
        my_larger_leaf_set.insert(pos, my_proximity_list(key_index))
        my_larger_leaf_set.dropRight(1)
      }
      //no need to update neighborhood set as it is already up to date
      
      //update routing table
      var lrow = (key.zip(my_node_id).takeWhile(Function.tupled(_ == _)).map(_._1).mkString).length()
      var lcol = column_of(lrow, key)
      if(my_routing_table(lrow)(lcol) == null)
        my_routing_table(lrow)(lcol) = my_proximity_list(my_proximity_list.indexWhere(p => p.path.name == key))
      else {
        var node = my_routing_table(lrow)(lcol)
        if (my_node_id.compare(key).abs < my_node_id.compare(node.path.name).abs)
          my_routing_table(lrow)(lcol) = my_proximity_list(my_proximity_list.indexWhere(p => p.path.name == key))
      }
    }
    
    def isAlive(node: ActorRef): Boolean = {
      var index = my_proximity_list.indexOf(node)
      if (dead.exists(p => p == index))
        return false
      else
        return true
    }
  }  
  
  //generates sha-1 hash
  def hex_Digest(s:String):String = {
    val sha = MessageDigest.getInstance("MD5")
	sha.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
  
}