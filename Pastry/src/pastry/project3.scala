package pastry

import akka.actor._
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import akka.routing._
import scala.util.Random
import scala.collection.immutable.HashMap

object project3 extends App {

sealed trait Msg
case object init extends Msg
case class buildRT(idList: List[NodeID]) extends Msg
case class join(message: String, key: NodeID, l: List[ActorRef]) extends Msg
case class routeMsg(message: String, key: NodeID, nodeList: List[ActorRef],hopCount: Int, compObj: Computation) extends Msg
case class nodeJoined(key: NodeID) extends Msg
case object msgAck extends Msg
case object stopSystem extends Msg

class NodeID(strRep: String, inteRep: Int){
  var stringRep = strRep
  var intRep = inteRep
  
  def padZero{
    var length = stringRep.length()
    var temp = new StringBuilder
    
    for( i <- 0 to 8-length-1){
      temp.append("0")
    } 
    temp.append(stringRep)
    stringRep =temp.toString
    println(stringRep)
  }
}

class Computation {
	var sum = 0.0
	var count = 0.0
}
	
class Master(numnodes: Int, numReq: Int) extends Actor {
	var totalRequestCount = numnodes * numReq
	var l = 8  
	var b = 3
	var reqCount = 0
	var ackCount = 0
	var compObj = new Computation()
	var idList = List[NodeID]()
	var numNodes = numnodes
	
	def receive = {
    	case `init` => {
    		var count = 0
    		var randObj = new Random
    		var random = 0
    		var nodeList = List[ActorRef]()
    		
    		//initialize the random nodeIDs
    		while ( count < numNodes ){
    			random = randObj.nextInt(Math.pow(2, b*l).toInt) 		
    			var base4_String = (Integer.toString(random, Math.pow(2, b).toInt))
    			var base4_Int = base4_String.toInt
    			var temp_base4 = new NodeID(base4_String,base4_Int)
    			temp_base4.padZero
    			if( !idList.contains(temp_base4) ){
    			  idList ::= temp_base4
//    			  println("NodeID ---> "+temp_base4.intRep+" "+temp_base4.stringRep)
    			  count += 1
    			}
    		}
    		idList = idList.distinct

    		//create the node actors and assign the nodeIDs
    		//Send each of the nodes buildRT msg to signal them to construct their routing table, leaf set and neighbor set
    		for ( i <- 0 to idList.length-1){
    			try{
    				nodeList ::= (context.actorOf(Props(new PastryNode(idList(i), numReq)),name = idList(i).stringRep))
    			}
    			catch{
    			  case e:Exception => {
    			    numNodes = numNodes - 1
    			  }
    			}
    		}
    		
    		for ( i <- 0 to numNodes-1){
    			nodeList(i) ! buildRT(idList)
    		}
    		
    		//Start of asynchronous JOIN of a new node. 
    		//Generate a new nodeID and add it to the idList and associate it with a new node to be added to the Pastry network
    		var flag = 0
    		var new_base4: NodeID = null
    		while (flag != 1) {
    			var randomObj = new Random()
    			val random = randomObj.nextInt(Math.pow(2, b*l).toInt)
    			var base4_String = (Integer.toString(random, Math.pow(2, b).toInt))
    			var base4_Int = base4_String.toInt
    			new_base4 = new NodeID(base4_String,base4_Int)
    			new_base4.padZero
    			if (!idList.contains(new_base4)) {
    				flag = 1
    			}
    		}	
//    		
    		//new node created and added to the actor system.
//    		nodeList ::= (context.actorOf(Props(new PastryNode(new_base4, numReq)),name = new_base4.stringRep))
//    		
    		//randomly pick a node in the pastry and ask him to join this new node to the system.
//    		nodeList(nodeList.length-2) ! join("Add this node X to the pastry",new_base4,nodeList)
    		
  
    		for(i <- 0 to nodeList.length -1){
    			for(j <- 0 to numReq - 1){
    				
    				do{
    				  random = randObj.nextInt(numNodes)
    				}while(random != i);
//    				println("Messages =========>"+idList(random).intRep)
    				nodeList(i) ! routeMsg("Hello",idList(random),nodeList,0,compObj)
    				reqCount += 1
    			}
    		}
		}
    	
    	case nodeJoined(key: NodeID) => {
//    	  println("Node joined"+key.intRep)
    	  context.actorSelection("/user/master/"+key.stringRep) ! buildRT(idList)
    	}
    	
    	case `msgAck` => {
//    		println("ACK reciiver "+ackCount)
    		ackCount += 1
    		var converge = (0.99 * reqCount)
    		if(ackCount == converge){
    			printResults(compObj)
    			Thread.sleep(1000)
    			self ! stopSystem
    		}
    	}
    	
    	case `stopSystem` => {
    		// stop the actor system.********************************
    		context.system.shutdown;
    	}
  }
	
	//Method to the print the final result of the execution of the Pastry network.
	def printResults(compObj: Computation){
	  
	  println("The average hopcount of the system with "+numNodes+" nodes and "+numReq+" requests per node is "+(compObj.count/(numNodes*numReq)))
	}
}

class PastryNode(id: NodeID, numReq: Int) extends Actor {
	var count = 0
	var nodeId: NodeID = id
	var leafSetSmall = List[NodeID]() 
	var leafSetBig = List[NodeID]()  
	var routingTable = Array.ofDim[NodeID](8, 8)
	var neighbourhoodSet = List[NodeID]() 
	var rowZero = 0
	
	def receive ={ 
		//Build the routing table, leaf set and neighbourhood set for the current node
	  	case buildRT(idList:List[NodeID]) => {
	  		var sortedList = idList.sortBy((A:NodeID)=> A.intRep)
	  		
	  		for( i <- 0 to idList.length-1){
	  			//Populating the leaf set of the node with the numerical closest nodeIDs
	  			if(sortedList(i) == this.nodeId){
	  				for (j <- i - 8 to i - 1) {
	  					if (j > 0)
	  						leafSetSmall = leafSetSmall ::: List(sortedList(j))
	  				}
	  				for (j <- i + 1 to i + 8) {
	  			
	  				  if (j < sortedList.length)
	  						leafSetBig = sortedList(j) :: leafSetBig
	  				}
	  			}
	  			//Populating the routing table of the current node
	  			else{
	  				// rowNum is the row of the current nodeID in the routing Table of the current Node
	  				var rowNum = commLen(sortedList(i),this.nodeId)
					// colNum is the column of the current nodeID in the routing Table of the current Node
					var colNum = digitAtl(sortedList(i),rowNum)
	  						
					//populating the routing table of the current node
					if(routingTable(rowNum)(colNum) != null && routingTable(rowNum)(colNum).intRep < sortedList(i).intRep ){
						routingTable(rowNum)(colNum) = sortedList(i)
					}
					else if (routingTable(rowNum)(colNum) == null){
						routingTable(rowNum)(colNum) = sortedList(i)
	  				}
	  			}
	  		}
	  		//Populating the Neighbourhood Set for the current set
	  		var i = idList.indexOf(this.nodeId)
	  		for ( j <- i-6 to i+6){
	  			if ( idList(i) != this.nodeId)
	  				neighbourhoodSet ::= idList(i) 
	  		}
	  	}
	
	  	//message Routing according to the routing algorithm
	  	case routeMsg(message: String, key: NodeID, nodeList: List[ActorRef],hopCount: Int, compObj: Computation) => {
	  	
	  		compObj.sum = compObj.sum + hopCount
			compObj.count += 1
			
			
			var smallest = 0
			if(!leafSetSmall.isEmpty)
				smallest = small(leafSetSmall).intRep
			
			var largest = Double.PositiveInfinity
			if(!leafSetBig.isEmpty)
				largest = small(leafSetBig).intRep
				
				
	  		//Case 1 : Message has reached the destination
	  		if (this.nodeId == key) {
//	  			println("ACK sent")
	  			context.actorSelection("..") ! msgAck
	  		}
	  		//case 2 : key is in the Leaf Set of the current node, the route it to the node such that difference is minimal
	  		else if( smallest <= key.intRep && key.intRep <= largest){
	  			var distance = Double.PositiveInfinity
	  			var nextNode: NodeID = null
	  			for (i <- 0 to leafSetSmall.length-1){
	  				if(Math.abs(key.intRep - leafSetSmall(i).intRep) < distance ){
	  					distance = Math.abs(key.intRep - leafSetSmall(i).intRep)
	  					nextNode = leafSetSmall(i)
	  				}
	  			}
	  			for (i <- 0 to leafSetBig.length-1){
	  				if(Math.abs(key.intRep - leafSetBig(i).intRep) < distance ){
	  					distance = Math.abs(key.intRep - leafSetBig(i).intRep)
	  					nextNode = leafSetBig(i)
	  				}
	  			}
	  			try{
	  			  context.actorSelection("../"+nextNode.stringRep) ! routeMsg(message,key,nodeList, hopCount+1,compObj)
	  			}
	  			catch{
	  			  case e: Exception => {
	  				  context.parent ! msgAck
	  			  } 
	  			}
	  		}
	  		//case 3 : Find an appropriate node to be routed to for the key
	  		else{
	  			var nextNode: NodeID = null
	  			var rowNum = commLen(key, this.nodeId)
	  			var colNum = digitAtl(key, rowNum)
	  			if(routingTable(rowNum)(colNum) != null){
	  				nextNode = routingTable(rowNum)(colNum) 
					context.actorSelection("../"+nextNode.stringRep) ! routeMsg(message,key,nodeList, hopCount+1,compObj)
	  			}
	  			//case 4 : Find an node from leaf set, routing table and neighbour Set who is numerically closest to the key
	  			else {
	  				var l = commLen(key, this.nodeId)
	  				// create a union of all the sets in the current state.
	  				var unionList = leafSetBig ::: leafSetSmall ::: neighbourhoodSet
	  				for (i <- 0 to 7)
	  					for(j <- 0 to 7)
	  						unionList ::= routingTable(i)(j)
	  						
	  				for(i <- 0 to unionList.size-1){
	  					if (commLen(key, unionList(i)) > commLen(key, this.nodeId) || (math.abs(unionList(i).intRep - this.nodeId.intRep) < math.abs(this.nodeId.intRep - key.intRep))) {
	  						nextNode = unionList(i) 
	  					}
	  				}
	  				context.actorSelection("../"+nextNode.stringRep) ! routeMsg(message,key,nodeList, hopCount+1,compObj)
	  			}
	  		}
	  	}
	  	
	  	case join(message: String, key: NodeID, nodeList: List[ActorRef]) => {
			
			
			var smallestSmall = 0
			if(!leafSetSmall.isEmpty)
				smallestSmall = small(leafSetSmall).intRep
			
			var largestSmall = 0
			if(!leafSetSmall.isEmpty)
				largestSmall = small(leafSetSmall).intRep
			
			var largestLarge = Double.PositiveInfinity
			if(!leafSetBig.isEmpty)
				largestLarge = large(leafSetBig).intRep

			var smallestLarge = Double.PositiveInfinity
			if(!leafSetBig.isEmpty)
				smallestLarge = large(leafSetBig).intRep
				
	  		//case 1 : key is in the Leaf Set of the current node, the route it to the node such that difference is minimal
			if( smallestSmall <= key.intRep && key.intRep <= largestLarge){
	  		
				var leafSetSize = leafSetBig.size + leafSetSmall.size
				if( leafSetSize < 16){
					if(key.intRep < largestSmall)
						leafSetSmall ::= key
					else
						leafSetBig ::= key  
				}
				else{
					if(key.intRep < largestSmall){
						var sortedSmall = leafSetSmall.sortBy((A:NodeID)=> A.intRep)
						leafSetSmall = sortedSmall.drop(1)
						leafSetSmall::=key
					}
					else{
						var sortedBig = leafSetBig.sortBy((A:NodeID)=> A.intRep)
						leafSetBig = sortedBig.dropRight(1)
						leafSetBig::=key
					}
				}
				context.actorSelection("..") ! nodeJoined(key)
			}
	  		//case 2 : Find an appropriate node to be routed to for the key
	  		else{
	  			var nextNode: NodeID = null
	  			var rowNum = commLen(key, this.nodeId)
	  			var colNum = digitAtl(key, rowNum)
	  			if(routingTable(rowNum)(colNum) != null){
	  				nextNode = routingTable(rowNum)(colNum) 
					context.actorSelection("../"+nextNode.stringRep) ! join(message,key,nodeList)
	  			}
	  			//case 4 : Find an node from leaf set, routing table and neighbour Set who is numerically closest to the key
	  			else {
	  				var l = commLen(key, this.nodeId)
	  				// create a union of all the sets in the current state.
	  				var unionList = leafSetBig ::: leafSetSmall ::: neighbourhoodSet
	  				for (i <- 0 to 7)
	  					for(j <- 0 to 7)
	  						unionList ::= routingTable(i)(j)
	  						
	  				for(i <- 0 to unionList.size-1){
	  					if (commLen(key, unionList(i)) > commLen(key, this.nodeId) || (math.abs(unionList(i).intRep - this.nodeId.intRep) < math.abs(this.nodeId.intRep - key.intRep))) {
	  						nextNode = unionList(i) 
	  					}
	  				}
	  				context.actorSelection("../"+nextNode.stringRep) ! join(message,key,nodeList)
	  			}
	  		}
	  	}
	}
	  		
	}	
	//Finds the smallest key in the List
	def small(list: List[NodeID]): NodeID = {
		var small: NodeID = null
		
		if (!list.isEmpty)
			small = list.head
		for (i <- 0 to list.length - 1) {
			if (list(i).intRep < small.intRep)
				small = list(i)
		}
		small
	}
	
	//Finds the largest key in the List
	def large(list: List[NodeID]): NodeID = {
		var large: NodeID = null
		if (!list.isEmpty)
			large = list.head
		for (i <- 0 to list.length - 1) {
			if (list(i).intRep > large.intRep)
				large = list(i)
		}
		large
	}
	
	//Finds the digit at the index 'l' in the string rep of the nodeID
	def digitAtl(key: NodeID, l: Int): Int = {
		val stringKey = key.stringRep
		(stringKey.charAt(l) - '0')
	}

	//Finds the number of first n consecutive common digits between 2 nodeIDs
	def commLen(key: NodeID, nodeId: NodeID): Int = {
		val stringKey = key.stringRep
		val stringThis = nodeId.stringRep
		var count = 0
		var flag = 0
		for (i <- 0 to stringKey.length - 1) {
			if (stringKey.charAt(i) == stringThis.charAt(i) && flag != 1) {
				count += 1
			} 
			else {
				flag = 1
			}
		}
		count
	}




	override def main(args: Array[String]) {

		if ( args.isEmpty || args.length < 2) {
			println("usage: scala project3.scala <NumNodes> <numRequests>")
			sys.exit()
		}
		println("Starting this system")
		startProcess(args(0).toInt, args(1).toInt)
		
	}
		def startProcess(numNodes: Int, numReq: Int){
			val system = ActorSystem("Proj3System")

			// create the master
			val master = system.actorOf(Props(new Master(numNodes, numReq)),name = "master")

			// start the calculation
			master ! init
		}
}