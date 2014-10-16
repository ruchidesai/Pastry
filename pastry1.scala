
import java.security.MessageDigest
import akka.actor._
import com.typesafe.config._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import java.lang.String
 object obj2  {
	  private val sha = MessageDigest.getInstance("SHA-1")
	  def main(args: Array[String]) {
	  	  	
			
				if ( args.isEmpty || args.length < 1) {
	  			println("usage: scala project2.scala <NumNodes> ")
	  			return
				}
				val numnodes = args(0).toInt
  			
	 	
				println("Hello")
				//initialize a list
				//initialize a counter	
				var seed = 5
				var w = seed.toString
				var list = new ListBuffer[String]()
				var list1 = new ListBuffer[String]()
				var counter = 0
				//var sortedarr  =new  Array[String](numnodes) 
				//while counter < number of nodes which is taken as command line argument, do the following
				/*
				hash = SHA-1_hash(seed)
				add hash to list
				seed ++
				*/
				while(counter< numnodes)
				{
					var hash = hex_Digest(w)
					list += hash
					seed +=1
					counter +=1	
					w = seed.toString		
					  
					  //display the entire list generated when number of nodes = 4 
					  
				}
				println(list)
				list1=list.sorted
				//println(list1)
				//list1.toList 
				//println(list1)
				var sortedarr:Array[String]= list1.toArray
				println(sortedarr.mkString(" "))

				
						
          }
          def hex_Digest(s:String):String = {
		sha.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
	  }	
}



