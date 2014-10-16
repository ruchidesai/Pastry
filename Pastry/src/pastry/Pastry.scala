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
	val numnodes = args(0).toInt
	var seed = 5
	var w = seed.toString
	var list = new ListBuffer[String]()
	var list1 = new ListBuffer[String]()
	var counter = 0
	while(counter< numnodes) {
	  var hash = hex_Digest(w)
	  list += hash
	  seed +=1
	  counter +=1	
	  w = seed.toString					  
	}
	list1=list.sorted
	var sortedarr:Array[String]= list1.toArray
  }
  def hex_Digest(s:String):String = {
	sha.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
}