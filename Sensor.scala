import scala.math._
import scala.util.Random
import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer

case object START
case object EXIT

case class ALERT(i:Int)
case class Neighbor(alln: ArrayBuffer[ActorRef])
case class Neighbor3d(alln: ListBuffer[ActorRef])
case class counter(count1:Int,i:Int)
case class GOSSIP(noOfActor:Int)
case class PushSum(s:Double,w:Double)
case class SETVALUE(s:Double)
case class threeD(nodes:Int)

class ActorGossip extends Actor
{

	var neighbor = new ArrayBuffer[ActorRef]()
			var acount : Int = 0
			var boss: ActorRef = null
			var previous_value,current_value:Double = 0
			var svalue, wvalue,pushcount:Double = 1

			def receive =
		{
		case Neighbor(alln) =>
    boss = sender
    neighbor ++= alln

    case Neighbor3d(alln) =>
      boss = sender
      neighbor ++= alln
      
		case GOSSIP =>
		    acount = acount + 1
        if(acount >= 10)
        {
          boss ! EXIT            
        }
      self ! ALERT

		case PushSum(s,w) =>

		svalue=(s+svalue)
		wvalue=(w+wvalue)

		current_value = svalue/wvalue

		if(abs(current_value-previous_value) < pow(10,-10))
			pushcount+=1
			else
				pushcount=0

				previous_value = current_value

				if(pushcount<3)
				{
					var r = Random.nextInt(neighbor.length)
							neighbor(r) ! PushSum(svalue/2,wvalue/2)
				}
				else
				{
					boss ! EXIT
				}        


		case ALERT =>
		if(acount < 10)
		{
			var r = Random.nextInt(neighbor.length)
					neighbor(r) ! GOSSIP
		}

		case SETVALUE(s) =>
		svalue = s
		previous_value = svalue / wvalue


		}
}


class Master(val numnodes:Int,val topology:String,val algorithm:String) extends Actor
{
      var time:Long=0
      time = System.currentTimeMillis()
        println(time+"strt")
        //      println(time)
			var actor,y = new ArrayBuffer[ActorRef]()
			var no:Int = 0
      var no1= no*no*no
			var finish = new Array[Int](no)
      var finish1 = new ArrayBuffer[Int](no+no+no)
			var ps:Int = 0
			no = numnodes
			if (no == 0) 
			{
				println("Enter more than zero number of nodes")
				System.exit(0)
			}

			if(topology.equalsIgnoreCase("line") || topology.equalsIgnoreCase("full"))
			{
				for(i <- 0 to no-1)
				{
					actor += context.actorOf(Props(new ActorGossip), name = "actor" + i)
							
				}
				if(topology.toLowerCase() == "line")           //LINE TOPOLOGY
				{    
					actor(0) ! Neighbor(y += actor(1))
					y = new ArrayBuffer[ActorRef]()

					actor(no-1) ! Neighbor(y += actor(no-2))
					y = new ArrayBuffer[ActorRef]()

					for(j <- 1 to no-2)
					{
						actor(j) ! Neighbor(y += (actor(j-1),actor(j+1)))
						y = new ArrayBuffer[ActorRef]()
					}
				}

				if(topology.toLowerCase() == "full")            //FULL TOPOLOGY
				{      
          for(i <- 0 until no)
          {
					actor(i) ! Neighbor(actor-actor(i))
					y = new ArrayBuffer[ActorRef]()
				  }
				}

				if(algorithm.equalsIgnoreCase("push-sum"))
				{
					for(i <- 0 to no-1)
					{
						actor(i) ! SETVALUE(i.toLong)
					}
				}
			}

			if(topology.equalsIgnoreCase("3d") || topology.equalsIgnoreCase("imperfect3d"))
			{
        var Nodes = (for { i <- 0 to no-1; j <- 0 to no-1; k <- 0 to no-1 } yield {
        val NodeRef = context.actorOf(Props[ActorGossip], s"Node_$i-$j-$k")
        ((i, j, k), NodeRef)
      }).toMap
    
      if(topology.equalsIgnoreCase("3d")){
      for (x <- 0 to no-1; y <- 0 to no-1; z <- 0 to no-1) {
        var neighborlist = new ListBuffer[ActorRef]()

        for (i <- x - 1 to x + 1; if ((i, y, z) != (x, y, z) && (i >= 0 && i <= no-1))) {
          neighborlist += Nodes(i, y, z)
        }
        for (i <- y - 1 to y + 1; if ((x, i, z) != (x, y, z) && (i >= 0 && i <= no-1))) {
          neighborlist += Nodes(x, i, z)
        }
        for (i <- z - 1 to z + 1; if ((x, y, i) != (x, y, z) && (i >= 0 && i <= no-1))) {
          neighborlist += Nodes(x, y, i)
        }

        Nodes(x, y, z) ! Neighbor3d(neighborlist)
              
      }
      }
       
        if(topology.equalsIgnoreCase("imperfect3d")){
        for (x <- 0 to no-1; y <- 0 to no-1; z <- 0 to no-1) {
        var flag = false
        var neighborlist = new ListBuffer[ActorRef]()

        for (i <- x - 1 to x + 1; if ((i, y, z) != (x, y, z) && (i >= 0 && i <= no-1))) {
          neighborlist += Nodes(i, y, z)
        }
        for (i <- y - 1 to y + 1; if ((x, i, z) != (x, y, z) && (i >= 0 && i <= no-1))) {
          neighborlist += Nodes(x, i, z)
        }
        for (i <- z - 1 to z + 1; if ((x, y, i) != (x, y, z) && (i >= 0 && i <= no-1))) {
          neighborlist += Nodes(x, y, i)
        }
        while(!flag){
        var n1 = Random.nextInt(no) 
        var n2 = Random.nextInt(no)
        var n3 = Random.nextInt(no)
        if(!neighborlist.contains(Nodes(n1,n2,n3)))
            {
            neighborlist += Nodes(n1,n2,n3)
            flag = true
            }           
        }
        Nodes(x, y, z) ! Neighbor3d(neighborlist)
              
      }
      }
        if (algorithm.toLowerCase() == "push-sum") 
        {
              var i = 1
                for(x <- 0 to no-1; y <- 0 to no-1; z <- 0 to no-1)
                {
                  Nodes(x, y, z) ! SETVALUE(i.toLong)
                  i = i + 1
                }
              
              var n1 : Int = Random.nextInt(no)
              var n2 : Int = Random.nextInt(no)
              var n3 : Int = Random.nextInt(no)
              Nodes(n1,n2,n3) ! PushSum(0, 0)
        }
        if (algorithm.toLowerCase() == "gossip")
        {
              var n1 : Int = Random.nextInt(no)
              var n2 : Int = Random.nextInt(no)
              var n3 : Int = Random.nextInt(no)
              Nodes(n1,n2,n3) ! GOSSIP
        }			

			}

			def receive = 
				{ 
				case START =>
        
				if (algorithm.toLowerCase() == "gossip" && (topology.toLowerCase() == "line" || topology.toLowerCase() == "full")) 
				{
					var n : Int = Random.nextInt(no)
							actor(n) ! GOSSIP
				} 

				if (algorithm.toLowerCase() == "push-sum" && (topology.toLowerCase() == "line" || topology.toLowerCase() == "full")) 
				{
					var n : Int = Random.nextInt(no)
							actor(n) ! PushSum(0, 0)
				}




				case EXIT =>
				if(topology.equalsIgnoreCase("3d") || topology.equalsIgnoreCase("imperfect3d"))
				{
					no = no*no*no
							println("Number of Nodes: " + no)
          println("end"+System.currentTimeMillis())
							println("Time: " + (System.currentTimeMillis() - time) + " milliseconds")
							System.exit(0)
				}
				else
				{
					println("Number of Nodes: " + no)
					println("end"+System.currentTimeMillis())
          println("Time: " + (System.currentTimeMillis() - time) + " milliseconds")
					System.exit(0)
				}   
				}

}



object project
{  
	var flag = 0

			def pushsum(numnodes:Int,topology:String,algorithm:String)
	{
		val system = ActorSystem("GossipSimulator")
				val master = system.actorOf(Props(new Master (numnodes, topology, algorithm)),name="master")
				master ! START
	}

	def main(args: Array[String])
	{
		while(flag!=1)
		{ 
			flag = 1
					var num:Double = 0
					var numnodes = args(0).toInt
					var topology = args(1)
					var algorithm = args(2)
					if ("3d".equalsIgnoreCase(topology) || "imperfect3d".equalsIgnoreCase(topology))
					{
						num = math.cbrt(numnodes)
								if(num%1 != 0)
								{
									numnodes = num.toInt + 1
								}else numnodes = num.toInt
					}   

			if(numnodes>0)
			{
				if(topology.equalsIgnoreCase("3d") || topology.equalsIgnoreCase("imperfect3d") || topology.equalsIgnoreCase("full") || topology.equalsIgnoreCase("line"))
				{
					if(algorithm.equalsIgnoreCase("gossip") || algorithm.equalsIgnoreCase("push-sum"))
					{
						pushsum(numnodes,topology,algorithm)            
					}
					else
						println("algorithm can only be gossip or push-sum")     
				}
				else
					println("topology can only be 3d, imperfect3d, full or line")
			}
			else
				println("numnodes should be integer and greater than zero")

		}
	}
}