package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Inbox;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.util.logging.*;
import java.io.IOException;

public class VirtualSynchrony 
{
  //File Logger 
  public static Logger logger = Logger.getLogger(VirtualSynchrony.class.getName());

  //List containing Message to deliver
  static List<Message> messagesList;  
  
  //Inhibits the access to the critical section
  static boolean avoidSends;
  
  //List containing All sent messages 
  static List<Message> messagesGlobal;

  //Counter for the delivered messages
  static int delivered = 0;  

  //List to Trace ID of nodes that delivered messages 
  static List<Integer> traceId;

  //Counter messages
  static  int numberMessage = 0;

  //List of crashed nodes
  static List<CrashedNodes> crashedNodes;

  //Count the arrived messages
  static int messageFinish;

  //If 1, message duplicated, otherwise not
  static boolean duplicated;

  //It contains the id number of nodes and its related number of delivered messages
  private static Map<Integer, Integer> countDelivered = new HashMap<>();

  //Counter of multicast
  static int numMulti = 0;

  //Request to join 
  public static class JoinMessage implements Serializable { }

  //When a node crashes
  public static class CrashedNodes implements Serializable 
    {

     final ActorRef crashNode;
     final boolean crashed;

     CrashedNodes(ActorRef crashNode, boolean crashed) 
     {
       this.crashNode = crashNode;
       this.crashed = crashed;
     }	
    }
  
  // Set a delay when a crash happens
  public static class Crash implements Serializable 
    {
        final int delay;
        Crash(int delay) 
		{
            this.delay = delay;
        }
    }

  // Messages class
  public static class Message implements Serializable 
    {
        final int numMessage;
        final ActorRef last;

     	Message(int numMessage, ActorRef last) 
		{
		  this.numMessage = numMessage;
		  this.last = last; 
        }
    }

   /*-- Common functionality for both Coordinator and Participants ------------*/
   public abstract static class Node extends AbstractActor 
 { 
   //Node ID
   int id = 0;
  
   private Random rnd = new Random();

   //This list contains views to be updated   
   private List<ViewUpdate> buf = new ArrayList<>();
  
   //Crashed?
   boolean crashed = false; 
  
   //Current View
   View view;

   public static class View implements Serializable 
    {
     //Actor of the view
     final List<ActorRef> group;        
   
     //List to print the nodes inside the view
     final List<String> participant_list ;    
   
     //It represents the number of Sequence View
     final int viewSeq;          

     View(List<ActorRef> group, List<String> participant_list, int viewSeq) 
	 {
        this.group = group;
        this.participant_list = participant_list;
        this.viewSeq = viewSeq;
     }
    }
 
    //This class implements the view update 
    public static class ViewUpdate implements Serializable 
	{
       	final View view;

        ViewUpdate(View view) 
		{
            this.view = view;
        }
    }

    //First Message
    public static class StartChatMsg implements Serializable { }

    //Init the participant node
    static class GiveId implements Serializable 
	{
       final int idNew;
	   final View view;
	   GiveId(int idNew,View view) 
	    {
          this.idNew = idNew;
	      this.view = view;
        }
    }
 
    //Print id node
    void print(String s) 
	{
      System.out.format("%2d: %s\n", id, s);
    }

    //What to do when a start message is created
    public void onStartChatMsg(StartChatMsg msg) 
	{
    	getContext().system().scheduler().schedule(
        Duration.create((int) ( 5000  * Math.random()) , TimeUnit.MILLISECONDS),
        Duration.create((int) ( 5000  * Math.random()) + 1000, TimeUnit.MILLISECONDS),
        this::sendMessage,
        getContext().system().dispatcher());

    }
  
    private void sendMessage()  
	{
    if (crashed || view == null || (avoidSends == true)) 
	    {
            return;
        }
    avoidSends = true;
	//Number of recipient nodes
    messageFinish = this.view.group.size() - 1;
  
    //If the number of the messages delivered is equal to the recipient nodes the message is stable, otherwise the message is unstable
    if (messageFinish == countDelivered.get(id) || countDelivered.get(id) ==  0)  
	{
		Message m = new Message(numberMessage, getSelf()); 
	    
		numberMessage = numberMessage + 1;	  
	
       	        delivered = 0 ;  
	         
		print("send multicast " + numMulti + " within " +this.view.viewSeq);
	  
	    VirtualSynchrony.logger.info(id + ": send multicast " + numMulti + " within " +this.view.viewSeq);
                  
        countDelivered.replace(id,delivered);
                  
		for (Message p : messagesGlobal)	
		{
            if  (m.numMessage == p.numMessage)  
			    {
                  duplicated =true;
                } 
        }

	 	multicast(this.view,m);
                
	} 
	else 
	{
        Message m = new Message(numberMessage, getSelf()); 
	      	
        numberMessage = numberMessage + 1;
		 
		delivered = 0 ;  

		print("send unstable multicast " + numMulti +" within view " +this.view.viewSeq);
	          
		VirtualSynchrony.logger.info(id + ": send multicast " + numMulti + " within " +this.view.viewSeq);
		
        countDelivered.replace(id,delivered);
	      
		multicast(this.view,m);
	 
	}  
  
    }
   
    //When a node receives a message 	
    public void onMessage(Message m) 
	{
        if (crashed) 
		{
        
		  messageFinish = messageFinish - 1 ;

          //When all the messages are arrived, we deliver them	
	      if (messageFinish == 0) 
		  {
		
		    deliver(m);
        
       	  }
	      	  
		  return;
        }
        
        print("ARRIVE MULTICAST "+ numMulti + " FROM " + getSender().path().name().substring(11) + " within " + this.view.viewSeq );
  
        messageFinish = messageFinish - 1 ;
      
	    //Add id of the recipient node to Ids list
        traceId.add(id);
        
	    //Add messages to the messages list 
        messagesList.add(m);
        
	    messagesGlobal.add(m);
   
	    //When all the messages are arrived, we deliver them	
	
	    if (messageFinish == 0) 
		{
		  deliver(m);
        }
    	 
    
    }

    //When all the messages are arrived, we deliver them
    public void deliver(Message m) 
	{   
        int messageSize = messagesList.size();
  	 
	    for (Message p : messagesList) 
		{	            
            //if the message is not duplicated we deliver it, otherwise not   
            if(duplicated == false) 
			{
		       System.out.println(" " + traceId.get(messageSize - 1)+": " + "DELIVER MULTICAST "+ numMulti + " FROM " + getSender().path().name().substring(11) + " within " + this.view.viewSeq  );
      
		       VirtualSynchrony.logger.info(" " + traceId.get(messageSize - 1)+": " + "deliver multicast "+ numMulti+ " from " + getSender().path().name().substring(11) + " within " + this.view.viewSeq)  ;
	      
		       messageSize = messageSize - 1;   
	 
	           delivered = delivered + 1;		 
		    }  
			else 
			{
		       System.out.println(" " + traceId.get(messageSize - 1)+": " + "NOT DELIVER MULTICAST "+ numMulti+ " FROM " + getSender().path().name().substring(11) + " within " + this.view.viewSeq  );

	           messageSize = messageSize - 1;   
		    }
	 
	    }

        //we reset this variable in case we deliver duplicated messages
	    duplicated = false ; 
        
		//increment the multicast counter
        numMulti++;
        
		//clean the messages list for the next delivering
        if(messagesList != null)
		{
	       messagesList.clear(); 
	    }
	    if(traceId != null) 
		{
           traceId.clear(); 
        }
         
  	    int id_togive_new = Integer.parseInt(getSender().path().name().substring(11));
        
	    //record the number of the delivered messages of a node 
	    countDelivered.replace(id_togive_new,delivered);
            
	    avoidSends = false; 

	}	


    void multicast(View view , Serializable m) 
	{
  
      List<ActorRef> shuffledGroup = new ArrayList<>(view.group); //in the given view
        
      Collections.shuffle(shuffledGroup);
 	  for (ActorRef p : shuffledGroup) 
	  {
        if (!p.equals(getSelf())) // not sending to self
		{ 
           p.tell(m, getSelf());
		   try 
		   {
              Thread.sleep(rnd.nextInt(10));
           } 
		   catch (InterruptedException e) 
		   {
               e.printStackTrace();
           }
        }
      }
     
    }

    //When the view is updated
    void onViewUpdate(ViewUpdate v) 
	{
        if (crashed) 
		{
            return;
        }
        
	//init the buffer with new view 
       	this.buf.add(v);

        //assign a value for group size
       	int size_group;
        
	//init the support list to trace next view
       	List<ViewUpdate> nextView = new ArrayList<>();
        
	//init the support list for actors
        ArrayList<ActorRef> app = new ArrayList<>(this.buf.get(0).view.group);
        
        //for each view in the initial buffer we remove from the support list "app" the different actors inside the group 
	    for (ViewUpdate f : this.buf) 
		{ 
		  app.retainAll(f.view.group); 
        }  
       	
	//mantains only the same elements comparing app and view.group removes elements from app
        size_group = app.size();
        
		//for each view in buffer we check the sequence of view and then we add it to the list of view
	    for (ViewUpdate f : this.buf) 
		{
            if (f.view.viewSeq == this.view.viewSeq + 1) 
			{
                nextView.add(f);
            }
        }

        if (nextView.size() == size_group - 1) 
		{
            //here we install the new view 
            this.view = nextView.get(0).view;
            
	    //remove the useless view
	        this.buf.removeAll(nextView);
	    
	        print("install View " + this.view.viewSeq + ": " + this.view.participant_list.toString());
           
	        VirtualSynchrony.logger.info(id + ": install View " + this.view.viewSeq + ": " + this.view.participant_list.toString());
           
            if(messagesList != null) 
			{
              messagesList.clear();
            }
	   
	     delivered  = 0 ;
	   
	    //when the view is updated we reset the value of the delivered messages
            countDelivered.replaceAll((key, oldValue) -> oldValue = 0);
  
	    }

    }
 }
  
 /*-- Coordinator -----------------------------------------------------------*/
 public static class Coordinator extends Node 
 {
   //It represents the id to be assigned to the node asking for joining 
   int id_togive = 0 ;
   
   //Coordinator View
   View overall;

   //Constructor
   public Coordinator(int id) 
   { 
     this.id = id ; 
	 
	 //new group to init the view
     List<ActorRef> group = new ArrayList<>();
     
	 //Add to the view the node coordinator
	 group.add(getSelf());
	 
	 //Init the list of actor name
     List<String> participant_list = new ArrayList<>(0);
     
	 participant_list.add(getSelf().path().name().substring(11));
     
	 //Create the init view
	 this.view = new View(group, participant_list, 0);
     
	 //Add to the list of crashed nodes
     CrashedNodes n = new CrashedNodes(getSelf(),false);

   	 crashedNodes.add(n);
     
	 //Init list of delivered messages 
	 countDelivered.put(0,0); 
    
   } 

   static public Props props(int id) 
   {
     return Props.create(Coordinator.class, () -> new Coordinator(id));
   }
  
   @Override
   public Receive createReceive() 
   {
      return receiveBuilder()
           .match(JoinMessage.class, this::onJoinMessage)
           .match(StartChatMsg.class, this::onStartChatMsg)
           .match(Message.class, this::onMessage)
           .match(ViewUpdate.class,this::onViewUpdate)
	   .build();

   }
 
   @Override
   public void preStart() 
   {
      getContext().system().scheduler().schedule(
                Duration.create(5, TimeUnit.SECONDS),
                Duration.create(10, TimeUnit.SECONDS),
                this::crashDetector,
                getContext().system().dispatcher());
   }
 
   //When a crash happens
   private void crashDetector() 
    {
	   //It represents the position into the list of the crashed nodes
	   int numCrashNode = 0; 
	   
	   for(CrashedNodes d : crashedNodes)
	    {
           //if the node is crashed    
		   if(d.crashed == true) 
		   {
           
		   //Remove from the view the crashed node
		   View newView = this.afterCrash(d.crashNode);     
           
		   //Restore to false the flag indicating if a node is crashed or not
		   CrashedNodes cn = new CrashedNodes(d.crashNode,false);

		   crashedNodes.set(numCrashNode,cn);
                       
		   //new multicast and set new view
		   multicast(newView, newView);
              
		   multicast(newView,new ViewUpdate(newView));
           }	
            
	       numCrashNode = numCrashNode + 1;
	    }
    }

    //Set the view after a crash happens
    private View afterCrash(ActorRef cra)
	{

        if (this.overall == null) 
		{
            this.overall = new View(this.view.group, this.view.participant_list, this.view.viewSeq);
        }

        //Remove the actor from the view
        List<ActorRef> actors = new ArrayList<>(this.overall.group);
        
	    actors.remove(cra);
        
	    //Remove the name of the actor from the view
	    List<String> names = new ArrayList<>(this.overall.participant_list);
        
        names.remove(cra.path().name().substring(11));
        
	    //Update the view sequence number
	    int i = this.overall.viewSeq + 1;
        
	    View v = new View(actors, names, i);

        this.overall = v;

        return v;
    }

    //What to do when we have a request of join
    public void onJoinMessage(JoinMessage j1) 
	{
   
        print("Join request of "+ getSender().path().name());
 
        if (overall == null) 
		{
            overall = new View(this.view.group, this.view.participant_list, this.view.viewSeq);
        } 

        //Takes the Id from substring
        id_togive = Integer.parseInt(getSender().path().name().substring(11));
     
        //Set the parameters of the participant
        GiveId id1 = new GiveId(id_togive,overall);
     
        getSender().tell(id1,getSelf());
     
        //Init the list of delivered messages 
        countDelivered.put(id_togive,0); 
     
        //Send the first message
        StartChatMsg st1 = new StartChatMsg();
        getSender().tell(st1, getSelf());
     
        //Init the list of crashed nodes to false
        CrashedNodes d1 = new CrashedNodes(getSender(),false);
        crashedNodes.add(d1);
     

        //Update the view with the new participant
        View upd_view = updateView(getSender());
    
        multicast(upd_view,upd_view);
        multicast(upd_view,new ViewUpdate(upd_view))    ;

    }

    //Update the view
    private View updateView(ActorRef r) 
	{
 
       if (this.overall == null) 
	    {
           this.overall = new View(this.view.group, this.view.participant_list, this.view.viewSeq);
        }      
	
        //List with the partecipants belonging to the coordinator's view
	    List<ActorRef> update = new ArrayList<>(this.overall.group);
        update.add(r);
        
	    //list of nodes name
        List<String> name = new ArrayList<>(this.overall.participant_list);
        name.add(r.path().name().substring(11));
        
	    //update the sequence number
        int i = this.overall.viewSeq + 1;
        View p = new View(update, name, i);
        
	    //update the view
        this.overall = p;
        return p;
    }
      
 }

 /*-- Participant -----------------------------------------------------------*/
 public static class Participant extends Node 
 {
    static public Props props() 
	{
        return Props.create(Participant.class, Participant::new);
    }

   @Override
   public Receive createReceive() 
   {
       return receiveBuilder()
       .match(GiveId.class, this::onGiveId)
       .match(Message.class, this::onMessage)
       .match(StartChatMsg.class, this::onStartChatMsg)
       .match(ViewUpdate.class,this::onViewUpdate)
       .match(View.class,this::viewParticipant)
       .match(Crash.class, this::onCrash)               //#p1
       .build();

    } 
   
    //Init the participant view
    void viewParticipant(View v1) 
	{
        if(this.crashed) 
		{
            return;
        }
        
	    
		//Send the view in multicast
	    multicast(v1,new ViewUpdate(v1));
    }
   
    //Init the id of participant
    public void onGiveId(GiveId g1) 
	{
        this.crashed = false;
        this.id = g1.idNew;    
        this.view = g1.view;
    }
   
    //When a crash happens      
    private void onCrash(Crash c) 
	{
        //set the crashed boolean to true
      	this.crashed = true;
        
		//Indicate the position into the list of the crashed node
	    int crashPos = 0;
	
	    for(CrashedNodes d : crashedNodes)
        {
        
	        if(getSelf() == d.crashNode) 
			{
               //Insert the crashed node into the crashed nodes list
		       CrashedNodes cn = new CrashedNodes(getSelf(),true);
  
      		   crashedNodes.set(crashPos,cn);
        	}     
	        crashPos = crashPos + 1;    
	    }
	
	    System.out.println("CRASH!!!" + getSelf().path().name());

        //Try to rejoin after a while
        getContext().system().scheduler().scheduleOnce(
                Duration.create(c.delay, TimeUnit.SECONDS),
                this.view.group.get(0),
                new JoinMessage(),
                getContext().system().dispatcher(), getSelf()
        );

    }
 
 }
  
 //Test join
 static void checkjoin(ActorRef coordinator, ActorSystem system) 
 {
    try {

           Thread.sleep(10000);
	       //uncomment this row to test duplicated message (here we force the message counter to a number that is surely already been sent)
	       //numberMessage = 0;
           ActorRef participant3 = system.actorOf(Participant.props(),"participant3");
           JoinMessage j3 = new JoinMessage();
	       coordinator.tell(j3, participant3);
        } 
   catch (Exception ioe) 
   {
   }
 }
 
 //Test crash
 static  void checkcrash(ActorRef participant2,ActorRef participant1, ActorSystem system) 
 {
    try 
	{
        Thread.sleep(10000);
        Crash cc = new Crash(20);
	    participant2.tell(cc, null);

        Thread.sleep(30000);
	    Crash ccc = new Crash(20);
	    participant1.tell(ccc, null);

        System.in.read();
	} 
    catch (Exception ioe) 
	{
    }
 } 

 /*-- Main ------------------------------------------------------------------*/
 public static void main(String[] args) 
 {
    //Init Lists
    messagesList = new ArrayList<>();
    traceId = new ArrayList<>();
    crashedNodes = new ArrayList<>();
    messagesGlobal = new ArrayList<>();
 
    try 
	{
            FileHandler fh;
            Handler handlerObj = new ConsoleHandler();
            handlerObj.setLevel(Level.ALL);
            logger.setLevel(Level.ALL);
            logger.setUseParentHandlers(false);

            // This block configure the logger with handler and formatter
            fh = new FileHandler("project.log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            logger.info("Start Log");

    } 
	catch (SecurityException e) 
	{
            e.printStackTrace();
    } 
	catch (IOException e) 
	{
            e.printStackTrace();
    }

    // Create the actor system 
    final ActorSystem system = ActorSystem.create("helloakka");

    // Create the coordinator
    ActorRef coordinator = system.actorOf(Coordinator.props(0), "coordinator0");
    
	// Create participants
    ActorRef participant1 = system.actorOf(Participant.props(), "participant1");
    ActorRef participant2 = system.actorOf(Participant.props(), "participant2");
   
    //Join Participants
    JoinMessage j1 =  new JoinMessage();
    coordinator.tell(j1, participant1 );
    JoinMessage j2 =  new JoinMessage();
    coordinator.tell(j2, participant2 );
     
    //Coordinator starts sending messages
    coordinator.tell(new Node.StartChatMsg(), coordinator);
   
    //Function to check join and crash functionality (to test remove/add comments)
    //checkjoin(coordinator,system); 
    //checkcrash(participant2,participant1,system); 
 }
}
