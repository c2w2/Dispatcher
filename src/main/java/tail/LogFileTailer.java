package tail;

import java.io.*;

import kafka.javaapi.producer.Producer; 
import kafka.producer.KeyedMessage; 
import kafka.producer.ProducerConfig; 

import java.util.*;



public class LogFileTailer
{
  
  private long sampleInterval = 5000;

  private StringTokenizer st;
  
  private KeyedMessage<String, String> message ;
  
  private int num_topic;

  List<String> ip = new ArrayList<String>();
 
  
 
  private File logfile;

 
  private boolean startAtBeginning = false;


  private boolean tailing = false;

 
  private Set listeners = new HashSet();


  public LogFileTailer( File file )
  {
    this.logfile = file;
    
    
  }


  public LogFileTailer( File file, long sampleInterval, boolean startAtBeginning , String num_topic)
  {
    this.logfile = file;
    this.sampleInterval = sampleInterval;
    this.num_topic = Integer.parseInt(num_topic);
  }

  public void addLogFileTailerListener( LogFileTailerListener l )
  {
    this.listeners.add( l );
  }

  public void removeLogFileTailerListener( LogFileTailerListener l )
  {
    this.listeners.remove( l );
  }

  protected void fireNewLogFileLine( String line )
  {
    for( Iterator i=this.listeners.iterator(); i.hasNext(); )
    {
      LogFileTailerListener l = ( LogFileTailerListener )i.next();
      l.newLogFileLine( line );
    }
  }

  public void stopTailing()
  {
    this.tailing = false;
  }
  
  

  public void run_1()
  {
    
    long filePointer = 0;

   
    if( this.startAtBeginning )
    {
      filePointer = 0;
    }
    else
    {
      filePointer = this.logfile.length();
    }

    try
    {
      
      this.tailing = true;
      RandomAccessFile file = new RandomAccessFile( logfile, "r" );
      boolean k=true;
    		Properties props = new Properties(); 
    		props.put("metadata.broker.list", "kafka1:9092,kafka2:9092,kafka3:9092"); 
    	    props.put("serializer.class", "kafka.serializer.StringEncoder"); 
    	    ProducerConfig  producerConfig = new ProducerConfig(props); 
    	    Producer<String, String> producer = new Producer<String, String>(producerConfig); 
     String[] topic_list = new String[num_topic];
     
     for(int i=0; i<num_topic; i++)
     {
    	 topic_list[i] = "tail" + Integer.toString(i+1);
     }
     
     
      while( this.tailing )
      {
    
    		    		
        try
        {  
         
          long fileLength = this.logfile.length();
          if( fileLength < filePointer ) 
          {
           
            file = new RandomAccessFile( logfile, "r" );
            filePointer = 0;
          }
          
          if( fileLength > filePointer ) 
          {
            
            file.seek( filePointer );
            String line = file.readLine();
            while( line != null )
            {
           System.out.println(line);
            	
           st=new StringTokenizer(line);
           String tmp = st.nextToken();
           boolean tt=true;
           for(int i=0; i<ip.size(); i++)
           {
        	   if(tmp==ip.get(i))
        	   {
    			   KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic_list[i%num_topic], "a"+line);   
    			   tt=false;
        	   }
           }
          
           if(tt){
        	   ip.add(tmp);
        	 
   			   KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic_list[ip.size()%num_topic], "a"+line);   
           			
        	  }
           
           		producer.send(message);
              this.fireNewLogFileLine( line );
              line = file.readLine();
            }
            filePointer = file.getFilePointer();
          }

          
        }
        catch( Exception e )
        {
        }
      }

      file.close();
    }
    catch( Exception e )
    {
      e.printStackTrace();
    }
  }
}
