package tail;



import java.util.*;
import java.io.*;

public class Tail implements LogFileTailerListener
{

private LogFileTailer tailer;


public Tail( String filename , String num_t)
{
 tailer = new LogFileTailer( new File( filename ), 1000, false , num_t);
 tailer.addLogFileTailerListener( this );
 tailer.run_1();
}


public void newLogFileLine(String line)
{
    	
    	
	
}


public static void main( String[] args )
{
 if( args.length < 1 )
 {
   System.out.println( "Usage: Tail <filename>" );
   System.exit( 0 );
 }

     

 Tail tail = new Tail( args[ 0 ] , args[1] );

}
}
