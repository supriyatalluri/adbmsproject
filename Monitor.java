package project;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Timer;
import java.util.TimerTask;
// import transaction.*;

public class Monitor
{
	boolean closed=false;
	boolean inputFromAll=false;
	ArrayList<ArrayList<String>> votes;
	ArrayList<String> vote_t;
	ArrayList<String> decision;
	ArrayList<SiteThread> t;
	// List<String> votes;
	ArrayList<String> names;
	// List<Transactions> activeTranscations;
	//List of Transaction managers for the database sites
	ArrayList<TransactionManager> stm;
	//Transaction manager for the central site
	TransactionManager mtm;
	// String reqSent;
	Scanner scan = new Scanner(System.in);

	public Monitor()
	{
		t = new ArrayList<SiteThread>();
		votes = new ArrayList<ArrayList<String>>();
		vote_t = new ArrayList<String>();
		decision = new ArrayList<String>();
		// votes = new ArrayList<String>();
		names = new ArrayList<String>();
		// activeTranscations = new ArrayList<Transactions>();
		stm = new ArrayList<TransactionManager>();
		mtm = new TransactionManager("Monitor");
		// reqSent = "no";
	}

	public static void main(String args[])
	{
		Socket SiteSocket = null;
		ServerSocket MonitorSocket = null;
		int port_number=1111;
		Monitor ser=new Monitor();
		Scanner s = new Scanner(System.in);		
		try
		{
			MonitorSocket = new ServerSocket(port_number);
		}
		catch (IOException e)
		{
			System.out.println("Exception : " + e);
		}

		while(!ser.closed)
		{
			try
			{
				SiteSocket = MonitorSocket.accept();
				SiteThread th = new SiteThread(ser,SiteSocket);
				(ser.t).add(th);
				if((ser.t).size() == 1)
				{
					System.out.println("_____CENTRAL SITE__________");	
					System.out.println("For every site, initializing a random completely replicated database.");
				}
				System.out.println("\nNow Total sites are : "+(ser.t).size());
				(ser.vote_t).add("null");
				(ser.decision).add("null");		
				// (ser.names).add("null");
				th.start();
			}
			catch (IOException e){}
		}

		try
		{
			MonitorSocket.close();
		}
		catch(Exception e1){}
	}
}
