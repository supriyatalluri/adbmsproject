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

	Monitor()
	{
		t = new ArrayList<SiteThread>();
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
				// (ser.votes).add("null");		
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

class SiteThread extends Thread
{
	DataInputStream is = null;
	String line;
	String name = "null";
	PrintStream os = null;
	Socket SiteSocket = null;
	String siteIdentity;
	Monitor ser;
	String checkinput = "null";
	String vote;
	String choice;
	Integer check = 1;
	TransactionManager tm;

	public SiteThread(Monitor ser,Socket SiteSocket)
	{
		this.SiteSocket = SiteSocket;
		this.ser=ser;	

	}

	public void run()
	{
		try
		{
			Scanner scan = new Scanner(System.in);
			int casenumber = 0;
			is = new DataInputStream(SiteSocket.getInputStream());
			os = new PrintStream(SiteSocket.getOutputStream());

			os.println("___________________CONCURRENCY CONTROLLED DISTRIBUTED DATABASE SYSTEM__________________");
			os.println("Enter your database site ID ");
			name = is.readLine();
			siteIdentity = name;
			(ser.names).add(siteIdentity);
			tm = new TransactionManager(siteIdentity);
			(ser.stm).add(tm);
			os.println("Found the Database site." );
			os.println("Data in the site is : " + tm.dataBase);

			os.println("Waiting for other sites to fill their information");
			int a = 1;
			while(a==1)
			{
				a = 0;
				for(int i=0; i<(ser.t).size(); i++)
				{
					if((((ser.t).get(i)).name).equalsIgnoreCase("null"))
					{
						a = 1;
					}
				}
			}

			check = 1;
			int tno = 1;
			while(check == 1)
			{
				os.println("Enter 1 if you want to initiate a transaction( any number of transactions) 0 otherwise : ");
				check = Integer.parseInt(is.readLine());
				if(check == 1)
				{
					os.println("Initiating transaction " + siteIdentity + String.valueOf(tno) );
					os.println("Enter the dataItem to perform transaction on (0-9) : ");
					int dobj = Integer.parseInt(is.readLine());
					os.println("Assuming ADD operation , how much value is to be added : ");
					int num = Integer.parseInt(is.readLine());
					String tid = siteIdentity + String.valueOf(tno);
					tno = tno + 1;
					Transactions t1 = new Transactions(siteIdentity , tid , dobj , "ADD" ,num);
					((ser.mtm).waiting).add(t1);
					System.out.println("\nReceived transaction from site " + siteIdentity);
					System.out.println("Transaction Details :  " + tid + "dataItem" + t1.dataItem + "  Timestamp : " + t1.timeStamp + "  Operation : " + t1.fun + "  " + t1.number );
				}
				if(check == 0)
				{
					break;
				}
			}
			a= 1;
			while(a==1)
			{
				a = 0;
				for(int i=0; i<(ser.t).size(); i++)
				{
					if((((ser.t).get(i)).check) == 1)
					{
						a = 1;
					}
				}
			}
			try
			{
				Thread.sleep(5000);
			}
			catch(Exception e)
			{
				System.out.println(e);
			}

			if((ser.t).indexOf(this) == 0)
			{
				System.out.println("Received Transaction details from all sites. The following are the active transactions.");
				System.out.println((ser.mtm).waiting);
				System.out.println("Executed Transactions Metadata will be sent to the sites for updates.");
				for(int j=0 ; j<((ser.mtm).waiting).size(); j++)
				{
					Transactions temp = ((ser.mtm).waiting).get(j);
					System.out.println("\nExcuting Transaction id: " + temp.tid + " dataObjects :" + temp.dataItem  + "  " + temp.fun + "  " + temp.number);
					tableEntry tempTE = (ser.mtm).execute(temp);
					System.out.println("Result database : " + (ser.mtm).dataBase);
					((ser.mtm).completed).add(temp);
					for(int i=0; i<(ser.t).size(); i++)
					{
						//Sending metadata to each site after executing the transaction.
						((((ser.t).get(i)).tm).listSiteTable).add(tempTE);
						((((ser.t).get(i)).tm).waiting).add(temp);
					}
					System.out.println("Sent metadata for the above transaction.");
				}
			}
			try
			{
				if((ser.t).indexOf(this) != 0)
				{
					Thread.sleep(5000);
				}
			}
			catch (Exception e)
			{
				System.out.println(e);
			}
			os.println("Received metadata from Central Site");
			os.println(tm.listSiteTable);
			// tm.algorithm(is , os);
			// temp = (tm.listSiteTable).size();
			// int i = 0;
			// int last_index = 0;
			// ArrayList<Integer> dobj_list = new ArrayList<Integer> ();
			// String tid_str = "null";
			// while(i<temp)
			// {
			// 	os.println("Central Site requesting lock for the following data items : " + ((tm.waiting).get(i)).dataObjects );
			// 	os.println("Locked items currently : " + tm.getLockedDataItems());
			// 	os.println("Enter ACCEPT to update according to the new transaction data REJECT otherwise ");
			// 	choice = is.readLine();
			// 	if(choice.equalsIgnoreCase("ACCEPT"))
			// 	{
			// 		ArrayList<tableEntry> temp_te = (tm.listSiteTable).get(i);
			// 		for(int j=0 ; j<temp_te.size() ; j++)
			// 		{
			// 			if(temp_te.get(j) != null)
			// 			{
			// 				(tm.siteTable).set(j , temp_te.get(j));
			// 				(tm.lock).set(j , true);
			// 			}
			// 		}
			// 		dobj_list = ((tm.waiting).get(i)).dataObjects ;
			// 		tid_str = ((tm.waiting).get(i)).tid ;
			// 		os.println("DataObjects  " + dobj_list + "are locked by the transaction " + tid_str);

			// 		os.println("Enter UNLOCK to complete updation of database LOCK otherwise");
			// 		choice = is.readLine();
			// 		if(choice.equalsIgnoreCase("UNLOCK"))
			// 		{
			// 			tm.updateDatabase();
			// 			os.println("Updated database and the dataObjects " +  dobj_list + " are unlocked");
			// 			i = i + 1;
			// 		}
			// 		else
			// 		{
			// 			last_index = i;
			// 			i = i + 1;
			// 		}
			// 	}
			// }

			ser.closed=true;
			SiteSocket.close();
		}
		catch(IOException e){};	
	}
}