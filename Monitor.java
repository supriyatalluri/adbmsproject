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
	Integer temp;
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

			os.println("Enter 1 if you want to initiate a transaction 0 otherwise : ");
			a = Integer.parseInt(is.readLine());
			if(a == 1)
			{
				os.println("Select a transaction to intiate \n1.Dataitems = [2,3,5,7,9]  ADD 2\n2.Dataitems = [0,1,4,9]  ADD 5\n3.Dataitems = [2,4,9] ADD 7\n4.Dataitems = [0,1,3] ADD 5");
				int tno = Integer.parseInt(is.readLine());
				String tid = siteIdentity;
				int created = 0;
				if(tno == 1)
				{
					ArrayList<Integer> dobj = new ArrayList<Integer>();
					dobj.add(2);
					dobj.add(3);
					dobj.add(5);
					dobj.add(7);
					dobj.add(9);
					String f = "ADD";
					int num = 2;
					Transactions t1 = new Transactions(siteIdentity , tid , dobj , f ,num);
					created = 1;

					((ser.mtm).waiting).add(t1);
					System.out.println("\nReceived transaction from site " + siteIdentity);
					System.out.println("Transaction Details :  " + t1.dataObjects + "  Timestamp : " + t1.timeStamp + "  Operation : " + t1.fun + "  " + t1.number );

				}
				if(tno == 2)
				{
					ArrayList<Integer> dobj = new ArrayList<Integer>();
					dobj.add(0);
					dobj.add(1);
					dobj.add(4);
					dobj.add(9);
					String f = "ADD";
					int num = 5;
					Transactions t1 = new Transactions(siteIdentity , tid , dobj , f ,num);
					created = 1;

					((ser.mtm).waiting).add(t1);
					System.out.println("\nReceived transaction from site " + siteIdentity);
					System.out.println("Transaction Details :  " + t1.dataObjects + "  Timestamp : " + t1.timeStamp + "  Operation : " + t1.fun + "  " + t1.number );

				}
				if(tno == 3)
				{
					ArrayList<Integer> dobj = new ArrayList<Integer>();
					dobj.add(2);
					dobj.add(4);
					dobj.add(9);
					String f = "ADD";
					int num = 7;
					Transactions t1 = new Transactions(siteIdentity , tid , dobj , f ,num);
					created = 1;

					((ser.mtm).waiting).add(t1);
					System.out.println("\nReceived transaction from site " + siteIdentity);
					System.out.println("Transaction Details :  " + t1.dataObjects + "  Timestamp : " + t1.timeStamp + "  Operation : " + t1.fun + "  " + t1.number );

				}
				if(tno == 4)
				{
					ArrayList<Integer> dobj = new ArrayList<Integer>();
					dobj.add(0);
					dobj.add(1);
					dobj.add(3);
					String f = "ADD";
					int num = 5;
					Transactions t1 = new Transactions(siteIdentity , tid , dobj , f ,num);
					created = 1;

					((ser.mtm).waiting).add(t1);
					System.out.println("\nReceived transaction from site " + siteIdentity);
					System.out.println("Transaction Details :  " + t1.dataObjects + "  Timestamp : " + t1.timeStamp + "  Operation : " + t1.fun + "  " + t1.number );
				}

				if(created == 0)
				{
					os.println("Error in creating Transaction.");
				}

				checkinput = "done";
			}
			else
			{
				checkinput = "done";
			}
			a= 1;
			while(a==1)
			{
				a = 0;
				for(int i=0; i<(ser.t).size(); i++)
				{
					if((((ser.t).get(i)).checkinput).equalsIgnoreCase("null"))
					{
						a = 1;
					}
				}
			}

			if((ser.t).indexOf(this) == 0)
			{
				System.out.println("Received Transaction details from all sites. The following are the active transactions.");
				System.out.println((ser.mtm).waiting);
				System.out.println("Executed Transactions Metadata will be sent to the sites for updates.");
				for(int j=0 ; j<((ser.mtm).waiting).size(); j++)
				{
					Transactions temp = ((ser.mtm).waiting).get(j);
					System.out.println("\nExcuting Transaction id: " + temp.tid + " dataObjects :" + temp.dataObjects  + "  " + temp.fun + "  " + temp.number);
					ArrayList<tableEntry> tempTE = (ser.mtm).execute(temp);
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
				Thread.sleep(5000);
			}
			catch (Exception e)
			{
				System.out.println(e);
			}
			os.println("Received metadata from Central Site");
			os.println(tm.listSiteTable);
			tm.algorithm(is , os);
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