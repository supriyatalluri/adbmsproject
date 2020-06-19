package project;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Timer;
import java.util.TimerTask;

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
	int Transaction_pointer = -1;
	int ptp = -1;
	int indexOfthis = -1;

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
			indexOfthis = (ser.t).indexOf(this);
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

			if((ser.t).indexOf(this) == 0)
			{
				System.out.println("__________Received Transactions from Sites__________");
			}

			while(check == 1)
			{
				os.println("_______________GENERATE TransactionS_____________");
				os.println("Enter 1 if you want to initiate a Transaction( any number of Transactions) 0 otherwise : ");
				check = Integer.parseInt(is.readLine());
				if(check == 1)
				{
					os.println("Initiating Transaction " + siteIdentity + String.valueOf(tno) );
					os.println("Enter the dataItem to perform Transaction on (0-9) : ");
					int dobj = Integer.parseInt(is.readLine());
					os.println("Assuming ADD operation , how much value is to be added : ");
					int num = Integer.parseInt(is.readLine());
					String tid = siteIdentity + String.valueOf(tno);
					tno = tno + 1;
					Transactions t1 = new Transactions(siteIdentity , tid , dobj , "ADD" , num , (ser.t).size() );
					((ser.mtm).waiting).add(t1);
					System.out.println("\nReceived Transaction from site " + siteIdentity);
					System.out.println("Pointer : " + t1 + "  Transaction Details :  " + tid + "   dataItem  : " + t1.dataItem + "  Timestamp : " + t1.timeStamp + "  Operation : " + t1.fun + "  " + t1.number );
					ArrayList<String> t12 = new ArrayList<String>();
					for(int i=0; i<(ser.t).size(); i++)
					{
						t12.add("null");
					}

					(ser.votes).add(t12);
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
				System.out.println("___________________EXECUTING TransactionS_____________");
				System.out.println("Received Transaction details from all sites. The following are the active Transactions.");
				System.out.println((ser.mtm).waiting);
				System.out.println("Executed Transactions Metadata will be sent to the sites for updates.");
				for(int j=0 ; j<((ser.mtm).waiting).size(); j++)
				{
					Transactions temp = ((ser.mtm).waiting).get(j);
					System.out.println("\nExcuting Transaction id: " + temp.tid + " dataObjects :" + temp.dataItem  + "  " + temp.fun + "  " + temp.number);
					tableEntry tempTE = (ser.mtm).execute(temp);
					System.out.println("Result dataItem : " + (ser.mtm).dataBase);
					((ser.mtm).completed).add(temp);
					for(int i=0; i<(ser.t).size(); i++)
					{
						//Sending metadata to each site after executing the Transaction.
						((((ser.t).get(i)).tm).listSiteTable).add(tempTE);
						((ser.mtm).listSiteTable).add(tempTE);
						((((ser.t).get(i)).tm).waiting).add(temp);
					}
					System.out.println("Generated metadata for the above Transactions.");
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

			int temptno = -1;

			while((tm.waiting).size()>0)
			{
				temptno = temptno + 1;
				if((ser.t).indexOf(this) == 0)
				{
					System.out.println("------------------------------");
					System.out.println("Transaction received : " + (tm.waiting).get(0));
				}

				System.out.println("Enter SEND to send the metadata of next Transaction to " + siteIdentity );
				String dec;
				System.out.println("Your decision : ");
				String decision = scan.nextLine();

				os.println("Request for Transaction " + (tm.waiting).get(0).dataItem +  "  " + (tm.waiting).get(0).fun +  "  " + (tm.waiting).get(0).number+ " received");
				os.println("Enter ACCEPT if the resource is free and you want to execute the Transaction. REJECT otherwise :");
				String v = is.readLine();

				if(v.equalsIgnoreCase("ACCEPT"))
				{
					((ser.votes).get(temptno)).set(indexOfthis , "ACCEPT");
				}
				else
				{
					((ser.votes).get(temptno)).set(indexOfthis , "REJECT");
				}
				// os.println(ser.votes);
				a= 1;
				while(a==1)
				{
					a = 0;
					for(int i=0; i<((ser.votes).get(temptno)).size(); i++)
					{
						if(((((ser.votes).get(temptno)).get(i))).equalsIgnoreCase("null"))
						{
							a = 1;
						}
					}
				}

				if(indexOfthis == 0)
				{
					System.out.println("Received votes from all sites..");
				}
				int accept = 0;
				int reject = 0;
				// os.println(ser.votes);os.
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
				// os.println((ser.votes).get(temptno));
				for(int i=0; i<((ser.votes).get(temptno)).size(); i++)
				{
					if(((((ser.votes).get(temptno)).get(i))).equalsIgnoreCase("ACCEPT"))
					{
						accept = accept + 1;
					}
					else
					{
						reject = reject + 1;
					}
				}

				if(siteIdentity.equalsIgnoreCase("A"))
				{
					System.out.println(accept + " Sites accepted the Request. " + reject + " sites rejected");
				}

				if(accept > reject && v.equalsIgnoreCase("ACCEPT"))
				{
					os.println("Majority sites accepted the request. Updating the database..");
					tableEntry temp3 = (tm.listSiteTable).get(0);
					int feedback = tm.updateDatabase(temp3);
					if(feedback == 0)
					{
						os.println(tm.listTimestamp);
						os.println(((tm.waiting).get(0)).timeStamp +  " " + ((tm.waiting).get(0)).dataItem);
						os.println("This is a older timestamp.Database is already updated , rejecting the request");
					}
					else
					{
						os.println("Database is now updated. " + tm.dataBase);
					}
					(tm.listSiteTable).remove(0);
					(tm.waiting).remove(0);
				}

				if(accept > reject && v.equalsIgnoreCase("REJECT"))
				{
					os.println("Transaction is accepted by majority of sites. Request will be done in regular intervals.");
					tableEntry temp1 = (tm.listSiteTable).get(0);
					(tm.rejected).add(temp1);
					(tm.listSiteTable).remove(0);
					(tm.rejectedTransactions).add((tm.waiting).get(0));
					(tm.waiting).remove(0);
				}

				if(reject >= accept )
				{
					os.println("Transaction is rejected by majority of sites. Request will be done again in regular intervals");
					tableEntry temp2 = (tm.listSiteTable).get(0);
					(tm.rejected).add(temp2);
					(tm.rejectedTransactions).add((tm.waiting).get(0));
					(tm.listSiteTable).remove(0);
					(tm.waiting).remove(0);
				}

				accept = 0;
				reject = 0;

			}


			while((tm.rejected).size()>0)
			{
				os.println("--------------------------");
				os.println("No of Transactions left : " + (tm.rejected).size());
				tableEntry temp = (tm.rejected).get(0);
				Transactions temptr = (tm.rejectedTransactions).get(0);
				System.out.println("Sending repeat request to site " + siteIdentity + " for Transaction " + temptr.tid +  " " + temp.dataItem +  "  " + temptr.fun +  "  " + temptr.number  );
				os.println("Second Request for Transaction " + temptr.tid +  " " + temp.dataItem +  "  " + temptr.fun +  "  " + temptr.number+ " received");
				os.print("Checking if it's an older timestamp");
				int isOld = tm.isOlderTransaction(temp);
				if(isOld == 0)
				{
					os.println("This is a older timestamp.Database is already updated , rejecting the request");
					(tm.rejected).remove(0);
					(tm.rejectedTransactions).remove(0);
					continue;
				}
				os.println(" --- It's not a older Transaction.");
				os.println("Enter ACCEPT if the resource is free and you want to execute the Transaction. REJECT otherwise :");
				String v = is.readLine();
				if(v.equalsIgnoreCase("ACCEPT"))
				{	
					os.println("Updating the database..");
					int feedback = tm.updateDatabase(temp);
					if(feedback == 0)
					{
						os.println("This is a older timestamp.Database is already updated , rejecting the request");
					}
					else
					{
						os.println("Database is now updated. " + tm.dataBase);
					}
					(tm.rejected).remove(0);
					(tm.rejectedTransactions).remove(0);
				}
				else
				{
					(tm.rejected).add(temp);
					(tm.rejectedTransactions).add(temptr);
					(tm.rejected).remove(0);
					(tm.rejectedTransactions).remove(0);
					os.println("Request rejected. Will be requested in regular intervals.");
				}

			}

			ser.closed=true;
			SiteSocket.close();
		}
		catch(IOException e){};	
	}
}