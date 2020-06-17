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
	int transaction_pointer = -1;
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
			if(siteIdentity.equalsIgnoreCase("generator"))
			{
				ser.generatorIndex = indexOfthis;
			}
			tm = new TransactionManager(siteIdentity);
			(ser.stm).add(tm);
			if(siteIdentity.equalsIgnoreCase("generator"))
			{
				os.println("This is Transaction generator site");
				System.out.println("Transaction generator site is created.");
			}
			else
			{
				os.println("Found the Database site." );
				os.println("Data in the site is : " + tm.dataBase);
			}
			os.println("Waiting for other sites to fill their information");
			int a = 1;
			int b = 1;
			while(a==1)
			{
				a = 0;
				for(int i=0; i<(ser.t).size(); i++)
				{
					if((((ser.t).get(i)).name).equalsIgnoreCase("null"))
					{
						a = 1;
					}
					if((((ser.t).get(i)).name).equalsIgnoreCase("generator"))
					{
						b = 0;
					}
				}
			}

			if(b == 1)
			{
				System.out.println("No generator site created.... Exiting....");
				os.println("No generator site created....Exiting...");
				System.exit(0);
			}

			check = 1;
			int tno = 1;
			if(!siteIdentity.equalsIgnoreCase("generator"))
			{
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
						Transactions t1 = new Transactions(siteIdentity , tid , dobj , "ADD" , num , (ser.t).size() );
						((ser.mtm).waiting).add(t1);
						System.out.println("\nReceived transaction from site " + siteIdentity);
						System.out.println("Transaction Details :  " + tid + "dataItem" + t1.dataItem + "  Timestamp : " + t1.timeStamp + "  Operation : " + t1.fun + "  " + t1.number );
					}
					if(check == 0)
					{
						break;
					}
				}
			}
			else
			{
				check = 0;
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
					System.out.println("Result dataItem : " + (ser.mtm).dataBase);
					((ser.mtm).completed).add(temp);
					for(int i=0; i<(ser.t).size(); i++)
					{
						//Sending metadata to each site after executing the transaction.
						((((ser.t).get(i)).tm).listSiteTable).add(tempTE);
						((((ser.t).get(i)).tm).waiting).add(temp);
					}
					System.out.println("Generated metadata for the above transactions.");
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

			if(siteIdentity.equalsIgnoreCase("generator"))
			{
				os.println("Transactions in Queue : "  + tm.waiting );
				os.println("Enter SEND to initiate the next transaction in the other sites");
				String decision;
				int no_of_transaction = (tm.waiting).size();
				int ptr_to_transaction = -1;
				// denoted the latest sent transaction
				while(((tm).waiting).size()>0)
				{
					os.println("Next Transaction : " + (tm.waiting).get(0));
					os.println("Your decision : ");
					decision = is.readLine();
					if(decision.equalsIgnoreCase("SEND"))
					{
						ptr_to_transaction = ptr_to_transaction + 1;
						for(int i=0; i<(ser.t).size(); i++)
						{
							((ser.t).get(i)).transaction_pointer = ptr_to_transaction;
						}

						(tm.waiting).remove(0);
					}
				}
			}
			else
			{
				os.println("ELSEEEE##################");
				while(true)
				{
					os.println(ptp + " " + transaction_pointer);
					if(ptp == transaction_pointer-1)
					{
						ptp = transaction_pointer;
						Transactions temp = (tm.waiting).get(ptp);
						os.println("Request for transaction " + transaction_pointer + " received");
						os.println("Resources required : " + ((tm.waiting).get(transaction_pointer)).dataItem  );
						os.println("Status of Resources : " + tm.lock);
						os.println("Enter ACCEPT if the resource is free and you want to execute the transaction. REJECT otherwise :");
						String v = is.readLine();
						if(v.equalsIgnoreCase("ACCEPT"))
						{
							(ser.vote_t).set(indexOfthis , "ACCEPT");
						}
						else
						{
							(ser.vote_t).set(indexOfthis , "REJECT");
						}

						a= 1;
						while(a==1)
						{
							a = 0;
							(ser.vote_t).set(ser.generatorIndex , "ACCEPT");
							for(int i=0; i<(ser.vote_t).size(); i++)
							{
								if((((ser.vote_t).get(i))).equalsIgnoreCase("null"))
								{
									a = 1;
								}
							}
						}

						if(siteIdentity.equalsIgnoreCase("A"))
						{
							System.out.println("Received votes from all sites..");
						}

						int accept = 0;
						int reject = 0;

						for(int i=0; i<(ser.vote_t).size(); i++)
							{
								if((((ser.vote_t).get(i))).equalsIgnoreCase("ACCEPT"))
								{
									accept = accept + 1;
								}
								else
								{
									reject = reject + 1;
								}
							}


						if(accept > reject && siteIdentity.equalsIgnoreCase("A"))
						{
							System.out.println(accept + " Sites accepted the Request. " + reject + " sites rejected");
						}

						if(accept > reject && v.equalsIgnoreCase("ACCEPT"))
						{
							os.println("Updating the database..");
							tableEntry temp3 = (tm.listSiteTable).get(0);
							int feedback = tm.updateDatabase(temp3);
							if(feedback == 0)
							{
								os.println("This is a older timestamp.Database is already updated , rejecting the request");
							}
							else
							{
								os.println("Database is now updated. " + tm.dataBase);
							}
						}

						if(accept > reject && v.equalsIgnoreCase("REJECT"))
						{
							os.println("Transaction is accepted by majority of sites. Request will be done in regular intervals.");
							tableEntry temp1 = (tm.listSiteTable).get(0);
							(tm.rejected).add(temp1);
						}

						if(reject >= accept &&  siteIdentity.equalsIgnoreCase("A"))
						{
							os.println("Transaction is rejected by majority of sites. Request will be done again in regular intervals");
							tableEntry temp2 = (tm.listSiteTable).get(0);
							(tm.rejected).add(temp2);
						}

						for(int i=0 ; i<(ser.t).size() ; i++)
						{
							(ser.vote_t).set(i,"null");
						}
					}

					while((tm.rejected).size()>0)
					{
						os.println("No of transactions left : " + (tm.rejected).size());
						tableEntry temp = (tm.rejected).get(0);
						os.println("Second request for updation");
						os.println("Resources required : " + temp.dataItem  );
						os.println("Status of Resources : " + tm.lock);
						os.println("Enter ACCEPT if the resource is free and you want to execute the transaction. REJECT otherwise :");
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
						}
						else
						{
							(tm.rejected).add(temp);
							(tm.rejected).remove(0);
							os.println("Request rejected. Will be requested in regular intervals.");
						}

					}
				}
			}
			

			ser.closed=true;
			SiteSocket.close();
		}
		catch(IOException e){};	
	}
}