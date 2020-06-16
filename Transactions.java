package project;
import java.io.*;
import java.net.*;
import java.util.*;
import java.sql.Timestamp;  
import java.time.Instant;  

public class Transactions
{
	String siteName;
	String tid;
	Timestamp timeStamp;
	// Originating site
	ArrayList<Integer> dataObjects;
	String fun;
	Integer number;
	Boolean statusAtMonitor;

	public Transactions(String s , String id , ArrayList<Integer> dobj , String f , int num)
	{
		this.siteName = s;
		this.tid = id;
		this.dataObjects = dobj;
		this.timeStamp = Timestamp.from(Instant.now());
		this.fun = f;
		this.number = num;
		this.statusAtMonitor = false;
	}

	public ArrayList<tableEntry> execute(String sender , ArrayList<Integer> db)
	{
		if((sender).equalsIgnoreCase("Monitor"))
		{
			ArrayList<tableEntry> returnList = new ArrayList<tableEntry>(10);
			for(int i=0; i<10; i++)
			{
				returnList.add(null);
			}
			tableEntry temp;
			for(int i=0 ; i<dataObjects.size() ; i++)
			{
				temp = new tableEntry();
				temp.transID = tid;
				temp.timeStamp = Timestamp.from(Instant.now());
				temp.dataItem = dataObjects.get(i);
				temp.dataValue = temp.dataItem + number;
				temp.status = true;

				returnList.set(temp.dataItem , temp);
			}
			
			return returnList;
		}
		else
		{
			System.out.println("Error. Only monitor can execute Transactions......");
			return null;
		}
	}

	public String getTid()
	{
		return this.tid;
	}

	public Timestamp getTimeStamp()
	{
		return this.timeStamp;
	}

	public String getSiteName()
	{
		return this.siteName;
	}

	public ArrayList<Integer> getDataObjects()
	{
		return this.dataObjects;
	}

	public int getNumber()
	{
		return this.number;
	}
}

