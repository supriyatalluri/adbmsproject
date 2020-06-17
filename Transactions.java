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
	Integer dataItem;
	String fun;
	Integer number;
	Boolean statusAtMonitor;
	ArrayList<String> votes;

	public Transactions(String s , String id , int dobj , String f , int num , int sitesno)
	{
		this.siteName = s;
		this.tid = id;
		this.dataItem = dobj;
		this.timeStamp = Timestamp.from(Instant.now());
		this.fun = f;
		this.number = num;
		this.statusAtMonitor = false;
		this.votes = new ArrayList<String>(sitesno);
		for(int i=0; i<sitesno ; i++)
		{
			(this.votes).add("null");
		}
	}

	public tableEntry execute(String sender , ArrayList<Integer> db)
	{
		if((sender).equalsIgnoreCase("Monitor"))
		{
			tableEntry temp = new tableEntry();
			temp.transID = tid;
			temp.timeStamp = Timestamp.from(Instant.now());
			temp.dataItem = dataItem;
			temp.dataValue = db.get(dataItem) + number;
			temp.status = true;
	
			return temp;
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

	public Integer getDataItem()
	{
		return this.dataItem;
	}

	public int getNumber()
	{
		return this.number;
	}
}

