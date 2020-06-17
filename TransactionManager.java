package project;
import java.io.*;
import java.net.*;
import java.util.*;
import java.sql.Timestamp;  
import java.time.Instant;  

public class TransactionManager
{
	String siteName;
	//Owner site name
	ArrayList<Transactions> completed;
	ArrayList<Transactions> waiting;
	ArrayList<tableEntry> rejected;
	ArrayList<Integer> dataBase;
	ArrayList<tableEntry> siteTable;
	ArrayList<Boolean> lock;
	// true means locked false means free
	tableEntry result;
	ArrayList<tableEntry> listSiteTable;
	ArrayList<Timestamp> listTimestamp;

	public TransactionManager(String s)
	{
		this.siteName = s;
		rejected = new ArrayList<tableEntry>();
		completed = new ArrayList<Transactions>();
		waiting = new ArrayList<Transactions>();
		dataBase = new ArrayList<Integer>(10);
		lock = new ArrayList<Boolean>(10);
		siteTable = new ArrayList<tableEntry>(10);
		listSiteTable = new ArrayList<tableEntry>();
		listTimestamp = new ArrayList<Timestamp>(10);

		for(int i=0; i<10; i++)
		{
			dataBase.add(i);
			siteTable.add(null);
			lock.add(false);
			listTimestamp.add(Timestamp.from(Instant.now()));
		}
	}

	public tableEntry execute(Transactions temp)
	{
		result = temp.execute("Monitor" , dataBase);
		if(result == null)
		{
			System.out.println("Error in executing the transaction");
		}
		else
		{
			dataBase.set(result.dataItem , result.dataValue);
			siteTable.set(result.dataItem , result);
		}

		return result;
	}

	public ArrayList<Integer> getLockedDataItems()
	{
		ArrayList<Integer> temp = new ArrayList<Integer>();
		for(int i=0 ; i<lock.size() ; i++)
		{
			if(lock.get(i) == true)
			{
				temp.add(i);
			}
		}
		return temp;
	}

	public int updateDatabase(tableEntry temp)
	{
		if((listTimestamp.get(temp.dataItem)).compareTo(temp.timeStamp) < 0)
		{
			dataBase.set(temp.dataItem , temp.dataValue);
			listTimestamp.set(temp.dataItem , temp.timeStamp);
			return 1;
		}

		return 0;
	}
}