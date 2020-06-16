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
	ArrayList<Integer> dataBase;
	ArrayList<tableEntry> siteTable;
	ArrayList<Boolean> lock;
	ArrayList<tableEntry> result;
	ArrayList<ArrayList<tableEntry>> listSiteTable;

	public TransactionManager(String s)
	{
		this.siteName = s;
		completed = new ArrayList<Transactions>();
		waiting = new ArrayList<Transactions>();
		dataBase = new ArrayList<Integer>(10);
		lock = new ArrayList<Boolean>(10);
		siteTable = new ArrayList<tableEntry>(10);
		listSiteTable = new ArrayList<ArrayList<tableEntry>>();

		for(int i=0; i<10; i++)
		{
			dataBase.add(i);
			siteTable.add(null);
			lock.add(false);
		}
	}

	public ArrayList<tableEntry> execute(Transactions temp)
	{
		result = temp.execute("Monitor" , dataBase);
		if(result == null)
		{
			System.out.println("Error in executing the transaction");
		}
		else
		{
			for(int i=0; i<result.size() ; i++)
			{
				if(result.get(i) != null)
				{
					dataBase.set(i , (result.get(i)).dataValue);
					siteTable.set(i , result.get(i));
				}
			}
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

	public void updateDatabase()
	{
		for(int i=0 ; i<siteTable.size(); i++)
		{
			if(siteTable.get(i) != null)
			{
				dataBase.set(i , (siteTable.get(i)).dataValue);
				lock.set(i, false);
			}
		}
	}
}