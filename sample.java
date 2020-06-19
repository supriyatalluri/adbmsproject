
import java.io.*;
import java.net.*;
import java.util.*;
import java.sql.Timestamp;  
import java.time.Instant;  

class Sample
{
	String a = "null";
	ArrayList<String> aa = new ArrayList<String>();
	ArrayList<String> bb = new ArrayList<String>();
	ArrayList<ArrayList<String>> aaa = new ArrayList<ArrayList<String>>();
	public Sample()
	{
		aa.add(this.a);
		aa.add(this.a);
		aa.add(this.a);
		bb.add("nuul");
		bb.add("nuul");
		bb.add("nuul");

		aaa.add(this.aa);
		aaa.add(this.bb);
	}

	public static void main(String args[])
	{
	Sample s = new Sample();
	(s.aa).set(0,"b");
	System.out.println(s.aa);
	(s.aaa).get(0).set(0, "c");
	System.out.println(s.aaa);
}
}


