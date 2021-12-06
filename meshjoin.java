package i191695_N_Project;
import java.util.concurrent.ArrayBlockingQueue;
import java.sql.*;
import java.util.*;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import java.text.SimpleDateFormat;  
import java.util.Date;  

public class meshjoin {

	public static void main(String[] args) {

		try {
			// Login credentials
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter user: ");
		String user = sc.nextLine();

		System.out.println("Enter password");
		String password = sc.nextLine();

		String msp = "jdbc:mysql://localhost:3306/";

		System.out.println("Enter Name Of DataBase eg Metro");
		String db = sc.nextLine();
		msp = msp + db;

		Connection mycon = DriverManager.getConnection(msp, user, password);
			//Connection mycon = DriverManager.getConnection("jdbc:mysql://localhost:3306/metro", "root", "1234"); //STREAM DATA
			Statement query = mycon.createStatement();
			
			Connection mycon2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/metro_warehouse",user, password);
			
			
			int iter = 0, iter2 = 0, iter3 = 0;
			List<List> arr = new ArrayList<List>();	
			ArrayBlockingQueue<List> queu = new ArrayBlockingQueue<List>(10);
			List<List> finall = new ArrayList<List>();
			MultiValuedMap<String, List> mapping = new ArrayListValuedHashMap<String, List>();

			int plz = 0;
			List<List> dequeuQ = new ArrayList<List>(); //For removing
			while (true) {

				if (queu.size() == 10) {
					
					dequeuQ.add(queu.peek());
					queu.poll();
				}
				arr.clear();

				String md_query = "select * from masterdata limit " + iter2 + ",10";
				PreparedStatement q = mycon.prepareStatement(md_query);
				ResultSet r = q.executeQuery();

				while (r.next()) {
					String t1 = r.getString("product_id");
					String t2 = r.getString("product_name");
					String t3 = r.getString("supplier_id");
					String t4 = r.getString("supplier_name");
					String t5 = r.getString("price");
					List<String> a = new ArrayList<String>();

					a.add(t1);
					a.add(t2);
					a.add(t3);
					a.add(t4);
					a.add(t5);

					ArrayList<String> copy = new ArrayList<String>(a);

					arr.add(copy);
					a.clear();
				}

				String transcations_query = "select * from transactions limit " + iter3 + ",50";
				PreparedStatement qq = mycon.prepareStatement(transcations_query);
				ResultSet rr = qq.executeQuery();

				ArrayList<String> a = new ArrayList<String>();// Transaction Data
				List<String> b = new ArrayList<String>();

				// System.out.println(arr.get(i).get(0));

				while (rr.next()) {
					String t1 = rr.getString("transaction_id");
					String t2 = rr.getString("product_id");
					String t3 = rr.getString("customer_id");
					String t4 = rr.getString("customer_name");
					String t5 = rr.getString("store_id");
					String t6 = rr.getString("store_name");
					String t7 = rr.getString("t_date");
					String t8 = rr.getString("quantity");

					a.add(t1);
					a.add(t2);
					a.add(t3);
					a.add(t4);
					a.add(t5);
					a.add(t6);
					a.add(t7);
					a.add(t8);
					// System.out.println(a);

					ArrayList<String> aa = new ArrayList<String>(a);
					mapping.put(aa.get(1), aa);
					b.add(t1);

					a.clear();
				}
				ArrayList<String> copy = new ArrayList<String>(b);
				queu.add(b);

				// -----JOIN-----
				
				List<List> arrData = new ArrayList<List>(arr);
				for (int j = 0; j < arrData.size(); j++)
				{
					List<String> temp2 = new ArrayList<String>(arrData.get(j));// QDATA [1,x],[3,y]
					String mdPid = temp2.get(0); // INV PID
				//	System.out.println(temp2);
					
					if (mapping.containsKey(mdPid))
					{
						List<List> mapDataInvList = new ArrayList(mapping.get(mdPid));
						for (int k = 0; k < mapping.get(mdPid).size(); k++)
						{
							List<String> SingleMachedTuple = new ArrayList<String>(mapDataInvList.get(k));
					    	//System.out.println(SingleMachedTuple);
							
							// INSERTING IN PRODUCT DIMENSION
							try {
								String p_query="insert into products values ('"+temp2.get(0)+"', '"+temp2.get(1)+"', '"+temp2.get(4)+"')";
								PreparedStatement insProductQ = mycon2.prepareStatement(p_query);
								insProductQ.execute();
							}
							catch(Exception exc)
							{
								
							}
							
							// INSERTING IN SUPPLIER DIMENSION
							try {
								
								String s_query="insert into supplier values('"+temp2.get(2)+"', '"+temp2.get(3)+"')";
								PreparedStatement insProductQ = mycon2.prepareStatement(s_query);
								
								if(temp2.get(2).equals("SP-15"))
								{
								
									s_query="Insert into supplier values ('SP-15','Casey''s General Stores Inc.')";
									PreparedStatement insProductQ2 = mycon2.prepareStatement(s_query);
									System.out.println(s_query);
									insProductQ2.execute();
									
								}
														
								insProductQ.execute();
							}
							catch(Exception exc)
							{
								//System.out.println("not so fast supplier");
							}
							
							// INSERTING IN CUSTOMER DIMENSION
							try {
								String customer_query="insert into customer values('"+SingleMachedTuple.get(2)+"', '"+SingleMachedTuple.get(3)+"')";
								System.out.println(customer_query);
								PreparedStatement insProductQ = mycon2.prepareStatement(customer_query);
								insProductQ.execute();
							}
							catch(Exception exc)
							{
								//System.out.println("not so fast customer");
							}
							
							// INSERTING IN STORE DIMENSION
							try {
								String store_query="insert into store values('"+SingleMachedTuple.get(4)+"', '"+SingleMachedTuple.get(5)+"')";
								PreparedStatement insProductQ = mycon2.prepareStatement(store_query);
								insProductQ.execute();
							}
							catch(Exception exc)
							{
								//System.out.println("not so fast customer");
							}
							
							
							// INSERTING IN TIME DIMENSION
							try {
								String time_query="insert into time_dimension (t_date) values('"+SingleMachedTuple.get(6)+"')";
								PreparedStatement timeQ = mycon2.prepareStatement(time_query);
								timeQ.execute();
								System.out.println(time_query);
							}
							catch(Exception exc)
							{
								//System.out.println("time");
							}
							
							
							// INSERTING IN FACT_TABLE
							try {
								//String fact_query="insert into fact_table values('"+temp2.get(0)+"', '"+SingleMachedTuple.get(2)+"')";
								int x1=Integer.valueOf(SingleMachedTuple.get(7));
								float x2=Float.valueOf(temp2.get(4));
								float Totalsale=x1*x2;
								
								String fact_query="insert into fact_table values('"+temp2.get(0)+"','"+SingleMachedTuple.get(2)+"','"+SingleMachedTuple.get(4)+"','"+SingleMachedTuple.get(0)+"','"+temp2.get(2)+"','"+SingleMachedTuple.get(7)+"','"+Totalsale+"','"+SingleMachedTuple.get(6)+"')";
								PreparedStatement FactQ = mycon2.prepareStatement(fact_query);
								System.out.println(fact_query);
								FactQ.execute(); 
							}
							catch(Exception exc)
							{
								//System.out.println("Fact table insertion problem");
							}
							
							
						}
						
					}
				
					System.out.println("------------------------------------------------------------------------------------------------------");
				}
				
				//********** REMOVING ***********
				if(dequeuQ.size()>0)
				{
					
					for(int i=0;i<100;i++)
					{
						String mid;
						mid="P-100"+i;
						if(i>=10)
						{
							mid="P-10"+i;
						}
						
						if (mapping.containsKey(mid))
						{
							List<List> mapDataInvList = new ArrayList(mapping.get(mid));
							for(int j=0;j<mapDataInvList.size();j++)
							{
								List<String> SingleMachedTuple = new ArrayList<String>(mapDataInvList.get(j));
								if(dequeuQ.get(0).contains(SingleMachedTuple.get(0)))
								{
									mapping.removeMapping(mid, SingleMachedTuple);	
//									System.out.println("removing");
//									System.out.println(SingleMachedTuple);
								}
							}
						}
					}
					
					
					
					dequeuQ.clear();
				}
				
				if (iter3 > 10000) 
					break;
				iter++;
				iter2 += 10;
				if (iter2 == 100)
					iter2 = 0;
				iter3 += 50;
			}
			
//			
//			int i=0;
//			String n;
//			int sss=0;
//			while(true)
//			{
//				n="p-100"+i;
//				if(i>=10)
//					n="P-10"+i;
//				System.out.println(n);
//				System.out.println(mapping.get(n).size());
//				sss+=mapping.get(n).size();
//				if(i==100)
//					break;	
//				i++;
//			}
//			System.out.println(sss);
//			System.out.println(iter3);
			
						
		} catch (Exception exc) {
			exc.printStackTrace();
		}

	}

}
