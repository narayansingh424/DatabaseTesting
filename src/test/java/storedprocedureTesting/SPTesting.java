package storedprocedureTesting;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.Assert;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterClass;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

@Test
public class SPTesting {

	Connection con=null;
	Statement stmt=null;
	ResultSet rs=null;
	CallableStatement cStmt;
	
	ResultSet rs1;
	ResultSet rs2;
	@BeforeClass
	void setup() throws SQLException
	{
		con=DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","");
	}
	
	
	@AfterClass
	void tearDown() throws SQLException
	{
		con.close();
	}
	
	@Test(priority=1)
	void test_storedProceduresExists() throws SQLException
	{
	stmt=(Statement) con.createStatement();	
	rs=((java.sql.Statement) stmt).executeQuery("SHOW PROCEDURE STATUS WHERE name='SelectAllCustomers'");
	rs.next();
	
	Assert.assertEquals(rs.getString("Name"),"SelectAllCustomers");
	}
	
	
	@Test(priority=2)
	void test_SelectAllCustomers() throws SQLException
	{
		cStmt=con.prepareCall("{call SelectAllCustomers()}");
		rs1=cStmt.executeQuery();
		Statement stmt=con.createStatement();
		rs2=stmt.executeQuery("select * from customers");
		
		Assert.assertEquals(compareResultSets(rs1,rs2),true);
	}
	
	@Test(priority=3)
	void test_SelectAllcustomerByCity() throws SQLException
	{
		cStmt=con.prepareCall("{call SelectAllCustomerByCity(?)}");
		cStmt.setString(1,"Singapore");
		rs1=cStmt.executeQuery();
		Statement stmt=con.createStatement();
		rs2= stmt.executeQuery("Select * from customers where city='Singapore'");
		
		Assert.assertEquals(compareResultSets(rs1,rs2), true);
		
	}
	
	@Test(priority=4)
	void test_SelectAllCustomersByCityAndPin() throws SQLException
	{
		cStmt=con.prepareCall("{call SelectAllCustomersByCityAndPin(?,?)}");
		cStmt.setString(1, "Singapore");
		cStmt.setString(2, "079903");
		rs1=cStmt.executeQuery();
		Statement stmt=con.createStatement();
		rs2=stmt.executeQuery("Select * from customers where city='Singapore' and postalCode='079903'");
		
		Assert.assertEquals(compareResultSets(rs1,rs2), true);
	}
	
	@Test(priority=5)
	void test_get_order_by_cust() throws SQLException
	{
		cStmt=con.prepareCall("{call get_order_by_cust(?,?,?,?,?)}");
		cStmt.setInt(1,141);
		
		cStmt.registerOutParameter(2, Types.INTEGER);
		cStmt.registerOutParameter(3, Types.INTEGER);
		cStmt.registerOutParameter(4, Types.INTEGER);
		cStmt.registerOutParameter(5, Types.INTEGER);
		
		cStmt.executeQuery();
		
		int shipped =cStmt.getInt(2);
		int canceled =cStmt.getInt(3);
		int resolved=cStmt.getInt(4);
		int disputed=cStmt.getInt(5);
		
		System.out.println(shipped+" "+canceled+" "+resolved+" "+disputed);
		
		Statement stmt=con.createStatement();
		rs=stmt.executeQuery("select(select count(*) as 'shipped' from orders where customerNumber=141 and status='Shipped')as Shipped,(select count(*) as 'canceled' from orders where customerNumber=141 and status='Canceled')as Canceled, (select count(*) as 'resolved' from orders where customerNumber=141 and status='Resolved')as Resolved,(select count(*) as 'disputed' from orders where customerNumber=141 and status='Disputed') as Disputed");		
	    rs.next();
	    
	    int exp_shipped=rs.getInt("shipped");
	    int exp_canceled=rs.getInt("canceled");
	    int exp_resolved=rs.getInt("resolved");
	    int exp_disputed=rs.getInt("disputed");
	    if(shipped==exp_shipped && canceled==exp_canceled && resolved==exp_resolved && disputed==exp_disputed)
	    	Assert.assertTrue(true);
	    else
	    	Assert.assertTrue(false);
	    
	}
	
	@Test(priority=6)
	void test_getCustomerShipping() throws SQLException
	{
		cStmt=con.prepareCall("{call getCustomerShipping(?,?)}");
		cStmt.setInt(1,112);
		cStmt.registerOutParameter(2, Types.VARCHAR);
		
		cStmt.executeQuery();
		String shippingTime=cStmt.getString(2);
		
		Statement stmt=con.createStatement();
		rs=stmt.executeQuery("select country, case when country='USA' THEN '2-day shipping' when country='canada' then '3-day shipping' else '5-days shipping' End as ShippingTime from customers where customerNumber=112");
	    rs.next();
	    
	    String exp_shippingTime=rs.getString("ShippingTime");
	    
	    Assert.assertEquals(shippingTime, exp_shippingTime);
	}
	
	public boolean compareResultSets(ResultSet resultSet1,ResultSet resultSet2) throws SQLException
	{
		while(resultSet1.next())
		{
			resultSet2.next();
			int count=resultSet1.getMetaData().getColumnCount();
			for(int i=1;i<=count;i++)
			{
				if(!StringUtils.equals(resultSet1.getString(i),resultSet2.getString(i)))
				{
					return false;
				}
			}
			
		}
		return true;
	}
	
	
	
}
