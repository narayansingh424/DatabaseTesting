package storedFunctionTesting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SFTesting {
     
	Connection con=null;
	Statement stmt=null;
	ResultSet rs=null;
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
	void test_storedFunctionExists() throws SQLException
	{
		rs=con.createStatement().executeQuery("show function status where name='CustomerLevel'");
		rs.next();
		Assert.assertEquals(rs.getString("Name"), "CustomerLevel");
	}
}
