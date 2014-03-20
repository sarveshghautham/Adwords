import java.sql.*;
import java.io.*;
 
public class adwords {
 
	public static void main(String[] argv) throws Exception {
 
		Connection connection = null;
		CallableStatement c_stmt;

		String input = null;
		String []inputArray = new String[4];
		int i = 0;
		String []user_pass = new String[2];
		int []task_num = new int[6];		

		BufferedReader br = new BufferedReader(new FileReader("system.in"));
		
		//Read username and password
		while (i < 2) {
			input = br.readLine();
			inputArray = input.split(" ", 3);
			input = "";
			user_pass[i] = inputArray[2];
			i++;
		}
	
		// Reading task numbers
		i = 0;
		while (i < 6) {
		
			//For Task1
			input = br.readLine();
			inputArray = input.split(" ", 4);
			input = "";
			task_num[i] = Integer.parseInt(inputArray[3]);
			i++;
		}
		
		br.close();

		try {
 
			Class.forName("oracle.jdbc.driver.OracleDriver");
 
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
 
		}
 
		try {
			connection = DriverManager.getConnection(
					"jdbc:oracle:thin:@oracle.cise.ufl.edu:1521:orcl", user_pass[0],
					user_pass[1]);
 
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
 
		if (connection == null) {
			System.out.println("Failed to make connection!");
		}

		//Create tables
		Process p = Runtime.getRuntime().exec("sqlplus "+user_pass[0]+"@orcl/"+user_pass[1]+" @create_tables.sql");
		p.waitFor();

		//Compiles the procedures	
		Process p1 = Runtime.getRuntime().exec("sqlplus "+user_pass[0]+"@orcl/"+user_pass[1]+" @adwords.sql");
		p1.waitFor();

		// Load data into tables
            	Process p3 = Runtime.getRuntime().exec("sqlldr "+user_pass[0]+"/"+user_pass[1]+"@orcl DATA=Queries.dat CONTROL=Queries.ctl LOG=Queries.log");
            	p3.waitFor();

		Process p2 = Runtime.getRuntime().exec("sqlldr "+user_pass[0]+"/"+user_pass[1]+"@orcl DATA=Advertisers.dat CONTROL=Advertisers.ctl LOG=Advertiser.log"); 
            	p2.waitFor();

		Process p4 = Runtime.getRuntime().exec("sqlldr "+user_pass[0]+"/"+user_pass[1]+"@orcl DATA=Keywords.dat CONTROL=Keywords.ctl LOG=Keywords.log");
            	p4.waitFor();
		// Initiate the process
		c_stmt = connection.prepareCall("{call start_process}");
		c_stmt.execute();
		c_stmt.close();  

		// Task 1
		c_stmt = connection.prepareCall("{call first_price_auction_task1(?)}"); 
 		c_stmt.setInt(1, task_num[0]);
 		c_stmt.execute();
 		c_stmt.close();
		write_to_file (connection, "system.out.1", "task1_output"); 

		// Task 2
		c_stmt = connection.prepareCall("{call second_price_auction_task2(?)}"); 
 		c_stmt.setInt(1,task_num[1]);
 		c_stmt.execute();
 		c_stmt.close();
		write_to_file (connection, "system.out.2", "task2_output"); 

		// Task 3
		c_stmt = connection.prepareCall("{call first_price_auction_task3(?)}"); 
 		c_stmt.setInt(1,task_num[2]);
 		c_stmt.execute();
 		c_stmt.close();
		write_to_file (connection, "system.out.3", "task3_output"); 
	
		// Task 4
		c_stmt = connection.prepareCall("{call second_price_auction_task4(?)}"); 
 		c_stmt.setInt(1,task_num[3]);
 		c_stmt.execute();
 		c_stmt.close();
		write_to_file (connection, "system.out.4", "task4_output"); 

		// Task 5
		c_stmt = connection.prepareCall("{call first_price_auction_task5(?)}"); 
 		c_stmt.setInt(1,task_num[4]);
 		c_stmt.execute();
 		c_stmt.close();
		write_to_file (connection, "system.out.5", "task5_output"); 

		// Task 6
		c_stmt = connection.prepareCall("{call second_price_auction_task6(?)}"); 
 		c_stmt.setInt(1,task_num[5]);
 		c_stmt.execute();
 		c_stmt.close();
		write_to_file (connection, "system.out.6", "task6_output"); 

		//Drop the tables. Clean up.
		Process p6 = Runtime.getRuntime().exec("sqlplus "+user_pass[0]+"@orcl/"+user_pass[1]+" @delete_tables.sql");
		p6.waitFor();
	}

	// Method to write to a file.
	public static void write_to_file (Connection conn, String file_name, String table_name) throws IOException
	{
		String table = table_name;
		FileOutputStream file_output = null;
		BufferedWriter file_write = null;
		try {
			file_output =new FileOutputStream(file_name,true);
			file_write = new BufferedWriter(new OutputStreamWriter(file_output));
			Statement display = conn.createStatement();
			ResultSet result = display.executeQuery("select * from "+table+" order by qid, rank");
			ResultSetMetaData result_metada = result.getMetaData();
			int column_count = result_metada.getColumnCount();
			while (result.next()) {
				StringBuilder each_row = new StringBuilder();
				for (int i = 1; i <= column_count; i++) {
					if (i%5 != 0) {
						each_row.append(result.getObject(i) + ", ");
					} 
					else {
						each_row.append(result.getObject(i));
					}
				}
				file_write.write(each_row.toString());
				file_write.newLine();	
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		file_write.close();
	}
}
