package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseOperation {
	static private Connection connect = null;
	static private Statement statement = null;
	static private PreparedStatement preparedStatement = null;
	static private ResultSet resultSet = null;
	String dbName = "emap"; 
	String userName = "jiu9x9uij"; 
	String password = "icysea99"; 
	String hostname = "comse6998.cbuoihga32cd.us-east-1.rds.amazonaws.com";
	String port = "3306";
	String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
	
	public void connect() throws Exception {
		// this will load the MySQL driver, each DB has its own driver
		Class.forName("com.mysql.jdbc.Driver");
		// setup the connection with the DB.
		System.out.println("JDBC CONNECTION URL IS:\n\t" + jdbcUrl);///
		connect = DriverManager.getConnection(jdbcUrl);
		if(connect != null) System.out.println("CONNECTED TO DATABASE");///		
	}
	
	// you need to close all three to make sure
	public void close() throws Exception{
		resultSet.close();
		statement.close();
		connect.close();
		System.out.println("CONNECTION TO DATABASE IS CLOSED");///
	}
	
	private void writeResultSet(String tableName, ResultSet resultSet) throws Exception {
		if(!resultSet.isBeforeFirst()) System.out.println("RESULT SET CONTAINS NO DATA");///
		// resultSet is initialised before the first data set
		while (resultSet.next()) {
			// it is possible to get the columns via name
			// also possible to get the columns via the column number
			// which starts at 1
			// e.g., resultSet.getSTring(2);
			String text = resultSet.getString("text");
			Double xlabel = resultSet.getDouble("xlabel");
			Double ylabel = resultSet.getDouble("ylabel");
			String emoticon = resultSet.getString("emoticon");
			System.out.println(text + "\t\t" + xlabel + "\t\t" + ylabel + "\t\t" + emoticon);
		}
	}

	private void writeMetaData(String tableName, ResultSet resultSet) throws SQLException {
		// now get some metadata (table info) from the database
	    System.out.println("The columns in the table are: ");
	    System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
	    for (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
	      System.out.println("Column " +i  + " "+ resultSet.getMetaData().getColumnName(i));
	    }
	}

	public void printTable(String tableName) throws Exception{
		System.out.println("PRINTING " + tableName + " TABLE\n------------------------------------");
		// statements allow to issue SQL queries to the database
		statement = connect.createStatement();
		// resultSet gets the result of the SQL query
		resultSet = statement.executeQuery("SELECT * FROM " + tableName + ";");
		System.out.println("\n|text\t\t|\tlocation\t|\n------------------------------------");
		writeResultSet(tableName, resultSet);
		System.out.println("------------------------------------");
	}

	public void addRowToTable(String tableName, String text, double xlabel, double ylabel, String emoticon) throws Exception{
		System.out.println("ADDING ROW ('" + text + "', " + xlabel + ", " + ylabel + ", '" + emoticon + "') TO " + tableName + " TABLE");///
		// preparedStatements can use variables and are more efficient
		preparedStatement = connect.prepareStatement("INSERT INTO " + tableName + " VALUES (?, ?, ?, ?)");
		// "myuser, webpage, datum, summary, COMMENTS from FEEDBACK.COMMENTS");
		// parameters start with 1
		preparedStatement.setString(1, text);
		preparedStatement.setDouble(2, xlabel);
		preparedStatement.setDouble(3, ylabel);
		preparedStatement.setString(4, emoticon);
		preparedStatement.executeUpdate();

		//printTable(tableNmae);///
	}
	
	public void deleteRowFromTable(String tableName, String text) throws Exception{
		// remove row from table according to text
		preparedStatement = connect.prepareStatement("DELETE FROM " + tableName + " WHERE text=?;");
		preparedStatement.setString(1, text);
		preparedStatement.executeUpdate();
	    
		//printTable(tableName);///
	}
	  
	public static void main(String[] args){
		 DatabaseOperation db = new DatabaseOperation();
		    try {
				db.connect();
				db.printTable("happiness");
				db.addRowToTable("happiness", "test text 4", 30, 90, ":)");
				//db.deleteRowFromTable("happiness", "test text 4");
				db.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
