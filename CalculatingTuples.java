package example;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set; 
import java.util.HashSet;

public class CalculatingTuples {
	
	
	protected HashMap<String, ArrayList<String>> tables_attrs = new HashMap<>();
	private Connection connection;
	private StringBuilder str = new StringBuilder();
    private int k;
	
	CalculatingTuples(Connection connection, int k) throws SQLException{
		this.connection = connection;
		DatabaseMetaData databaseMetaData = connection.getMetaData();
		setUp();
		this.k = k;
	}
	
	private void setUp() throws SQLException {
		DatabaseMetaData databaseMetaData = this.connection.getMetaData();
		ResultSet resultset = databaseMetaData.getTables(null, null, null, new String[] {"TABLE"});
		while (resultset.next()) {
			this.tables_attrs.put(resultset.getString("TABLE_NAME"), new ArrayList<String>());
		}
		onlyOneTable();
	}
	
	
	public void onlyOneTable() throws SQLException {
		if (this.tables_attrs.size() == 1) {
			Statement statement = connection.createStatement();
			ResultSet resultset = statement.executeQuery("select * from movies FETCH first " + String.valueOf(k) + " rows only");
			int numOfColumns = resultset.getMetaData().getColumnCount();
			
			while (resultset.next()) {
				for (int i = 1; i <= numOfColumns; i ++) {
					str.append(resultset.getString(i));
					str.append(" ");
				}
				System.out.println(str);
				this.str = new StringBuilder();
			}
			
		}
		else {
			existsSubset();
		}
	}
	
	public void existsSubset() throws SQLException {
		
		Integer sizeOfSmallestTable = Integer.MAX_VALUE;
		Set<String> intersection = new HashSet<String>();
		ArrayList<String> smallestTable = new ArrayList<String>();
		
		Statement statement = connection.createStatement();
		for (String key: tables_attrs.keySet()) {
			ResultSet resultset = statement.executeQuery("select * from " + key + " FETCH first 1 rows only");
			ResultSetMetaData rsmd = resultset.getMetaData();
			
			int numOfColumns = rsmd.getColumnCount();
			if (numOfColumns < sizeOfSmallestTable) {
				smallestTable.clear();
				smallestTable.add(key);
			}
			else if (numOfColumns == sizeOfSmallestTable) {
				smallestTable.add(key);
			}
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				tables_attrs.get(key).add(rsmd.getColumnName(i));
				intersection.add(rsmd.getColumnName(i));
			}
		}
		System.out.println(intersection);
		System.out.println(tables_attrs);
		for (ArrayList<String> attrs: tables_attrs.values()) {
			intersection.retainAll(attrs);
		}
		System.out.println(intersection);

		boolean existsSubset = false;

		String subsetTableName = null;
		if (smallestTable.size() == 1 && tables_attrs.get(smallestTable.get(0)).size() == sizeOfSmallestTable) {
			existsSubset = true;
			subsetTableName = smallestTable.get(0);
		}
        System.out.println(subsetTableName);		
		if (existsSubset) {
			ResultSet subsetData = statement.executeQuery("select count(*) from " + subsetTableName);
			subsetData.next();
			int numOfRows = subsetData.getInt(1);
			ArrayList<Integer> numOfRemovable = new ArrayList<Integer>();
			
			String query_groupby = tables_attrs.get(subsetTableName).toString();
			query_groupby = query_groupby.substring(1, query_groupby.length()-1);
			
			String query_from = "";
			for (String key: tables_attrs.keySet()) {
				query_from += key + " NATURAL JOIN ";
			}
			query_from = query_from.substring(0, query_from.length()-14);
			// sorted in increasing order
			subsetData = statement.executeQuery("select count(*), " + query_groupby + " from " + query_from + " group by " + query_groupby + " order by count(*)");
			// move cursor to last
			subsetData.afterLast();
			int removed = 0;
			while (subsetData.previous() && removed < k) {
				removed += subsetData.getInt(1);
			}
			
		}
		// general cases
		else {
			dividable();
		}	
	}
	
	public void dividable() {
		
		for (String key: tables_attrs.keySet()) {
			
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	

}
