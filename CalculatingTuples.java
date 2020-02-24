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
	
	
	protected HashMap<String, ArrayList<String>> tables_attrs = new HashMap<>();     // the attributes in each table
	private Connection connection;
	private StringBuilder str = new StringBuilder();
	private int k;
	private HashMap<String, Integer> attr_id = new HashMap<>();
	private HashMap<Integer, String> id_attr = new HashMap<>();
	
	CalculatingTuples(Connection connection, int k) throws SQLException{
		this.connection = connection;
		this.k = k;
		setUp();
	}
	
	private void setUp() throws SQLException {
		// store table names
		DatabaseMetaData databaseMetaData = this.connection.getMetaData();
		ResultSet resultset = databaseMetaData.getTables(null, null, null, new String[] {"TABLE"});
		while (resultset.next()) {
			this.tables_attrs.put(resultset.getString("TABLE_NAME"), new ArrayList<String>());
		}
		
		// test base case 1
		onlyOneTable();
	}
	
	
	public void onlyOneTable() throws SQLException {
		// only 1 table
		if (this.tables_attrs.size() == 1) {
			Statement statement = connection.createStatement();
			
			// get number of rows
			String tableName = (String) tables_attrs.keySet().toArray()[0];
			ResultSet resultset = statement.executeQuery("select count(*) from " + tableName);
			int numOfRows = resultset.getInt(1);
			
			// not enough data
			if (numOfRows < k) {
				System.out.println("There are less than " + String.valueOf(k) + " final tuples.");				
			}
			// print out the first k data
			else {
				resultset = statement.executeQuery("select * from movies FETCH first " + String.valueOf(k) + " rows only");
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
		}
		// multiple tables: test base case 2
		else {
			existsSubset();
		}
	}
	
	/*
	 * The attributes in each table are put into sets. Find the intersection of all these sets.
	 * If the # of attributes in the table with fewest attributes is the same as the size of the intersection, 
	 * then all attributes in this table appear in every other tables.
	 */
	public void existsSubset() throws SQLException {
		
		Set<String> intersection = new HashSet<String>();
		// keep track of the table with fewest attributes
		Integer sizeOfSmallestTable = Integer.MAX_VALUE;
		String smallestTable = "";
		
		Statement statement = connection.createStatement();
		// iterate through every table
		for (String key: tables_attrs.keySet()) {
			ResultSet resultset = statement.executeQuery("select * from " + key + " FETCH first 1 rows only");
			ResultSetMetaData rsmd = resultset.getMetaData();
			
			// update the table with fewest attributes
			int numOfColumns = rsmd.getColumnCount();
			if (numOfColumns < sizeOfSmallestTable) {
				smallestTable = key;
			}

			// find the union of all attributes
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				tables_attrs.get(key).add(rsmd.getColumnName(i));
				intersection.add(rsmd.getColumnName(i));
			}
		}
		int i = 0;
		for (String attr: intersection) {
			attr_id.put(attr, Integer.valueOf(i) );
			id_attr.put(Integer.valueOf(i), attr);
			i += 1;
		}
		//System.out.println(intersection);
		//System.out.println(tables_attrs);
		
		// find the intersection of the sets of attributes
		for (ArrayList<String> attrs: tables_attrs.values()) {
			intersection.retainAll(attrs);
		}
		//System.out.println(intersection);

		boolean existsSubset = false;
		String subsetTableName = null;
		
		// if there exists such table
		if (tables_attrs.get(smallestTable).size() == sizeOfSmallestTable) {
			existsSubset = true;
			subsetTableName = smallestTable;
		}
		
		if (existsSubset) {
			
			// Construct query groupby: attr_1, attr_2, ..., attr_x
			String query_groupby = tables_attrs.get(subsetTableName).toString();
			query_groupby = query_groupby.substring(1, query_groupby.length()-1);
			
			// Construct query from: Table1 NATURAL JOIN Table2 ... NATURAL JOIN Tabley
			String query_from = "";
			for (String key: tables_attrs.keySet()) {
				query_from += key + " NATURAL JOIN ";
			}
			query_from = query_from.substring(0, query_from.length()-14);
			
			// sort in increasing order
			ResultSet subsetData = statement.executeQuery("select count(*), " + query_groupby + " from " + query_from + " group by " + query_groupby + " order by count(*)");
			
			// move cursor to last
			subsetData.afterLast();
			int finalTupleRemoved = 0;
			int TupleRemoved = 0;
			while (subsetData.previous() && finalTupleRemoved < k) {
				finalTupleRemoved += subsetData.getInt(1);
				TupleRemoved += 1;
			}
			
			if (finalTupleRemoved >= k) {
				System.out.println("A minimum of " + String.valueOf(TupleRemoved) + " tuples needed to be deleted to remove " + String.valueOf(k)+ " final tuples.");
			    // it is possible to print out the set of tuples we are removing - not implemented yet
			}
			else {
				System.out.println("There are less than " + String.valueOf(k) + " final tuples.");
			}
		
		}
		// general cases
		else {
			dividable();
		}	
	}
	
	/*
	 * Construct adjacency matrix for the attributes, then do BFS on one attribute. If there exists attributes not reachable,
	 * then the table containing that attribute can be considered separately. 
	 */
	public void dividable() {
		HashSet<String> group1 = new HashSet<String>();
		HashSet<String> group2 = new HashSet<String>();
		
		int [][] adjacency = new int[attr_id.size()][attr_id.size()];
		for (int i = 0; i < adjacency.length; i++) {
			for (int j = 0; j < adjacency.length; j++) {
				adjacency[i][j] = 0;
			}
		}
		
		for (ArrayList<String> attrs : tables_attrs.values()) {
			for (int i = 1; i < attrs.size(); i++) {
				int base = attr_id.get(attrs.get(0));
				adjacency[base][attr_id.get(attrs.get(i))] = 1;
			}
		}
		// perform BFS
		HashSet<Integer> visited = new HashSet<Integer>();
		ArrayList<Integer> current = new ArrayList<Integer>();
		ArrayList<Integer> next = new ArrayList<Integer>();
		current.add(0);
		visited.add(0);
		while (current.size() != 0) {
			for (int c: current) {
				for (int i = 0; i < adjacency.length; i ++) {
					if (adjacency[c][i] == 1 && !visited.contains(i)) {
						next.add(i);
						visited.add(i);
					}
				}
			}
			current = next;
			next.clear();
		}
		
		for (String table: tables_attrs.keySet()) {
			boolean inGroup2 = true;
			for (String attr: tables_attrs.get(table)) {
				if (visited.contains(attr_id.get(attr))) {
					group1.add(table);					
					inGroup2 = false;
					break;
				}
			}
			if (inGroup2) {
				group2.add(table);
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	

}
