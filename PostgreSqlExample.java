package example;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;




public class PostgreSqlExample {

	
	PostgreSqlExample(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs){
		
		int result = setUp(k, statement, tables_attrs, attrs);	
		System.out.println(result);
		
	}
	
	private int setUp(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs) {
		
		if (k == 0) {
			return 0;
		}
		
		// find intersection of attributes in each table
		Integer sizeOfSmallestTable = Integer.MAX_VALUE;
		String smallestTable = "";
		HashSet<String> intersection = new HashSet<String>();
		for (String attr: attrs) {
			intersection.add(attr);
		}
		
		for (String tableName: tables_attrs.keySet()) {
			// find the intersection of the attributes of all tables
			intersection.retainAll(tables_attrs.get(tableName));
			// find the table with fewest attributes
			if (tables_attrs.get(tableName).size() < sizeOfSmallestTable) {
				sizeOfSmallestTable = tables_attrs.get(tableName).size();
				smallestTable = tableName;
			}
		}
		
		
		try {
			    // base case 1
				int base_case_1 = this.onlyOneTable(k, statement, tables_attrs, attrs);
				// base case 2
				int base_case_2 = Integer.MIN_VALUE;
				// general case 1
				int general_case_1 = Integer.MIN_VALUE;
				// general case 2
				int general_case_2 = Integer.MIN_VALUE;
				
				switch (base_case_1) {
				    case -1:
				    	// not enough data for k
				        break;
				    case 0:
				    	// base case 2
				    	base_case_2 = this.existsSubset(k, statement, tables_attrs, attrs, intersection, smallestTable);
				        break;
				    default:
				    	return base_case_1;
				}
				
				switch (base_case_2) {
				    case Integer.MIN_VALUE:
				        // done in base_case_1
				    	break;
				    case -1:
				    	// not enough data for k
				    	break;
				    case 0:
				    	// general case 1
				    	ArrayList<HashSet<String>> groups = dividable(k, statement, tables_attrs, attrs);
				    	if (groups.get(1).size() == 0) { // not dividable
				    		// general case 2
				    		if (intersection.size() != 0) {
				    			general_case_2 = generalDP(k, statement, tables_attrs, attrs, intersection); 
				    		}
				    		else {
				    			// NP-hard
				    			return 100000 * k;
				    		}
				    	}
				    	else {
				    		general_case_1 = decompose(k, statement, tables_attrs, attrs, groups);
				    		return general_case_1;
				    	}
				    	break;
				    default:
				    	return base_case_2;
				}
				
				
			} catch (SQLException e) {
				System.out.println("Connection failure.");
				e.printStackTrace();
			}
		return 0;
		}
		
	

	private int onlyOneTable(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs) throws SQLException {
		// only 1 table
		System.out.print("if: ");
		System.out.println(tables_attrs.size());
		if (tables_attrs.size() == 1) {
			
			// get number of rows
			String tableName = (String) tables_attrs.keySet().toArray()[0];
			ResultSet resultset = statement.executeQuery("select count(*) from " + tableName);
			resultset.next();
			int numOfRows = resultset.getInt(1);
			System.out.print("Num of rows: ");
			System.out.println(numOfRows);
			// not enough data
			if (numOfRows < k) {
				return 100000 * k;			
			}
			// print out the first k data
			/*
			else {
				resultset = statement.executeQuery("select * from movies FETCH first " + String.valueOf(k) + " rows only");
				int numOfColumns = resultset.getMetaData().getColumnCount();
				StringBuilder str = new StringBuilder();
				while (resultset.next()) {
					for (int i = 1; i <= numOfColumns; i ++) {
						str.append(resultset.getString(i));
						str.append(" ");
					}
					System.out.println(str);
					str = new StringBuilder();
				}
			}	
			*/

			return k;
		}
		else {
			return 0;
		}
	}
	
	/*
	 * The attributes in each table are put into sets. Find the intersection of all these sets.
	 * If the # of attributes in the table with fewest attributes is the same as the size of the intersection, 
	 * then all attributes in this table appear in every other tables.
	 */
	private int existsSubset(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, String smallestTable) throws SQLException {
		
		// keep track of the table with fewest attributes
		
		
		
		System.out.print("Intersection: ");
		System.out.println(intersection);
		//System.out.println(tables_attrs);

		boolean existsSubset = false;
		String subsetTableName = null;
		
		// determine if there exists such table
		if (tables_attrs.get(smallestTable).size() == intersection.size()) {
			existsSubset = true;
			subsetTableName = smallestTable;
		}
		
		if (existsSubset) {
			System.out.println("exists subset");
			// Construct query groupby: attr_1, attr_2, ..., attr_x
			String query_groupby = tables_attrs.get(subsetTableName).toString();
			query_groupby = query_groupby.substring(1, query_groupby.length()-1);
			
			// Construct query from: Table1 NATURAL JOIN Table2 ... NATURAL JOIN Tabley
			String query_from = "";
			for (String key: tables_attrs.keySet()) {
				query_from += key + " NATURAL JOIN ";
			}
			query_from = query_from.substring(0, query_from.length()-14);
			
			System.out.println("select count(*), " + query_groupby + " from " + query_from + " group by " + query_groupby + " order by count(*) desc");
			// sort in descending order
			ResultSet subsetData = statement.executeQuery("select count(*), " + query_groupby + " from " + query_from + " group by " + query_groupby + " order by count(*) desc");
			
			
			int finalTupleRemoved = 0;
			int tupleRemoved = 0;
			System.out.println("k = " + String.valueOf(k));
			while (subsetData.next() && finalTupleRemoved < k) {
				System.out.println(subsetData.getInt(1));
				finalTupleRemoved += subsetData.getInt(1);
				tupleRemoved += 1;
			}
			
			if (finalTupleRemoved >= k) {
			    // it is possible to print out the set of tuples we are removing - not implemented yet
				System.out.println("Tuple removed: " + String.valueOf(tupleRemoved));
				System.out.println("Final tuple removed: " + String.valueOf(finalTupleRemoved));
				return tupleRemoved;
			}
			else {
				return 100000 * k;
			}
		}
		else {
			return 0;
		}	
	}
	
	/*
	 * Construct adjacency matrix for the attributes, then do BFS on one attribute. If there exists attributes not reachable,
	 * then the table containing that attribute can be considered separately. 
	 */
	private ArrayList<HashSet<String>> dividable(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs) {
		
		ArrayList<HashSet<String>> groups = new ArrayList<HashSet<String>>();
		groups.add(new HashSet<String>());
		groups.add(new HashSet<String>());
		HashMap<String, Integer> attr_id = new HashMap<String, Integer>();
		
		int index = 0;
		for (String attr: attrs) {
			attr_id.put(attr, index);
			index += 1;
		}
		
		
		int [][] adjacency = new int[attr_id.size()][attr_id.size()];
		for (int i = 0; i < adjacency.length; i++) {
			for (int j = 0; j < adjacency.length; j++) {
				adjacency[i][j] = 0;
			}
		}
		//System.out.println(attrs.size());
		for (ArrayList<String> attr : tables_attrs.values()) {
			for (int i = 1; i < attr.size(); i++) {
				int base = attr_id.get(attr.get(0));				
				//System.out.println("Base:" + String.valueOf(base));
				//System.out.println(attr_id.get(attr.get(i)));
				adjacency[base][attr_id.get(attr.get(i))] = 1;
				adjacency[attr_id.get(attr.get(i))][base] = 1;
			}
		}
		//System.out.println(Arrays.deepToString(adjacency));
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
			current.clear();
			for (int id: next) {
				current.add(id);
			}
			next.clear();
		}
		
		//System.out.println(visited);
		
		for (String tableName: tables_attrs.keySet()) {
			for (String attr: tables_attrs.get(tableName)) {
				if (visited.contains(attr_id.get(attr))) {
					groups.get(0).add(tableName);					
					break;
				}
				else {
					groups.get(1).add(tableName);
				}
			}
		}
		
		return groups;
	}
	
	private int decompose(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, ArrayList<HashSet<String>> groups) throws SQLException {
		HashSet<String> group1 = groups.get(0);
		HashSet<String> group2 = groups.get(1);
		ArrayList<Integer> minK = new ArrayList<Integer>();
		
		String queryGroup1 = "";
		for (String tableName: group1) {
			queryGroup1 += tableName + " NATURAL JOIN ";
		}
		queryGroup1 = "select count (*) from "+ queryGroup1.substring(0, queryGroup1.length()-14);
		
		String queryGroup2 = "";
		for (String tableName: group2) {
			queryGroup2 += tableName + " NATURAL JOIN ";
		}
		queryGroup2 = "select count (*) from "+ queryGroup2.substring(0, queryGroup2.length()-14);
		System.out.println(queryGroup1);
		System.out.println(queryGroup2);
		ResultSet rs = statement.executeQuery(queryGroup1);
		rs.next();
		int sizeOfGroup1InRelations = rs.getInt(1);
		
		rs = statement.executeQuery(queryGroup2);
		rs.next();
		int sizeOfGroup2InRelations = rs.getInt(1);
		
		
		for (int k1 = 0; k1 <= k; k1 ++) {
			for (int k2 = 0; k2 <= k; k2 ++) {
				// select count(*) from join gropu1/group2
				if (k1*sizeOfGroup2InRelations + k2*sizeOfGroup1InRelations - k1*k2 < k) {
					continue;
				}
				else {
                    
					HashMap<String, ArrayList<String>> tables_attrs_1 = new HashMap<>();
	    			HashMap<String, ArrayList<String>> tables_attrs_2 = new HashMap<>();
	    			HashSet<String> attrs_1 = new HashSet<String>();
	    			HashSet<String> attrs_2 = new HashSet<String>();						    			
	    			
	    			for (String tableName: group1) {
	    				tables_attrs_1.put(tableName, tables_attrs.get(tableName));
	    				attrs_1.addAll(tables_attrs.get(tableName));
	    			}
	    			for (String tableName: group2) {
	    				tables_attrs_2.put(tableName, tables_attrs.get(tableName));
	    				attrs_2.addAll(tables_attrs.get(tableName));
	    			}
	    			
	    			int group1Removed = setUp(k1, statement, tables_attrs_1, attrs_1);
	    			int group2Removed = setUp(k2, statement, tables_attrs_2, attrs_2);
	    			System.out.println(k1);
	    			System.out.println(group1Removed);
	    			System.out.println(k2);
	    			System.out.println(group2Removed);
	    			minK.add(group1Removed + group2Removed);
	    			
	    			break;						    			
				}
			}			    			
		}
		System.out.println(minK);
	    Collections.sort(minK);
	    return minK.get(0);
	}
	
	private int generalDP(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection) throws SQLException {
		
		
		int [][] dp = new int[tables_attrs.size()][k];
		for (int i = 0; i < dp.length; i++) {
			for (int j = 0; j < dp[0].length; j++) {
				int[] ij = new int[k+1];
				for (int kk = 0; kk <= k; kk++) {
					
				}
			}
		}
		
		
		
		
		return 100000 * k;
	}
	
	
	
	
	public static void main(String[] args) {
		// movielens is an example
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/movielens", "postgres", "postgres")) {

			System.out.println("Connected to PostgreSQL database!");
			
			/*
			Statement statement = connection.createStatement();
			for (int i = 1; i <= 1000; i++) {
				//String a = String.valueOf(r.nextInt((943 - 1) + 1) + 1);
				statement.executeUpdate("insert into decompose (decompose) values (" + String.valueOf(i) + ")");
			}
			*/
	        
			// initialize k
			int k = 1001;
		    HashMap<String, ArrayList<String>> tables_attrs = new HashMap<>();     // the attributes in each table
		    HashSet<String> attrs = new HashSet<String>();
		
			// find table names
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			ResultSet resultset = databaseMetaData.getTables(null, null, null, new String[] {"TABLE"});
			while (resultset.next()) {
				tables_attrs.put(resultset.getString("TABLE_NAME"), new ArrayList<String>());
			}
			
			// find attributes in each table
			Statement statement = connection.createStatement();
			// iterate through every table
			for (String key: tables_attrs.keySet()) {
				resultset = statement.executeQuery("select * from " + key + " FETCH first 1 rows only");
				ResultSetMetaData rsmd = resultset.getMetaData();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					tables_attrs.get(key).add(rsmd.getColumnName(i));
					attrs.add(rsmd.getColumnName(i));
				}
			}
			
			

			// start calculation
			PostgreSqlExample test = new PostgreSqlExample(k, statement, tables_attrs, attrs);
		
		}
		catch (SQLException e) {
			System.out.println("Connection failure.");
			e.printStackTrace();
		}
	
    }
    
}
