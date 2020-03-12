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

	
	PostgreSqlExample(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashMap<String, String> attrs_types) throws SQLException{
		
		int result = setUp(k, statement, tables_attrs, attrs_types);	
		System.out.println(result);
	}
	
	private int setUp(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashMap<String, String> attrs_types) throws SQLException {
		
		if (k == 0) {
			return 0;
		}

		// find intersection of attributes in each table
		Integer sizeOfSmallestTable = Integer.MAX_VALUE;
		String smallestTable = "";
		HashSet<String> intersection = new HashSet<String>();
		for (String attr: attrs_types.keySet()) {
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
				int base_case_1 = this.onlyOneTable(k, statement, tables_attrs, attrs_types);
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
				    	base_case_2 = this.existsSubset(k, statement, tables_attrs, attrs_types, intersection, smallestTable);
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
				    	ArrayList<HashSet<String>> groups = dividable(k, statement, tables_attrs, attrs_types);
				    	if (groups.get(1).size() == 0) { // not dividable
				    		// general case 2
				    		if (intersection.size() != 0) {
				    			return generalDP(k, statement, tables_attrs, attrs_types, intersection); 
				    		}
				    		else {
				    			// NP-hard
				    			return 100000 * k;
				    		}
				    	}
				    	else {
				    		general_case_1 = decompose(k, statement, tables_attrs, attrs_types, groups);
				    		return general_case_1;
				    	}
				default:
				    	return base_case_2;
				}
				
				
			} catch (SQLException e) {
				//System.out.println("Connection failure.");
				e.printStackTrace();
			}
		return 0;
		}
		
	

	private int onlyOneTable(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashMap<String, String> attrs_types) throws SQLException {
		// only 1 table
		//System.out.println(tables_attrs.size());
		if (tables_attrs.size() == 1) {
			
			// get number of rows
			String tableName = "\"" + (String) tables_attrs.keySet().toArray()[0] + "\"";
			ResultSet resultset = statement.executeQuery("select count(*) from " + tableName);
			resultset.next();
			int numOfRows = resultset.getInt(1);
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
	private int existsSubset(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashMap<String, String> attrs_types, HashSet<String> intersection, String smallestTable) throws SQLException {
		
		// keep track of the table with fewest attributes
		
		

		boolean existsSubset = false;
		String subsetTableName = null;
		
		// determine if there exists such table
		if (tables_attrs.get(smallestTable).size() == intersection.size()) {
			existsSubset = true;
			subsetTableName = smallestTable;
		}
		
		if (existsSubset) {
			// Construct query groupby: attr_1, attr_2, ..., attr_x
			String query_groupby = tables_attrs.get(subsetTableName).toString();
			query_groupby = query_groupby.substring(1, query_groupby.length()-1);
			
			// Construct query from: Table1 NATURAL JOIN Table2 ... NATURAL JOIN TableX
			String query_from = "";
			for (String key: tables_attrs.keySet()) {
				query_from += "\"" + key + "\" NATURAL JOIN ";
			}
			query_from = query_from.substring(0, query_from.length()-14);
			
			// sort in descending order
			ResultSet subsetData = statement.executeQuery("select count(*), " + query_groupby + " from " + query_from + " group by " + query_groupby + " order by count(*) desc");
			
			
			int finalTupleRemoved = 0;
			int tupleRemoved = 0;
			while (subsetData.next() && finalTupleRemoved < k) {
				finalTupleRemoved += subsetData.getInt(1);
				tupleRemoved += 1;
			}
			
			if (finalTupleRemoved >= k) {
			    // it is possible to print out the set of tuples we are removing - not implemented yet
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
	private ArrayList<HashSet<String>> dividable(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashMap<String, String> attrs_types) throws SQLException {
		ArrayList<HashSet<String>> groups = new ArrayList<HashSet<String>>();
		groups.add(new HashSet<String>());
		groups.add(new HashSet<String>());
		HashMap<String, Integer> attr_id = new HashMap<String, Integer>();
		
		int index = 0;
		for (String attr: attrs_types.keySet()) {
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
	
	private int decompose(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashMap<String, String> attrs_types, ArrayList<HashSet<String>> groups) throws SQLException {
		HashSet<String> group1 = groups.get(0);
		HashSet<String> group2 = groups.get(1);
		ArrayList<Integer> minK = new ArrayList<Integer>();
		
		String queryGroup1 = "";
		for (String tableName: group1) {
			queryGroup1 += "\"" + tableName + "\" NATURAL JOIN ";
		}
		queryGroup1 = "select count (*) from "+ queryGroup1.substring(0, queryGroup1.length()-14);
		//System.out.println(queryGroup1);
		String queryGroup2 = "";
		for (String tableName: group2) {
			queryGroup2 += "\"" + tableName + "\" NATURAL JOIN ";
		}
		queryGroup2 = "select count (*) from "+ queryGroup2.substring(0, queryGroup2.length()-14);
		//System.out.println(queryGroup2);
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
	    			HashMap<String, String> attrs_types_1 = new HashMap<String, String>();
	    			HashMap<String, String> attrs_types_2 = new HashMap<String, String>();						    			
	    			
	    			for (String tableName: group1) {
	    				tables_attrs_1.put(tableName, tables_attrs.get(tableName));
	    			}
	    			for (String tableName: group2) {
	    				tables_attrs_2.put(tableName, tables_attrs.get(tableName));
	    			}
	    			for (ArrayList<String> attrs: tables_attrs_1.values()) {
	    				for (String attrName: attrs) {
	    					attrs_types_1.put(attrName, attrs_types.get(attrName));
	    				}
	    			}
	    			for (ArrayList<String> attrs: tables_attrs_2.values()) {
	    				for (String attrName: attrs) {
	    					attrs_types_2.put(attrName, attrs_types.get(attrName));
	    				}
	    			}
	    			
	    			int group1Removed = setUp(k1, statement, tables_attrs_1, attrs_types_1);
	    			int group2Removed = setUp(k2, statement, tables_attrs_2, attrs_types_2);
	    			minK.add(group1Removed + group2Removed);
	    			//System.out.print("group1Removed: ");
	    			//System.out.println(group1Removed);
	    			//System.out.print("group2Removed: ");
	    			//System.out.println(group2Removed);
	    			break;						    			
				}
			}			    			
		}
		//System.out.println(minK);
	    Collections.sort(minK);
	    return minK.get(0);
	}
	
	private int generalDP(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashMap<String, String> attrs_types, HashSet<String> intersection) throws SQLException {
		Object[] tables = tables_attrs.keySet().toArray();
		ArrayList<ArrayList<Integer>> dp = new ArrayList<ArrayList<Integer>>();
		Object[] commonAttrs = intersection.toArray();
		
		String queryFrom = "";
		for (Object tableName: tables) {
			queryFrom += "\"" + (String) tableName + "\" NATURAL JOIN ";
		}
		queryFrom = queryFrom.substring(0, queryFrom.length()-14);
		
		String queryGroupby = "";
		for (String attrName: intersection) {
			queryGroupby += "\"" + attrName + "\",";
		}
		queryGroupby = queryGroupby.substring(0, queryGroupby.length()-1);
		
		//System.out.println("select " + queryGroupby + " from " + queryFrom + " group by " + queryGroupby);

		ResultSet tableData = statement.executeQuery("select " + queryGroupby + " from " + queryFrom + " group by " + queryGroupby);
		ArrayList<ArrayList<Object>> commonAttrsVals = new ArrayList<ArrayList<Object>>();
		int commonCount = 0;
		while (tableData.next()) {
			ArrayList<Object> commonAttrVal = new ArrayList<Object>();
			for (int i = 0; i < commonAttrs.length; i++) {
				  commonAttrVal.add(tableData.getObject((String)commonAttrs[i]));
			}	
			commonAttrsVals.add(commonAttrVal);
		}
		// create new table
		for (int i = 0; i < tables.length; i++) {	
			String newTable = "";
			for (String attrNames: tables_attrs.get(tables[i])) {
				if (!intersection.contains(attrNames)) {
					newTable += " \"" + attrNames + "\" " + attrs_types.get(attrNames) + ", ";
				}
			}
			newTable = newTable.substring(0, newTable.length()-2);
			newTable = "create table \"sub" + tables[i] + "\" (" + newTable + ")"; 
			//System.out.println(newTable);
			statement.executeUpdate(newTable);
		}
		for (ArrayList<Object> commonAttrsVal: commonAttrsVals) {
			
			
			HashMap<String, ArrayList<String>> subTablesAttrs = new HashMap<String, ArrayList<String>>();
			HashMap<String, String> subAttrs_types = new HashMap<String, String>();
				
			
			
			// insert the tuples into new tables
		    for (int i = 0; i < tables.length; i++) {
		    	ArrayList<String> tableAttrs = new ArrayList<String>();
		    	String select = "";
		    	for (String attrName: tables_attrs.get(tables[i])) {
		    		if (!intersection.contains(attrName)) {
		    			tableAttrs.add(attrName);
		    			subAttrs_types.put(attrName, attrs_types.get(attrName));
		    			select += "\"" + attrName + "\" ";
		    		}
		    		subTablesAttrs.put("sub"+tables[i], tableAttrs);
		    	}
		    	String insert = "select " + select + " from \"" + tables[i] + "\" where ";
		    	for (int j = 0; j < commonAttrs.length; j++) {
		    		insert += "\"" + commonAttrs[j] + "\" = " + commonAttrsVal.get(j) + " and ";
		    	}
		    	insert = insert.substring(0, insert.length()-5);
		    	insert = "insert into \"sub" + tables[i] + "\" " + insert;
		    	//System.out.println(insert);
		    	statement.executeUpdate(insert);
		    }
		    
		    
		    
		    
		    ArrayList<Integer> row  = new ArrayList<Integer>();
		    for (int i = 0; i <= k; i++) {
		    	if (dp.size() == 0) {
		    		row.add(setUp(i, statement, subTablesAttrs, subAttrs_types));
		    	}
		    	else {
		    		 ArrayList<Integer> candidates = new ArrayList<Integer>();
		    		 for (int j = 0; j <= i; j ++) {
		    			 //System.out.println(i-j);
		    			 candidates.add(dp.get(commonCount-1).get(j) + setUp(i-j, statement, subTablesAttrs, subAttrs_types));
		    		 }
		    		 int minVal = Integer.MAX_VALUE;
		    		 for (int j = 0; j < candidates.size(); j ++) {
		    			 if (candidates.get(j) < minVal) {
		    				 minVal = candidates.get(j);
		    			 }
		    		 }
		    		 row.add(minVal);
		    	}
		    	
		    }
		    dp.add(row);
		    System.out.println(dp);
		    commonCount += 1;
    
		    for (int i = 0; i < tables.length; i++) {
		    	String tableName = "sub" + (String) tables[i];
		    	statement.executeUpdate("delete from \"" + tableName + "\"");
		    }
		
		
		}

		return dp.get(dp.size()-1).get(k);
		
		
	}
	
	
	
	
	public static void main(String[] args) {
		// movielens is an example
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test2", "postgres", "postgres")) {

			
			/*
			Statement statement = connection.createStatement();
			
			Random r = new Random();
			for (int i = 1; i <= 10; i++) {
				
				//Integer a = r.nextInt((1000 - 1) + 1) + 1;
				
				Integer c = r.nextInt((3-1)+1)+1;
				statement.executeUpdate("insert into \"testAC\" (\"A\", \"C\") values (" + String.valueOf(i) + ", " + String.valueOf(c)  + ")");
			}
			*/
			
			// initialize k
			int k = 6;
		    HashMap<String, ArrayList<String>> tables_attrs = new HashMap<>();     // the attributes in each table
		    HashMap<String, String> attrs_types = new HashMap<String, String>();
		
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
				resultset = statement.executeQuery("select * from \"" + key + "\" WHERE 1 < 0");
				ResultSetMetaData rsmd = resultset.getMetaData();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					tables_attrs.get(key).add(rsmd.getColumnName(i));
					attrs_types.put(rsmd.getColumnName(i), rsmd.getColumnTypeName(i));
					
				}
			}
			
			

			// start calculation
			PostgreSqlExample test = new PostgreSqlExample(k, statement, tables_attrs, attrs_types);
		
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	
    }
    
}
