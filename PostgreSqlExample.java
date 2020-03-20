package example;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;


public class PostgreSqlExample {

	
	PostgreSqlExample(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, ArrayList<String> projection) throws SQLException{
		int result = setUp(k, statement, tables_attrs, attrs, new HashMap<String, Object>(), projection);	
		System.out.println(result);
	}
	
	Integer setUp(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection) throws SQLException {
		
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
				int base_case_1 = this.onlyOneTable(k, statement, tables_attrs, attrs, constraints, projection);
				// base case 2
				int base_case_2 = Integer.MIN_VALUE;
				// general case 1
				int general_case_1 = Integer.MIN_VALUE;
				// general case 2
				int general_case_2 = Integer.MIN_VALUE;
				
				if (base_case_1 == -1) {
					// not enough data for k
					return -1;
				}
				else if (base_case_1 == 0) {
					// next base case
			    	base_case_2 = this.existsSubset(k, statement, tables_attrs, attrs, intersection, smallestTable, constraints);
				}
				else {
					return base_case_1;
				}
				
				if (base_case_2 == -1) {
					// not enough data for k
					return -1;
				}
				else if (base_case_2 == 0) {
					// goto general case 1
					ArrayList<HashSet<String>> groups = dividable(k, statement, tables_attrs, attrs, constraints);
					if (groups.get(1).size() != 0) { 				// is dividable
						return decompose(k, statement, tables_attrs, attrs, groups, constraints, projection);						
					}
					else { // not dividable, goto general case 2
			    		if (intersection.size() != 0) {
			    			return commonAttrs(k, statement, tables_attrs, attrs, intersection, constraints, projection); 
			    		}
			    		else {
			    			// NP-hard
			    			return null;
			    		}
			    	}
				}
				else {
					return base_case_2;
				}
				
			} catch (SQLException e) {
				//System.out.println("Connection failure.");
				e.printStackTrace();
			}
		return null;
		}
	

	private int onlyOneTable(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection) throws SQLException {
		// only 1 table
		//System.out.println(tables_attrs.size());
		if (tables_attrs.size() == 1) {
			if (projection.size() == 0) {
				// get number of rows
				String tableName = "\"" + (String) tables_attrs.keySet().toArray()[0] + "\"";
				ResultSet resultset;
				if (constraints.size() == 0) {
					resultset = statement.executeQuery("select count(*) from " + tableName);
				}
				else {
					String where = " where ";
					
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
					//System.out.println("select count(*) from " + tableName + where);
					resultset = statement.executeQuery("select count(*) from " + tableName + where);
				}
				
				resultset.next();
				int numOfRows = resultset.getInt(1);
				// not enough data
				if (numOfRows < k) {
					return -1;			
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
			    //System.out.println(tables_attrs);
			    //System.out.println(k);
				return k;
			}
			else {
				
				// find non-dangling tuples
				String query = "";
				for (String tableName: tables_attrs.keySet()) {
					query += "\"" + tableName + "\" NATURAL JOIN ";
				}
				String where = "";
			
				String p = "distinct ";
				for (String attrName: projection) {
					p += "\"" + attrName + "\", ";
				}
				p = p.substring(0, p.length()-2);
				query = "select " + p + " from" + query.substring(0, query.length()-14);
				
				if (constraints.size() != 0) {
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
					query += " where " + where;
				}
				HashSet<HashMap<String, Object>> nondanglings = new HashSet<HashMap<String, Object>>();
				ResultSet resultset = statement.executeQuery(query);
				while (resultset.next()) {
					HashMap<String, Object> nondangling = new HashMap<String, Object>();
					for (int i = 1; i <= projection.size(); i++) {
						nondangling.put(projection.get(i-1), resultset.getObject(i));
					}
					nondanglings.add(nondangling);
				}
			
				// find all projection tuples
				query = "select count (*), ";
				for (String attrName: projection) {
					query += "\"" + attrName + "\", ";
				}
				query = query.substring(0, query.length()-2);
				String tableName = "\"" + (String) tables_attrs.keySet().toArray()[0] + "\"";
				query += "from " + tableName;
				if (constraints.size() != 0) {
					where = "";
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
					query += " where " + where;
				}
			    String groupby = "group by ";
			    for (String attrName: projection) {
			    	groupby += "\"" + attrName + "\", "; 
			    }
			    groupby = groupby.substring(0, groupby.length()-2);
			    query += groupby;
			    resultset = statement.executeQuery(query);
			    ArrayList<Integer> candidates = new ArrayList<Integer>();
			    HashSet<HashMap<String, Object>> visited = new HashSet<HashMap<String, Object>>();
			    while (resultset.next()) {
			    	HashMap<String, Object> tuple = new HashMap<String, Object>();
			    	for (int i = 2; i <= projection.size()+1; i++) {
			    		tuple.put(projection.get(i-2), resultset.getObject(i));
			    	}
			    	if (nondanglings.contains(tuple) && !visited.contains(tuple)) {
			    		candidates.add(resultset.getInt(1));
			    	}
			    }
			    Collections.sort(candidates);
			    int finalTupleRemoved = 0;
				int tupleRemoved = 0;
				int index = 0;
				while (index < candidates.size() && finalTupleRemoved < k) {
					finalTupleRemoved += 1;
					tupleRemoved += candidates.get(k);
				}
				if (finalTupleRemoved < k) {
					return -1;
				}
				else {
					return tupleRemoved;
				}
			}
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
	private int existsSubset(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, String smallestTable, HashMap<String, Object> constraints) throws SQLException {
		
		// keep track of the table with fewest attributes


		boolean existsSubset = false;
		String subsetTableName = null;
		
		// determine if there exists such table
		if (tables_attrs.get(smallestTable).size() == intersection.size()) {
			existsSubset = true;
			subsetTableName = smallestTable;
		}
		//System.out.println(tables_attrs);
		//System.out.println(attrs);
		if (existsSubset) {
			// Construct query groupby: attr_1, attr_2, ..., attr_x
			String groupby = "";
			for (String attrName: tables_attrs.get(subsetTableName)) {
				groupby += "\"" + attrName + "\", ";
			}
			groupby = groupby.substring(0, groupby.length()-2);
			
			// Construct query from: Table1 NATURAL JOIN Table2 ... NATURAL JOIN TableX
			String query_from = "";
			for (String key: tables_attrs.keySet()) {
				query_from += "\"" + key + "\" NATURAL JOIN ";
			}
			query_from = query_from.substring(0, query_from.length()-14);
			
			ResultSet subsetData;
			// sort in descending order
			if (constraints.size() == 0) {
				subsetData = statement.executeQuery("select count(*) " + " from " + query_from + " group by " + groupby + " order by count(*) desc");
			}
			else {
				String where = " where ";
				
				for (String attrName: constraints.keySet()) {
					where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
				}
			    where = where.substring(0, where.length()-5);
				
				//System.out.println("select count(*) " + " from " + query_from + where + " group by " + groupby + " order by count(*) desc");
				subsetData = statement.executeQuery("select count(*) " + " from " + query_from + where + " group by " + groupby + " order by count(*) desc");
			}
			
			
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
				// not enough data
				return -1;
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
	private ArrayList<HashSet<String>> dividable(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints) throws SQLException {
		ArrayList<HashSet<String>> groups = new ArrayList<HashSet<String>>();
		groups.add(new HashSet<String>());
		groups.add(new HashSet<String>());
		HashMap<String, Integer> attr_id = new HashMap<String, Integer>();

		
		int index = 0;
		for (String attr: attrs) {
			attr_id.put(attr, index);
			index += 1;
		}
		
		System.out.println(attr_id);
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
//		System.out.print("visited: ");
//		System.out.println(visited);
//	    System.out.println(groups);
		return groups;
	}
	
	private Integer decompose(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, ArrayList<HashSet<String>> groups, HashMap<String, Object> constraints, ArrayList<String> projection) throws SQLException {
		HashSet<String> group1 = groups.get(0);
		HashSet<String> group2 = groups.get(1);
		ArrayList<Integer> minK = new ArrayList<Integer>();
		boolean group1BooleanQuery = false;
		boolean group2BooleanQuery = false;
		int sizeOfGroup1InRelations = 0;
		int sizeOfGroup2InRelations = 1;
		
		ArrayList<String> attrs_group1 = new ArrayList<String>();
		for (String tableName: group1) {
			attrs_group1.addAll(tables_attrs.get(tableName));
		}
		int attrs1_size = attrs_group1.size();
		attrs_group1.removeAll(projection);
		if (attrs1_size == attrs_group1.size()) {     // non-boolean query	
			String queryGroup1 = "";
			for (String tableName: group1) {
				queryGroup1 += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
//					System.out.print("group1: ");
//					System.out.println(group1);
			if (projection.size() == 0) {
				queryGroup1 = "select * from "+ queryGroup1.substring(0, queryGroup1.length()-14);
			}
			else {
				String p = "distinct ";
				for (String attrName: projection) {
					p += "\"" + attrName + "\", ";
				}
				p = p.substring(0, p.length()-2);
				queryGroup1 = "select " + p + " from" + queryGroup1.substring(0, queryGroup1.length()-14);
			}
			if (constraints.size() != 0) {
				for (String attrName: constraints.keySet()) {
					where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
				}
			    where = where.substring(0, where.length()-5);
				queryGroup1 += " where " + where;
			}
//					System.out.print("queryGroup1: ");
//					System.out.println(queryGroup1);
			ResultSet rs = statement.executeQuery(queryGroup1);
			while (rs.next()) {
				sizeOfGroup1InRelations += 1;
			}			
		}
		else {				//  boolean query
			sizeOfGroup1InRelations = 1;
		}
		
		
		
		ArrayList<String> attrs_group2 = new ArrayList<String>();
		for (String tableName: group1) {
			attrs_group2.addAll(tables_attrs.get(tableName));
		}
		int attrs2_size = attrs_group2.size();
		attrs_group2.removeAll(projection);
		if (attrs2_size == attrs_group2.size()) {	
			String queryGroup2 = "";
			for (String tableName: group2) {
				queryGroup2 += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
			if (projection.size() == 0) {
				queryGroup2 = "select * from "+ queryGroup2.substring(0, queryGroup2.length()-14);
			}
			else {
				String p = "distinct ";
				for (String attrName: projection) {
					p += "\"" + attrName + "\", ";
				}
				p = p.substring(0, p.length()-2);
				queryGroup2 = "select " + p + " from " + queryGroup2.substring(0, queryGroup2.length()-14);
			}
			if (constraints.size() != 0) {
				for (String attrName: constraints.keySet()) {
					where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
				}
			    where = where.substring(0, where.length()-5);
				queryGroup2 += " where " + where;
			}
			
				
	//		System.out.print("group2: ");
	//		System.out.println(group2);
			System.out.print("queryGroup2: ");
			System.out.println(queryGroup2);
			
			ResultSet rs = statement.executeQuery(queryGroup2);
			while (rs.next()) {
				sizeOfGroup2InRelations += 1;
			}		
		}
		else {
			sizeOfGroup2InRelations = 1;
		}    
		
		for (int k1 = 0; k1 <= k; k1 ++) {
//			System.out.println(constraints);
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
	    				
	    			}
	    			for (String tableName: group2) {
	    				tables_attrs_2.put(tableName, tables_attrs.get(tableName));
	    			}
	    			for (ArrayList<String> attrNames: tables_attrs_1.values()) {
	    				attrs_1.addAll(attrNames);
	    			}
	    			for (ArrayList<String> attrNames: tables_attrs_2.values()) {
	    				attrs_2.addAll(attrNames);
	    			}	    			
//	    			System.out.println(constraints);
                    System.out.print("k: ");
                    System.out.println(k);
	    			System.out.print("k1: ");
	    			System.out.println(k1);
                    System.out.println(tables_attrs_1);
                    System.out.println(attrs_1);
	    			System.out.println(constraints);

	    			Integer group1Removed = setUp(k1, statement, tables_attrs_1, attrs_1, constraints, projection);
                    System.out.print("k: ");
                    System.out.println(k);
	    			System.out.print("k2: ");
	    			System.out.println(k2);
	    			System.out.println(tables_attrs_2);
                    System.out.println(attrs_2);
	    			System.out.println(constraints);
	    			Integer group2Removed = setUp(k2, statement, tables_attrs_2, attrs_2, constraints, projection);
	    			if (group1Removed == null || group2Removed == null) {
	    				return null;
	    			}
	    			if (group1Removed >= 0 && group2Removed >= 0) {
	    				minK.add(group1Removed + group2Removed);
	    			}
	    			System.out.println(constraints);
//	    			System.out.print("group1Removed: ");
//	    			System.out.println(group1Removed);
//	    			System.out.print("group2Removed: ");
//	    			System.out.println(group2Removed);

				}
			}			    			
		}
		
	    Collections.sort(minK);
	    if (minK.size() > 0) {
	    	return minK.get(0);
	    }
	    return -1;
	}
	
	private Integer commonAttrs(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, HashMap<String, Object> constraints, ArrayList<String> projection) throws SQLException {
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
		
//		System.out.println("select " + queryGroupby + " from " + queryFrom + " group by " + queryGroupby);

		// all values of common attributes
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
//		System.out.println(commonAttrsVals);
		HashMap<String, Object> newConstraints = new HashMap<String, Object>();
		if (constraints.size() != 0) {
			for (String key: constraints.keySet()) {
				newConstraints.put(key, constraints.get(key));
			}
		}
        
        
        HashMap<String, ArrayList<String>> new_tables_attrs = new HashMap<String, ArrayList<String>>();
        HashSet<String> new_attrs = new HashSet<String>();
        
        for (String tableName: tables_attrs.keySet()) {
            ArrayList<String> table_attrs = tables_attrs.get(tableName);
            ArrayList<String> new_table_attrs = new ArrayList<String>();
            for (String attrName: table_attrs) {
                if (!constraints.containsKey(attrName)) {
                    new_table_attrs.add(attrName);
                }
            }
            new_tables_attrs.put(tableName, new_table_attrs);
            
        }
        for (String attrName: attrs) {
            if (!constraints.containsKey(attrName)){
                new_attrs.add(attrName);
            }
        }
		ArrayList<String> new_projection = new ArrayList<String>();
		for (String attrName: projection) {
			if (!constraints.containsKey(attrName)) {
				new_projection.add(attrName);
			}
		}
        
		for (ArrayList<Object> commonAttrsVal: commonAttrsVals) {
			for (int i = 0; i < commonAttrs.length; i++) {
				newConstraints.put((String)commonAttrs[i], commonAttrsVal.get(i));
			}
            

            System.out.println(tables_attrs);
            System.out.println(attrs);
            System.out.println(projection);
            System.out.println(new_tables_attrs);
            System.out.println(new_attrs);
            System.out.println(new_projection);
			
		    ArrayList<Integer> row  = new ArrayList<Integer>();
		    for (int i = 0; i <= k; i++) {
   
		    	if (dp.size() == 0) {
		    		Integer j = setUp(i, statement, new_tables_attrs, new_attrs, newConstraints, projection);
		    		if (j == null) {
		    			return null;
		    		}
		    		if (j >= 0) {
		    			row.add(j);
		    		}
		    		else {
		    			row.add(-1);
		    		}
		    	}
		    	else {
		    		 ArrayList<Integer> candidates = new ArrayList<Integer>();
		    		 // the new possible common attributes values to remove j final tuples
		    		 for (int j = 0; j <= i; j ++) {
		    			 //System.out.println(i-j);
		    			 if (dp.get(commonCount-1).get(j) >= 0) {
		    				 int c = setUp(i-j, statement, new_tables_attrs, new_attrs, newConstraints, projection);
		    				 if (c >= 0) {
		    					 candidates.add(dp.get(commonCount-1).get(j) + c);
		    				 }
		    			 }
		    		 }
		    		 int minVal = Integer.MAX_VALUE;
		    		 for (int j = 0; j < candidates.size(); j ++) {
		    			 if (candidates.get(j) < minVal) {
		    				 minVal = candidates.get(j);
		    			 }
		    		 }
		    		 if (minVal == Integer.MAX_VALUE) {
		    			 row.add(-1);
		    		 }
		    		 else {
		    			 row.add(minVal);
		    		 }	 
		    	}
		    }
		    dp.add(row);
		    System.out.println(dp);
		    commonCount += 1;
		}
		System.out.print("abc");
		System.out.println(dp);
		
		return dp.get(dp.size()-1).get(k);
		
	}
	
	
	
	
	public static void main(String[] args) {
		// movielens is an example
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "postgres", "postgres")) {

			
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
			int k = 2;
			ArrayList<String> projection = new ArrayList<String>();
			projection.add("B");
			projection.add("C");
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
				resultset = statement.executeQuery("select * from \"" + key + "\" WHERE 1 < 0");
				ResultSetMetaData rsmd = resultset.getMetaData();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					tables_attrs.get(key).add(rsmd.getColumnName(i));
					attrs.add(rsmd.getColumnName(i));
					
				}
			}
			long start = System.currentTimeMillis();
			
			System.out.println(start);
			// start calculation
			PostgreSqlExample test = new PostgreSqlExample(k, statement, tables_attrs, attrs, projection);

		    System.out.println(System.currentTimeMillis() - start);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	
    }
    
}
