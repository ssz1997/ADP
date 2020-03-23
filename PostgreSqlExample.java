package example;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;


public class PostgreSqlExample {

	
	PostgreSqlExample(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, ArrayList<String> projection, boolean fullJoin) throws SQLException{
		Integer result = setUp(k, statement, tables_attrs, attrs, new HashMap<String, Object>(), projection, fullJoin);	
		System.out.println(result);
	}
	
	Integer setUp(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		
		if (k == 0) {
			return 0;
		}
		
		// if head is empty, test if triad; if not, networkflow
		if (!fullJoin && projection.isEmpty()) {
			ArrayList<String> endogenous = existTriad(tables_attrs);
			if (endogenous == null) {
				return null;
			}
			else {
				return booleanQuery(k, statement, tables_attrs, attrs, constraints);
			}
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
				int base_case_1 = this.onlyOneTable(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
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
			    	base_case_2 = this.existsSubset(k, statement, tables_attrs, attrs, intersection, smallestTable, constraints, projection, fullJoin);
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
					ArrayList<HashSet<String>> groups = dividable(statement, tables_attrs, attrs, constraints);
					if (groups.get(1).size() != 0) { 				// is dividable
						return decompose(k, statement, tables_attrs, attrs, groups, constraints, projection, fullJoin);						
					}
					else { // not dividable, goto general case 2
						if (!fullJoin) {
							ArrayList<String> intersectionCopy = new ArrayList<String>();
							intersectionCopy.addAll(intersection);
							intersectionCopy.retainAll(projection);
							if (intersection.size() != 0 && intersectionCopy.size() != 0) {
				    			return commonAttrs(k, statement, tables_attrs, attrs, intersection, constraints, projection, fullJoin); 
							}
							else {
								return null;
							}
						}
						else {
							if (intersection.size() != 0) {
								return commonAttrs(k, statement, tables_attrs, attrs, intersection, constraints, projection, fullJoin); 

							}
							else {
								return null;
							}
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
	

	private int onlyOneTable(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		// only 1 table
		//System.out.println(tables_attrs.size());
		if (tables_attrs.size() == 1) {
			if (fullJoin) {
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
				return k;
			}
			else {
				String query = "select count (*) ";		
				String tableName = "\"" + (String) tables_attrs.keySet().toArray()[0] + "\"";
				query += " from " + tableName;
				if (constraints.size() != 0) {
					String where = "";
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-4);
					query += " where " + where;
				}
			    String groupby = " group by ";
			    for (String attrName: projection) {
			    	groupby += "\"" + attrName + "\", "; 
			    }
			    groupby = groupby.substring(0, groupby.length()-2);
			    query += groupby;
			    query += " order by count(*)";
			    //System.out.println(query);
			    ResultSet resultset = statement.executeQuery(query);
			    int finalTupleRemoved = 0;
			    int tupleRemoved = 0;
			    while (resultset.next() && finalTupleRemoved < k ) {
			    	tupleRemoved += resultset.getInt(1);
			    	finalTupleRemoved += 1;
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
	private int existsSubset(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, String smallestTable, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		
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
			if (fullJoin) {
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
					//System.out.println("select count(*) " + " from " + query_from + " group by " + groupby + " order by count(*) desc");
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
			else if (projection.containsAll(tables_attrs.get(subsetTableName))){
				String query = "select distinct ";
				for (String attrName: projection) {
					query += "\"" + attrName + "\", ";
				}
				query = query.substring(0, query.length()-2);
				query += " from ";
				for (String tableName: tables_attrs.keySet()) {
					query += "\"" + tableName + "\" natural join ";
				}
				query = query.substring(0, query.length()-14);
				if (constraints.size() != 0) {
					String where = "";
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
					query += " where " + where;
				}
				//System.out.println(query);
				ResultSet resultset = statement.executeQuery(query);
				HashMap<ArrayList<Object>, Integer> count = new HashMap<ArrayList<Object>, Integer>();
				while (resultset.next()) {
					ArrayList<Object> vals = new ArrayList<Object>();
					for (int i = 0; i < tables_attrs.get(smallestTable).size(); i ++) {
						vals.add(resultset.getObject(tables_attrs.get(smallestTable).get(i)));
					}
					//System.out.println(vals);
					if (count.containsKey(vals)) {
						Integer c = count.get(vals) + 1;
					    count.put(vals, c);
					}
					else {
						count.put(vals, 1);
					}
				}
				//System.out.println(count);
				// find the tuples that contribute most
				int tupleRemoved = 0;
				int finalTupleRemoved = 0;
				ArrayList<Integer> remove = new ArrayList<Integer>(count.values());
				Collections.sort(remove, Collections.reverseOrder());
				//System.out.println(remove);
				while (finalTupleRemoved < k && tupleRemoved < remove.size()) {
					finalTupleRemoved += remove.get(tupleRemoved);
					tupleRemoved += 1;
				}
				if (finalTupleRemoved < k) {
					return -1;
				}
				else {
					return tupleRemoved;
				}
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
				//System.out.println(query);
				HashSet<HashMap<String, Object>> nondanglings = new HashSet<HashMap<String, Object>>();
				ResultSet resultset = statement.executeQuery(query);
				while (resultset.next()) {
					HashMap<String, Object> nondangling = new HashMap<String, Object>();
					for (int i = 1; i <= projection.size(); i++) {
						nondangling.put(projection.get(i-1), resultset.getObject(i));
					}
					nondanglings.add(nondangling);
				}
			    //System.out.println(nondanglings);
				// find all projection tuples
				query = "select count (*), ";
				for (String attrName: projection) {
					query += "\"" + attrName + "\", ";
				}
				query = query.substring(0, query.length()-2);
				String tableName = "\"" + (String) tables_attrs.keySet().toArray()[0] + "\"";
				query += " from " + tableName;
				if (constraints.size() != 0) {
					where = "";
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-4);
					query += " where " + where;
				}
			    String groupby = "group by ";
			    for (String attrName: projection) {
			    	groupby += "\"" + attrName + "\", "; 
			    }
			    groupby = groupby.substring(0, groupby.length()-2);
			    query += groupby;
			    //System.out.println(query);
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
			    //System.out.println(candidates);
			    int finalTupleRemoved = 0;
				int tupleRemoved = 0;
				int index = 0;
				while (index < candidates.size() && finalTupleRemoved < k) {
					tupleRemoved += candidates.get(finalTupleRemoved);
					finalTupleRemoved += 1;	
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
	 * Construct adjacency matrix for the attributes, then do BFS on one attribute. If there exists attributes not reachable,
	 * then the table containing that attribute can be considered separately. 
	 */
	private ArrayList<HashSet<String>> dividable(Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints) throws SQLException {
		ArrayList<HashSet<String>> groups = new ArrayList<HashSet<String>>();
		groups.add(new HashSet<String>());
		groups.add(new HashSet<String>());
		HashMap<String, Integer> attr_id = new HashMap<String, Integer>();

		
		int index = 0;
		for (String attr: attrs) {
			attr_id.put(attr, index);
			index += 1;
		}
		
		//System.out.println(attr_id);
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
	
	private Integer decompose(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, ArrayList<HashSet<String>> groups, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		HashSet<String> group1 = groups.get(0);
		HashSet<String> group2 = groups.get(1);
		int minK = Integer.MAX_VALUE;
		boolean group1Boolean = false;
		boolean group2Boolean = false;
		int sizeOfGroup1InRelations = 0; 
		int sizeOfGroup2InRelations = 0;
		
		ArrayList<String> attrs_group1 = new ArrayList<String>();
		for (String tableName: group1) {
			attrs_group1.addAll(tables_attrs.get(tableName));
		}
		int attrs1_size = attrs_group1.size();
		attrs_group1.removeAll(projection);
		if (attrs1_size != attrs_group1.size()) {     // non-boolean query	
			String queryGroup1 = "";
			for (String tableName: group1) {
				queryGroup1 += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
//					System.out.print("group1: ");
//					System.out.println(group1);
			if (fullJoin) {
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
					System.out.println(queryGroup1);
			ResultSet rs = statement.executeQuery(queryGroup1);
			while (rs.next()) {
				sizeOfGroup1InRelations += 1;
			}			
		}
		else {				//  boolean query
			sizeOfGroup1InRelations = 1;
			group1Boolean = true;
		}
		
		
		
		
		ArrayList<String> attrs_group2 = new ArrayList<String>();
		for (String tableName: group2) {
			attrs_group2.addAll(tables_attrs.get(tableName));
		}
		int attrs2_size = attrs_group2.size();
//		System.out.println(attrs_group2);
//		System.out.println("???");
//		System.out.println(projection);
		attrs_group2.removeAll(projection);
//		System.out.println(attrs_group2);
		if (attrs2_size != attrs_group2.size()) {	
			String queryGroup2 = "";
			for (String tableName: group2) {
				queryGroup2 += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
			if (fullJoin) {
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
//			System.out.print("queryGroup2: ");
//			System.out.println(queryGroup2);
			
			ResultSet rs = statement.executeQuery(queryGroup2);
			while (rs.next()) {
				sizeOfGroup2InRelations += 1;
			}		
		}
		else {
			sizeOfGroup2InRelations = 1;
			group2Boolean = true;
		}   
		
		HashMap<String, ArrayList<String>> tables_attrs_1 = new HashMap<>();
		HashMap<String, ArrayList<String>> tables_attrs_2 = new HashMap<>();
		HashSet<String> attrs_1 = new HashSet<String>();
		HashSet<String> attrs_2 = new HashSet<String>();
		ArrayList<String> projection_1 = new ArrayList<String>();
		ArrayList<String> projection_2 = new ArrayList<String>();
		
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
		for (String project: projection) {
			if (attrs_1.contains(project)) {
				projection_1.add(project);
			}
			else {
				projection_2.add(project);
			}
		}
		
		
		
		if (group1Boolean && group2Boolean) {
			if (k == 1) {
				//System.out.println(setUp(1, statement, tables_attrs_1, attrs_1, constraints, projection_1, fullJoin));
				//System.out.println(setUp(1, statement, tables_attrs_2, attrs_2, constraints, projection_2, fullJoin));
			    //System.out.println(tables_attrs_1, attrs_1, constraints, projection_1, fullJoin);
				return Math.min(setUp(1, statement, tables_attrs_1, attrs_1, constraints, projection_1, fullJoin), setUp(1, statement, tables_attrs_2, attrs_2, constraints, projection_2, fullJoin));
			}
			else {
				return -1;
			}
		}
		else if (group1Boolean) {
			int remove1 = Integer.MAX_VALUE;
			if (sizeOfGroup2InRelations >= k) {
				remove1 = setUp(1, statement, tables_attrs_1, attrs_1, constraints, projection_1, fullJoin);
			}
			int remove2 = setUp(k, statement, tables_attrs_2, attrs_2, constraints, projection_2, fullJoin);
			if (remove2 < 0 && remove1 == Integer.MAX_VALUE) {
				return -1;
			}
			if (remove2 > 0 && remove1 == Integer.MAX_VALUE) {
				return remove2;
			}
			if (remove2 < 0 && remove1 < Integer.MAX_VALUE) {
				return remove1;
			}
			if (remove2 > 0 && remove1 < Integer.MAX_VALUE) {
				return Math.min(remove1, remove2);
			}
			
		}
		else if (group2Boolean) {
			int remove2 = Integer.MAX_VALUE;
			if (sizeOfGroup1InRelations >= k) {
				remove2 = setUp(1, statement, tables_attrs_2, attrs_2, constraints, projection_2, fullJoin);
			}
			System.out.println(tables_attrs_2);
			System.out.println(attrs_2);
			System.out.println(constraints);
			System.out.println(projection_2);
			int remove1 = setUp(k, statement, tables_attrs_1, attrs_1, constraints, projection_1, fullJoin);
			if (remove1 < 0 && remove2 == Integer.MAX_VALUE) {
				return -1;
			}
			if (remove1 > 0 && remove2 == Integer.MAX_VALUE) {
				return remove1;
			}
			if (remove1 < 0 && remove2 < Integer.MAX_VALUE) {
				return remove2;
			}
			if (remove1 > 0 && remove2 < Integer.MAX_VALUE) {
				return Math.min(remove1, remove2);
			}
		}
		
		
		for (int k1 = 0; k1 <= k; k1 ++) {
//			System.out.println(constraints);
			for (int k2 = 0; k2 <= k; k2 ++) {
				// select count(*) from join gropu1/group2
				if (k1*sizeOfGroup2InRelations + k2*sizeOfGroup1InRelations - k1*k2 < k) {
					continue;
				}
				else {
                    
					
//	    			System.out.println(constraints);
//                    System.out.print("k: ");
//                    System.out.println(k);
//	    			System.out.print("k1: ");
//	    			System.out.println(k1);
//                    System.out.println(tables_attrs_1);
//                    System.out.println(attrs_1);
//                    System.out.println(projection_1);
//	    			System.out.println(constraints);
//	    		    System.out.println(sizeOfGroup1InRelations);
//	    		    System.out.println(sizeOfGroup2InRelations);
	    			Integer group1Removed = setUp(k1, statement, tables_attrs_1, attrs_1, constraints, projection_1, fullJoin);
//                    System.out.print("k: ");
//                    System.out.println(k);
//	    			System.out.print("k2: ");
//	    			System.out.println(k2);
//	    			System.out.println(tables_attrs_2);
//                    System.out.println(attrs_2);
//                    System.out.println(projection_2);
//	    			System.out.println(constraints);
	    			Integer group2Removed = setUp(k2, statement, tables_attrs_2, attrs_2, constraints, projection_2, fullJoin);
	    			if (group1Removed == null || group2Removed == null) {
	    				return null;
	    			}
	    			if (group1Removed >= 0 && group2Removed >= 0 && group1Removed + group2Removed < minK) {
	    				minK = group1Removed + group2Removed;
	    			}
//	    			System.out.println(constraints);
//	    			System.out.print("group1Removed: ");
//	    			System.out.println(group1Removed);
//	    			System.out.print("group2Removed: ");
//	    			System.out.println(group2Removed);

				}
			}			    			
		}
		
	    if (minK != Integer.MAX_VALUE) {
	    	return minK;
	    }
	    return -1;
	}
	
	private Integer commonAttrs(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
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
	
		
		String query = "select " + queryGroupby + " from " + queryFrom;

		// all values of common attributes
		
		if (constraints.size() != 0) {
			String queryWhere = " where ";
			for (String attrName: constraints.keySet()) {
				queryWhere += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
			}
		    queryWhere = queryWhere.substring(0, queryWhere.length()-5);
		    query += queryWhere;
		}
		query += " group by " + queryGroupby;
		System.out.println(query);
		ResultSet tableData = statement.executeQuery(query);
		ArrayList<ArrayList<Object>> commonAttrsVals = new ArrayList<ArrayList<Object>>();
		int commonCount = 0;
		while (tableData.next()) {
			ArrayList<Object> commonAttrVal = new ArrayList<Object>();
			for (int i = 0; i < commonAttrs.length; i++) {
				  commonAttrVal.add(tableData.getObject((String)commonAttrs[i]));
			}	
			commonAttrsVals.add(commonAttrVal);
		}
		//System.out.println(commonAttrsVals);
		
        
		for (ArrayList<Object> commonAttrsVal: commonAttrsVals) {
			System.out.print("commonAttrsVal: ");
			System.out.println(commonAttrsVal);
			
			HashMap<String, Object> newConstraints = new HashMap<String, Object>();
			if (constraints.size() != 0) {
				for (String key: constraints.keySet()) {
					newConstraints.put(key, constraints.get(key));
				}
			}     
			for (int i = 0; i < commonAttrs.length; i++) {
				newConstraints.put((String)commonAttrs[i], commonAttrsVal.get(i));
			}
	        
	        HashMap<String, ArrayList<String>> new_tables_attrs = new HashMap<String, ArrayList<String>>();
	        HashSet<String> new_attrs = new HashSet<String>();
	        
	        for (String tableName: tables_attrs.keySet()) {
	            ArrayList<String> table_attrs = tables_attrs.get(tableName);
	            ArrayList<String> new_table_attrs = new ArrayList<String>();
	  
            	for (String attrName: table_attrs) {
	                if (!newConstraints.containsKey(attrName)) {
	                    new_table_attrs.add(attrName);
	                }
	            }
	            new_tables_attrs.put(tableName, new_table_attrs);
            
	        }
	        for (String attrName: attrs) {
	            if (!newConstraints.containsKey(attrName)){
	                new_attrs.add(attrName);
	            }
	        }
	        ArrayList<String> new_projection = new ArrayList<String>();
	        if (!fullJoin) {
				for (String attrName: projection) {
					if (!newConstraints.containsKey(attrName)) {
						new_projection.add(attrName);
					}
				}
	        }
	        else {
	        	new_projection = projection;
	        }
			
			
			
			
            

            System.out.println(tables_attrs);
            System.out.println(attrs);
            System.out.println(projection);
            System.out.println(constraints);
            System.out.println(new_tables_attrs);
            System.out.println(new_attrs);
            System.out.println(new_projection);
            System.out.println(newConstraints);
			
		    ArrayList<Integer> row  = new ArrayList<Integer>();
		    for (int i = 0; i <= k; i++) {
		    	if (dp.size() == 0) {
		    		Integer j = setUp(i, statement, new_tables_attrs, new_attrs, newConstraints, new_projection, fullJoin);
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
		    				 System.out.println(new_tables_attrs);
		    				 System.out.println(new_attrs);
		    				 System.out.println(newConstraints);
		    				 System.out.println(new_projection);
		    				 int c = setUp(i-j, statement, new_tables_attrs, new_attrs, newConstraints, new_projection, fullJoin);
		    				 if (c >= 0) {
		    					 candidates.add(dp.get(commonCount-1).get(j) + c);
		    				 }
		    			 }
		    		 }
		    		 System.out.print("bpcandidates: ");
		    		 System.out.println(candidates);
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
		    commonCount += 1;
		    System.out.println(dp);
		}
		
		
		return dp.get(dp.size()-1).get(k);
		
	}
	
	private ArrayList<String> existTriad(HashMap<String, ArrayList<String>> tables_attrs) {
		ArrayList<String> endogenous = new ArrayList<String>();
		ArrayList<String> nonendogenous = new ArrayList<String>();
		
		// find endogenous/nonendogenous relations
		int currentTableSize = 0;
		while (endogenous.size() + nonendogenous.size() != tables_attrs.size()) {
			String smallestTable = "";
			int smallestTableSize = Integer.MAX_VALUE;
			for (String tableName: tables_attrs.keySet()) {
				if (tables_attrs.get(tableName).size() >= currentTableSize && tables_attrs.get(tableName).size() < smallestTableSize && !endogenous.contains(tableName) && !nonendogenous.contains(tableName)) {
					smallestTable = tableName;
					smallestTableSize = tables_attrs.get(tableName).size();
				}
			}
			
			ArrayList<String> attrsSmallest = tables_attrs.get(smallestTable);
			Collections.sort(attrsSmallest);
			if (!nonendogenous.contains(smallestTable)) {
				Iterator ite = tables_attrs.keySet().iterator();
				while (ite.hasNext()) {
					String tableName = (String) ite.next();
					if (!tableName.equals(smallestTable)) {
						ArrayList<String> attrs2 = tables_attrs.get(tableName);
						Collections.sort(attrs2);
						if (attrs2.equals(attrsSmallest)) {
							ite.remove();
						}
						else if (tables_attrs.get(tableName).size() >= smallestTableSize) {
							if (attrs2.containsAll(attrsSmallest) && !nonendogenous.contains(tableName)) {
								nonendogenous.add(tableName);
							}
						}
					}
					
				}
				endogenous.add(smallestTable);
			}	
			currentTableSize = smallestTableSize;
		}
		//System.out.println(nonendogenous);
		
		// find triad
		for (int i = 0; i < endogenous.size(); i++) {
			for (int j = i+1; j < endogenous.size(); j++) {
				for (int k = j+1; k < endogenous.size(); k++) {
					// i to j
					ArrayList<String> queue = new ArrayList<String>();
					queue.add(endogenous.get(i));
					HashSet<String> visited = new HashSet<String>();
					visited.add(endogenous.get(i));
					while (!queue.isEmpty()) {
						//System.out.println(queue);
						ArrayList<String> attr_base = new ArrayList<String>();
						attr_base.addAll(tables_attrs.get(queue.get(0)));
						attr_base.removeAll(tables_attrs.get(endogenous.get(k)));
						for (String tableName: tables_attrs.keySet()) {
							ArrayList<String> attr_compare = new ArrayList<String>();
							attr_compare.addAll(tables_attrs.get(tableName));
							int size = attr_compare.size();
							attr_compare.removeAll(attr_base);
							if (attr_compare.size() != size && !visited.contains(tableName)) {
								queue.add(tableName);
								visited.add(tableName);
							}
						}
						queue.remove(0);
					}
					//System.out.println(visited);
					if (!visited.contains(endogenous.get(j))) {
						//System.out.println("?");
						continue;
					}
							
					// i to k
					queue.clear();
					queue.add(endogenous.get(i));
					visited.clear();
					visited.add(endogenous.get(i));
					while (!queue.isEmpty()) {
						ArrayList<String> attr_base = new ArrayList<String>();
						attr_base.addAll(tables_attrs.get(queue.get(0)));
						attr_base.removeAll(tables_attrs.get(endogenous.get(j)));
						for (String tableName: tables_attrs.keySet()) {
							ArrayList<String> attr_compare = new ArrayList<String>();
							attr_compare.addAll(tables_attrs.get(tableName));
							int size = attr_compare.size();
							attr_compare.removeAll(attr_base);
							if (attr_compare.size() != size && !visited.contains(tableName)) {
								queue.add(tableName);
								visited.add(tableName);
							}
						}
						queue.remove(0);
					}
					//System.out.println(visited);
					if (!visited.contains(endogenous.get(k))) {
						continue;
					}
	
					//j to k
					queue.clear();
					queue.add(endogenous.get(j));
					visited.clear();
					visited.add(endogenous.get(j));
					while (!queue.isEmpty()) {
						ArrayList<String> attr_base = new ArrayList<String>();
						attr_base.addAll(tables_attrs.get(queue.get(0)));
						attr_base.removeAll(tables_attrs.get(endogenous.get(i)));
						for (String tableName: tables_attrs.keySet()) {
							ArrayList<String> attr_compare = new ArrayList<String>();
							attr_compare.addAll(tables_attrs.get(tableName));
							int size = attr_compare.size();
							attr_compare.removeAll(attr_base);
							if (attr_compare.size() != size && !visited.contains(tableName)) {
								queue.add(tableName);
								visited.add(tableName);
							}
						}
						queue.remove(0);
					}
					//System.out.println(visited);
					//System.out.println(endogenous.get(k));
					if (visited.contains(endogenous.get(k))) { 			// find triad
						System.out.println("triad");
						return null;
					}
				}
			}
		}
		return endogenous;	
	}
	
	private int booleanQuery (int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints) throws SQLException{
		if (k > 1) {
			return -1;
		}
		ArrayList<HashSet<String>> groups = dividable(statement, tables_attrs, attrs, constraints);
		if (groups.get(1).size() != 0) {    // dividable
			HashSet<String> group1 = groups.get(0);
			HashMap<String, ArrayList<String>> tables_attrs_1 = new HashMap<String, ArrayList<String>>();
			HashSet<String> attrs_1 = new HashSet<String>();
			HashMap<String, Object> constraints_1 = new HashMap<String, Object>();
			for (String tableName: group1) {
				tables_attrs_1.put(tableName, tables_attrs.get(tableName));
			}
			for (ArrayList<String> attr: tables_attrs_1.values()) {
				attrs_1.addAll(attr);
			}
			HashSet<String> group2 = groups.get(1);
			HashMap<String, ArrayList<String>> tables_attrs_2 = new HashMap<String, ArrayList<String>>();
			HashSet<String> attrs_2 = new HashSet<String>();
			HashMap<String, Object> constraints_2= new HashMap<String, Object>();
			for (String tableName: group2) {
				tables_attrs_2.put(tableName, tables_attrs.get(tableName));
			}
			for (ArrayList<String> attr: tables_attrs_2.values()) {
				attrs_2.addAll(attr);
			}
			return Math.min(booleanQuery(k, statement, tables_attrs_1, attrs_1, constraints), booleanQuery(k, statement, tables_attrs_2, attrs_2, constraints));
		}
		else {		// not dividable, base case
			
			if (tables_attrs.size() == 1) {				// only one table - remove all
				String query = "select count(*) from ";
				String tableName = "\"" + (String) tables_attrs.keySet().toArray()[0] + "\"";
				query += tableName;
				if (constraints.size() != 0) {
					String where = " where ";
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
					query += where;
				}
				System.out.println(query);
				ResultSet rs = statement.executeQuery(query);
				rs.next();
				
				return rs.getInt(1);
			}

		    ArrayList<String> tableNames = new ArrayList<String>(tables_attrs.keySet());
		    ArrayList<String> order = new ArrayList<String>();
		    HashMap<String, HashSet<Integer>> attrPos = new HashMap<String, HashSet<Integer>>();
		    order.add(tableNames.get(0));
		    
		    for (String attr: tables_attrs.get(tableNames.get(0))) {
		    	HashSet<Integer> pos = new HashSet<Integer>();
		    	pos.add(0);
		    	pos.add(1);
		    	attrPos.put(attr, pos);
		    }
		    tableNames.remove(0);
		    
			while (tableNames.size() != 0) {
				String tableName = tableNames.get(0);
				ArrayList<Integer> tablePos = new ArrayList<Integer>();
				for (int i = 0; i < tables_attrs.size(); i++) {
					tablePos.add(i);
				}
				for (String attr: tables_attrs.get(tableName)) {
					if (attrPos.containsKey(attr)) {
						tablePos.retainAll(attrPos.get(attr));
					}
				}
				if (tablePos.size() != tables_attrs.size()) {
					order.add(tablePos.get(0), tableName);
				}
				else {
					order.add(tableName);
				}
				int pos = order.indexOf(tableName);
				for (String attrName: tables_attrs.get(tableName)) {
					if (attrPos.containsKey(attrName)) {
						HashSet<Integer> attrP = attrPos.get(attrName);
						if (pos > 0) {
							attrP.add(pos - 1);
						}
						attrP.add(pos);
						if (pos < tables_attrs.size() - 1) {
							attrP.add(pos + 1);
						}
					}	
					else {
						HashSet<Integer> attrP = new HashSet<Integer>();
						if (pos > 0) {
							attrP.add(pos - 1);
						}
						attrP.add(pos);
						if (pos < tables_attrs.size() - 1) {
							attrP.add(pos + 1);
						}
						attrPos.put(attrName, attrP);
					}
				}
				tableNames.remove(tableName);
			}
			
			ArrayList<ArrayList<String>> intersections = new ArrayList<ArrayList<String>>();
			for (int i = 0; i < order.size() - 1; i++) {
				ArrayList<String> attrs_1_copy = new ArrayList<String>();
				attrs_1_copy.addAll(tables_attrs.get(order.get(i)));
				ArrayList<String> attrs_2 = tables_attrs.get(order.get(i+1));
				attrs_1_copy.retainAll(attrs_2);
				intersections.add(attrs_1_copy);
			}
			
			ArrayList<ArrayList<ArrayList<Object>>> vertices = new ArrayList<ArrayList<ArrayList<Object>>>();
			int numOfVertices = 2;
			for (int i = 0; i < intersections.size(); i++) {
				ArrayList<ArrayList<Object>> v = new ArrayList<ArrayList<Object>>();
				String query = "select distinct ";
				for (String attrName: intersections.get(i)) {
					query += "\"" + attrName + "\", ";
				}
				query = query.substring(0, query.length()-2);
				query += " from " + "\"" + order.get(i) + "\"";
				if (constraints.size() != 0) {
					String where = " where ";
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
					query += where;
				}
				ResultSet rs = statement.executeQuery(query);
				while (rs.next()) {
					numOfVertices += 1;
					ArrayList<Object> vv = new ArrayList<Object>();
					for (int j = 1; j <= intersections.get(i).size(); j ++) {
						vv.add(rs.getObject(j));
					}
					v.add(vv);
				}
				vertices.add(v);
			}
			
			ArrayList<ArrayList<Integer>> graph = new ArrayList<ArrayList<Integer>>();
			int counter1 = 1;
			int counter2 = 1;
			// source
			String query = "select ";
		    for (String attrName: intersections.get(0)) {
		    	query += "\"" + attrName + "\", ";
		    }
			query = query.substring(0, query.length()-2);
			query += " from " + "\"" + order.get(0) + "\"";
			if (constraints.size() != 0) {
				String where = " where ";
				for (String attrName: constraints.keySet()) {
					where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
				}
			    where = where.substring(0, where.length()-5);
				query += where;
			}
			ResultSet rs = statement.executeQuery(query);
			ArrayList<Integer> source = new ArrayList<Integer>();
			while (source.size() < numOfVertices) {
				source.add(0);
			}
			while (rs.next()) {
				ArrayList<Object> data = new ArrayList<Object>();
				for (int i = 1; i <= intersections.get(0).size(); i++) {
					data.add(rs.getObject(i));
				}
				int pos = vertices.get(0).indexOf(data) + counter1;
				int c = source.get(pos);
				c += 1;
				source.set(pos, c);
			}
			graph.add(source);
			while (graph.size() != numOfVertices - 1) {
				ArrayList<Integer> vertex = new ArrayList<Integer>();
				while (vertex.size() < numOfVertices) {
					vertex.add(0);
				}
				graph.add(vertex);
			}
			// middle part
			for (int i = 1; i < intersections.size(); i ++) {
				counter2 = counter1 + vertices.get(i-1).size();
				query = "select ";
				for (String attrName: intersections.get(i-1)){
					query += "\"" + attrName + "\", ";
				}
				for (String attrName: intersections.get(i)) {
					query += "\"" + attrName + "\", ";
				}
				query = query.substring(0, query.length()-2);
				query += " from " + "\"" + order.get(i) + "\"";
				//System.out.println(query);
				rs = statement.executeQuery(query);
				while (rs.next()) {
					ArrayList<Object> l = new ArrayList<Object>();
					ArrayList<Object> r = new ArrayList<Object>();
					for (int j = 1; j <= intersections.get(i-1).size(); j++) {
						l.add(rs.getObject(j));
					}
					for (int j = intersections.get(i-1).size() + 1; j <= intersections.get(i-1).size() + intersections.get(i).size(); j++) {
						r.add(rs.getObject(j));
					}
					int lPos = vertices.get(i-1).indexOf(l) + counter1;
					int rPos = vertices.get(i).indexOf(r) + counter2;
					if (lPos < counter1 || rPos < counter2) {
						continue;
					}
//					System.out.println(l);
//					System.out.println(r);
//					System.out.println(lPos);
//					System.out.println(rPos);
					ArrayList<Integer> vertex = graph.get(lPos);
					int c = vertex.get(rPos);
					c += 1;
					vertex.set(rPos, c);
					graph.set(lPos, vertex);
					
				}
				counter1 += vertices.get(i-1).size();
				//System.out.println(graph);
			}
			
			// sink
			query = "select ";
		    for (String attrName: intersections.get(intersections.size()-1)) {
		    	query += "\"" + attrName + "\", ";
		    }
			query = query.substring(0, query.length()-2);
			query += " from " + "\"" + order.get(order.size()-1) + "\"";
			rs = statement.executeQuery(query);
			//System.out.println(query);
		
			while (rs.next()) {
				ArrayList<Object> data = new ArrayList<Object>();
				for (int i = 1; i <= intersections.get(intersections.size()-1).size(); i++) {
					data.add(rs.getObject(i));
				}
				int pos = vertices.get(vertices.size()-1).indexOf(data) + counter1;
				if (pos < counter1) {
					continue;
				}
				ArrayList<Integer> l = graph.get(pos);
				int c = l.get(l.size()-1) + 1;
				l.set(l.size()-1, c);
				graph.set(pos, l);
			}
			ArrayList<Integer> sink = new ArrayList<Integer>();
			while (sink.size() != numOfVertices) {
				sink.add(0);
			}
			graph.add(sink);
			Integer[][] graph_arr = new Integer[graph.size()][graph.get(0).size()];
			for (int i = 0; i < graph.size(); i++) {
				ArrayList<Integer> row = graph.get(i);
				graph_arr[i] = row.toArray(new Integer[numOfVertices]);
			}
			return ff(numOfVertices, graph_arr);
		}
		
		
		
	}
	
	private int ff(int vertices, Integer[][] graph) {
		int source = 0;
		int sink = vertices-1;
		int[][] residualGraph = new int[vertices][vertices];

        //initialize residual graph same as original graph
        for (int i = 0; i <vertices ; i++) {
            for (int j = 0; j <vertices ; j++) {
                residualGraph[i][j] = graph[i][j];
            }
        }

        //initialize parent [] to store the path Source to destination
        int [] parent = new int[vertices];

        int max_flow = 0; //initialize the max flow

        while(isPathExist_BFS(residualGraph, source, sink, parent, vertices)){
            //if here means still path exist from source to destination

            //parent [] will have the path from source to destination
            //find the capacity which can be passed though the path (in parent[])

            int flow_capacity = Integer.MAX_VALUE;

            int t = sink;
            while(t!=source){
                int s = parent[t];
                flow_capacity = Math.min(flow_capacity, residualGraph[s][t]);
                t = s;
            }

            //update the residual graph
            //reduce the capacity on fwd edge by flow_capacity
            //add the capacity on back edge by flow_capacity
            t = sink;
            while(t!=source){
                int s = parent[t];
                residualGraph[s][t]-=flow_capacity;
                residualGraph[t][s]+=flow_capacity;
                t = s;
            }

            //add flow_capacity to max value
            max_flow+=flow_capacity;
        }
        return max_flow;
	}
	
	private boolean isPathExist_BFS(int [][] residualGraph, int src, int dest, int [] parent, int vertices){
        boolean pathFound = false;

        //create visited array [] to
        //keep track of visited vertices
        boolean [] visited = new boolean[vertices];

        //Create a queue for BFS
        Queue<Integer> queue = new LinkedList<>();

        //insert the source vertex, mark it visited
        queue.add(src);
        parent[src] = -1;
        visited[src] = true;

        while(queue.isEmpty()==false){
            int u = queue.poll();

            //visit all the adjacent vertices
            for (int v = 0; v <vertices ; v++) {
                //if vertex is not already visited and u-v edge weight >0
                if(visited[v]==false && residualGraph[u][v]>0) {
                    queue.add(v);
                    parent[v] = u;
                    visited[v] = true;
                }
            }
        }
        //check if dest is reached during BFS
        pathFound = visited[dest];
        return pathFound;
    }

	
	
	
	
	public static void main(String[] args) {

		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/basecase2", "postgres", "postgres")) {
			
			// initialize k
			int k = 4;
			ArrayList<String> projection = new ArrayList<String>();
			projection.add("A");
			projection.add("B");


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
			
			//System.out.println(start);
			// start calculation
			boolean fullJoin;
			if (projection.size() < attrs.size()) {
				fullJoin = false;
			}
			else {
				fullJoin = true;
			}
			PostgreSqlExample test = new PostgreSqlExample(k, statement, tables_attrs, attrs, projection, fullJoin);

		    //System.out.println(System.currentTimeMillis() - start);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	
    }
    
}
