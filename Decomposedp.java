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


public class Decomposedp {

	
	Decomposedp(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException{
		HashMap<String, ArrayList<HashMap<String, Object>>> result = setUp(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);	
		System.out.println(result);
		
		
		System.out.println(num(result));
	}
	
	private HashMap<String, ArrayList<HashMap<String, Object>>> setUp(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		
		if (k == 0) {
			return new HashMap<String, ArrayList<HashMap<String, Object>>>();
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
				HashMap<String, ArrayList<HashMap<String, Object>>> base_case_1 = onlyOneTable(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
				// base case 2
				HashMap<String, ArrayList<HashMap<String, Object>>> base_case_2 = null;
				// general case 1
				int general_case_1 = Integer.MIN_VALUE;
				// general case 2
				int general_case_2 = Integer.MIN_VALUE;
				
				if (base_case_1 == null) {
					// not enough data for k
					return null;
				}
				else if (base_case_1.keySet().toArray()[0] == null) {
					// next base case
			    	base_case_2 = this.existsSubset(k, statement, tables_attrs, attrs, intersection, smallestTable, constraints, projection, fullJoin);
				}
				else {
					return base_case_1;
				}
				
				if (base_case_2 == null) {
					// not enough data for k
					return null;
				}
				else if (base_case_2.keySet().toArray()[0] == null) {
					// goto general case 1
					ArrayList<HashSet<String>> groups = dividable(tables_attrs, attrs);
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
								System.out.println("np-hard");
								
								return hard(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
							}
						}
						else {
							if (intersection.size() != 0) {
								return commonAttrs(k, statement, tables_attrs, attrs, intersection, constraints, projection, fullJoin); 

							}
							else {
								System.out.println("np-hard");
								return hard(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
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
	

	private HashMap<String, ArrayList<HashMap<String, Object>>> onlyOneTable(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		// only 1 table
		//System.out.println(tables_attrs.size());
		ArrayList<HashMap<String, Object>> ret = new ArrayList<HashMap<String, Object>>();
		if (tables_attrs.size() == 1) {
			if (fullJoin) {
				// get number of rows
				String tableName = "\"" + (String) tables_attrs.keySet().toArray()[0] + "\"";
				ResultSet resultset;
				
				String query = "";
				String select = "select ";
				for (String attrName: attrs) {
					select += "\"" + attrName + "\", ";
				}
				select = select.substring(0, select.length()-2);
				query += select + " from " + tableName;
				if (constraints.size() != 0) {
					String where = " where ";
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
				    query += where;
				}   
				resultset = statement.executeQuery(query);
				//System.out.println(query);
	
				int tupleRemoved = 0;
				while (resultset.next() && tupleRemoved < k) {
					HashMap<String, Object> tup = new HashMap<String, Object>();
					for (String attrName: attrs) {
						tup.put(attrName, resultset.getObject(attrName));
					}
					if (constraints.size() != 0) {
						for (String attrName: constraints.keySet()) {
							tup.put(attrName, constraints.get(attrName));
						}
					}
					ret.add(tup);
					tupleRemoved += 1;
				}
				
				// not enough data
				if (tupleRemoved < k) {
					return null;		
				}
				//System.out.println(ret);
				HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
				r.put((String)tables_attrs.keySet().toArray()[0], ret);
				return r;
			}
			else {
				String query = "select count (*), ";	
				for (String attrName: projection) {
					query += "\"" + attrName + "\", ";
				}
				query = query.substring(0, query.length()-2);
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
			    query += " order by count(*) desc";
			    //System.out.println(query);
			    ResultSet resultset = statement.executeQuery(query);
			    int finalTupleRemoved = 0;
			    while (resultset.next() && finalTupleRemoved < k ) {
			    	HashMap<String, Object> tup = new HashMap<String, Object>();
			    	int tupleRemoved = resultset.getInt(1);
			    	finalTupleRemoved += 1;
			    	for (String attrName: attrs) {
			    		if (!projection.contains(attrName)) {
			    			tup.put(attrName, attrName);
			    		}
			    		else {
			    			tup.put(attrName, resultset.getObject(attrName));
			    		}
					}
			    	if (constraints.size() != 0) {
						for (String attrName: constraints.keySet()) {
							tup.put(attrName, constraints.get(attrName));
						}
					}
			    	tup.put("number", tupleRemoved);
					ret.add(tup);
			    }
			    if (finalTupleRemoved < k) {
			    	return null;
			    }
			    else {
			    	HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
					r.put((String)tables_attrs.keySet().toArray()[0], ret);
					return r;
			    }			
			}
		}
		
		else {
			HashMap<String, ArrayList<HashMap<String, Object>>> not_applicable = new HashMap<String, ArrayList<HashMap<String, Object>>>();
			not_applicable.put(null, null);
			return not_applicable;
		}
	}
	
	/*
	 * The attributes in each table are put into sets. Find the intersection of all these sets.
	 * If the # of attributes in the table with fewest attributes is the same as the size of the intersection, 
	 * then all attributes in this table appear in every other tables.
	 */
	private HashMap<String, ArrayList<HashMap<String, Object>>> existsSubset(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, String smallestTable, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		ArrayList<HashMap<String, Object>> ret = new ArrayList<HashMap<String, Object>>();
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
			ArrayList<String> intersectionCopy = new ArrayList<String>();
			for (String i: intersection) {
				intersectionCopy.add(i);
			}
			intersectionCopy.retainAll(projection);
			if (intersectionCopy.size() == 0) {
				return null;
			}
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
					subsetData = statement.executeQuery("select count(*), " + groupby + " from " + query_from + " group by " + groupby + " order by count(*) desc");
				}
				else {
					String where = " where ";
					
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
					
					//System.out.println("select count(*) " + " from " + query_from + where + " group by " + groupby + " order by count(*) desc");
					subsetData = statement.executeQuery("select count(*), " + groupby + " from " + query_from + where + " group by " + groupby + " order by count(*) desc");
				}
				
				
				int finalTupleRemoved = 0;
				int tupleRemoved = 0;
				
				while (subsetData.next() && finalTupleRemoved < k) {
					HashMap<String, Object> tup = new HashMap<String, Object>();
					finalTupleRemoved += subsetData.getInt(1);
					tupleRemoved += 1;
					for (String attrName: tables_attrs.get(smallestTable)) {
						tup.put(attrName, subsetData.getObject(attrName));
					}
					if (constraints.size() != 0) {
						for (String attrName: constraints.keySet()) {
							tup.put(attrName, constraints.get(attrName));
						}
					}
					ret.add(tup);
				}
				
				if (finalTupleRemoved >= k) {
					HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
					r.put(smallestTable, ret);
					return r;
				}
				else {
					// not enough data
					return null;
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
				HashMap<HashMap<String, Object>, Integer> count = new HashMap<HashMap<String, Object>, Integer>();
				while (resultset.next()) {
					HashMap<String, Object> vals = new HashMap<String, Object>();
					for (String attrName: tables_attrs.get(smallestTable)) {
						vals.put(attrName, resultset.getObject(attrName));
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
				HashMap<String, Object> tup = new HashMap<String, Object>();
				while (finalTupleRemoved < k && tupleRemoved < remove.size()) {
					for (HashMap<String, Object> toRemove: count.keySet()) {
						if (count.get(toRemove) == remove.get(tupleRemoved)) {						
							tup = toRemove;						
							break;
						}
					}
					
					finalTupleRemoved += remove.get(tupleRemoved);
					tupleRemoved += 1;
					count.remove(tup);
					if (constraints.size() != 0) {
						for (String attrName: constraints.keySet()) {
							tup.put(attrName, constraints.get(attrName));
						}
					}
					ret.add(tup);

				}
				if (finalTupleRemoved < k) {
					return null;
				}
				else {
					HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
					r.put(smallestTable, ret);
					return r;
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
				ArrayList<HashMap<String, Object>> nondanglings = new ArrayList<HashMap<String, Object>>();
				ResultSet resultset = statement.executeQuery(query);
				while (resultset.next()) {
					HashMap<String, Object> nondangling = new HashMap<String, Object>();
					for (String attrName: tables_attrs.get(smallestTable)) {
						if (!projection.contains(attrName)) {
							nondangling.put(attrName, attrName);
						}
						else {
							nondangling.put(attrName, resultset.getObject(attrName));
						}
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
			    String groupby = " group by ";
			    for (String attrName: projection) {
			    	groupby += "\"" + attrName + "\", "; 
			    }
			    groupby = groupby.substring(0, groupby.length()-2);
			    query += groupby;
			    //System.out.println(query);
			    resultset = statement.executeQuery(query);
			    HashMap<HashMap<String, Object>, Integer> candidates = new HashMap<HashMap<String, Object>, Integer>();
			    while (resultset.next()) {
			    	HashMap<String, Object> tuple = new HashMap<String, Object>();
			    	for (String attrName: tables_attrs.get(smallestTable)) {
			    		if (!projection.contains(attrName)) {
			    			tuple.put(attrName, attrName);
			    		}
			    		else {
			    			tuple.put(attrName, resultset.getObject(attrName));
			    		}
			    		
			    	}
			    	
			    	if (nondanglings.contains(tuple)) {
			    		candidates.put(tuple, resultset.getInt(1));
			    		
			    	}
			    }
			    ArrayList<Integer> remove = new ArrayList<Integer>(candidates.values());
			    Collections.sort(remove);
			    //System.out.println(remove);
			    int finalTupleRemoved = 0;
				int tupleRemoved = 0;
				while (finalTupleRemoved < k && finalTupleRemoved < remove.size()) {
					tupleRemoved += remove.get(finalTupleRemoved);
					HashMap<String, Object> tup = new HashMap<String, Object>();
					for (HashMap<String, Object> toRemove: candidates.keySet()) {
						if (candidates.get(toRemove) == remove.get(finalTupleRemoved)) {
							tup = toRemove;
							break;
						}
					}
					candidates.remove(tup);
					tup.put("number", remove.get(finalTupleRemoved));
					if (constraints.size() != 0) {
						for (String attrName: constraints.keySet()) {
							tup.put(attrName, constraints.get(attrName));
						}
					}
					ret.add(tup);
					finalTupleRemoved += 1;	
				}
				if (finalTupleRemoved < k) {
					return null;
				}
				else {
					HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
					r.put(smallestTable, ret);
					return r;
				}
			}	
		}
		else {
			HashMap<String, ArrayList<HashMap<String, Object>>> not_applicable = new HashMap<String, ArrayList<HashMap<String, Object>>>();
			not_applicable.put(null, null);
			return not_applicable;
		}	
	}
	
	/*
	 * Construct adjacency matrix for the attributes, then do BFS on one attribute. If there exists attributes not reachable,
	 * then the table containing that attribute can be considered separately. 
	 */
	private ArrayList<HashSet<String>> dividable(HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs) throws SQLException {
		ArrayList<HashSet<String>> groups = new ArrayList<HashSet<String>>();
		groups.add(new HashSet<String>());
		groups.add(new HashSet<String>());
		HashMap<String, Integer> attr_id = new HashMap<String, Integer>();

		System.out.println(tables_attrs);
		System.out.println(attrs);
		
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
		
		if (groups.get(1).size() != 0) {
			HashMap<String, ArrayList<String>> tables_attrs_1 = new HashMap<String, ArrayList<String>>();
			HashSet<String> attrs_1 = new HashSet<String>();
			ArrayList<HashSet<String>> newGroups;
			for (String tableName: groups.get(1)) {
				tables_attrs_1.put(tableName, tables_attrs.get(tableName));
				attrs_1.addAll(tables_attrs.get(tableName));
			}
			while (true) {
				newGroups = dividable(tables_attrs_1, attrs_1);
				groups.add(newGroups.get(0));
				if (newGroups.get(1).size() == 0) {
					break;
				}
				else {
					tables_attrs_1.clear();
					attrs_1.clear();
					for (String tableName: newGroups.get(1)) {
						tables_attrs_1.put(tableName, tables_attrs.get(tableName));
						attrs_1.addAll(tables_attrs.get(tableName));
					}
				}
			}
		}
//		//System.out.print("visited: ");
//		//System.out.println(visited);
	    System.out.println(groups);
		return groups;
	}
	
	private HashMap<String, ArrayList<HashMap<String, Object>>> decompose(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, ArrayList<HashSet<String>> groups, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		groups.remove(1);
		System.out.println(groups);
		ArrayList<ArrayList<Object>> dp = new ArrayList<ArrayList<Object>>();
		
		
		HashMap<String, ArrayList<String>>tables_attrs_1 = new HashMap<String, ArrayList<String>>();
		HashSet<String> attrs_1 = new HashSet<String>();
		ArrayList<String> projection_1 = new ArrayList<String>();
		
		
		ArrayList<Integer> m = new ArrayList<Integer>();
		for (int i = 0; i < groups.size(); i++) {
			int size = 0;
			String queryGroup = "";
			
			for (String tableName: groups.get(i)) {
				queryGroup += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
			if (fullJoin) {
				queryGroup = "select * from "+ queryGroup.substring(0, queryGroup.length()-14);
			}
			if (constraints.size() != 0) {
				for (String attrName: constraints.keySet()) {
					where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
				}
			    where = where.substring(0, where.length()-5);
				queryGroup += " where " + where;
			}
			
			ResultSet rs = statement.executeQuery(queryGroup);
			while (rs.next()) {
				size += 1;
			}	
			m.add(size);
		}
		

		
		for (String tableName: groups.get(0)) {
			tables_attrs_1.put(tableName, tables_attrs.get(tableName));
			for (String attrName: tables_attrs.get(tableName)) {
				if (!constraints.containsKey(attrName)) {
					attrs_1.add(attrName);
				}
				if (!projection_1.contains(attrName)) {
					projection_1.add(attrName);
				}
			}
		}

		ArrayList<Object> row0 = new ArrayList<Object>();
		for (int i = 0; i <= k; i++) {
			System.out.println(">>");
			System.out.println(tables_attrs_1);
			System.out.println(attrs_1);
			System.out.println(constraints);
			row0.add(setUp(i, statement, tables_attrs_1, attrs_1, constraints, projection_1, fullJoin));
			System.out.print("row: ");
			System.out.println(row0);
		}
		dp.add(row0);
		
		for (int i = 1; i < groups.size(); i++) {
			int m1 = 1;
			for (int j = 0; j < i; j++) {
				m1 *= m.get(j);
			}
			int m2 = m.get(i);
			
            ArrayList<Object> row  = new ArrayList<Object>();

			for (int j = 0; j <= k; j++) {
				int kMin = Integer.MAX_VALUE;
	    		HashMap<String, ArrayList<HashMap<String, Object>>> toAdd = null;

				for (int k1 = 0; k1 <= j; k1 ++) {
					for (int k2 = 0; k2 <= j; k2 ++) {
						if (k1*m2 + k2*m1 - k1*k2 >= j) {
							HashMap<String, ArrayList<String>> tables_attrs_i = new HashMap<String, ArrayList<String>>();
							HashSet<String> attrs_i = new HashSet<String>();
							HashMap<String, Object> constraints_i = new HashMap<String, Object>();
							ArrayList<String> projection_i = new ArrayList<String>();
							
							for (String tableName: tables_attrs.keySet()) {
								tables_attrs_i.put(tableName, tables_attrs.get(tableName));
								for (String attrName: tables_attrs.get(tableName)) {
									if (!constraints.containsKey(attrName)) {
										attrs_i.add(attrName);
									}
									if (!projection_i.contains(attrName)) {
										projection_i.add(attrName);
									}
								}
							}
							for (String attrName: constraints.keySet()) {
								if (projection_i.contains(attrName)) {
									constraints_i.put(attrName, constraints.get(attrName));
								}
							}
							
							Object cik2 = setUp(k2, statement, tables_attrs_i, attrs_i, constraints, projection_i, true);
							
							if (cik2 == null) {
								break;
							}
							HashMap<String,ArrayList<HashMap<String,Object>>> g1 = (HashMap<String,ArrayList<HashMap<String,Object>>>) dp.get(dp.size()-1).get(k1);
							HashMap<String,ArrayList<HashMap<String,Object>>> g2 = (HashMap<String,ArrayList<HashMap<String,Object>>>)cik2;
							if (num(g1) + num(g2) < kMin) {
								kMin = num(g1) + num(g2);
								for (String tableName: g1.keySet()) {
									g2.put(tableName, g1.get(tableName));
								}
								toAdd = g2;
							}
						}
					}
				}
				row.add(toAdd);
			}
			dp.add(row);
		}
		
		
		return (HashMap<String, ArrayList<HashMap<String, Object>>>) dp.get(dp.size()-1).get(k);
		
		
		
	}
	
	private HashMap<String, ArrayList<HashMap<String, Object>>> commonAttrs(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		Object[] tables = tables_attrs.keySet().toArray();
		ArrayList<ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>> dp = new ArrayList<ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>>();
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
		//System.out.println(query);
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
		System.out.println(commonAttrsVals);
		
        
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
            
			
            ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> row  = new ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
		    for (int i = 0; i <= k; i++) {
		    	System.out.print("i: ");
		    	System.out.println(i);
		    	if (dp.size() == 0) {
		    		HashMap<String, ArrayList<HashMap<String, Object>>> j = setUp(i, statement, new_tables_attrs, new_attrs, newConstraints, new_projection, fullJoin);
		    		if (j == null) {
		    			row.add(null);
		    		}
		    		else {
		    			row.add(j);
		    		}
		    	}
		    	else {
		    		 int minSize = Integer.MAX_VALUE;
		    		 HashMap<String, ArrayList<HashMap<String, Object>>> toAdd = null;
		    		 // the new possible common attributes values to remove j final tuples
		    		 for (int j = 0; j <= i; j ++) {
		    			 //System.out.println(i-j);
		    			 if (dp.get(commonCount-1).get(j) != null) {
		    				 //System.out.println(new_tables_attrs);
		    				 //System.out.println(new_attrs);
		    				 //System.out.println(newConstraints);
		    				 //System.out.println(new_projection);
		    				 HashMap<String, ArrayList<HashMap<String, Object>>> c = setUp(i-j, statement, new_tables_attrs, new_attrs, newConstraints, new_projection, fullJoin);
		    				 if (c != null && c.size() >= 0) {
		    					 for (String tableName: dp.get(commonCount-1).get(j).keySet()) {
		    						 if (!c.containsKey(tableName)) {
		    							 c.put(tableName, dp.get(commonCount-1).get(j).get(tableName));
		    						 }
		    						 else {
		    							 ArrayList<HashMap<String, Object>> existed = dp.get(commonCount-1).get(j).get(tableName);
		    							 c.get(tableName).addAll(existed);
		    						 }
		    					 }
		    					 int cSize = num(c);
		    					 
		    					 if (cSize < minSize) {
		    						 minSize = cSize;
		    						 toAdd = c;
		    					 }
		    				 }
		    			 }
		    		 }
		    		 if (minSize == Integer.MAX_VALUE) {
		    			 row.add(null);
		    		 }
		    		 else {
		    			 row.add(toAdd);
		    		 }	 
		    	}
		    }
		    dp.add(row);
		    commonCount += 1;
		    //System.out.println(dp);
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
						//System.out.println("triad");
						return null;
					}
				}
			}
		}
		return endogenous;	
	}
	
	private HashMap<String, ArrayList<HashMap<String, Object>>> booleanQuery (int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints) throws SQLException{
		if (k > 1) {
			return null;
		}
		ArrayList<HashSet<String>> groups = dividable(tables_attrs, attrs);
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
			HashMap<String, ArrayList<HashMap<String, Object>>> group1ToRemove = booleanQuery(k, statement, tables_attrs_1, attrs_1, constraints);
			HashMap<String, ArrayList<HashMap<String, Object>>> group2ToRemove = booleanQuery(k, statement, tables_attrs_2, attrs_2, constraints);
			int group1Size = 0;
			int group2Size = 0;
			for (ArrayList<HashMap<String, Object>> table_attrs: group1ToRemove.values()) {
				group1Size += table_attrs.size();
			}
			for (ArrayList<HashMap<String, Object>> table_attrs: group2ToRemove.values()) {
				group2Size += table_attrs.size();
			}
			if (group1Size > group2Size) {
				return group2ToRemove;
			}
			else {
				return group1ToRemove;
			}
		}
		else {		// not dividable, base case
			if (tables_attrs.size() == 1) {				// only one table - remove all
				String query = "select ";
				for (String attrName: attrs) {
					query += "\"" + attrName + "\", ";
				}
				query = query.substring(0, query.length()-2);
				query += " from ";		
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
				//System.out.println(query);
				ResultSet rs = statement.executeQuery(query);
				ArrayList<HashMap<String, Object>> ret = new ArrayList<HashMap<String, Object>>();
				while (rs.next()) {
					HashMap<String, Object> row = new HashMap<String, Object>();
					for (String attrName: attrs) {
						row.put(attrName, rs.getObject(attrName));
					}
					if (constraints.size() != 0) {
						for (String attrName: constraints.keySet()) {
							row.put(attrName, constraints.get(attrName));
						}
					}
					ret.add(row);
				}
				HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
				r.put((String)tables_attrs.keySet().toArray()[0], ret);
				return r;
			}
			// more than one table
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
			//System.out.println(order);
			ArrayList<ArrayList<String>> intersections = new ArrayList<ArrayList<String>>();
			for (int i = 0; i < order.size() - 1; i++) {
				ArrayList<String> attrs_1_copy = new ArrayList<String>();
				attrs_1_copy.addAll(tables_attrs.get(order.get(i)));
				ArrayList<String> attrs_2 = tables_attrs.get(order.get(i+1));
				attrs_1_copy.retainAll(attrs_2);
				intersections.add(attrs_1_copy);
			}
			
			ArrayList<ArrayList<HashMap<String, Object>>> vertices = new ArrayList<ArrayList<HashMap<String, Object>>>();
			int numOfVertices = 2;
			for (int i = 0; i < intersections.size(); i++) {
				ArrayList<HashMap<String, Object>> v = new ArrayList<HashMap<String, Object>>();
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
					HashMap<String, Object> vv = new HashMap<String, Object>();
					for (String attrName: intersections.get(i)) {
						vv.put(attrName, rs.getObject(attrName));
					}
					v.add(vv);
				}
				vertices.add(v);
			}
			//System.out.println(vertices);
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
				HashMap<String, Object> data = new HashMap<String, Object>();
				for (String attrName: intersections.get(0)) {
					data.put(attrName, rs.getObject(attrName));
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
					HashMap<String, Object> l = new HashMap<String, Object>();
					HashMap<String, Object> r = new HashMap<String, Object>();
					for (int j = 1; j <= intersections.get(i-1).size(); j++) {
						l.put(intersections.get(i-1).get(j-1), rs.getObject(j));
					}
					for (int j = intersections.get(i-1).size() + 1; j <= intersections.get(i-1).size() + intersections.get(i).size(); j++) {
						r.put(intersections.get(i).get(j - intersections.get(i-1).size() - 1), rs.getObject(j));
					}
					int lPos = vertices.get(i-1).indexOf(l) + counter1;
					int rPos = vertices.get(i).indexOf(r) + counter2;
					if (lPos < counter1 || rPos < counter2) {
						continue;
					}
//					//System.out.println(l);
//					//System.out.println(r);
//					//System.out.println(lPos);
//					//System.out.println(rPos);
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
		
			while (rs.next()) {
				HashMap<String, Object> data = new HashMap<String, Object>();
				for (String attrName: intersections.get(intersections.size()-1)) {
					data.put(attrName, rs.getObject(attrName));
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
			//System.out.println(Arrays.deepToString(graph_arr));
			ArrayList<ArrayList<Integer>> edges =  minCut(graph_arr, 0, graph_arr.length-1);
			HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
			//System.out.println(edges);
			for (ArrayList<Integer> edge: edges) {
				ArrayList<HashMap<String, Object>> ret = new ArrayList<HashMap<String, Object>>();
				int vertex1, vertex2;
				if (edge.get(0) > edge.get(1)) {
					vertex1 = edge.get(1);
					vertex2 = edge.get(0);
				}
				else {
					vertex2 = edge.get(1);
					vertex1 = edge.get(0);
				}
				
				
				int counter = 1;
				int intersection = 0;
				//System.out.println(vertex1);
				//System.out.println(vertex2);
				
				if (vertex1 == 0) {			// go to order[0] to find the tuples
					query = "select count(*) from \"" + order.get(0) + "\" where ";
					while (counter + vertices.get(intersection).size() < vertex2) {
						counter += vertices.get(intersection).size();
						intersection += 1;
					}
					String where = "";
					//System.out.println(vertices.get(intersection));
					HashMap<String, Object> intersection2 = vertices.get(intersection).get(vertex2 - counter);
					for (String attrName: intersection2.keySet()) {
						where += "\"" + attrName + "\" = " + intersection2.get(attrName).toString() + " AND ";
					}
					where = where.substring(0, where.length()-5);
					query += where;
					//System.out.println(query);
					rs = statement.executeQuery(query);
					rs.next();
					HashMap<String, Object> toRemove = new HashMap<String, Object>();
					for (String attrName: tables_attrs.get(order.get(0))) {
						if (intersection2.keySet().contains(attrName)) {
							toRemove.put(attrName, intersection2.get(attrName));
						}
						else {
							toRemove.put(attrName, attrName);
						}
					}
					toRemove.put("number", rs.getInt(1));
					ret.add(toRemove);
					//System.out.println(toRemove);
					//System.out.println(ret);
					if (!r.containsKey(order.get(0))) {
						r.put(order.get(0), ret);
					}
					else {
						ArrayList<HashMap<String, Object>> exist = r.get(order.get(0));
						exist.addAll(ret);
					}
				}
				
				else if (vertex2 == numOfVertices-1) {		// go to order[-1] to find the tuples
					query = "select count(*) from \"" + order.get(order.size()-1) + "\" where ";
					while (counter + vertices.get(intersection).size() < vertex1) {
						counter += vertices.get(intersection).size();
						intersection += 1;
					}
					String where = "";
					//System.out.println(vertices.get(intersection));
					HashMap<String, Object> intersection1 = vertices.get(intersection).get(vertex1 - counter);
					for (String attrName: intersection1.keySet()) {
						where += "\"" + attrName + "\" = " + intersection1.get(attrName).toString() + " AND ";
					}
					where = where.substring(0, where.length()-5);
					query += where;
					rs = statement.executeQuery(query);
					rs.next();
					HashMap<String, Object> toRemove = new HashMap<String, Object>();
					for (String attrName: tables_attrs.get(order.get(0))) {
						if (intersection1.keySet().contains(attrName)) {
							toRemove.put(attrName, intersection1.get(attrName));
						}
						else {
							toRemove.put(attrName, attrName);
						}
					}
					toRemove.put("number", rs.getInt(1));
					ret.add(toRemove);
					if (!r.containsKey(order.get(order.size()-1))) {
						r.put(order.get(order.size()-1), ret);
					}
					else {
						ArrayList<HashMap<String, Object>> exist = r.get(order.get(intersection));
						exist.addAll(ret);
					}
				}
				else {
					HashMap<String, Object> toRemove = new HashMap<String, Object>();
					while (counter + vertices.get(intersection).size() < vertex1) {
						counter += vertices.get(intersection).size();
						intersection += 1;
					}
					//System.out.println(intersection);
					HashMap<String, Object> intersection1 = vertices.get(intersection).get(vertex1 - counter);
					//System.out.println(intersection1);
					counter += vertices.get(intersection).size();
					intersection += 1;
					HashMap<String, Object> intersection2 = vertices.get(intersection).get(vertex2 - counter);
					for (String attrName: intersection1.keySet()) {
						toRemove.put(attrName, intersection1.get(attrName));
					}
					for (String attrName: intersection2.keySet()) {
						toRemove.put(attrName, intersection2.get(attrName));
					}
					ret.add(toRemove);
					if (!r.containsKey(order.get(intersection))) {
						r.put(order.get(intersection), ret);
					}
					else {
						ArrayList<HashMap<String, Object>> exist = r.get(order.get(intersection));
						exist.addAll(ret);
					}
					
				}
				
				
			}
			return r;
		}
	}
	
	private static ArrayList<ArrayList<Integer>> minCut(Integer[][] graph, int s, int t) { 
		int u,v; 
		
		// Create a residual graph and fill the residual 
		// graph with given capacities in the original 
		// graph as residual capacities in residual graph 
		// rGraph[i][j] indicates residual capacity of edge i-j 
		int[][] rGraph = new int[graph.length][graph.length]; 
		for (int i = 0; i < graph.length; i++) { 
			for (int j = 0; j < graph.length; j++) { 
				rGraph[i][j] = graph[i][j]; 
			} 
		} 

		// This array is filled by BFS and to store path 
		int[] parent = new int[graph.length]; 
		
		// Augment the flow while tere is path from source to sink	 
		while (bfs(rGraph, s, t, parent)) { 
			
			// Find minimum residual capacity of the edhes 
			// along the path filled by BFS. Or we can say 
			// find the maximum flow through the path found. 
			int pathFlow = Integer.MAX_VALUE;		 
			for (v = t; v != s; v = parent[v]) { 
				u = parent[v]; 
				pathFlow = Math.min(pathFlow, rGraph[u][v]); 
			} 
			
			// update residual capacities of the edges and 
			// reverse edges along the path 
			for (v = t; v != s; v = parent[v]) { 
				u = parent[v]; 
				rGraph[u][v] = rGraph[u][v] - pathFlow; 
				rGraph[v][u] = rGraph[v][u] + pathFlow; 
			} 
		} 
		
		// Flow is maximum now, find vertices reachable from s	 
		boolean[] isVisited = new boolean[graph.length];	 
		dfs(rGraph, s, isVisited); 
		
		// Print all edges that are from a reachable vertex to 
		// non-reachable vertex in the original graph	 
		ArrayList<ArrayList<Integer>> ret = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < graph.length; i++) { 
			for (int j = 0; j < graph.length; j++) { 
				if (graph[i][j] > 0 && isVisited[i] && !isVisited[j]) { 
					ArrayList<Integer> edge = new ArrayList<Integer>();
					edge.add(i);
					edge.add(j);
					ret.add(edge);
				} 
			} 
		} 
		return ret;
	} 
	
	// Returns true if there is a path 
		// from source 's' to sink 't' in residual 
		// graph. Also fills parent[] to store the path 
		private static boolean bfs(int[][] rGraph, int s, 
									int t, int[] parent) { 
			
			// Create a visited array and mark 
			// all vertices as not visited	 
			boolean[] visited = new boolean[rGraph.length]; 
			
			// Create a queue, enqueue source vertex 
			// and mark source vertex as visited	 
			Queue<Integer> q = new LinkedList<Integer>(); 
			q.add(s); 
			visited[s] = true; 
			parent[s] = -1; 
			
			// Standard BFS Loop	 
			while (!q.isEmpty()) { 
				int v = q.poll(); 
				for (int i = 0; i < rGraph.length; i++) { 
					if (rGraph[v][i] > 0 && !visited[i]) { 
						q.offer(i); 
						visited[i] = true; 
						parent[i] = v; 
					} 
				} 
			} 
			
			// If we reached sink in BFS starting 
			// from source, then return true, else false	 
			return (visited[t] == true); 
		} 
		
		// A DFS based function to find all reachable 
		// vertices from s. The function marks visited[i] 
		// as true if i is reachable from s. The initial 
		// values in visited[] must be false. We can also 
		// use BFS to find reachable vertices 
		private static void dfs(int[][] rGraph, int s, 
									boolean[] visited) { 
			visited[s] = true; 
			for (int i = 0; i < rGraph.length; i++) { 
					if (rGraph[s][i] > 0 && !visited[i]) { 
						dfs(rGraph, i, visited); 
					} 
			} 
		} 

	 
	private HashMap<String, ArrayList<HashMap<String, Object>>> hard(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs,  HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException { 
		HashMap<String, Object> removed = new HashMap<String, Object>();
		HashMap<String, ArrayList<HashMap<String, Object>>> ret = new HashMap<String, ArrayList<HashMap<String, Object>>>();
		
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
		
		//System.out.println(endogenous);
		
		int count = 0;
		while (count < k) {
			HashMap<String, Object> toRemoveTuple = new HashMap<String, Object>();
			String toRemoveTableName = "";
			int toRemoveNum = 0;
			for (String tableName: endogenous) {
				ArrayList<String> attrs_table = tables_attrs.get(tableName);
				String query = "";
				// Construct query groupby: attr_1, attr_2, ..., attr_x
				String groupby = "";
				for (String attrName: attrs_table) {
					groupby += "\"" + attrName + "\", ";
				}
				groupby = groupby.substring(0, groupby.length()-2);
				
				// Construct query from: Table1 NATURAL JOIN Table2 ... NATURAL JOIN TableX
				String query_from = "";
				for (String key: tables_attrs.keySet()) {
					query_from += "\"" + key + "\" NATURAL JOIN ";
				}
				query_from = query_from.substring(0, query_from.length()-14);
				String where = "";
				if (constraints.size() != 0 && ret.size() != 0) {
					where = " where ";
					
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
					
					for (ArrayList<HashMap<String, Object>> notequal: ret.values()) {
						for (HashMap<String, Object> notequal2: notequal) {
							for (String attrName: notequal2.keySet()) {
								where += "\"" + attrName + "\" != " + notequal2.get(attrName).toString() + " AND ";
							}
						}
					}
				    where = where.substring(0, where.length()-5);
				}
				else if (constraints.size() != 0 && ret.size() == 0){
					where = " where ";
					
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
					where = where.substring(0, where.length()-5);
				}
				else if (constraints.size() == 0 && ret.size() != 0) {
					where = " where ";
					for (ArrayList<HashMap<String, Object>> notequal: ret.values()) {
						for (HashMap<String, Object> notequal2: notequal) {
							for (String attrName: notequal2.keySet()) {
								where += "\"" + attrName + "\" != " + notequal2.get(attrName).toString() + " AND ";
							}
						}
					}
				    where = where.substring(0, where.length()-5);
				}
				query = "select count(*), " + groupby + " from " + query_from + where + " group by " + groupby + " order by count(*) desc";
				//System.out.println(query);
				ResultSet rs = statement.executeQuery(query);
				rs.next();
				if (rs.getInt(1) > toRemoveNum) {
					toRemoveNum = rs.getInt(1);
					HashMap<String, Object> tup = new HashMap<String, Object>();
					for (String attrName: attrs_table) {
						tup.put(attrName, rs.getObject(attrName));
					}
					if (constraints.size() != 0) {
						for (String attrName: constraints.keySet()) {
							tup.put(attrName, constraints.get(attrName));
						}
				    }
					System.out.println(tup);
					toRemoveTuple = tup;
					toRemoveTableName = tableName;
				}
			}
			if (ret.containsKey(toRemoveTableName)) {
				ret.get(toRemoveTableName).add(toRemoveTuple);
				count += toRemoveNum;
				toRemoveNum = 0;
			}
			else {
				ArrayList<HashMap<String, Object>> r = new ArrayList<HashMap<String, Object>>();
				r.add(toRemoveTuple);
				ret.put(toRemoveTableName, r);
				count += toRemoveNum;
				toRemoveNum = 0;
			}
		}
		return ret;
	}
	
		
		
		
		
		
		
		
		
		
		
		
    private int num(HashMap<String, ArrayList<HashMap<String, Object>>> result) {
    	int num = 0;
		if (result != null) {
			for (String tableName: result.keySet()) {
				ArrayList<HashMap<String, Object>> toRemove = result.get(tableName);
				num += toRemove.size();
				for (HashMap<String, Object> tuple: toRemove) {
					if (tuple.containsKey("number")) {
						num += (int) tuple.get("number") - 1;
					}
				}
			}
		}
		return num;
    }
	
	
	
	
	public static void main(String[] args) {

		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/decompose", "postgres", "postgres")) {
			
			int k = (int) 1;
			ArrayList<String> projection = new ArrayList<String>();
			
			projection.add("A1");
			projection.add("B1");
			projection.add("C1");
			projection.add("A2");
			projection.add("B2");
			projection.add("C2");
			projection.add("A3");
			projection.add("B3");
			projection.add("C3");
			projection.add("A4");
			projection.add("B4");
			projection.add("C4");
			projection.add("A5");
			projection.add("B5");
			projection.add("C5");
//			projection.add("D");
//			projection.add("E");
			/*
			projection.add("A");
			projection.add("B");
			projection.add("C");
			*/

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
		
			//System.out.println(start);
			// start calculation
			boolean fullJoin;
			if (projection.size() < attrs.size()) {
				fullJoin = false;
			}
			else {
				fullJoin = true;
			}
			//System.out.println(fullJoin);
			
			
			HashMap<String, Object> constraints = new HashMap<String, Object>();
			
//			constraints.put("C", 1);
//			HashMap<String, ArrayList<String>> new_tables_attrs = new HashMap<String, ArrayList<String>>();
//	        HashSet<String> new_attrs = new HashSet<String>();
//	        
//	        for (String tableName: tables_attrs.keySet()) {
//	            ArrayList<String> table_attrs = tables_attrs.get(tableName);
//	            ArrayList<String> new_table_attrs = new ArrayList<String>();
//	  
//            	for (String attrName: table_attrs) {
//	                if (!constraints.containsKey(attrName)) {
//	                    new_table_attrs.add(attrName);
//	                }
//	            }
//	            new_tables_attrs.put(tableName, new_table_attrs);
//            
//	        }
//	        for (String attrName: attrs) {
//	            if (!constraints.containsKey(attrName)){
//	                new_attrs.add(attrName);
//	            }
//	        }
//	        
	        
            for (int q = 0; q < 1; q++) {
				
				long start = System.currentTimeMillis();
				Decomposedp test = new Decomposedp(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
				System.out.print(System.currentTimeMillis() - start);
			    System.out.print(", ");
            }
			
            connection.close();
			
			
			
			/*
			
			
			
			// initialize k and projections
			int k = (int) 10;
			ArrayList<String> projection = new ArrayList<String>();
			projection.add("A");
			projection.add("B");
			projection.add("C");
			projection.add("D");
			projection.add("E");




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
		
			//System.out.println(start);
			// start calculation
			boolean fullJoin;
			if (projection.size() < attrs.size()) {
				fullJoin = false;
			}
			else {
				fullJoin = true;
			}
			//System.out.println(fullJoin);
			
			
			HashMap<String, Object> constraints = new HashMap<String, Object>();
			/*
			constraints.put("C", 1);
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
	        
	        
            for (int q = 0; q < 1; q++) {
				
				long start = System.currentTimeMillis();
				PostgreSqlExample test = new PostgreSqlExample(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
				System.out.print(System.currentTimeMillis() - start);
			    System.out.print(", ");
            }
			
            connection.close();
			
			/*
			HashMap<String, Object> constraints = new HashMap<String, Object>();
			constraints.put("C", 6);
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
	        System.out.println(new_tables_attrs);
	        System.out.println(new_attrs);
			for (int i = 0; i <= 6; i++) {
				
				long start = System.currentTimeMillis();
				PostgreSqlExample test = new PostgreSqlExample(i, statement, new_tables_attrs, new_attrs, constraints, projection, fullJoin);
				System.out.print("Time for i = " + String.valueOf(i) + ": ");
				System.out.print(System.currentTimeMillis() - start);
				System.out.println("ms.");
			
			
			}
			
			
			
			HashMap<String, Object> constraints = new HashMap<String, Object>();
			constraints.put("partkey", 13369);
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
			for (int i = 1; i <= 140; i++) {
				
				long start = System.currentTimeMillis();
				PostgreSqlExample test = new PostgreSqlExample(i, statement, new_tables_attrs, new_attrs, constraints, projection, fullJoin);
				System.out.print("Time for i = " + String.valueOf(i) + ": ");
				System.out.print(System.currentTimeMillis() - start);
				System.out.println("ms.");
			}

		    //
			
			constraints = new HashMap<String, Object>();
			constraints.put("partkey", 13371);
			new_tables_attrs = new HashMap<String, ArrayList<String>>();
	        new_attrs = new HashSet<String>();
	        
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
			for (int i = 1; i <= 136; i++) {
				
				long start = System.currentTimeMillis();
				PostgreSqlExample test = new PostgreSqlExample(i, statement, new_tables_attrs, new_attrs, constraints, projection, fullJoin);
				System.out.print("Time for i = " + String.valueOf(i) + ": ");
				System.out.print(System.currentTimeMillis() - start);
				System.out.println("ms.");
			}
			*/
			
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	
    }
    
}
