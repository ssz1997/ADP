import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

public class ADPNum {
	
	ADPNum(long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException{
		
		long result = setUp(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);	
		System.out.println(result);
	} 
	
	private long setUp(long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {

		if (k == 0) {
			return 0;
		}
		
			// if head is empty, test if triad; if not, networkflow
		if (!fullJoin && projection.isEmpty()) {
			ArrayList<String> endogenous = existTriad(tables_attrs);
			if (endogenous == null) {
				return -1;
			}
			else {
				return booleanQuery(k, statement, tables_attrs, attrs, constraints);
			}
		}
	
		// find intersection of attributes in each table
		HashSet<String> intersection = this.findIntersection(tables_attrs, attrs);
		int sizeOfSmallestTable = Integer.MAX_VALUE;
		String smallestTable = "";
		for (String tableName: tables_attrs.keySet()) {
			// find the table with fewest attributes
			if (tables_attrs.get(tableName).size() < sizeOfSmallestTable) {
				sizeOfSmallestTable = tables_attrs.get(tableName).size();
				smallestTable = tableName;
			}
		}
			
		try {
			// Singleton
			ArrayList<Integer> base_case = this.singleton(k, statement, tables_attrs, attrs, intersection, smallestTable, constraints, projection, fullJoin);
			// Decompose
			ArrayList<Integer> general_case_1 = null;
			// UniversalAttribute
			ArrayList<Integer> general_case_2 = null;
			

			if (base_case.size() == 0) {
				// goto general case 1
				
				ArrayList<HashSet<String>> groups = dividable(tables_attrs, attrs);
				if (groups.size() > 1) { 				// is dividable
					general_case_1 = decompose(k, statement, tables_attrs, attrs, groups, constraints, projection, fullJoin, true);	
					return general_case_1.get(general_case_1.size()-1);				
				}
				else { // not dividable, goto general case 2
					if (!fullJoin) {
						ArrayList<String> intersectionCopy = new ArrayList<String>();
						intersectionCopy.addAll(intersection);
						intersectionCopy.retainAll(projection);
						if (intersection.size() != 0 && intersectionCopy.size() != 0) {
							general_case_2 = UniversalAttrs(k, statement, tables_attrs, attrs, intersection, constraints, projection, fullJoin); 
			    			return general_case_2.get(general_case_2.size()-1);
						}
						else {
							
							return hard(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
						}
					}
					else {
						if (intersection.size() != 0) {
							general_case_2 = UniversalAttrs(k, statement, tables_attrs, attrs, intersection, constraints, projection, fullJoin); 
							return general_case_2.get(general_case_2.size()-1);

						}
						else {
							
							return hard(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
						}
					}
		    		
		    	}
			}
			else if (base_case.get(base_case.size() - 1) == null) {
				return -1;
			}
			else {
				return base_case.get(base_case.size() - 1);
			}
				
		} catch (SQLException e) {
				e.printStackTrace();
		}
		return -2;
	}
	
	private HashSet<String> findIntersection(HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs) {
		// find intersection of attributes in each table
		HashSet<String> intersection = new HashSet<String>();
		for (String attr: attrs) {
			intersection.add(attr);
		}
		
		for (String tableName: tables_attrs.keySet()) {
			// find the intersection of the attributes of all tables
			intersection.retainAll(tables_attrs.get(tableName));
		}

		return intersection;
	}
	
	
	
	
	private ArrayList<Integer> singleton(long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, String smallestTable, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		ArrayList<Integer> ret = new  ArrayList<Integer>();
		ret.add(0);
		// keep track of the table with fewest attributes
		boolean singleton = false;
		String subsetTableName = null;
		
		// determine if there exists such table
		if (tables_attrs.get(smallestTable).size() == intersection.size()) {
			singleton = true;
			subsetTableName = smallestTable;
		}

		if (singleton) {
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
					subsetData = statement.executeQuery("select count(*), " + groupby + " from " + query_from + " group by " + groupby + " order by count(*) desc");
				}
				else {
					String where = " where ";
					
					for (String attrName: constraints.keySet()) {
						where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
					}
				    where = where.substring(0, where.length()-5);
					subsetData = statement.executeQuery("select count(*), " + groupby + " from " + query_from + where + " group by " + groupby + " order by count(*) desc");
				}
				
				
				int finalTupleRemoved = 0;		
				int i = 1;
				int tupleRemoved = 0;
				while (subsetData.next() && i <= k) {
					tupleRemoved += 1;
					finalTupleRemoved += subsetData.getInt(1);
					while (finalTupleRemoved >= i && i <= k) {
						ret.add(tupleRemoved);
						i ++;
					}
				}
				if (finalTupleRemoved < k) {
					ret.add(null);
					return ret;
				}
				else {
					return ret;
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
				ResultSet resultset = statement.executeQuery(query);
				HashMap<HashMap<String, Object>, Integer> count = new HashMap<HashMap<String, Object>, Integer>();
				while (resultset.next()) {
					HashMap<String, Object> vals = new HashMap<String, Object>();
					for (String attrName: tables_attrs.get(smallestTable)) {
						vals.put(attrName, resultset.getObject(attrName));
					}
					if (count.containsKey(vals)) {
						Integer c = count.get(vals) + 1;
					    count.put(vals, c);
					}
					else {
						count.put(vals, 1);
					}
				}
				// find the tuples that contribute most
				int tupleRemoved = 0;
				int finalTupleRemoved = 0;
				ArrayList<Integer> remove = new ArrayList<Integer>(count.values());
				Collections.sort(remove, Collections.reverseOrder());
				while (finalTupleRemoved < k && tupleRemoved < remove.size()) {
					
					finalTupleRemoved += remove.get(tupleRemoved);
					tupleRemoved += 1;

				}
				if (finalTupleRemoved < k) {
					ret.add(null);
					return ret;
				}
				else {
					return ret;
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
			    int finalTupleRemoved = 0;
			    int tupleRemoved = 0;
				while (finalTupleRemoved < k && finalTupleRemoved < remove.size()) {
					tupleRemoved += remove.get(finalTupleRemoved);
					finalTupleRemoved += 1;	
					ret.add(tupleRemoved);
				}
				if (finalTupleRemoved < k) {
					ret.add(null);
					return ret;
				}
				else {
					return ret;
				}
			}	
		}
		else {
			ArrayList<Integer> not_applicable = new ArrayList<Integer>();
			return not_applicable;
		}		
	}

	
	/*
	 * Construct adjacency matrix for the attributes, then do BFS on one attribute. If there exists attributes not reachable,
	 * then the table containing that attribute can be considered separately. 
	 */
	private ArrayList<HashSet<String>> dividable(HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs) throws SQLException {
		ArrayList<HashSet<String>> groups = new ArrayList<HashSet<String>>();
		HashMap<String, Integer> attr_id = new HashMap<String, Integer>();
		HashMap<Integer, String> id_attr = new HashMap<Integer, String>();
		
		int index = 0;
		for (String attr: attrs) {
			attr_id.put(attr, index);
			id_attr.put(index, attr);
			index += 1;
		}
		
		int [][] adjacency = new int[attr_id.size()][attr_id.size()];
		for (int i = 0; i < adjacency.length; i++) {
			for (int j = 0; j < adjacency.length; j++) {
				adjacency[i][j] = 0;
			}
		}
		for (ArrayList<String> attr : tables_attrs.values()) {
			for (int i = 1; i < attr.size(); i++) {
				int base = attr_id.get(attr.get(0));				
				adjacency[base][attr_id.get(attr.get(i))] = 1;
				adjacency[attr_id.get(attr.get(i))][base] = 1;
			}
		}
		// perform BFS
		ArrayList<HashSet<Integer>> attr_groups = new ArrayList<HashSet<Integer>>();
		
		HashSet<Integer> visited = new HashSet<Integer>();
		ArrayList<Integer> current = new ArrayList<Integer>();
		ArrayList<Integer> next = new ArrayList<Integer>();
		while (visited.size() != attrs.size()) {
			for (int i = 0; i < attrs.size(); i++) {
				if (!visited.contains(i)) {
					current.add(i);
					visited.add(i);
					attr_groups.add(new HashSet<Integer>());
					attr_groups.get(attr_groups.size()-1).add(i);
					break;
				}
			}
			while (current.size() != 0) {
				for (int c: current) {
					for (int i = 0; i < adjacency.length; i ++) {
						if (adjacency[c][i] == 1 && !visited.contains(i)) {
							next.add(i);
							visited.add(i);
							attr_groups.get(attr_groups.size()-1).add(i);
						}
					}
				}
				current.clear();
				for (int id: next) {
					current.add(id);
				}
				next.clear();
			}
		}
		
		while (groups.size() != attr_groups.size()) {
			groups.add(new HashSet<String>());
		}
		for (String tableName: tables_attrs.keySet()) {
			int id = attr_id.get(tables_attrs.get(tableName).get(0));
			for (int i = 0; i < attr_groups.size(); i++) {
				if (attr_groups.get(i).contains(id)) {
					groups.get(i).add(tableName);
					break;
				}
			}
		}
		return groups;
	}
	
	
	
	
	
	private ArrayList<Integer> decompose(long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, ArrayList<HashSet<String>> groups, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin, boolean finalStep) throws SQLException {
		ArrayList<ArrayList<Integer>> dp = new ArrayList<ArrayList<Integer>>();
		ArrayList<Integer> groupSizes = new ArrayList<Integer>();
		for (int i = 0; i < groups.size(); i++) {
			int size = 0;
			String queryGroup = "";
			
			for (String tableName: groups.get(i)) {
				queryGroup += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
			
			queryGroup = "select * from "+ queryGroup.substring(0, queryGroup.length()-14);
			
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
			groupSizes.add(size);
		}
		ArrayList<ArrayList<Integer>> dp_helper = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < groups.size(); i++) {
			HashMap<String, ArrayList<String>>tables_attrs_i = new HashMap<String, ArrayList<String>>();
			HashSet<String> attrs_i = new HashSet<String>();
			ArrayList<String> projection_i = new ArrayList<String>();

			for (String tableName: groups.get(i)) {
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

			HashSet<String> intersection = this.findIntersection(tables_attrs_i, attrs_i);
			
			int sizeOfSmallestTable = Integer.MAX_VALUE;
			String smallestTable = "";
			for (String tableName: tables_attrs_i.keySet()) {
				// find the table with fewest attributes
				if (tables_attrs_i.get(tableName).size() < sizeOfSmallestTable) {
					sizeOfSmallestTable = tables_attrs_i.get(tableName).size();
					smallestTable = tableName;
				}
			}

			ArrayList<Integer> removeGroupi = this.singleton(k, statement, tables_attrs_i, attrs_i, intersection, smallestTable, constraints, projection_i, fullJoin);
			if (removeGroupi.size() == 0) {
				removeGroupi = this.UniversalAttrs(k, statement, tables_attrs_i, attrs_i, intersection, constraints, projection_i, fullJoin);
			}
			dp_helper.add(removeGroupi);
		}
		
		dp.add(dp_helper.get(0));
		for (int i = 1; i < groups.size(); i++) {
			int m1 = 1;
			for (int j = 0; j < i; j++) {
				m1 *= groupSizes.get(j);
			}
			int m2 = groupSizes.get(i);
			
			ArrayList<Integer> row  = new ArrayList<Integer>();
            if (!finalStep) {
            	for (int j = 1; j <= k; j++) {
    				int kMin = Integer.MAX_VALUE;
    			    for (int k1 = 0; k1 <= Math.min(j, dp.get(dp.size()-1).size() - 1); k1++) {
    					for (int k2 = 0; k2 <= Math.min(j, dp_helper.get(i).size() - 1); k2 ++) {
    						if (k1*m2 + k2*m1 - k1*k2 >= j) {
    							Integer g1 = dp.get(dp.size()-1).get(k1);
    							Integer g2 = dp_helper.get(i).get(k2);
    							if (g1 == null || g2 == null) {
    								continue;
    							}
    							if (g1 + g2 < kMin) {
    								kMin = g1 + g2;

    							}
    						}
    						
    					}
    				}
    				row.add(kMin);
    			}
            }
            else {
				int kMin = Integer.MAX_VALUE;
out:			for (int k1 = 0; k1 <= k; k1++) {
					for (int k2 = 0; k2 <= k; k2 ++) {
						
						if (k1 >= dp.get(dp.size()-1).size() || k2 >= dp_helper.get(i).size()) {
							break;
						}

						if ((long)k1*m2 + (long)k2*m1 - k1*k2 >= k) {
							Integer g1 = dp.get(dp.size()-1).get(k1);
							Integer g2 = dp_helper.get(i).get(k2);				
							if (g2 == null) {
								break;
							}
							if (g1 > kMin) {
								break out;
							}
							if (g1 + g2 < kMin) {
								kMin = g1 +g2;
							}
							break;
						}
					}
    			}
			    row.add(kMin);
			    return row;
            }
			
			dp.add(row);
		}
		return dp.get(dp.size()-1);
	}
	
	
	
	
	private ArrayList<Integer> UniversalAttrs(long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		ArrayList<ArrayList<Integer>> dp = new ArrayList<ArrayList<Integer>>();

		Object[] commonAttrs = intersection.toArray();
		String queryFrom = "";
		for (String tableName: tables_attrs.keySet()) {
			queryFrom += "\"" + tableName + "\" NATURAL JOIN ";
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
		ResultSet tableData = statement.executeQuery(query);
		ArrayList<ArrayList<Object>> commonAttrsVals = new ArrayList<ArrayList<Object>>();
		while (tableData.next()) {
			ArrayList<Object> commonAttrVal = new ArrayList<Object>();
			for (int i = 0; i < commonAttrs.length; i++) {
				  commonAttrVal.add(tableData.getObject((String)commonAttrs[i]));
			}	
			commonAttrsVals.add(commonAttrVal);
		}
		
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
                if (!intersection.contains(attrName)) {
                    new_table_attrs.add(attrName);
                }
            }
            new_tables_attrs.put(tableName, new_table_attrs);
        
        }
        for (String attrName: attrs) {
            if (!intersection.contains(attrName)){
                new_attrs.add(attrName);
            }
        }
        ArrayList<String> new_projection = new ArrayList<String>();
		for (String attrName: projection) {
			if (!intersection.contains(attrName)) {
				new_projection.add(attrName);
			}
		}
		
		ArrayList<HashSet<String>> groups = this.dividable(new_tables_attrs, new_attrs);
		for (ArrayList<Object> commonAttrsVal: commonAttrsVals) {
			for (int i = 0; i < commonAttrs.length; i++) {
				newConstraints.put((String)commonAttrs[i], commonAttrsVal.get(i));
			}
        	ArrayList<Integer> row  = new ArrayList<Integer>();
	        if (dp.size() == 0) {
	    		ArrayList<Integer> first = this.decompose(k, statement, new_tables_attrs, new_attrs, groups, newConstraints, new_projection, fullJoin, false);
	    		dp.add(first);
	        } 
	        else {
				ArrayList<Integer> different_constraints = this.decompose(k, statement, new_tables_attrs, new_attrs, groups, newConstraints, new_projection, fullJoin, false);
				int maximumRemove = (different_constraints.get(different_constraints.size()-1) == null)? different_constraints.indexOf(null) - 1 : different_constraints.size() - 1;
		
				for (int i = 0; i <= k; i++) {
					int minSize = Integer.MAX_VALUE;
					Integer current;
					for (int j = 0; j <= i; j++) { 
						Integer previous = dp.get(dp.size()-1).get(j);
						if (previous == null) {
						    break;
						}
						if (i - j > maximumRemove) {
							continue;
						}
						else {
							current = different_constraints.get(i - j);
						}
						
						int cSize = previous + current;
						
						if (cSize < minSize) {
							minSize = cSize;
						}
					}
					if (minSize == Integer.MAX_VALUE) {
						row.add(null);
						break;
					}
					else {
						row.add(minSize);
					}	 
				} 
				dp.add(row);
				
	        }  
		}	
		return dp.get(dp.size()-1);
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
				Iterator<String> ite = tables_attrs.keySet().iterator();
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
					if (!visited.contains(endogenous.get(j))) {
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
					if (visited.contains(endogenous.get(k))) { 			// find triad
						return null;
					}
				}
			}
		}
		return null;	
	}
		
	private Integer booleanQuery (long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints) throws SQLException{
		if (k > 1) {
			return null;
		}
		ArrayList<HashSet<String>> groups = dividable(tables_attrs, attrs);
		if (groups.size() != 0) {    // dividable
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
			Integer group1ToRemove = booleanQuery(k, statement, tables_attrs_1, attrs_1, constraints_1);
			Integer group2ToRemove = booleanQuery(k, statement, tables_attrs_2, attrs_2, constraints_2);
			if (group1ToRemove > group2ToRemove) {
				return group2ToRemove;
			}
			else {
				return group1ToRemove;
			}
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
				ResultSet rs = statement.executeQuery(query);
				if (rs.next()) {
					return rs.getInt(1);
				}
				else {
					return null;
				}
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
					ArrayList<Integer> vertex = graph.get(lPos);
					int c = vertex.get(rPos);
					c += 1;
					vertex.set(rPos, c);
					graph.set(lPos, vertex);
					
				}
				counter1 += vertices.get(i-1).size();
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
			return  minCut(graph_arr, 0, graph_arr.length-1);	
		}
	}		
		

	private int minCut(Integer graph[][], int s, int t) 
    { 
        int u, v; 
        int V = graph.length;
        int rGraph[][] = new int[V][V]; 
  
        for (u = 0; u < V; u++) 
            for (v = 0; v < V; v++) 
                rGraph[u][v] = graph[u][v]; 
  
        int parent[] = new int[V]; 
  
        int max_flow = 0; 

        while (bfs(rGraph, s, t, parent)) 
        { 

            int path_flow = Integer.MAX_VALUE; 
            for (v=t; v!=s; v=parent[v]) 
            { 
                u = parent[v]; 
                path_flow = Math.min(path_flow, rGraph[u][v]); 
            } 
  

            for (v=t; v != s; v=parent[v]) 
            { 
                u = parent[v]; 
                rGraph[u][v] -= path_flow; 
                rGraph[v][u] += path_flow; 
            } 
  
            max_flow += path_flow; 
        } 

        return max_flow; 
    } 
	

	boolean bfs(int rGraph[][], int s, int t, int parent[]) 
    { 
        int V = rGraph.length;
        boolean visited[] = new boolean[V]; 
        for(int i=0; i<V; ++i) 
            visited[i]=false; 
  
        LinkedList<Integer> queue = new LinkedList<Integer>(); 
        queue.add(s); 
        visited[s] = true; 
        parent[s]=-1; 
  
        while (queue.size()!=0) 
        { 
            int u = queue.poll(); 
  
            for (int v=0; v<V; v++) 
            { 
                if (visited[v]==false && rGraph[u][v] > 0) 
                { 
                    queue.add(v); 
                    parent[v] = u; 
                    visited[v] = true; 
                } 
            } 
        } 

        return (visited[t] == true); 
    } 
	
	
	
	private Integer hard(long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs,  HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException { 
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
				Iterator<String> ite = tables_attrs.keySet().iterator();
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
		
		if (fullJoin) {
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
					System.out.println(query);
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
			return num(ret);
		}
		else {
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
								where += "not (";
								for (String attrName: notequal2.keySet()) {
									where += "\"" + attrName + "\" = " + notequal2.get(attrName).toString() + " AND ";
								}
								where = where.substring(0, where.length()-5) + ")" + " AND ";
							}
							
						}
					   where = where.substring(0, where.length()-5);
					}
					query = "select count(*), " + groupby + " from " + query_from + where + " group by " + groupby + " order by count(*) desc";
					ResultSet rs = statement.executeQuery(query);
					if (!rs.next()) {
						continue;
					}
					//rs.next();
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
						toRemoveTuple = tup;
						toRemoveTableName = tableName;
					}
				}
				if (ret.containsKey(toRemoveTableName)) {
					ret.get(toRemoveTableName).add(toRemoveTuple);
					toRemoveNum = 0;
					String c = "select count(*) from ";
					String query_from = "";
					for (String key: tables_attrs.keySet()) {
						query_from += "\"" + key + "\" NATURAL JOIN ";
					}
					query_from = query_from.substring(0, query_from.length()-14);
					String condition = " where ";
					for (String attrName: toRemoveTuple.keySet()) {
						condition += "\"" + attrName + "\" = " + toRemoveTuple.get(attrName) + " and ";
					}
					condition = condition.substring(0, condition.length() - 5);
					c += query_from + condition;
					ResultSet rs = statement.executeQuery(c);
					rs.next();
					count += rs.getInt(1);
				}
				else {
					ArrayList<HashMap<String, Object>> r = new ArrayList<HashMap<String, Object>>();
					r.add(toRemoveTuple);
					ret.put(toRemoveTableName, r);
					toRemoveNum = 0;
					String c = "select count(*) from ";
					String query_from = "";
					for (String key: tables_attrs.keySet()) {
						query_from += "\"" + key + "\" NATURAL JOIN ";
					}
					query_from = query_from.substring(0, query_from.length()-14);
					String condition = " where ";
					for (String attrName: toRemoveTuple.keySet()) {
						condition += "\"" + attrName + "\" = " + toRemoveTuple.get(attrName) + " and ";
					}
					condition = condition.substring(0, condition.length() - 5);
					c += query_from + condition;
					ResultSet rs = statement.executeQuery(c);
					rs.next();
					count += rs.getInt(1);
				}
			}
			return num(ret);
		}
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

		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/1skew1M", "postgres", "postgres")) {
			double[] percentage = new double[4];
			percentage[0] = 0.75;
 
			// initialize k and projections
			for (int x = 0; x < 1; x++) {
				for (int j = 0; j < 1; j++) {
					double remove = 600000 * percentage[0];
					long k = (long) remove;
					ArrayList<String> projection = new ArrayList<String>();
					projection.add("A");
					projection.add("B");
//					projection.add("C");
//					projection.add("D");
//					projection.add("E");
//					projection.add("F");
//					projection.add("G");

//					projection.add("B1");
//					projection.add("C1");
//					projection.add("B2");
//					projection.add("C2");
//					projection.add("B3");
//					projection.add("C3");
					
			
					



				    HashMap<String, ArrayList<String>> tables_attrs = new HashMap<>();     // the attributes in each table
				    HashSet<String> attrs = new HashSet<String>();
				
//					
//					// find table names
//					DatabaseMetaData databaseMetaData = connection.getMetaData();
//					ResultSet resultset = databaseMetaData.getTables(null, null, null, new String[] {"TABLE"});
//					while (resultset.next()) {
//						tables_attrs.put(resultset.getString("TABLE_NAME"), new ArrayList<String>());
//					}
//					
//					// find attributes in each table
					Statement statement = connection.createStatement();
//					// iterate through every table
//					for (String key: tables_attrs.keySet()) {
//						resultset = statement.executeQuery("select * from \"" + key + "\" WHERE 1 < 0");
//						ResultSetMetaData rsmd = resultset.getMetaData();
//						for (int j = 1; j <= rsmd.getColumnCount(); j++) {
//							tables_attrs.get(key).add(rsmd.getColumnName(j));
//							attrs.add(rsmd.getColumnName(j));
//						}
//					}
				    ArrayList<String> tableA_attrs = new ArrayList<String>();
				    tableA_attrs.add("A");
				    tables_attrs.put("A", tableA_attrs);
				    ArrayList<String> tableAB_attrs = new ArrayList<String>();
				    tableAB_attrs.add("A");
				    tableAB_attrs.add("B");
				    tables_attrs.put("AB", tableAB_attrs);
				    
				    attrs.add("A");
				    attrs.add("B");
					
					
					// start calculation
					boolean fullJoin;
					if (projection.size() < attrs.size()) {
						fullJoin = false;
					}
					else {
						fullJoin = true;
					}
				
					
					
					HashMap<String, Object> constraints = new HashMap<String, Object>();
//					constraints.put("C", 1);
//					HashMap<String, ArrayList<String>> new_tables_attrs = new HashMap<String, ArrayList<String>>();
//			        HashSet<String> new_attrs = new HashSet<String>();
//			        
//			        for (String tableName: tables_attrs.keySet()) {
//			            ArrayList<String> table_attrs = tables_attrs.get(tableName);
//			            ArrayList<String> new_table_attrs = new ArrayList<String>();
//			  
//		            	for (String attrName: table_attrs) {
//			                if (!constraints.containsKey(attrName)) {
//			                    new_table_attrs.add(attrName);
//			                }
//			            }
//			            new_tables_attrs.put(tableName, new_table_attrs);
//		            
//			        }
//			        for (String attrName: attrs) {
//			            if (!constraints.containsKey(attrName)){
//			                new_attrs.add(attrName);
//			            }
//			        }
	//

					long start = System.currentTimeMillis();
					ADPNum test = new ADPNum(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
				    System.out.println(System.currentTimeMillis() - start);

					
					
				}
				
			}
		}	
		catch (SQLException e) {
			e.printStackTrace();
		}
	}	
}
