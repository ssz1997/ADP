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

public class RemoveAsWhole {
	
	RemoveAsWhole(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException{
		HashMap<String, ArrayList<HashMap<String, Object>>> result = setUp(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);	
		//HashMap<String, ArrayList<HashMap<String, Object>>> result = hard2(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
		System.out.println(result);
		System.out.println(this.num(result));
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
			ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> base_case = this.singleton(k, statement, tables_attrs, attrs, intersection, smallestTable, constraints, projection, fullJoin);
			// Decompose
			ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> general_case_1 = null;
			// UniversalAttribute
			ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> general_case_2 = null;
			

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
				return null;
			}
			else {
				return base_case.get(base_case.size() - 1);
			}
				
		} catch (SQLException e) {
				e.printStackTrace();
		}
		return null;
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
	
	
	
	
	private ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> singleton(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, String smallestTable, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		if (tables_attrs.size() == 1) {
			
		
			ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> ret = new  ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
			ret.add(new HashMap<String, ArrayList<HashMap<String, Object>>>());
			ArrayList<HashMap<String, Object>> subRet = new ArrayList<HashMap<String, Object>>();
			// keep track of the table with fewest attributes
			boolean singleton = false;
			String subsetTableName = null;
			
			// determine if there exists such table
			if (tables_attrs.get(smallestTable).size() == intersection.size()) {
				singleton = true;
				subsetTableName = smallestTable;
			}
	
			if (singleton) {
				if (fullJoin) {
					// Construct query groupby: attr_1, attr_2, ..., attr_x
					String groupby = "";
					for (String attrName: tables_attrs.get(subsetTableName)) {
						groupby += "\"" + attrName + "\", ";
					}
					for (String attrName: constraints.keySet()) {
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
					while (subsetData.next() && i <= k) {
						HashMap<String, Object> tup = new HashMap<String, Object>();
						finalTupleRemoved += subsetData.getInt(1);
						for (String attrName: tables_attrs.get(smallestTable)) {
							tup.put(attrName, subsetData.getObject(attrName));
						}
						if (constraints.size() != 0) {
							for (String attrName: constraints.keySet()) {
								tup.put(attrName, constraints.get(attrName));
							}
						}
						subRet.add(tup);
						ArrayList<HashMap<String, Object>> subRetCopy = new ArrayList<HashMap<String, Object>>();
						HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
						for (HashMap<String, Object> table: subRet) {
							subRetCopy.add(table);
						}
						r.put(smallestTable, subRetCopy);
						while (finalTupleRemoved >= i && i <= k) {
							ret.add(r);
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
						subRet.add(tup);
						ArrayList<HashMap<String, Object>> subRetCopy = new ArrayList<HashMap<String, Object>>();
						HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
						for (HashMap<String, Object> table: subRet) {
							subRetCopy.add(table);
						}
						r.put(smallestTable, subRetCopy);
	
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
					while (finalTupleRemoved < k && finalTupleRemoved < remove.size()) {
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
						subRet.add(tup);
						finalTupleRemoved += 1;	
						ArrayList<HashMap<String, Object>> subRetCopy = new ArrayList<HashMap<String, Object>>();
						HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
						for (HashMap<String, Object> table: subRet) {
							subRetCopy.add(table);
						}
						r.put(smallestTable, subRetCopy);
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
				ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> not_applicable = new ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
				return not_applicable;
			}
		}
		ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> not_applicable = new ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
		return not_applicable;
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
			ArrayList<String> remainingAttrs = tables_attrs.get(tableName);
			if (remainingAttrs.size() == 0) {
				HashSet<String> singleton = new HashSet<String>();
				singleton.add(tableName);
				groups.add(singleton);
			}
			else {
				int id = attr_id.get(tables_attrs.get(tableName).get(0));
				for (int i = 0; i < attr_groups.size(); i++) {
					if (attr_groups.get(i).contains(id)) {
						groups.get(i).add(tableName);
						break;
					}
				}
			}
		}
		return groups;
	}
	
	
	
	
	
	private ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> decompose(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, ArrayList<HashSet<String>> groups, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin, boolean finalStep) throws SQLException {
		ArrayList<ArrayList<HashMap<String,ArrayList<HashMap<String,Object>>>>> dp = new ArrayList<ArrayList<HashMap<String,ArrayList<HashMap<String,Object>>>>>();
		Gson gson = new GsonBuilder().create();
		Type type = new TypeToken<HashMap<String, ArrayList<HashMap<String, Object>>>>() {}.getType();
		ArrayList<Integer> groupSizes = new ArrayList<Integer>();
		for (int i = 0; i < groups.size(); i++) {
			String queryGroup = "";
			
			for (String tableName: groups.get(i)) {
				queryGroup += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
			
			queryGroup = "select count(*) from "+ queryGroup.substring(0, queryGroup.length()-14);
			
			if (constraints.size() != 0) {
				for (String attrName: constraints.keySet()) {
					where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
				}
			    where = where.substring(0, where.length()-5);
				queryGroup += " where " + where;
			}
			ResultSet rs = statement.executeQuery(queryGroup);
			rs.next();
			groupSizes.add(rs.getInt(1));
		}
		ArrayList<ArrayList<HashMap<String,ArrayList<HashMap<String,Object>>>>> dp_helper = new ArrayList<ArrayList<HashMap<String,ArrayList<HashMap<String,Object>>>>>();
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
			
			boolean fullJoin_i = true;
			for (String attrName: attrs_i) {
				if (!projection_i.contains(attrName)) {
					fullJoin_i = false;
					break;
				}
			}
			ArrayList<HashMap<String,ArrayList<HashMap<String,Object>>>> removeGroupi = this.singleton(k, statement, tables_attrs_i, attrs_i, intersection, smallestTable, constraints, projection_i, fullJoin_i);
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
			
            ArrayList<HashMap<String,ArrayList<HashMap<String,Object>>>> row  = new ArrayList<HashMap<String,ArrayList<HashMap<String,Object>>>>();
            row.add(new HashMap<String,ArrayList<HashMap<String,Object>>>());
            
            if ((groups.size() != 2 && i != groups.size() - 1) || (!finalStep)) {
            	for (int j = 1; j <= k; j++) {
    				int kMin = Integer.MAX_VALUE;
    	    		HashMap<String, ArrayList<HashMap<String, Object>>> toAdd = null;
    			    for (int k1 = 0; k1 <= Math.min(j, dp.get(dp.size()-1).size() - 1); k1++) {
 out:   				for (int k2 = 0; k2 <= Math.min(j, dp_helper.get(i).size() - 1); k2 ++) {
    						if (k1*m2 + k2*m1 - k1*k2 >= j) {
    							HashMap<String,ArrayList<HashMap<String,Object>>> g1 = dp.get(dp.size()-1).get(k1);
    							HashMap<String,ArrayList<HashMap<String,Object>>> g2 = dp_helper.get(i).get(k2);
    							if (g1 == null) {
    								break out;
    							}
    							if (g2 == null) {
    								break;
    							}
    							if (num(g1) + num(g2) < kMin) {
    								kMin = num(g1) + num(g2);
    								HashMap<String, ArrayList<HashMap<String, Object>>> candidate = gson.fromJson(gson.toJson(g1, g1.getClass()), type);

    								for (String tableName: g2.keySet()) {
    									candidate.put(tableName, g2.get(tableName));
    								}
    								toAdd = candidate;
    							}
    						}
    						
    					}
    				}
    				row.add(toAdd);
    				if (toAdd == null) {
    					break;
    				}
    			}
            }
            else {
				int kMin = Integer.MAX_VALUE;
	    		HashMap<String, ArrayList<HashMap<String, Object>>> toAdd = null;
out:			   for (int k1 = 0; k1 <= k; k1 ++) {
					for (int k2 = 0; k2 <= k; k2 ++) {
						if (k1 >= dp.get(dp.size()-1).size()) {
							break out;
						}
						if (k2 >= dp_helper.get(i).size()) {
							break;
						}
					
						if ((long)k1*m2 + (long)k2*m1 - k1*k2 >= k) {
							HashMap<String,ArrayList<HashMap<String,Object>>> g1 = dp.get(dp.size()-1).get(k1);
							HashMap<String,ArrayList<HashMap<String,Object>>> g2 = dp_helper.get(i).get(k2);
							if (g1 == null || g2 == null) {
								break;
							}
							if (num(g1) > num(toAdd)) {
								break out;
							}
							if (num(g1) + num(g2) < kMin) {
								kMin = num(g1) + num(g2);
								HashMap<String, ArrayList<HashMap<String, Object>>> candidate = gson.fromJson(gson.toJson(g1, g1.getClass()), type);
								for (String tableName: g2.keySet()) {
									candidate.put(tableName, g2.get(tableName));
								}
								toAdd = candidate;
							}
							break;
						}					
					}
    			}
			    row.add(toAdd);
			    return row;
            }      
			dp.add(row);
		}
		return dp.get(dp.size()-1);
	}
	
	
	
	
	private ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> UniversalAttrs(int k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs, HashSet<String> intersection, HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException {
		ArrayList<ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>> dp = new ArrayList<ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>>();
		Gson gson = new GsonBuilder().create();
		Type type = new TypeToken<HashMap<String, ArrayList<HashMap<String, Object>>>>() {}.getType();
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
		for (ArrayList<Object> commonAttrsVal: commonAttrsVals) {
			for (int i = 0; i < commonAttrs.length; i++) {
				newConstraints.put((String)commonAttrs[i], commonAttrsVal.get(i));
			}
        	ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> row  = new ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
	        if (dp.size() == 0) {
	        	ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> first = new ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
        		first.add(new HashMap<String, ArrayList<HashMap<String, Object>>>());
        		for (String tableName: new_tables_attrs.keySet()) {
        			if (new_tables_attrs.get(tableName).size() == 0) {
        				HashMap<String, Object> newConstraintsCopy = new HashMap<String, Object>();
						for (String attrName: newConstraints.keySet()) {
							newConstraintsCopy.put(attrName, newConstraints.get(attrName));
						}
        				query = "select count(*) from ";
        				for (String tablesName: tables_attrs.keySet()) {
        					query += "\"" + tablesName + "\"" + " natural join ";
        				}
        				query = query.substring(0, query.length() - 14);
        				query += " where ";
        				for (String attrName: newConstraints.keySet()) {
        					query += "\"" + attrName + "\"" + "=" + String.valueOf(newConstraints.get(attrName)) + " and ";
        				}
        				query = query.substring(0, query.length() - 5);
        				ResultSet rs = statement.executeQuery(query);
        				rs.next();
        				int canRemove = rs.getInt(1);
        				
        				if (canRemove >= k) {	
        					HashMap<String, ArrayList<HashMap<String, Object>>> cur = new HashMap<String, ArrayList<HashMap<String, Object>>>();
    						ArrayList<HashMap<String, Object>> curcur = new ArrayList<HashMap<String, Object>>();
    						curcur.add(newConstraintsCopy);
    						cur.put(tableName, curcur);
        					for (int i = 0; i < k; i++) {
        						first.add(cur);
        					}
        				}
        				else {
        					HashMap<String, ArrayList<HashMap<String, Object>>> cur = new HashMap<String, ArrayList<HashMap<String, Object>>>();
    						ArrayList<HashMap<String, Object>> curcur = new ArrayList<HashMap<String, Object>>();	
    						curcur.add(newConstraintsCopy);
    						cur.put(tableName, curcur);
        					for (int i = 0; i < canRemove; i++) {
        						first.add(cur);
        					}
        					first.add(null);
        				}
        			    break;
        			}		
        		}
        		dp.add(first);
	        } 
	        else {
				ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> different_constraints;
				different_constraints = new ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
        		different_constraints.add(new HashMap<String, ArrayList<HashMap<String, Object>>>());
        		for (String tableName: new_tables_attrs.keySet()) {
        			if (new_tables_attrs.get(tableName).size() == 0) {
        				HashMap<String, Object> newConstraintsCopy = new HashMap<String, Object>();
						for (String attrName: newConstraints.keySet()) {
							newConstraintsCopy.put(attrName, newConstraints.get(attrName));
						}
        				query = "select count(*) from ";
        				for (String tablesName: tables_attrs.keySet()) {
        					query += "\"" + tablesName + "\"" + " natural join ";
        				}
        				query = query.substring(0, query.length() - 14);
        				query += " where ";
        				for (String attrName: newConstraints.keySet()) {
        					query += "\"" + attrName + "\"" + "=" + String.valueOf(newConstraints.get(attrName)) + " and ";
        				}
        				query = query.substring(0, query.length() - 5);
        				ResultSet rs = statement.executeQuery(query);
        				rs.next();
        				int canRemove = rs.getInt(1);
        				
        				if (canRemove >= k) {
        					HashMap<String, ArrayList<HashMap<String, Object>>> cur = new HashMap<String, ArrayList<HashMap<String, Object>>>();
    						ArrayList<HashMap<String, Object>> curcur = new ArrayList<HashMap<String, Object>>();
    						curcur.add(newConstraintsCopy);
    						cur.put(tableName, curcur);
        					for (int i = 0; i < k; i++) {    						
        						different_constraints.add(cur);
        					}
        				}
        				else {
    						HashMap<String, ArrayList<HashMap<String, Object>>> cur = new HashMap<String, ArrayList<HashMap<String, Object>>>();
    						ArrayList<HashMap<String, Object>> curcur = new ArrayList<HashMap<String, Object>>();
    						curcur.add(newConstraintsCopy);
    						cur.put(tableName, curcur);
        					for (int i = 0; i < canRemove; i++) {
        						different_constraints.add(cur);
        					}
        					different_constraints.add(null);
        				}
        			    break;
        			}		
        		} 
				int maximumRemove = (different_constraints.get(different_constraints.size()-1) == null)? different_constraints.indexOf(null) - 1 : different_constraints.size() - 1;
		
				for (int i = 0; i <= k; i++) {
					int minSize = Integer.MAX_VALUE;
					HashMap<String, ArrayList<HashMap<String, Object>>> toAdd = null;
					HashMap<String, ArrayList<HashMap<String, Object>>> current = new HashMap<String, ArrayList<HashMap<String, Object>>>();
					for (int j = 0; j <= i; j++) {
						HashMap<String, ArrayList<HashMap<String, Object>>> previous = dp.get(dp.size()-1).get(j);
						if (previous == null) {
						    break;
						}
						if (i - j > maximumRemove) {
							continue;
						}
						else {
							current = different_constraints.get(i - j);
						}

						HashMap<String, ArrayList<HashMap<String, Object>>> candidate = gson.fromJson(gson.toJson(previous, previous.getClass()), type);
						
						for (String tableName: current.keySet()) {
							if (!candidate.containsKey(tableName)) {
								candidate.put(tableName, current.get(tableName));
							}
							else {
								ArrayList<HashMap<String, Object>> existed = current.get(tableName);
								candidate.get(tableName).addAll(existed);
							}
						}
						int cSize = num(candidate);	
						if (cSize < minSize) {
	
							minSize = cSize;
							toAdd = candidate;
						}
					}
					if (minSize == Integer.MAX_VALUE) {
						row.add(null);
						break;
					}
					else {
						row.add(toAdd);
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
			HashMap<String, ArrayList<HashMap<String, Object>>> group1ToRemove = booleanQuery(k, statement, tables_attrs_1, attrs_1, constraints_1);
			HashMap<String, ArrayList<HashMap<String, Object>>> group2ToRemove = booleanQuery(k, statement, tables_attrs_2, attrs_2, constraints_2);
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
			ArrayList<ArrayList<Integer>> edges =  minCut(graph_arr, 0, graph_arr.length-1);
			HashMap<String, ArrayList<HashMap<String, Object>>> r = new HashMap<String, ArrayList<HashMap<String, Object>>>();
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
				
				if (vertex1 == 0) {			// go to order[0] to find the tuples
					query = "select count(*) from \"" + order.get(0) + "\" where ";
					while (counter + vertices.get(intersection).size() < vertex2) {
						counter += vertices.get(intersection).size();
						intersection += 1;
					}
					String where = "";
					HashMap<String, Object> intersection2 = vertices.get(intersection).get(vertex2 - counter);
					for (String attrName: intersection2.keySet()) {
						where += "\"" + attrName + "\" = " + intersection2.get(attrName).toString() + " AND ";
					}
					where = where.substring(0, where.length()-5);
					query += where;
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
					HashMap<String, Object> intersection1 = vertices.get(intersection).get(vertex1 - counter);
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
		
		int[][] rGraph = new int[graph.length][graph.length]; 
		for (int i = 0; i < graph.length; i++) { 
			for (int j = 0; j < graph.length; j++) { 
				rGraph[i][j] = graph[i][j]; 
			} 
		} 

		int[] parent = new int[graph.length]; 
		
		while (bfs(rGraph, s, t, parent)) { 
			
			int pathFlow = Integer.MAX_VALUE;		 
			for (v = t; v != s; v = parent[v]) { 
				u = parent[v]; 
				pathFlow = Math.min(pathFlow, rGraph[u][v]); 
			} 
			
			for (v = t; v != s; v = parent[v]) { 
				u = parent[v]; 
				rGraph[u][v] = rGraph[u][v] - pathFlow; 
				rGraph[v][u] = rGraph[v][u] + pathFlow; 
			} 
		} 
		
		boolean[] isVisited = new boolean[graph.length];	 
		dfs(rGraph, s, isVisited); 
		 
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
	

	private static boolean bfs(int[][] rGraph, int s, int t, int[] parent) { 

		boolean[] visited = new boolean[rGraph.length]; 

		Queue<Integer> q = new LinkedList<Integer>(); 
		q.add(s); 
		visited[s] = true; 
		parent[s] = -1; 
		
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
		 
		return (visited[t] == true); 
	} 

	private static void dfs(int[][] rGraph, int s, 
								boolean[] visited) { 
		visited[s] = true; 
		for (int i = 0; i < rGraph.length; i++) { 
				if (rGraph[s][i] > 0 && !visited[i]) { 
					dfs(rGraph, i, visited); 
				} 
		} 
	} 
	
	
	
	private HashMap<String, ArrayList<HashMap<String, Object>>> hard(long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs,  HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException { 
		HashMap<String, ArrayList<HashMap<String, Object>>> ret = new HashMap<String, ArrayList<HashMap<String, Object>>>();
		ArrayList<HashSet<String>> groups = dividable(tables_attrs, attrs);
		ArrayList<Long> groupSizes = new ArrayList<Long>();
		for (int i = 0; i < groups.size(); i++) {
			String queryGroup = "";
			
			for (String tableName: groups.get(i)) {
				queryGroup += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
			
			queryGroup = "select count(*) from "+ queryGroup.substring(0, queryGroup.length()-14);
			
			if (constraints.size() != 0) {
				for (String attrName: constraints.keySet()) {
					where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
				}
			    where = where.substring(0, where.length()-5);
				queryGroup += " where " + where;
			}
			ResultSet rs = statement.executeQuery(queryGroup);
			rs.next();
			groupSizes.add(rs.getLong(1));
		}
		long totalSize = 1;
		for (long size: groupSizes) {
			totalSize = (long) totalSize * size;
		}
		for (int i = 0; i < groupSizes.size(); i++) {
			groupSizes.set(i, (long) totalSize / groupSizes.get(i));
		}
		
		
		ArrayList<String> endogenous = new ArrayList<String>();
		ArrayList<String> nonendogenous = new ArrayList<String>();
		HashMap<String, Integer> tableToGroup = new HashMap<String, Integer>();
		
		for (int i = 0; i < groups.size(); i++) {
			HashSet<String> group = groups.get(i);
			ArrayList<String> subEndogenous = new ArrayList<String>();
			ArrayList<String> subNonEndogenous = new ArrayList<String>();
			
			int currentTableSize = 0;
			while (subEndogenous.size() + subNonEndogenous.size() != group.size()) {
				String smallestTable = "";
				int smallestTableSize = Integer.MAX_VALUE;
				for (String tableName: group) {
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
									subNonEndogenous.add(tableName);
								}
							}
						}
						
					}
					endogenous.add(smallestTable);
					subEndogenous.add(smallestTable);
					tableToGroup.put(smallestTable, i);
				}	
				currentTableSize = smallestTableSize;
			}
			
		}
		
		ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> groupRets = new ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
		for (int i = 0; i < groups.size(); i++) {
			HashMap<String, ArrayList<HashMap<String, Object>>> subRet = new HashMap<String, ArrayList<HashMap<String, Object>>>();
			groupRets.add(subRet);
		}
		
		if (fullJoin) {
			long count = 0;
			while (count < k) {
				HashMap<String, Object> toRemoveTuple = new HashMap<String, Object>();
				String toRemoveTableName = "";
				long toRemoveNum = 0;
				for (String tableName: endogenous) {
					int groupNum = tableToGroup.get(tableName);
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
					for (String key: groups.get(groupNum)) {
						query_from += "\"" + key + "\" NATURAL JOIN ";
					}
					query_from = query_from.substring(0, query_from.length()-14);
					String where = "";
					if (constraints.size() != 0 && groupRets.get(groupNum).size() != 0) {
						where = " where ";
						
						for (String attrName: constraints.keySet()) {
							where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
						}
						
						for (ArrayList<HashMap<String, Object>> notequal: groupRets.get(groupNum).values()) {
							for (HashMap<String, Object> notequal2: notequal) {
							    where += "(";
								for (String attrName: notequal2.keySet()) {
									where += "\"" + attrName + "\" != " + notequal2.get(attrName).toString() + " OR ";
								}
								where = where.substring(0, where.length() - 4);
								where += ") AND ";
							}
						}
					    where = where.substring(0, where.length()-5);
					}
					else if (constraints.size() != 0 && groupRets.get(groupNum).size() == 0){
						where = " where ";
						
						for (String attrName: constraints.keySet()) {
							where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
						}
						where = where.substring(0, where.length()-5);
					}
					else if (constraints.size() == 0 && groupRets.get(groupNum).size() != 0) {
						where = " where ";
						for (ArrayList<HashMap<String, Object>> notequal: groupRets.get(groupNum).values()) {
							for (HashMap<String, Object> notequal2: notequal) {
								where += "(";
								for (String attrName: notequal2.keySet()) {
									where += "\"" + attrName + "\" != " + notequal2.get(attrName).toString() + " OR ";
								}
								where = where.substring(0, where.length() - 4);
								where += ") AND ";
							}
						}
					    where = where.substring(0, where.length()-5);
					}
					query = "select count(*), " + groupby + " from " + query_from + where + " group by " + groupby + " order by count(*) desc";
					ResultSet rs = statement.executeQuery(query);
					rs.next();
					long num = (long) rs.getInt(1) * groupSizes.get(groupNum);
					if (num > toRemoveNum) {
						toRemoveNum = num;
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
				if (groupRets.get(tableToGroup.get(toRemoveTableName)).containsKey(toRemoveTableName)) {
					groupRets.get(tableToGroup.get(toRemoveTableName)).get(toRemoveTableName).add(toRemoveTuple);
					count += toRemoveNum;
					toRemoveNum = 0;
				}
				else {
					ArrayList<HashMap<String, Object>> r = new ArrayList<HashMap<String, Object>>();
					r.add(toRemoveTuple);
					groupRets.get(tableToGroup.get(toRemoveTableName)).put(toRemoveTableName, r);
					count += toRemoveNum;
					toRemoveNum = 0;
				}
				
				
			}
			for (HashMap<String, ArrayList<HashMap<String, Object>>> subRet: groupRets) {
				for (String tableName: subRet.keySet()) {
					ret.put(tableName, subRet.get(tableName));
				}
			}
			return ret;
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
			return ret;
		}
	}
	
	
	private HashMap<String, ArrayList<HashMap<String, Object>>> hard2(long k, Statement statement, HashMap<String, ArrayList<String>> tables_attrs, HashSet<String> attrs,  HashMap<String, Object> constraints, ArrayList<String> projection, boolean fullJoin) throws SQLException { 
		HashMap<String, ArrayList<HashMap<String, Object>>> ret = new HashMap<String, ArrayList<HashMap<String, Object>>>();
		ArrayList<String> endogenous = new ArrayList<String>();
		ArrayList<String> nonendogenous = new ArrayList<String>();
		
		ArrayList<HashSet<String>> groups = dividable(tables_attrs, attrs);
		ArrayList<Long> groupSizes = new ArrayList<Long>();
		for (int i = 0; i < groups.size(); i++) {
			String queryGroup = "";
			
			for (String tableName: groups.get(i)) {
				queryGroup += "\"" + tableName + "\" NATURAL JOIN ";
			}
			String where = "";
			
			queryGroup = "select count(*) from "+ queryGroup.substring(0, queryGroup.length()-14);
			
			if (constraints.size() != 0) {
				for (String attrName: constraints.keySet()) {
					where += "\"" + attrName + "\" = " + constraints.get(attrName).toString() + " AND ";
				}
			    where = where.substring(0, where.length()-5);
				queryGroup += " where " + where;
			}
			ResultSet rs = statement.executeQuery(queryGroup);
			rs.next();
			groupSizes.add(rs.getLong(1));
		}
		long totalSize = 1;
		for (long size: groupSizes) {
			totalSize = (long) totalSize * size;
		}
		for (int i = 0; i < groupSizes.size(); i++) {
			groupSizes.set(i, (long) totalSize / groupSizes.get(i));
		}
		
		HashMap<String, Integer> tableToGroup = new HashMap<String, Integer>();
		
		for (int i = 0; i < groups.size(); i++) {
			HashSet<String> group = groups.get(i);
			ArrayList<String> subEndogenous = new ArrayList<String>();
			ArrayList<String> subNonEndogenous = new ArrayList<String>();
			
			int currentTableSize = 0;
			while (subEndogenous.size() + subNonEndogenous.size() != group.size()) {
				String smallestTable = "";
				int smallestTableSize = Integer.MAX_VALUE;
				for (String tableName: group) {
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
									subNonEndogenous.add(tableName);
								}
							}
						}
						
					}
					endogenous.add(smallestTable);
					subEndogenous.add(smallestTable);
					tableToGroup.put(smallestTable, i);
				}	
				currentTableSize = smallestTableSize;
			}
			
		}
		
		ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>> groupRets = new ArrayList<HashMap<String, ArrayList<HashMap<String, Object>>>>();
		for (int i = 0; i < groups.size(); i++) {
			HashMap<String, ArrayList<HashMap<String, Object>>> subRet = new HashMap<String, ArrayList<HashMap<String, Object>>>();
			groupRets.add(subRet);
		}
		
		for (String tableName: endogenous) {
			int groupNum = tableToGroup.get(tableName);
			HashMap<String, ArrayList<HashMap<String, Object>>> can = new HashMap<String, ArrayList<HashMap<String, Object>>>();
			ArrayList<HashMap<String, Object>> tuples = new ArrayList<HashMap<String, Object>>();
			can.put(tableName, tuples);
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
			for (String key: groups.get(groupNum)) {
				query_from += "\"" + key + "\" NATURAL JOIN ";
			}
			query_from = query_from.substring(0, query_from.length()-14);
			
			query = "select count(*), " + groupby + " from " + query_from + " group by " + groupby + " order by count(*) desc";
			ResultSet rs = statement.executeQuery(query);
			long count = 0;
			while (rs.next()) {
				count += (long) rs.getInt(1) * groupSizes.get(groupNum);
				HashMap<String, Object> tuple = new HashMap<String, Object>();
				for (String attrName: attrs_table) {
					tuple.put(attrName, rs.getObject(attrName));
				}
				tuples.add(tuple);
				if (count >= k) {
					if (num(can) < num(ret) || ret.size() == 0) {
						ret = can;
					}
					break;
				}
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

		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/singleton", "postgres", "postgres")) {
			double[] remove = new double[4];
			remove[0] = 0.75;
//			remove[1] = 0.25;
//			remove[2] = 0.5;
//			remove[3] = 0.75;
			for (int i = 0; i < 1; i++) {
				//for (int a = 0; a < 4; a++) {
				double kk =  95 * remove[i];
				int k = (int) kk;
				ArrayList<String> projection = new ArrayList<String>();
				projection.add("A");
				projection.add("B");
				projection.add("C");
				projection.add("D");
				projection.add("E");
				projection.add("F");
				projection.add("G");

//				projection.add("B1");
//				projection.add("C1");
//				projection.add("B2");
//				projection.add("C2");
//				projection.add("B3");
//				projection.add("C3");
				
		
				



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
					for (int j = 1; j <= rsmd.getColumnCount(); j++) {
						tables_attrs.get(key).add(rsmd.getColumnName(j));
						attrs.add(rsmd.getColumnName(j));
					}
				}
				
				
				// start calculation
				boolean fullJoin;
				if (projection.size() < attrs.size()) {
					fullJoin = false;
				}
				else {
					fullJoin = true;
				}
				
				
				
				HashMap<String, Object> constraints = new HashMap<String, Object>();
//				constraints.put("C", 1);
//				HashMap<String, ArrayList<String>> new_tables_attrs = new HashMap<String, ArrayList<String>>();
//		        HashSet<String> new_attrs = new HashSet<String>();
//		        
//		        for (String tableName: tables_attrs.keySet()) {
//		            ArrayList<String> table_attrs = tables_attrs.get(tableName);
//		            ArrayList<String> new_table_attrs = new ArrayList<String>();
//		  
//	            	for (String attrName: table_attrs) {
//		                if (!constraints.containsKey(attrName)) {
//		                    new_table_attrs.add(attrName);
//		                }
//		            }
//		            new_tables_attrs.put(tableName, new_table_attrs);
//	            
//		        }
//		        for (String attrName: attrs) {
//		            if (!constraints.containsKey(attrName)){
//		                new_attrs.add(attrName);
//		            }
//		        }
//
				long start = System.currentTimeMillis();
				RemoveAsWhole test = new RemoveAsWhole(k, statement, tables_attrs, attrs, constraints, projection, fullJoin);
			    System.out.println(System.currentTimeMillis() - start);
				
			}			
			
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}	
}
