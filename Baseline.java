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

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

public class Baseline {

	
	
	public static void main(String[] args) {
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/nphard", "postgres", "postgres")) {
			
			// initialize k and projections
			int k = 16;
			ArrayList<String> projection = new ArrayList<String>();
			projection.add("A");
			projection.add("B");
//			projection.add("C");
//			projection.add("D");
//			projection.add("E");
//			projection.add("F");
//			projection.add("G");

//			projection.add("B1");
//			projection.add("C1");
//			projection.add("B2");
//			projection.add("C2");
//			projection.add("B3");
//			projection.add("C3");
			

			Gson gson = new GsonBuilder().create();
			Type type = new TypeToken<ArrayList<HashMap<String, Object>>>() {}.getType();



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
			
			// start calculation
			boolean fullJoin;
			if (projection.size() < attrs.size()) {
				fullJoin = false;
			}
			else {
				fullJoin = true;
			}
			
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
			
			ArrayList<ArrayList<HashMap<String, Object>>> endogenousInputData = new ArrayList<ArrayList<HashMap<String, Object>>>();
			
			for (String tableName: endogenous) {
				
				String query = "select ";
				for (String attrName: tables_attrs.get(tableName)) {
					query += "\"" + attrName + "\", ";
				}
				query = query.substring(0, query.length() - 2);
				query += " from " + "\"" + tableName + "\"";
				resultset = statement.executeQuery(query);
				
				while (resultset.next()) {
					ArrayList<HashMap<String, Object>> row = new ArrayList<HashMap<String, Object>>();
					for (String attrName: tables_attrs.get(tableName)) {
						HashMap<String, Object> attr = new HashMap<String, Object>();
						attr.put(attrName, resultset.getObject(attrName));
						row.add(attr);
					}
					endogenousInputData.add(row);
				}
			}
			HashSet<ArrayList<HashMap<String, Object>>> finalTuples = new HashSet<ArrayList<HashMap<String, Object>>>();
			HashSet<ArrayList<HashMap<String, Object>>> selectedTuples = new HashSet<ArrayList<HashMap<String, Object>>>();
			
			ArrayList<ArrayList<HashMap<String, Object>>> cur = endogenousInputData;
			ArrayList<ArrayList<HashMap<String, Object>>> next = new ArrayList<ArrayList<HashMap<String, Object>>>();
			
			for (int i = 0; i < cur.size(); i++) {
				selectedTuples.add(cur.get(i));
				String query = "select count(*) from ";
				for (String tableName: tables_attrs.keySet()) {
					query += "\"" + tableName + "\" natural join ";
				}
				query = query.substring(0, query.length() - 14) + " where ";
				for (ArrayList<HashMap<String, Object>> SelectedTuple: selectedTuples) {
					query += "(";
					for (HashMap<String, Object> t: SelectedTuple) {
						for (String attrName: t.keySet()) {
							 query += "\"" + attrName + "\"=" + t.get(attrName) + " and ";
						}
					}
					query = query.substring(0, query.length() - 5) + ") or ";
				}
				query = query.substring(0, query.length() - 4);
				resultset = statement.executeQuery(query);
				resultset.next();
				int remove = resultset.getInt(1);
				if (remove >= k) {
					finalTuples = selectedTuples;
					break;
				}
				else {
					selectedTuples.clear();
				}
			}
			
			if (finalTuples.size() != 0) {
				System.out.println(finalTuples);
			}
			else {
outer:			while (true) {
					for (int i = 0; i < cur.size(); i++) {
						ArrayList<HashMap<String, Object>> last = new ArrayList<HashMap<String, Object>>();
						last.add(cur.get(i).get(cur.get(i).size()-1));
						int a = endogenousInputData.indexOf(last);
						for (int j = a + 1; j < endogenousInputData.size(); j++) {
							ArrayList<HashMap<String, Object>> b = gson.fromJson(gson.toJson(cur.get(i), cur.get(i).getClass()), type);
							b.addAll(endogenousInputData.get(j));
							String query = "select count(*) from ";
							for (String tableName: tables_attrs.keySet()) {
								query += "\"" + tableName + "\" natural join ";
							}
							query = query.substring(0, query.length() - 14) + " where ";
							for (HashMap<String, Object> SelectedTuple: b) {

									for (String attrName: SelectedTuple.keySet()) {
										 query += "(\"" + attrName + "\"=" + SelectedTuple.get(attrName) + " and ";
									}
								
								query = query.substring(0, query.length() - 5) + ") or ";
							}
							query = query.substring(0, query.length() - 4);
							
							resultset = statement.executeQuery(query);
							resultset.next();
							int remove = resultset.getInt(1);
							if (remove >= k) {
								System.out.println(b);
								break outer;
							}
							next.add(b);
						}
					}
					cur = next;
					next = new ArrayList<ArrayList<HashMap<String, Object>>>();
				}	
			}
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			

		    System.out.println(System.currentTimeMillis() - start);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
