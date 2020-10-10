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
		double percentage = 0.1;
		for (int x = 0; x < 1; x++) {
			for (int y = 4; y < 5; y++) {
				try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/bruteforce400", "postgres", "postgres")) {
					
					double kk = 278 * (1-percentage);
					int k = (int) kk;
					
					ArrayList<String> projection = new ArrayList<String>();
					projection.add("A");
					projection.add("B");
							projection.add("C");
							projection.add("D");
//							projection.add("E");
//							projection.add("F");
//							projection.add("G");

//							projection.add("B1");
//							projection.add("C1");
//							projection.add("B2");
//							projection.add("C2");
//							projection.add("B3");
//							projection.add("C3");
					

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
//					ArrayList<String> nonendogenous = new ArrayList<String>();
//					
//					// find endogenous/nonendogenous relations
//					int currentTableSize = 0;
//					while (endogenous.size() + nonendogenous.size() != tables_attrs.size()) {
//						String smallestTable = "";
//						int smallestTableSize = Integer.MAX_VALUE;
//						for (String tableName: tables_attrs.keySet()) {
//							if (tables_attrs.get(tableName).size() >= currentTableSize && tables_attrs.get(tableName).size() < smallestTableSize && !endogenous.contains(tableName) && !nonendogenous.contains(tableName)) {
//								smallestTable = tableName;
//								smallestTableSize = tables_attrs.get(tableName).size();
//							}
//						}
//						
//						ArrayList<String> attrsSmallest = tables_attrs.get(smallestTable);
//						Collections.sort(attrsSmallest);
//						if (!nonendogenous.contains(smallestTable)) {
//							Iterator ite = tables_attrs.keySet().iterator();
//							while (ite.hasNext()) {
//								String tableName = (String) ite.next();
//								if (!tableName.equals(smallestTable)) {
//									ArrayList<String> attrs2 = tables_attrs.get(tableName);
//									Collections.sort(attrs2);
//									if (attrs2.equals(attrsSmallest)) {
//										ite.remove();
//									}
//									else if (tables_attrs.get(tableName).size() >= smallestTableSize) {
//										if (attrs2.containsAll(attrsSmallest) && !nonendogenous.contains(tableName)) {
//											nonendogenous.add(tableName);
//										}
//									}
//								}
//								
//							}
//							endogenous.add(smallestTable);
//						}	
//						currentTableSize = smallestTableSize;
//					}
					endogenous.add("AB");
					endogenous.add("BC");
					endogenous.add("CD");
					
					ArrayList<HashMap<String, Object>> endogenousInputData = new ArrayList<HashMap<String, Object>>();
					
					for (String tableName: endogenous) {
						
						String query = "select ";
						for (String attrName: tables_attrs.get(tableName)) {
							query += "\"" + attrName + "\", ";
						}
						query = query.substring(0, query.length() - 2);
						query += " from " + "\"" + tableName + "\"";
						resultset = statement.executeQuery(query);
						
						while (resultset.next()) {
							HashMap<String, Object> row = new HashMap<String, Object>();
							for (String attrName: tables_attrs.get(tableName)) {
								row.put(attrName, resultset.getObject(attrName));
							}
							endogenousInputData.add(row);
						}
					}
					ArrayList<Integer> finalTuplesIndex = new ArrayList<Integer>();
					int layer = 1;
					ArrayList<Integer> selectedTuplesIndex = new ArrayList<Integer>();
					ArrayList<Integer> selectedTuplesIndexNext = new ArrayList<Integer>();
					selectedTuplesIndex.add(0);
					
					
					
					
					while (true) {
						if (selectedTuplesIndex.size() != layer) {
							System.out.println(layer);
							selectedTuplesIndex.clear();
							selectedTuplesIndexNext.clear();
							for (int i = 0; i < layer; i++) {
								selectedTuplesIndex.add(i);
							}
						}	
						if (selectedTuplesIndexNext.size() == 0) {
							for (String tableName: endogenous) {
								statement.execute("DELETE FROM \"" + tableName + tableName + "\"");
							}
							for (int i = 0; i < layer; i++) {
								HashMap<String, Object> tuple = endogenousInputData.get(i);
								if (tuple.keySet().contains("A")) {
									statement.execute("insert into \"ABAB\" (\"A\", \"B\") values (" + String.valueOf(tuple.get("A")) + ", " + String.valueOf(tuple.get("B")) + ")");
								}
								else if (tuple.keySet().contains("D")) {
									statement.execute("insert into \"CDCD\" (\"C\", \"D\") values (" + String.valueOf(tuple.get("C")) + ", " + String.valueOf(tuple.get("D")) + ")");
								}
								else {
									statement.execute("insert into \"BCBC\" (\"B\", \"C\") values (" + String.valueOf(tuple.get("B")) + ", " + String.valueOf(tuple.get("C")) + ")");
								}
							}
							
							resultset = statement.executeQuery("WITH R11 as (SELECT \"A\", \"B\" FROM \"AB\" EXCEPT SELECT \"A\", \"B\" FROM \"ABAB\"), R22 as (SELECT \"B\", \"C\" from \"BC\" EXCEPT SELECT \"B\", \"C\" FROM \"BCBC\"), R33 as (SELECT \"C\", \"D\" FROM \"CD\" EXCEPT SELECT \"C\", \"D\" FROM \"CDCD\") SELECT COUNT(*) FROM R11, R22, R33 WHERE R11.\"B\" = R22.\"B\" AND R22.\"C\" = R33.\"C\"");
							resultset.next();
							if (resultset.getInt(1) <= k) {
								finalTuplesIndex = selectedTuplesIndex;
								break;
							}
							if (layer != endogenousInputData.size()) {
								for (int i = 0; i < selectedTuplesIndex.size()-1; i++) {
									selectedTuplesIndexNext.add(selectedTuplesIndex.get(i));
								}
								selectedTuplesIndexNext.add(selectedTuplesIndex.get(layer-1)+1);
							}
						}
						else {
							for (int i = selectedTuplesIndexNext.size()-1; i >= 0; i--) {
								if (selectedTuplesIndex.get(i) != selectedTuplesIndexNext.get(i)) {
									HashMap<String, Object> outdatedTuple = endogenousInputData.get(selectedTuplesIndex.get(i));
									HashMap<String, Object> newTuple = endogenousInputData.get(selectedTuplesIndexNext.get(i));
									if (outdatedTuple.keySet().contains("A")) {
										statement.execute("delete from \"ABAB\" where \"A\"=" + String.valueOf(outdatedTuple.get("A")) + " AND \"B\"=" + String.valueOf(outdatedTuple.get("B")));
									}
									else if (outdatedTuple.keySet().contains("D")) { 
										statement.execute("delete from \"CDCD\" where \"C\"=" + String.valueOf(outdatedTuple.get("C")) + " AND \"D\"=" + String.valueOf(outdatedTuple.get("D")));
									}
									else {
										statement.execute("delete from \"BCBC\" where \"B\"=" + String.valueOf(outdatedTuple.get("B")) + "AND \"C\"=" + String.valueOf(outdatedTuple.get("C")));
									}
									if (newTuple.keySet().contains("A")) {
										statement.execute("insert into \"ABAB\" (\"A\", \"B\") values (" + String.valueOf(newTuple.get("A")) + ", " + String.valueOf(newTuple.get("B")) + ")");
									}
									else if (newTuple.keySet().contains("D")) { 
										statement.execute("insert into \"CDCD\" (\"C\", \"D\") values (" + String.valueOf(newTuple.get("C")) + ", " + String.valueOf(newTuple.get("D")) + ")");
									}
									else {
										statement.execute("insert into \"BCBC\" (\"B\", \"C\") values (" + String.valueOf(newTuple.get("B")) + ", " + String.valueOf(newTuple.get("C")) + ")");
									}
								}
								else {
									break;
								} 
							}
							
							resultset = statement.executeQuery("WITH R11 as (SELECT \"A\", \"B\" FROM \"AB\" EXCEPT SELECT \"A\", \"B\" FROM \"ABAB\"), R22 as (SELECT \"B\", \"C\" FROM \"BC\" EXCEPT SELECT \"B\", \"C\" FROM \"BCBC\"), R33 as (SELECT \"C\", \"D\" FROM \"CD\" EXCEPT SELECT \"C\", \"D\" FROM \"CDCD\") SELECT COUNT(*) FROM R11, R22, R33 WHERE R11.\"B\" = R22.\"B\" AND R22.\"C\" = R33.\"C\"");
							resultset.next();
							if (resultset.getInt(1) <= k) {
								finalTuplesIndex = selectedTuplesIndex;
								break;
							}
							selectedTuplesIndex.clear();
							for (int i = 0; i < selectedTuplesIndexNext.size(); i++) {
								selectedTuplesIndex.add(selectedTuplesIndexNext.get(i));
							}
							for (int i = selectedTuplesIndexNext.size() - 1; i >= 0; i--) {
								int tupleIndex = selectedTuplesIndexNext.get(i);
								if (tupleIndex >= endogenousInputData.size() - (selectedTuplesIndexNext.size()-i)) {
									selectedTuplesIndexNext.set(i, -1);
								}
								else {
									tupleIndex += 1;
									selectedTuplesIndexNext.set(i, tupleIndex);
									for (int j = i + 1; j < selectedTuplesIndexNext.size(); j++) {
										selectedTuplesIndexNext.set(j, selectedTuplesIndexNext.get(j-1)+1);
									}
									break;
								}
							}
							if (selectedTuplesIndexNext.get(0) == -1) {
								layer += 1;
							}
						}
					}
					
					ArrayList<HashMap<String, Object>> ret = new ArrayList<HashMap<String, Object>>();
					for (int i: finalTuplesIndex) {
						ret.add(endogenousInputData.get(i));
					}
					System.out.println(ret);
				    System.out.println(System.currentTimeMillis() - start);
				    
				}
			
				catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
}
