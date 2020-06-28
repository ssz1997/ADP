import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Baseline {

	
	
	public static void main(String[] args) {
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test3", "postgres", "postgres")) {
			
			// initialize k and projections
			int k = 155;
			ArrayList<String> projection = new ArrayList<String>();
			projection.add("A");
			projection.add("B");
			projection.add("C");
			projection.add("D");
			projection.add("E");
			projection.add("F");
//			projection.add("G");

//			projection.add("B1");
//			projection.add("C1");
//			projection.add("B2");
//			projection.add("C2");
//			projection.add("B3");
//			projection.add("C3");
			

			



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
			
			int numOfTuples = 0;
			for (String tableName: tables_attrs.keySet()) {
				resultset = statement.executeQuery("SELECT COUNT(\"" + tables_attrs.get(tableName).get(0) + "\") FROM " + tableName);
				resultset.next();
				numOfTuples += resultset.getInt(1);
			}
			
			double expo = Math.pow(2, numOfTuples) - 1;
			NumberFormat fmt = NumberFormat.getInstance();
			fmt.setGroupingUsed(false);
			fmt.setMaximumIntegerDigits(999);
			for (int i = 1; i <= expo; i++) {
				String usage = String.valueOf(i);
				String leadingZeros = "0".repeat(numOfTuples - usage.length());
					
				}
			}
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			

		    //System.out.println(System.currentTimeMillis() - start);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
