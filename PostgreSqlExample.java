package example;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set; 




public class PostgreSqlExample {
	
	
	public static void main(String[] args) {
		// movielens is an example
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/movielens", "postgres", "postgres")) {

			System.out.println("Connected to PostgreSQL database!");
			
			// initialize k
			int k = 10;
			
			// start calculation
			CalculatingTuples test = new CalculatingTuples(connection, k);
				
			
		} catch (SQLException e) {
			System.out.println("Connection failure.");
			e.printStackTrace();
		}
	}
}
