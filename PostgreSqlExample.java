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
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/movielens", "postgres", "postgres")) {


			System.out.println("Connected to PostgreSQL database!");
			
			// get tables 
			int k = 10;
			CalculatingTuples test = new CalculatingTuples(connection, k);
			
			// only one table

			
			// there is a relation which is a subset of all tables
			
			
			
			
			
			
			Statement statement = connection.createStatement();
			//System.out.println("Reading car records...");
			//System.out.printf("%-30.30s  %-30.30s%n", "Model", "Price");
			//ResultSet resultSet = statement.executeQuery("SELECT * FROM movies");
			//while (resultSet.next()) {
				//System.out.println(resultSet.getString("movie_title"));
			//}

		} /*catch (ClassNotFoundException e) {
			System.out.println("PostgreSQL JDBC driver not found.");
			e.printStackTrace();
		}*/ catch (SQLException e) {
			System.out.println("Connection failure.");
			e.printStackTrace();
		}
	}
}
