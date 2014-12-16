package com.learninghadoop2.hive.client;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
     private static String driverName = "org.apache.hive.jdbc.HiveDriver";
     
     // connection string
     public static String URL = "jdbc:hive2://localhost:10000";

     // Show all tables in the default database
     public static String QUERY = "show tables";

     public static void main(String[] args) throws SQLException {
          try {
               Class.forName (driverName);
          } 
          catch (ClassNotFoundException e) {
               e.printStackTrace();
               System.exit(1);
          }
          Connection con = DriverManager.getConnection (URL);
          Statement stmt = con.createStatement();
          
          ResultSet resultSet = stmt.executeQuery(QUERY);
          while (resultSet.next()) {
               System.out.println(resultSet.getString(1));
          }
    }
}
