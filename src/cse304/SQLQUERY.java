/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cse304;

import java.sql.*;

/**
 * A JDBC SELECT (JDBC query) example program.
 */
class SQLQUERY {
 
    public static void main (String[] args) {
        try {
            String url = "jdbc:sqlserver://localhost:1433;" +
			"databaseName=master;integratedSecurity=true;";
            Connection conn = DriverManager.getConnection(url,"","");
            Statement stmt = conn.createStatement();
            ResultSet rs;
 
            //rs = stmt.executeQuery("SELECT action FROM dbo.wikiPageLog");
            long startTime = System.currentTimeMillis();
            rs = stmt.executeQuery("select logtitle as total from dbo.wikiPageLog order by timestamp asc;");
            int counter=0;
            while ( rs.next() ) {
                counter++;
                String name = rs.getString("total");
               //  count = rs.getInt("total");
                System.out.println("logtitle" + " " + name);
            }
            conn.close();
            long endTime   = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            System.out.println("timeTaken "+totalTime);
            System.out.println("Total logtitle sorted "+counter);
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }
        
       try {
            String url = "jdbc:sqlserver://localhost:1433;" +
			"databaseName=master;integratedSecurity=true;";
            Connection conn = DriverManager.getConnection(url,"","");
            Statement stmt = conn.createStatement();
            ResultSet rs;
 
            //rs = stmt.executeQuery("SELECT action FROM dbo.wikiPageLog");
            long startTime = System.currentTimeMillis();
            rs = stmt.executeQuery("select distinct contributor_name as name from dbo.wikiPageLog where action LIKE '%delete%'");
            int counter=0;
            while ( rs.next() ) {
                counter++;
               // String name = rs.getString("contributor_name");
                String count = rs.getString("name");
                System.out.println(count);
            }
            conn.close();
            long endTime   = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            System.out.println("timeTaken "+totalTime);
            System.out.println("Total deletes contributed :"+ counter);
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }
        
          
        
          try {
            String url = "jdbc:sqlserver://localhost:1433;" +
			"databaseName=master;integratedSecurity=true;";
            Connection conn = DriverManager.getConnection(url,"","");
            Statement stmt = conn.createStatement();
            ResultSet rs;
 
            //rs = stmt.executeQuery("SELECT action FROM dbo.wikiPageLog");
            long startTime = System.currentTimeMillis();
            rs = stmt.executeQuery("select logtitle as name from dbo.wikiPageLog where logtitle LIKE '%bush%'");
            int counter=0;
            while ( rs.next() ) {
                counter++;
                String name = rs.getString("name");
         //       String count = rs.getString("actionCount");
                System.out.println(name );
            }
            conn.close();
            long endTime   = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            System.out.println("timeTaken " +totalTime);
            System.out.println("total titles containing bush "+counter);
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }
        
    }
}