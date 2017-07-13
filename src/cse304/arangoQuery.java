/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cse304;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.util.MapBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.exception.VPackException;
import java.util.Map;

public class arangoQuery {
    public static void main (String[] args) {
        ArangoDB arangoDB = new ArangoDB.Builder().user("root").password("1234").build();
        String dbName = "database";
        String collectionName = "wikiPageLog";
        String string="\"%9%\"";
        
        try {
                        long startTime = System.currentTimeMillis();
			final String query = "FOR t IN wikiPageLog\n"
                               // + " FILTER t.active==1\n"
                               +"SORT t.@name "
                                + " RETURN t";
                                
                               
                           
                             
			final Map<String, Object> bindVars = new MapBuilder().put("name", "timestamp").get();
			final ArangoCursor<BaseDocument> cursor = arangoDB.db(dbName).query(query, bindVars, null,
				BaseDocument.class);
                    //    System.out.println(cursor.getCount());
                        int counter=0;
			for (; cursor.hasNext();) {
				System.out.println(" " + cursor.next().getAttribute("logtitle"));
                                counter++;
			}
                        
                        
                        long endTime   = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        System.out.println("timeTaken "+totalTime);
                        System.out.println("total row "+counter);
		} catch (final ArangoDBException e) {
			System.err.println("Failed to execute query. " + e.getMessage());
		}
        
        
        try {
                        long startTime = System.currentTimeMillis();
			final String query = "FOR t IN wikiPageLog FILTER LIKE(t.action, @name) RETURN t";
			final Map<String, Object> bindVars = new MapBuilder().put("name", "%delete%").get();
			final ArangoCursor<BaseDocument> cursor = arangoDB.db(dbName).query(query, bindVars, null,
				BaseDocument.class);
                        int counter=0;
			for (; cursor.hasNext();) {
				System.out.println(" " + cursor.next().getAttribute("contributor_name"));
                                counter++;
			}
                        
                        long endTime   = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        System.out.println("timeTaken "+totalTime);
                        System.out.println("total delete "+counter);
		} catch (final ArangoDBException e) {
			System.err.println("Failed to execute query. " + e.getMessage());
		}
 
 
 
 try {
                        long startTime = System.currentTimeMillis();
			final String query = "FOR t IN wikiPageLog FILTER LIKE(t.logtitle, @name) RETURN t";
			final Map<String, Object> bindVars = new MapBuilder().put("name", "%bush%").get();
			final ArangoCursor<BaseDocument> cursor = arangoDB.db(dbName).query(query, bindVars, null,
				BaseDocument.class);
                        int counter=0;
			for (; cursor.hasNext();) {
				System.out.println(" " + cursor.next().getAttribute("logtitle"));
                                counter++;
                        }
                       
                        
                        long endTime   = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        System.out.println("timeTaken "+totalTime);
                        System.out.println("total logtitles containg bush "+ counter);
		} catch (final ArangoDBException e) {
			System.err.println("Failed to execute query. " + e.getMessage());
		}
    
    }
}