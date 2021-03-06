
package cse304;

import java.util.Map;
 
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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.text.Document;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.json.JSONException;
import org.json.JSONML;
import org.json.JSONObject;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
 
public class CSE304SQLInsert {
  public static void main(String[] args) throws JSONException, IOException {
              
                String connectionUrl = "jdbc:sqlserver://localhost:1433;" +
			"databaseName=master;integratedSecurity=true;";
                        
		// Declare the JDBC objects.
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
                try{
                    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            		con = DriverManager.getConnection(connectionUrl);
                        stmt = con.createStatement();
                }
                catch (Exception e) {
			e.printStackTrace();
		}
      
	 // ArangoDB arangoDB = new ArangoDB.Builder().user("root").password("hr").build();
	  String logtitle="";
          String contributor_name="";
	  String action="";
          String type="";
          String comment="";
          int id=0; //eita dibi hocche primary key
          int contributor_id=0;
          String timestamp="";
          
	  
	

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      
      try {
          DocumentBuilder builder = factory.newDocumentBuilder();
          for(int loger=489;loger<600;loger++){
          try {
              
              org.w3c.dom.Document doc = builder.parse("D:\\xml dataset\\enwiki-20170620-pages-logging-"+Integer.toString(loger)+".xml");
              NodeList itemList = doc.getElementsByTagName("logitem");
              NodeList nL= doc.getElementsByTagName("contributor");
              for(int i=0; i<itemList.getLength(); i++)
              {
                  Node p = itemList.item(i);
                  Element noD=(Element)p;
                  NodeList noDs=noD.getChildNodes();
                //  BaseDocument myObject = new BaseDocument();
                  
                 for(int j=0; j<noDs.getLength(); j++)
                      {
                          Node n = noDs.item(j);
                          
                          
                          if(n.getNodeType()==Node.ELEMENT_NODE && n.getNodeName().equals("contributor"))
                          {
                        
                                   
                                //    for(int ii=0;ii<itemListA.getLength();ii++){
                                  //      Node inode=itemListA.item(ii);
                                    //    Element on=(Element)inode;
                                      //  System.out.println(on.getTagName()+"hehehehehe");
                                   // }
                                    
                 /////////////////////////////////////////////////
                                                 
                                    Node noder=nL.item(i);
                                    Element name = (Element)noder;
                                    NodeList conL=name.getChildNodes();
                                    if(conL.item(1)!=null){
                                    noder=conL.item(1);
                                    name=(Element)noder;
                                    if(name.getTextContent()!=null){
                                      //  myObject.addAttribute("contributor_name",name.getTextContent());
                                        contributor_name=name.getTextContent();
                                        contributor_name=contributor_name.replace("'", " ");
                                        contributor_name=contributor_name.trim();
                                    }}
                                    if(conL.item(3)!=null){
                                    noder=conL.item(3);
                                    name=(Element)noder;
                                    if(name.getTextContent()!=null){
                                     //   myObject.addAttribute("contributor_id",name.getTextContent());
                                        contributor_id=Integer.parseInt(name.getTextContent());
                                    }}
                                    ////////////////////////////////////////////////////////////////////////////////
                                //    Node nnn=contriButorList.item(0);
                                    
                             //       System.out.println(name.getTagName()+"ore baba re baba");
                               /*     int emnei=0;
                                    StringTokenizer st = new StringTokenizer(name.getTextContent(),"\n ");
                                    while (st.hasMoreTokens()) {
                                         arr[emnei]=st.nextToken();
                                         emnei++;
                                          }
                                    emnei=0; */
                                //        String[] dic = name.getTextContent().split("\\W+");
                                  //      myObject.addAttribute("contributor_name",dic[1]);
                                    //    myObject.addAttribute("contributor_id", dic[2]);
                                        
                           //   System.out.println(dic[0]+"  ha j  "+ dic[1]+"  a  "+dic[2]);
                                    // System.out.println( "contributor_name"+" "+ dic[1]);
                                    // System.out.println( "contributor_id"+" "+ dic[2]);
                                    
                                
                              
                          }
                          else if(n.getNodeType()==Node.ELEMENT_NODE && !n.getNodeName().equals("params"))
                          {
                        
                                    Element name = (Element)n;
                                   
                                    if(name.getTagName().equals("id")) id=Integer.parseInt(name.getTextContent());
                                    else if(name.getTagName().equals("comment")) comment=name.getTextContent();
                                    else if(name.getTagName().equals("type")) type=name.getTextContent();
                                    else if(name.getTagName().equals("action")) action=name.getTextContent();
                                    else if(name.getTagName().equals("logtitle")) logtitle=name.getTextContent();
                                    else if(name.getTagName().equals("timestamp")) timestamp=name.getTextContent();
                                   
                          }
                          
                                    timestamp=timestamp.replace('T',' ');
                                    timestamp=timestamp.replace('Z',' ');
                                    comment=comment.replace("'", " ");
                                    comment=comment.trim();
                                    type=type.trim();
                                    action=action.trim();
                                    logtitle=logtitle.replace("'"," ");
                                    logtitle=logtitle.trim();
                                    timestamp=timestamp.trim();
                      }
                 
                 ///////////////eikhane sql e insert er code gula jabe :3 :3 
                  /*  try {
                        arangoDB.db(dbName).collection(collectionName).insertDocument(myObject);
                        System.out.println("Document created");
                    } catch (ArangoDBException e) {
                        System.err.println("Failed to create document. " + e.getMessage());
                    }
                    */
                    try {
        		// Establish the connection.
        		
                        
            		// Create and execute an SQL statement that returns some data.
            	//	String SQL = "SELECT * FROM dbo.spt_monitor";
                       
                        String sql = "INSERT INTO wikiPageLog " +
                   "VALUES ("+Integer.toString(id)+", '"+logtitle+"', '"+action+"', '"+type+"', '"+comment+"', '"+timestamp+"', '"+contributor_name+"', "+Integer.toString(contributor_id)+")";
                     //   sql.replace("'", "''");
            	/*	PreparedStatement prep = con.prepareStatement("INSERT INTO Ww VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
prep.setInt(1, id);
prep.setString(2, logtitle);
prep.setString(3, action);
prep.setString(4, type);
prep.setString(5, comment);
prep.setString(6, timestamp);
prep.setString(7,contributor_name);
prep.setInt(8, contributor_id);
prep.executeUpdate(); */
                     
                    
                     stmt.executeUpdate(sql);
                     System.out.println("inserted into sql server "); 
                    
                     System.out.println(id);
        	}
        
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		}
                 
              }
              
          } catch (SAXException ex) {
              Logger.getLogger(CSE304arangoInsert.class.getName()).log(Level.SEVERE, null, ex);
          } catch (IOException ex) {
              Logger.getLogger(CSE304arangoInsert.class.getName()).log(Level.SEVERE, null, ex);
          }
          }
           
         
      } catch (ParserConfigurationException ex) {
          Logger.getLogger(CSE304arangoInsert.class.getName()).log(Level.SEVERE, null, ex);
      }       
         
  }
}