/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.jdbc.BaseDataSource;
import com.metamatrix.jdbc.EmbeddedDataSource;
import com.metamatrix.jdbc.MMDataSource;

/**
* 
*/
public class DataSourceConnection extends ConnectionStrategy {

	private String driver = null;
	private String username = null;
	private String pwd = null;
	private String applName = null;
	private String databaseName = null;
	private String serverName = null;
	private String portNumber = null;
	
	
    private XAConnection xaConnection;    
 
   public DataSourceConnection(Properties props) throws QueryTestFailedException {
	   super(props);
   }

   

	public void validate()  {
	   	databaseName = this.getEnvironment().getProperty(DS_DATABASENAME);
   	if (databaseName == null || databaseName.length() == 0) {
   		throw new TransactionRuntimeException("Property " + DS_DATABASENAME + " was not specified");
   	}
   	
	   	serverName = this.getEnvironment().getProperty(DS_SERVERNAME);
   	if (serverName == null || serverName.length() == 0) {
   		throw new TransactionRuntimeException("Property " + DS_SERVERNAME + " was not specified");
   	}

   	
   	this.portNumber = this.getEnvironment().getProperty(DS_SERVERPORT);
       
       this.applName = this.getEnvironment().getProperty(DS_APPLICATION_NAME);
       
       driver = this.getEnvironment().getProperty(DS_DRIVER);
   	if (driver == null || driver.length() == 0) {
   		throw new TransactionRuntimeException("Property " + DS_DRIVER + " was not specified");
   	}
   	
   	this.username = this.getEnvironment().getProperty(DS_USER);
   	this.pwd = this.getEnvironment().getProperty(DS_PASSWORD);
      	
	}

//	
//	   public synchronized XAConnection getXAAdminConnection() throws QueryTestFailedException {
//	       if (xaConnection == null) {
//	           try {
//	               xaConnection = createAdminConnection();
//	           } catch (Exception e) {
//	               throw new QueryTestFailedException(e);
//	           }
//	       }
//	       return xaConnection;
//	   } 


	public Connection getConnection() throws QueryTestFailedException {
       try {
           return getXAConnection().getConnection();
       } catch (QueryTestFailedException qtf) {
    	   throw qtf;
       } catch (Exception e) {
    	   e.printStackTrace();
           throw new QueryTestFailedException(e);
       }
   }
   
   public synchronized XAConnection getXAConnection() throws QueryTestFailedException {
       if (xaConnection == null) {
       	  validate();
           try {
               xaConnection = createConnection();
           } catch (Exception e) {
               throw new QueryTestFailedException(e);
           }
       }
       return xaConnection;
   }    

   private XAConnection createConnection() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
       BaseDataSource dataSource = (BaseDataSource)Class.forName(this.driver).newInstance();

       dataSource.setDatabaseName(this.databaseName);
       if (this.applName != null) {
       	dataSource.setApplicationName(this.applName);
       }
       
       if (dataSource instanceof EmbeddedDataSource) {
           ((EmbeddedDataSource)dataSource).setBootstrapFile(this.serverName);
       } else {
           ((MMDataSource)dataSource).setServerName(this.serverName);
           ((MMDataSource)dataSource).setPortNumber(Integer.parseInt(this.portNumber));            
       }
       
       if (this.username != null) {
       	dataSource.setUser(this.username);
       	dataSource.setPassword(this.pwd);
       }
	    	   
       
       return ((XADataSource)dataSource).getXAConnection();
   }
   
//   private XAConnection createAdminConnection() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
//       BaseDataSource dataSource = (BaseDataSource)Class.forName(this.driver).newInstance();
//
//       dataSource.setDatabaseName("Admin");
//       if (this.applName != null) {
//       	dataSource.setApplicationName(this.applName);
//       }
//       
//       if (dataSource instanceof EmbeddedDataSource) {
//           ((EmbeddedDataSource)dataSource).setBootstrapFile(this.serverName);
//       } else {
//           ((MMDataSource)dataSource).setServerName(this.serverName);
//           ((MMDataSource)dataSource).setPortNumber(Integer.parseInt(this.portNumber));            
//       }
//       
//       if (this.username != null) {
//       	dataSource.setUser(this.username);
//       	dataSource.setPassword(this.pwd);
//       }
//	    	   
//       
//       return ((XADataSource)dataSource).getXAConnection();
//   }

//   private static XAConnection getDataSource(String prefix, Properties props) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
//       // depending upon the DataDirect class hirarchy..
//   	DataSource dataSource = (DataSource)Class.forName(props.getProperty(DS_DRIVER)).newInstance();
//   	PropertiesUtils.setBeanProperties(dataSource, props, prefix);
//   	
//       return ((XADataSource)dataSource).getXAConnection();
//   }
   
  

   public void shutdown() {
       try {


           if (this.xaConnection != null) {
               this.xaConnection.close();
           }
       } catch (SQLException e) {
           // ignore..
       }
       
       this.xaConnection = null;
   }
}
