/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.jdbc.api;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.extensions.TestSetup;
import junit.framework.Assert;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.admin.api.core.Admin;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.EmbeddedDriver;
import com.metamatrix.script.io.MetadataReader;
import com.metamatrix.script.io.ResultSetReader;
import com.metamatrix.script.io.StringArrayReader;


/** 
 * This class can be used as the base class to write MMQuery based tests for 
 * integration testing. Just like the scripted one this one should provide all
 * those required flexibility in testing.
 */
public class AbstractMMQueryTestCase extends TestCase {
	
	static {
		new EmbeddedDriver();
	}
	
	public interface ConnectionFactory {
		
		com.metamatrix.jdbc.api.Connection createSingleConnection() throws Exception;
	}
    
    protected com.metamatrix.jdbc.api.Connection internalConnection = null;
    protected static com.metamatrix.jdbc.api.Connection SINGLE_CONNECTION;
    protected ResultSet internalResultSet = null;
    protected Statement internalStatement = null;
    protected String DELIMITER = "    "; //$NON-NLS-1$ 
    
    public AbstractMMQueryTestCase() {
    }
    
    public AbstractMMQueryTestCase(String name) {
    	super(name);
    }
    
    @Override
    protected void setUp() throws Exception {
    	if (SINGLE_CONNECTION != null) {
    		this.internalConnection = SINGLE_CONNECTION;
    	}
    }
    
    public static TestSetup createOnceRunSuite(TestSuite suite, final ConnectionFactory factory) {
		TestSetup wrapper = new TestSetup(suite) {
			@Override
			protected void setUp() throws Exception {
				SINGLE_CONNECTION = factory.createSingleConnection();
			}
			@Override
			protected void tearDown() throws Exception {
				SINGLE_CONNECTION.close();
				SINGLE_CONNECTION = null;
			}
		};
		return wrapper;
	}
          
    public com.metamatrix.jdbc.api.Connection getConnection(String vdb){
        String propsFile = UnitTestUtil.getTestDataPath()+"/mmquery/mm.properties"; //$NON-NLS-1$
        return getConnection(vdb, propsFile);
    }
    
    public Admin getAdmin() {
        try {
            assertNotNull(this.internalConnection);
            return this.internalConnection.getAdminAPI();
        } catch (SQLException e) {
        	throw new RuntimeException(e);
        }
    }
    
    public com.metamatrix.jdbc.api.Connection getConnection(String vdb, String propsFile){
        return getConnection(vdb, propsFile, "");  //$NON-NLS-1$
    }
    
    public com.metamatrix.jdbc.api.Connection getConnection(String vdb, String propsFile, String addtionalStuff){
        try {
			this.internalConnection = createConnection(vdb, propsFile, addtionalStuff);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} 
        return this.internalConnection;
    }
    
    public static com.metamatrix.jdbc.api.Connection createConnection(String vdb, String propsFile, String addtionalStuff) throws SQLException {
        String url = "jdbc:metamatrix:"+vdb+"@" + propsFile+addtionalStuff; //$NON-NLS-1$ //$NON-NLS-2$
        return (com.metamatrix.jdbc.api.Connection)DriverManager.getConnection(url); 
    }    
    
    
    public ResultSet execute(String sql) {
        return execute(sql, new Object[] {});
    }
    
    public ResultSet execute(String sql, Object[] params) {
        try {
            assertNotNull(this.internalConnection);
            assertTrue(!this.internalConnection.isClosed());
            
            if (sql.indexOf("?") != -1) { //$NON-NLS-1$
                executePreparedStatement(sql, params);
            }
            
            this.internalStatement = this.internalConnection.createStatement();
            this.internalResultSet = this.internalStatement.executeQuery(sql);
            return this.internalResultSet;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
    }
    
    public ResultSet executePreparedStatement(String sql, Object[] params) {
        try {
            assertNotNull(this.internalConnection);
            assertTrue(!this.internalConnection.isClosed());
            
            if (sql.startsWith("exec ")) { //$NON-NLS-1$
                executeStoredProcedure(sql, params);
            }
            
            this.internalStatement = this.internalConnection.prepareStatement(sql);
            setParameters((PreparedStatement)this.internalStatement, params);
            this.internalResultSet = ((PreparedStatement)this.internalStatement).executeQuery();
            return this.internalResultSet;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
    }    
    
    public ResultSet executeStoredProcedure(String sql, Object[] params) {
        try {
            assertNotNull(this.internalConnection);
            assertTrue(!this.internalConnection.isClosed());

            if (sql.startsWith("exec ")) { //$NON-NLS-1$
                sql = sql.substring(5);
            }
            this.internalStatement = this.internalConnection.prepareCall("{?=call "+sql+"}"); //$NON-NLS-1$ //$NON-NLS-2$
            setParameters((CallableStatement)this.internalStatement, params);
            this.internalResultSet = ((CallableStatement)this.internalStatement).executeQuery();
            return this.internalResultSet;                        
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
    }
    
    private void setParameters(PreparedStatement stmt, Object[] params) throws SQLException{
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i+1, params[i]);
        }
    }
    
    public void assertResultsSetEquals(File expected) {
    	assertResultsSetEquals(this.internalResultSet, expected);
    }
    
    public void assertResultsSetEquals(ResultSet resultSet, File expected) {
        assertNotNull(resultSet);
        
        BufferedReader  resultReader = null;
        BufferedReader  expectedReader = null;
        try {
            resultReader = new BufferedReader(new ResultSetReader(resultSet, DELIMITER));
            expectedReader = new BufferedReader(new FileReader(expected));    
            compareResults(resultReader, expectedReader);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }finally {
            try {
                resultReader.close();
                expectedReader.close();
            } catch (IOException e) {
            	throw new RuntimeException(e);
            }               
        }
    }

    public void assertResultsSetEquals(String expected) {
    	assertResultsSetEquals(this.internalResultSet, expected);
    }
    
    public void assertResultsSetEquals(ResultSet resultSet,String expected) {
        assertNotNull(resultSet);
        
        BufferedReader  resultReader = null;
        BufferedReader  expectedReader = null;
        try {
            resultReader = new BufferedReader(new ResultSetReader(resultSet, DELIMITER));
            expectedReader = new BufferedReader(new StringReader(expected));        
            compareResults(resultReader, expectedReader);
        }catch(Exception e){
        	throw new RuntimeException(e);
        }finally {
            try {
                resultReader.close();
                expectedReader.close();
            } catch (IOException e) {
            	throw new RuntimeException(e);
            }               
        }
    }

    public void assertResults(String[] expected) {
        assertResultsSetEquals(expected);
    }

    public void assertResultsSetEquals(String[] expected) {
    	assertResultsSetEquals(this.internalResultSet, expected);
    }
    
    public void assertResultsSetEquals(ResultSet resultSet, String[] expected) {
        assertNotNull(resultSet);
        
        BufferedReader  resultReader = null;
        BufferedReader  expectedReader = null;
        try {
            resultReader = new BufferedReader(new ResultSetReader(resultSet, DELIMITER));
            expectedReader = new BufferedReader(new StringArrayReader(expected));
            compareResults(resultReader, expectedReader);
        }catch(Exception e){
        	throw new RuntimeException(e);
        }finally {
            try {
                resultReader.close();
                expectedReader.close();
            } catch (IOException e) {
            	throw new RuntimeException(e);
            } 
        }
    }
    
    public void assertEquals(Reader expected, Reader reader) {
        
        BufferedReader  resultReader = null;
        BufferedReader  expectedReader = null;        
        try {
            expectedReader = new BufferedReader(expected);
            resultReader = new BufferedReader(reader);
            compareResults(resultReader, expectedReader);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        } finally {
            try {
                resultReader.close();
                expectedReader.close();
            } catch (IOException e) {
            	throw new RuntimeException(e);
            }             
        }
    }
    
    public void assertResultsSetMetadataEquals(ResultSetMetaData metadata, File expected) {
        assertNotNull(metadata);
        
        BufferedReader  resultReader = null;
        BufferedReader  expectedReader = null;
        try {
            resultReader = new BufferedReader(new MetadataReader(metadata, DELIMITER));
            expectedReader = new BufferedReader(new FileReader(expected));    
            compareResults(resultReader, expectedReader);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }finally {
            try {
                resultReader.close();
                expectedReader.close();
            } catch (IOException e) {
            	throw new RuntimeException(e);
            }               
        }    	
    }

    public void assertResultsSetMetadataEquals(ResultSetMetaData metadata, String[] expected) {
        assertNotNull(metadata);
        
        BufferedReader  resultReader = null;
        BufferedReader  expectedReader = null;
        try {
            resultReader = new BufferedReader(new MetadataReader(metadata, DELIMITER));
            expectedReader = new BufferedReader(new StringArrayReader(expected));
            compareResults(resultReader, expectedReader);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }finally {
            try {
                resultReader.close();
                expectedReader.close();
            } catch (IOException e) {
            	throw new RuntimeException(e);
            }               
        }    	
    }
    

    int compareResults(BufferedReader resultReader, BufferedReader expectedReader) throws IOException {
        int lineCount = 0;    
        while(true) {
            // Line count
            lineCount++;
                
            String resultLine = resultReader.readLine();
            String expectedLine = expectedReader.readLine();
            
            if (resultLine == null && expectedLine != null) {
                // more results available
                fail("More expected result lines available than actual results, line="+lineCount); //$NON-NLS-1$
                return 1;
            }
            else if (resultLine != null && expectedLine == null) {
                // less results available
                fail("More actual results are available than expected results, line="+lineCount + "\n" + resultLine); //$NON-NLS-1$ //$NON-NLS-2$
                return -1;
            }
            
            if (resultLine == null && expectedLine == null) {
                // matched
                return 0;
            }

            resultLine = resultLine.trim();
            expectedLine = expectedLine.trim();
            Assert.assertEquals(expectedLine, resultLine);
        }    
    }
    
    public void printResults() {
        printResults(this.internalResultSet);
    }

    public void printResults(ResultSet results) {
        printResults(results, false);
    }

    public void printResults(boolean comparePrint) {
        assertNotNull(this.internalResultSet);
        printResults(this.internalResultSet, comparePrint);
    }

    public void walkResults() {
        assertNotNull(this.internalResultSet);
        
        try {
            int columnCount = this.internalResultSet.getMetaData().getColumnCount();
            while(this.internalResultSet.next()) {
                for (int col = 1; col <= columnCount; col++) {
                    this.internalResultSet.getObject(col);
                }
            }    
            closeResultSet();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void printResults(ResultSet results, boolean comparePrint) {
        if(results == null) {
            System.out.println("ResultSet is null"); //$NON-NLS-1$
            return;
        }        
        int row;
        try {
            row = -1;
            BufferedReader in = new BufferedReader(new ResultSetReader(results, DELIMITER));
            String line = in.readLine();
            while(line != null) {
                row++;
                if (comparePrint) {
                    line=line.replaceAll("\"", "\\\\\""); //$NON-NLS-1$ //$NON-NLS-2$
                    System.out.println("\""+line+"\","); //$NON-NLS-1$ //$NON-NLS-2$
                }
                else {
                    System.out.println(line);
                }
                line = in.readLine();
            }
            System.out.println("Fetched "+row+" rows\n"); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (IOException e) {
            throw new RuntimeException(e);
        }        
    }


    public void assertRowCount(int expected) {
        int count = getRowCount();
        assertEquals(expected, count);
    }

    public int getRowCount() {
        assertNotNull(this.internalResultSet);
        // Count all
        try {
            int count = 0;
            while(this.internalResultSet.next()) {
                count++;
            }
            return count;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }    
    
    public void closeStatement() {
        closeResultSet();

        if (this.internalStatement != null){
            try {
                this.internalStatement.close();
                this.internalStatement = null;
            } catch(SQLException e) {
            	throw new RuntimeException(e);
            }    
        }
    }

    public void closeResultSet() {
        if (this.internalResultSet != null) {        
            try {
                this.internalResultSet.close();
                this.internalResultSet = null;
            } catch(SQLException e) {
                // ignore
            }
        }
    }    

    public void closeConnection() {
        closeStatement();
        if (this.internalConnection != null) {
            try {
                this.internalConnection.close();
                this.internalConnection = null;
            } catch(SQLException e) {
            	throw new RuntimeException(e);
            }
        }
    }
    
    public void print(String msg) {
        System.out.println(msg);
    }
    
    public void print(Throwable e) {
        e.printStackTrace();
    }
    
    protected void helpTest(String query, String[] expected, String vdb, String props, String urlProperties) {
        getConnection(vdb, props, urlProperties);
        executeAndAssertResults(query, expected);
        closeConnection();
    }
    
    protected void executeAndAssertResults(String query, String[] expected) {
        execute(query);
        if (expected != null) {
            assertResults(expected);
        }
        else {
            printResults();
        }    	
    }
}