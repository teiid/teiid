/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jdbc;


import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.teiid.script.io.ResultSetReader;



/**
 * This class can be used as the base class to write Query tests for
 * integration testing. Just like the scripted one this one should provide all
 * those required flexibility in testing.
 */
public abstract class AbstractQueryTest {

    //NOTE not all tests will pass with this set to true, only those with scrollable resultsets
    static boolean WRITE_ACTUAL = false;

    protected Connection internalConnection = null;
    protected ResultSet internalResultSet = null;
    protected Statement internalStatement = null;
    protected SQLException internalException = null;
    protected int updateCount = -1;
    protected String DELIMITER = "    "; //$NON-NLS-1$

    public AbstractQueryTest() {
        super();
    }


    public AbstractQueryTest(Connection conn) {
        super();
        this.internalConnection = conn;

    }


    @After public void tearDown() throws Exception {
        closeConnection();
    }

    public void setConnection(Connection c) {
        this.internalConnection = c;
    }

    public Connection getConnection() {
        return this.internalConnection;
    }

    public boolean execute(String sql) throws SQLException {
        return execute(sql, new Object[] {});
    }

    public boolean execute(String sql, Object... params) throws SQLException {
        closeResultSet();
        closeStatement();
        this.updateCount = -1;
        try {
            assertNotNull(this.internalConnection);
            assertTrue(!this.internalConnection.isClosed());
            boolean result = false;
            if (params != null && params.length > 0) {
                if (sql.startsWith("exec ")) { //$NON-NLS-1$
                    sql = sql.substring(5);
                    this.internalStatement = createPrepareCallStatement(sql);
                } else {
                    this.internalStatement = createPrepareStatement(sql);
                }
                setParameters((PreparedStatement)this.internalStatement, params);
                assignExecutionProperties(this.internalStatement);
                result = ((PreparedStatement)this.internalStatement).execute();
            } else {
                this.internalStatement = createStatement();
                assignExecutionProperties(this.internalStatement);
                result = this.internalStatement.execute(sql);
            }
            if (result) {
                this.internalResultSet = this.internalStatement.getResultSet();
            } else {
                this.updateCount = this.internalStatement.getUpdateCount();
            }
            return result;
        } catch (SQLException e) {
            this.internalException = e;
            if (!exceptionExpected()) {
                throw e;
            }
        }
        return false;
    }

    protected Statement createPrepareCallStatement(String sql) throws SQLException{
        return this.internalConnection.prepareCall("{?=call "+sql+"}");  //$NON-NLS-1$ //$NON-NLS-2$
    }

    protected Statement createPrepareStatement(String sql) throws SQLException{
        return this.internalConnection.prepareStatement(sql);
    }

    protected Statement createStatement() throws SQLException{
        return this.internalConnection.createStatement();
    }

    private void setParameters(PreparedStatement stmt, Object[] params) throws SQLException{
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i+1, params[i]);
        }
    }

    public int[] executeBatch(String[] sql) {
        return executeBatch(sql, -1);
    }

    public int[] executeBatch(String[] sql, int timeout)  {
        closeResultSet();
        closeStatement();

        try {
            assertNotNull(this.internalConnection);
            assertTrue(!this.internalConnection.isClosed());

            for (int i = 0; i < sql.length; i++) {
                if (sql[i].indexOf("?") != -1) { //$NON-NLS-1$
                    throw new RuntimeException("no prepared statements allowed in the batch command"); //$NON-NLS-1$
                }
            }

            this.internalStatement = createStatement();
            assignExecutionProperties(this.internalStatement);

            if (timeout != -1) {
                this.internalStatement.setQueryTimeout(timeout);
            }
            for (int i = 0; i < sql.length; i++) {
                this.internalStatement.addBatch(sql[i]);
            }

            return this.internalStatement.executeBatch();

        } catch (SQLException e) {
            this.internalException = e;
            if (!exceptionExpected()) {
                throw new RuntimeException(e);
            }
       }

        return null;

    }

    /**
     * Override when you need to set an execution property on the statement before execution.
     *
     * <p>Example:
     *<code>if (this.executionProperties.getProperty(ExecutionProperties.PROP_FETCH_SIZE) != null) {
     *               statement.setExecutionProperty(ExecutionProperties.PROP_FETCH_SIZE, this.executionProperties.getProperty(ExecutionProperties.PROP_FETCH_SIZE));
     *      }
     *</code>
     *
     * @param stmt
     *
     * @since
     */


    protected void assignExecutionProperties(Statement stmt) {
    }


    public boolean exceptionOccurred() {
        return this.internalException != null;
    }

    public boolean exceptionExpected() {
        return false;
    }


    public SQLException getLastException() {
        return this.internalException;
    }

    public void assertResultsSetEquals(File expected) {
        assertResultsSetEquals(this.internalResultSet, expected);
    }

    public void assertResultsSetEquals(ResultSet resultSet, File expected) {
        assertNotNull(resultSet);
        try {
            writeResultSet(expected, new BufferedReader(new ResultSetReader(resultSet, DELIMITER)));
            if (resultSet.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                resultSet.beforeFirst();
            }
            assertReaderEquals(new ResultSetReader(resultSet, DELIMITER), new FileReader(expected));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeResultSet(File expected, BufferedReader resultReader)
            throws IOException {
        if (WRITE_ACTUAL) {
            BufferedWriter bw = new BufferedWriter(new FileWriter(expected));
            String s = null;
            while ((s = resultReader.readLine()) != null) {
                bw.write(s);
                bw.write("\n"); //$NON-NLS-1$
            }
            bw.close();
        }
    }

    public void assertResultsSetEquals(String expected) {
        assertResultsSetEquals(this.internalResultSet, expected);
    }

    public void assertResultsSetEquals(ResultSet resultSet,String expected) {
        assertNotNull(resultSet);
        assertReaderEquals(new ResultSetReader(resultSet, DELIMITER), new StringReader(expected));
    }

    public void assertResults(String[] expected) {
        assertResultsSetEquals(expected);
    }

    public void assertResultsSetEquals(String[] expected) {
        assertResultsSetEquals(this.internalResultSet, expected);
    }

    public void assertResultsSetEquals(ResultSet resultSet, String[] expected) {
        assertNotNull(resultSet);
        assertReaderEquals(new ResultSetReader(resultSet, DELIMITER), new StringArrayReader(expected));
    }

    public void assertReaderEquals(Reader expected, Reader reader) {
        BufferedReader  resultReader = new BufferedReader(expected);
        BufferedReader  expectedReader = new BufferedReader(reader);
        try {
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
        try {
            writeResultSet(expected, new BufferedReader(new MetadataReader(metadata, DELIMITER)));
            assertReaderEquals(new MetadataReader(metadata, DELIMITER), new FileReader(expected));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void assertResultsSetMetadataEquals(ResultSetMetaData metadata, String[] expected) {
        assertNotNull(metadata);
        assertReaderEquals(new MetadataReader(metadata, DELIMITER), new StringArrayReader(expected));
    }

   protected static String read(BufferedReader r, boolean casesensitive) throws IOException {
        StringBuffer result = new StringBuffer();
        String s = null;
        try {
            while ((s = r.readLine()) != null) {
                result.append(  (casesensitive ? s.trim() : s.trim().toLowerCase()) );
                result.append("\n"); //$NON-NLS-1$
            }
        } finally {
            r.close();
        }
        return result.toString();
    }

    protected void compareResults(BufferedReader resultReader, BufferedReader expectedReader) throws IOException {
        assertEquals(read(expectedReader, compareCaseSensitive()) , read(resultReader, compareCaseSensitive()));
    }

    protected boolean compareCaseSensitive() {
    return true;
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

    public void assertUpdateCount(int expected) {
        assertEquals(expected, updateCount);
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
            } catch(SQLException e) {
                throw new RuntimeException(e);
            } finally {
                this.internalStatement = null;
            }
        }
    }

    public void closeResultSet() {
        this.internalException = null;

        if (this.internalResultSet != null) {
            try {
                this.internalResultSet.close();
            } catch(SQLException e) {
                // ignore
            } finally {
                this.internalResultSet = null;
            }
        }
    }

    public void closeConnection() {
        closeStatement();
        try {
            if (this.internalConnection != null) {
                try {
                    this.internalConnection.close();
                } catch(SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            this.internalConnection = null;
        }
    }

    public void cancelQuery() throws SQLException {
        assertNotNull(this.internalConnection);
        assertTrue(!this.internalConnection.isClosed());
        assertNotNull(this.internalStatement);
        this.internalStatement.cancel();
    }

    public void print(String msg) {
        System.out.println(msg);
    }

    public void print(Throwable e) {
        e.printStackTrace();
    }


    protected void executeAndAssertResults(String query, String[] expected) throws SQLException {
        execute(query);
        if (expected != null) {
            assertResults(expected);
        }
        else {
            printResults(true);
        }
    }
}