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


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Stack;



/**
 * This class can be used as the base class to write Query based tests using
 * the Teiid Driver for integration testing. Just like the scripted one this one should provide all
 * those required flexibility in testing.
 */
public class AbstractMMQueryTestCase extends AbstractQueryTest {

    static {
        new TeiidDriver();
    }

    private Stack<java.sql.Connection> contexts = new Stack<java.sql.Connection>();

    public void pushConnection() {
        this.contexts.push(this.internalConnection);
        this.internalConnection = null;
    }

    public void popConnection() {
        this.internalConnection = this.contexts.pop();
    }

     public Connection getConnection(String vdb, String propsFile){
        return getConnection(vdb, propsFile, "");  //$NON-NLS-1$
    }

    public Connection getConnection(String vdb, String propsFile, String addtionalStuff){
        closeResultSet();
        closeStatement();
        closeConnection();
        try {
            this.internalConnection = createConnection(vdb, propsFile, addtionalStuff);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this.internalConnection;
    }

    public static Connection createConnection(String vdb, String propsFile, String addtionalStuff) throws SQLException {
        String url = "jdbc:teiid:"+vdb+"@" + propsFile+addtionalStuff; //$NON-NLS-1$ //$NON-NLS-2$
        return DriverManager.getConnection(url);
    }


    protected void helpTest(String query, String[] expected, String vdb, String props, String urlProperties) throws SQLException {
        getConnection(vdb, props, urlProperties);
        executeAndAssertResults(query, expected);
        closeConnection();
    }

}