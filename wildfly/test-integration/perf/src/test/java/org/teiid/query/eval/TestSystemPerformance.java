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

package org.teiid.query.eval;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.TranslatorException;

@SuppressWarnings({"nls"})
public class TestSystemPerformance extends AbstractQueryTest {

    private static final int TABLES = 2000;
    private static final int COLS = 16;
    EmbeddedServer es;

    @Before public void setup() throws VirtualDatabaseException, ConnectorManagerException, TranslatorException {
        es = new EmbeddedServer();
        es.start(new EmbeddedConfiguration());
        ModelMetaData mmm = new ModelMetaData();
        mmm.setName("test");
        mmm.setSchemaSourceType("native");
        mmm.addSourceMapping("x", "hc", null);
        HardCodedExecutionFactory hardCodedExecutionFactory = new HardCodedExecutionFactory() {
            @Override
            public void getMetadata(MetadataFactory metadataFactory, Object conn)
                    throws TranslatorException {
                String[] colNames = new String[COLS];
                for (int i = 0; i < colNames.length; i++) {
                    colNames[i] = "col" + i;
                }
                for (int i = 0; i < TABLES; i++) {
                    Table t = metadataFactory.addTable("x" + i);
                    for (int j = 0; j < COLS; j++) {
                        metadataFactory.addColumn(colNames[j], "string", t);
                    }
                }
            }

            @Override
            public boolean isSourceRequiredForMetadata() {
                return false;
            }
        };
        es.addTranslator("hc", hardCodedExecutionFactory);
        es.deployVDB("test", mmm);
    }

    @After public void teardown() {
        es.stop();
    }

    @Test public void testColumnPerformance() throws Exception {
        Connection c = es.getDriver().connect("jdbc:teiid:test", null);
        setConnection(c);
        DatabaseMetaData metadata = c.getMetaData();
        for (int i = 0; i < TABLES; i++) {
            internalResultSet = metadata.getColumns(null, "test", "x" + i, null);
            assertRowCount(COLS);
            internalResultSet.close();
        }
    }

    @Test public void testSQLXML() throws Exception {
        Connection c = es.getDriver().connect("jdbc:teiid:test", null);
        String sql = "select xmlelement(root, xmlelement(root1, xmlagg(x))) from (select xmlelement(x, tablename, xmlagg(xmlforest(name)), '\n') as x from sys.columns group by tablename) as y"; //$NON-NLS-1$
        PreparedStatement s = c.prepareStatement(sql);
        for (int i = 0; i < 100; i++) {
            s.execute();
            ResultSet rs = s.getResultSet();
            rs.next();
            rs.getString(1);
            rs.close();
        }
        c.close();
    }

}
