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

package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;

@SuppressWarnings("nls")
public class TestMetadataProcessor {

    @Test public void testInvalidIndex() throws SQLException {
        JDBCMetadataProcessor processor = new JDBCMetadataProcessor();
        processor.setImportIndexes(true);
        processor.setWidenUnsingedTypes(false);
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        DatabaseMetaData dmd = Mockito.mock(DatabaseMetaData.class);
        Table t = mf.addTable("c");
        JDBCMetadataProcessor.TableInfo ti = new JDBCMetadataProcessor.TableInfo("a", "b", "c", t);

        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.stub(rs.next()).toAnswer(new Answer<Boolean>() {
            int count = 0;
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                if (count++ == 0) {
                    return true;
                }
                return false;
            }
        });
        //intentionally leave the column name null

        Mockito.stub(rs.getShort(7)).toReturn(DatabaseMetaData.tableIndexOther);
        Mockito.stub(dmd.getIndexInfo("a", "b", "c", false, true)).toReturn(rs);

        processor.getIndexes(mf, dmd, Arrays.asList(ti), false);
        Mockito.verify(rs).getString(9);
        assertTrue(t.getIndexes().isEmpty());
    }

    /**
     * JDBC says to return an empty string, but some sources return null and we need to handle the null case anyways
     */
    @Test public void testQuoteStringNull() {
        JDBCMetadataProcessor jmp = new JDBCMetadataProcessor();
        assertEquals("x", jmp.quoteName("x"));
    }

    @Test public void testArrayRuntimeType() {
        JDBCMetadataProcessor jmp = new JDBCMetadataProcessor() {
            @Override
            protected String getNativeComponentType(String typeName) {
                return typeName.substring(0, typeName.length() -2);
            }
        };
        jmp.typeMapping.put("varchar", Types.VARCHAR);

        assertEquals("string[]", jmp.getRuntimeType(Types.ARRAY, "varchar[]", 100, 0));
    }

}
