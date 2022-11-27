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

import java.util.Map;

import org.teiid.dqp.internal.process.MetaDataProcessor;

import junit.framework.TestCase;


/**
 */
@SuppressWarnings("nls")
public class TestResultsMetadataWithProvider extends TestCase {

    /**
     * Constructor for TestResultsMetadataWithProvider.
     * @param name
     */
    public TestResultsMetadataWithProvider(String name) {
        super(name);
    }

    public MetadataProvider exampleProvider() throws Exception {
        MetaDataProcessor processor = new MetaDataProcessor(null, null, "vdb", 1); //$NON-NLS-1$
        Map col1 = processor.getDefaultColumn("table", "col1", "col1Label", String.class); //$NON-NLS-1$ //$NON-NLS-2$
        Map col2 = processor.getDefaultColumn("table", "col2", "col2Label", Integer.class); //$NON-NLS-1$ //$NON-NLS-2$

        Map[] columnMetadata = new Map[] {
            col1, col2
        };

        MetadataProvider provider = new MetadataProvider(columnMetadata);
        return provider;
    }

    public void test1() throws Exception {
        ResultSetMetaDataImpl rmd = new ResultSetMetaDataImpl(exampleProvider(), null);

        assertEquals(false, rmd.isAutoIncrement(1));
        assertEquals(true, rmd.isCaseSensitive(1));
        assertEquals(false, rmd.isCurrency(1));
        assertEquals(true, rmd.isDefinitelyWritable(1));
        assertEquals(false, rmd.isReadOnly(1));
        assertEquals(true, rmd.isSearchable(1));
        assertEquals(true, rmd.isSigned(1));
        assertEquals(true, rmd.isWritable(1));
        assertEquals("vdb", rmd.getCatalogName(1)); //$NON-NLS-1$
        assertEquals(null, rmd.getSchemaName(1));
        assertEquals("table", rmd.getTableName(1)); //$NON-NLS-1$
        assertEquals("col1", rmd.getColumnName(1)); //$NON-NLS-1$
        assertEquals("col1Label", rmd.getColumnLabel(1)); //$NON-NLS-1$
        assertEquals("string", rmd.getColumnTypeName(1)); //$NON-NLS-1$
    }

    public void test2BackwardCompatibilityTest() throws Exception {
        ResultSetMetaDataImpl rmd = new ResultSetMetaDataImpl(exampleProvider(), "false");

        assertEquals(false, rmd.isAutoIncrement(1));
        assertEquals(true, rmd.isCaseSensitive(1));
        assertEquals(false, rmd.isCurrency(1));
        assertEquals(true, rmd.isDefinitelyWritable(1));
        assertEquals(false, rmd.isReadOnly(1));
        assertEquals(true, rmd.isSearchable(1));
        assertEquals(true, rmd.isSigned(1));
        assertEquals(true, rmd.isWritable(1));
        assertEquals("vdb", rmd.getCatalogName(1)); //$NON-NLS-1$
        assertEquals(null, rmd.getSchemaName(1));
        assertEquals("table", rmd.getTableName(1)); //$NON-NLS-1$
        assertEquals("col1Label", rmd.getColumnName(1)); //$NON-NLS-1$
        assertEquals("col1Label", rmd.getColumnLabel(1)); //$NON-NLS-1$
        assertEquals("string", rmd.getColumnTypeName(1)); //$NON-NLS-1$
    }
}
