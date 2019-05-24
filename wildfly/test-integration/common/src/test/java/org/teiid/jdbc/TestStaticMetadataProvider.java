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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.teiid.client.metadata.ResultsMetadataConstants;
import org.teiid.dqp.internal.process.MetaDataProcessor;
import org.teiid.jdbc.MetadataProvider;


/**
 */
public class TestStaticMetadataProvider extends TestCase {

    /**
     * Constructor for TestStaticMetadataProvider.
     * @param name
     */
    public TestStaticMetadataProvider(String name) {
        super(name);
    }

    private MetadataProvider example1() throws Exception {
        MetaDataProcessor processor = new MetaDataProcessor(null, null, "vdb", 1); //$NON-NLS-1$
        Map[] columnMetadata = new Map[] {
            processor.getDefaultColumn("table", "c1", String.class), //$NON-NLS-1$ //$NON-NLS-2$
            processor.getDefaultColumn("table", "c2", Integer.class) //$NON-NLS-1$ //$NON-NLS-2$
        };

        return new MetadataProvider(columnMetadata);
    }

    public void testMetadata() throws Exception {
        MetadataProvider provider = example1();
        assertEquals(2, provider.getColumnCount());

        for(int i=0; i<provider.getColumnCount(); i++) {
            assertNotNull(provider.getValue(i, ResultsMetadataConstants.VIRTUAL_DATABASE_NAME));
            assertNotNull(provider.getValue(i, ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION));
            assertNotNull(provider.getValue(i, ResultsMetadataConstants.GROUP_NAME));
            assertNotNull(provider.getValue(i, ResultsMetadataConstants.ELEMENT_NAME));

        }
    }

    public void testGetStringValue() throws Exception {
        Integer property = ResultsMetadataConstants.VIRTUAL_DATABASE_NAME;
        String value = "vdb"; //$NON-NLS-1$

        Map columnMetadata = new HashMap();
        columnMetadata.put(property, value);

        MetadataProvider md = new MetadataProvider(new Map[] {columnMetadata});

        String actualValue = md.getStringValue(0, property);
        assertEquals(value, actualValue);
    }

    public void testGetIntValue() throws Exception {
        Integer property = ResultsMetadataConstants.VIRTUAL_DATABASE_NAME;
        Integer value = new Integer(10);

        Map columnMetadata = new HashMap();
        columnMetadata.put(property, value);

        MetadataProvider md = new MetadataProvider(new Map[] {columnMetadata});

        int actualValue = md.getIntValue(0, property);
        assertEquals(10, actualValue);
    }

    public void testGetBooleanValue() throws Exception {
        Integer property = ResultsMetadataConstants.VIRTUAL_DATABASE_NAME;
        Boolean value = Boolean.TRUE;

        Map columnMetadata = new HashMap();
        columnMetadata.put(property, value);

        MetadataProvider md = new MetadataProvider(new Map[] {columnMetadata});

        boolean actualValue = md.getBooleanValue(0, property);
        assertEquals(true, actualValue);
    }

}
