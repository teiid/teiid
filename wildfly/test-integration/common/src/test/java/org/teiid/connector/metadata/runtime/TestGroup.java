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

package org.teiid.connector.metadata.runtime;

import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.metadata.Table;


/**
 */
public class TestGroup extends TestCase {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());

    /**
     * Constructor for TestGroup.
     * @param name
     */
    public TestGroup(String name) {
        super(name);
    }

    private static String getTestVDBName() {
        return UnitTestUtil.getTestDataPath() + "/ConnectorMetadata.vdb"; //$NON-NLS-1$
    }

    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);
    }

    // ################ TEST GROUP METADATAID ######################

    public Table getGroup(String groupName, TranslationUtility transUtil) throws Exception {
        Select query = (Select) transUtil.parseCommand("SELECT 1 FROM " + groupName); //$NON-NLS-1$
        NamedTable group = (NamedTable) query.getFrom().get(0);
        return group.getMetadataObject();
    }

    public void helpTestGroup(String fullGroupName, String nameInSource, Properties expectedProps, TranslationUtility transUtil) throws Exception {
        Table group = getGroup(fullGroupName, transUtil);
        assertEquals("table name in source", group.getNameInSource()); //$NON-NLS-1$

        Map<String, String> extProps = group.getProperties();
        assertEquals(expectedProps, extProps);
    }

    public void testGroup() throws Exception {
        Properties props = new Properties();
        props.put("TestExtraProperty", "extension prop value"); //$NON-NLS-1$ //$NON-NLS-2$
        helpTestGroup("ConnectorMetadata.TestTable", "TestTable", props, CONNECTOR_METADATA_UTILITY);//$NON-NLS-1$ //$NON-NLS-2$
    }


}
