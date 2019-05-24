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

import java.util.HashMap;
import java.util.Map;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Select;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;

import junit.framework.TestCase;


/**
 */
public class TestElement extends TestCase {

    private static TranslationUtility CONNECTOR_METADATA_UTILITY = createTranslationUtility(getTestVDBName());
    Map<String, String> props;
    /**
     * @param name
     */
    public TestElement(String name) {
        super(name);
    }

    private static String getTestVDBName() {
        return UnitTestUtil.getTestDataPath() + "/ConnectorMetadata.vdb"; //$NON-NLS-1$
    }

    public static TranslationUtility createTranslationUtility(String vdbName) {
        return new TranslationUtility(vdbName);
    }

    public Column getElement(String groupName, String elementName, TranslationUtility transUtil) throws Exception {
        Select query = (Select) transUtil.parseCommand("SELECT " + elementName + " FROM " + groupName); //$NON-NLS-1$ //$NON-NLS-2$
        DerivedColumn symbol = query.getDerivedColumns().get(0);
        ColumnReference element = (ColumnReference) symbol.getExpression();
        return element.getMetadataObject();
    }

    public void helpTestElement(String fullGroupName, String elementShortName, TranslationUtility transUtil,
        String nameInSource, Object defaultValue, Object minValue, Object maxValue,
        Class<?> javaType, int length, NullType nullable, int position, SearchType searchable,
        boolean autoIncrement, boolean caseSensitive, Map<String, String> expectedProps)
    throws Exception {

        Column element = getElement(fullGroupName, elementShortName, transUtil);
        assertEquals(nameInSource, element.getNameInSource());
        assertEquals(defaultValue, element.getDefaultValue());
        assertEquals(minValue, element.getMinimumValue());
        assertEquals(maxValue, element.getMaximumValue());
        assertEquals(javaType, element.getJavaType());
        assertEquals(length, element.getLength());
        assertEquals(nullable, element.getNullType());
        assertEquals(position + 1, element.getPosition());
        assertEquals(searchable, element.getSearchType());
        assertEquals(autoIncrement, element.isAutoIncremented());
        assertEquals(caseSensitive, element.isCaseSensitive());

        Map<String, String> extProps = element.getProperties();
        assertEquals(expectedProps, extProps);
    }

    public void testElement1() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestTable",  // group name       //$NON-NLS-1$
            "TestNameInSource",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            "the name in source",           // name in source   //$NON-NLS-1$
            null,                           // default value
            null,                           // minimum value
            null,                           // maximum value
            String.class,                   // java type
            10,                             // length
            NullType.Nullable,               // nullable
            0,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testElement2() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestTable",  // group name       //$NON-NLS-1$
            "TestDefaultValue",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            "1000",                         // default value //$NON-NLS-1$
            null,                           // minimum value
            null,                           // maximum value
            Integer.class,                   // java type
            0,                             // length
            NullType.Nullable,               // nullable
            1,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testElement3() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestTable",  // group name       //$NON-NLS-1$
            "TestMinMaxValue",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            "500",                       // minimum value //$NON-NLS-1$
            "25000",                        // maximum value             //$NON-NLS-1$
            Integer.class,                   // java type
            0,                             // length
            NullType.Nullable,               // nullable
            2,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testElement4() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestTable",  // group name       //$NON-NLS-1$
            "TestAutoIncrement",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            Long.class,                   // java type
            0,                             // length
            NullType.Nullable,               // nullable
            3,                              // position
            SearchType.Searchable,             // searchable
            true,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testElement5() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestTable",  // group name       //$NON-NLS-1$
            "TestCaseSensitive",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            String.class,                   // java type
            10,                             // length
            NullType.Nullable,               // nullable
            4,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            false,                          // case sensitive
            props
            );
    }

    public void testElement6() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestTable",  // group name       //$NON-NLS-1$
            "TestExtensionProp",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            Integer.class,                   // java type
            0,                             // length
            NullType.Nullable,               // nullable
            5,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testEnterpriseDataTypes() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestTable",  // group name       //$NON-NLS-1$
            "TestDataType",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            String.class,                   // java type
            DataTypeManager.MAX_STRING_LENGTH,                             // length
            NullType.Nullable,               // nullable
            6,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testUnsearchable() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestSearchableColumns",  // group name       //$NON-NLS-1$
            "TestUnsearchable",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            Integer.class,                   // java type
            0,                             // length
            NullType.Nullable,               // nullable
            0,                              // position
            SearchType.Unsearchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testSearchableLike() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestSearchableColumns",  // group name       //$NON-NLS-1$
            "TestSearchableLike",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            String.class,                   // java type
            10,                             // length
            NullType.Nullable,               // nullable
            1,                              // position
            SearchType.Like_Only,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testSearchableComparable() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestSearchableColumns",  // group name       //$NON-NLS-1$
            "TestSearchableComparable",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            Integer.class,                   // java type
            0,                             // length
            NullType.Nullable,               // nullable
            2,                              // position
            SearchType.All_Except_Like,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testSearchable() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestSearchableColumns",  // group name       //$NON-NLS-1$
            "TestSearchable",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            String.class,                   // java type
            10,                             // length
            NullType.Nullable,               // nullable
            3,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testNullable() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestNullableColumns",  // group name       //$NON-NLS-1$
            "TestNullable",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            Integer.class,                   // java type
            0,                             // length
            NullType.Nullable,               // nullable
            0,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testNotNullable() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestNullableColumns",  // group name       //$NON-NLS-1$
            "TestNotNullable",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            String.class,                   // java type
            10,                             // length
            NullType.No_Nulls,               // nullable
            1,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testNullableUnknown() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestNullableColumns",  // group name       //$NON-NLS-1$
            "TestNullableUnknown",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            String.class,                   // java type
            10,                             // length
            NullType.Unknown,               // nullable
            2,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );
    }

    public void testElementWithCategories() throws Exception {
        helpTestElement(
            "ConnectorMetadata.TestCatalog.TestSchema.TestTable2",  // group name       //$NON-NLS-1$
            "TestCol",             // element name     //$NON-NLS-1$
            CONNECTOR_METADATA_UTILITY,     // translation utility
            null,                          // name in source
            null,                          // default value
            null,                       // minimum value
            null,                        // maximum value
            Integer.class,                   // java type
            0,                             // length
            NullType.Nullable,               // nullable
            0,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props
            );

    }

    @Override
    protected void setUp() throws Exception {
        props = new HashMap<String, String>();
        props.put("ColProp", "defaultvalue"); //$NON-NLS-1$ //$NON-NLS-2$
    }
}
