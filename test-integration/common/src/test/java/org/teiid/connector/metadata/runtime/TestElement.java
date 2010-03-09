/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package org.teiid.connector.metadata.runtime;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.teiid.connector.language.ColumnReference;
import org.teiid.connector.language.DerivedColumn;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.BaseColumn.NullType;
import org.teiid.connector.metadata.runtime.Column.SearchType;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.core.util.UnitTestUtil;

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
        boolean autoIncrement, boolean caseSensitive, Map<String, String> expectedProps, 
        String modeledType, String modeledBaseType, String modeledPrimitiveType) 
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
        
//System.out.println("\n" + element.getModeledType() + "\n" + element.getModeledBaseType() + "\n" + element.getModeledPrimitiveType());        

        assertEquals(modeledType, element.getDatatypeID());
        assertEquals(modeledBaseType, element.getBaseTypeID());
        assertEquals(modeledPrimitiveType, element.getPrimitiveTypeID());
        
               
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#string",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#anySimpleType",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#string"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#int",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#long",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#decimal"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#int",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#long",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#decimal"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#long",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#integer",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#decimal"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#string",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#anySimpleType",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#string"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#int",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#long",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#decimal"                             // modeled primitive type   //$NON-NLS-1$
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
            0,                             // length
            NullType.Nullable,               // nullable            
            6,                              // position
            SearchType.Searchable,             // searchable
            false,                          // auto incremented
            true,                          // case sensitive
            props,                // extension properties
            "http://www.metamatrix.com/XMLSchema/DataSets/Books/BookDatatypes#PublicationYear",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#gYear",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#gYear"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#int",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#long",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#decimal"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#string",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#anySimpleType",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#string"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#int",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#long",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#decimal"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#string",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#anySimpleType",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#string"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#int",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#long",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#decimal"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#string",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#anySimpleType",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#string"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#string",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#anySimpleType",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#string"                             // modeled primitive type   //$NON-NLS-1$
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
            props,                // extension properties
            "http://www.w3.org/2001/XMLSchema#int",                             // modeled type   //$NON-NLS-1$
            "http://www.w3.org/2001/XMLSchema#long",                             // modeled base type   //$NON-NLS-1$ 
            "http://www.w3.org/2001/XMLSchema#decimal"                             // modeled primitive type   //$NON-NLS-1$
            );
        
    }

	@Override
	protected void setUp() throws Exception {
        props = new HashMap<String, String>();
        props.put("ColProp", "defaultvalue"); //$NON-NLS-1$ //$NON-NLS-2$
	}                     
}
