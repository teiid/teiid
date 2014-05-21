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
package org.teiid.translator.infinispan.dsl.util;
 
import java.util.List;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

/**
 * This VDBUtility is building the metadata based on the JDG quickstart:  remote-query
 * because this is where the protobuf definitions are defined that are used in unit tests.
 * 
 * Also, the JDG quickstart in Teiid depends upon that quickstart so that user doesn't 
 * have to go generate the proto bin's.
 * 
 * 
 * 
 * @author vanhalbert
 *
 */

@SuppressWarnings("nls")
public class VDBUtility {


	  public static QueryMetadataInterface exampleTrades() { 
	    	MetadataStore store = new MetadataStore();
	        // Create models
	        Schema objectModel = RealMetadataFactory.createPhysicalModel("Persons_Object_Model", store); //$NON-NLS-1$
	        
	        // Create Person group
	        Table personTable = RealMetadataFactory.createPhysicalGroup("Persons_Cache", objectModel); //$NON-NLS-1$
	        personTable.setNameInSource("Persons"); //$NON-NLS-1$
	        personTable.setProperty("Supports Query", Boolean.TRUE.toString()); //$NON-NLS-1$
//	        accountTable.setProperty(Constants.SUPPORTS_RETRIEVE, Boolean.TRUE.toString());
	        // Create Account Columns
	        String[] acctNames = new String[] {
	            "PersonObject", "PersonID", "PersonName", "Email", "PhoneNumber", "PhoneType"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	        };
	        String[] acctTypes = new String[] {  
	            DataTypeManager.DefaultDataTypes.OBJECT, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING
	        };
	        
	        List<Column> acctCols = RealMetadataFactory.createElements(personTable, acctNames, acctTypes);
	        acctCols.get(0).setSearchType(SearchType.Unsearchable);
	        // Set name in source on each column
	        String[] accountNameInSource = new String[] {
	        		"this", "id", "name", "email", "PhoneNumber.number", "PhoneNumber.type"     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$  
	        };
	        for(int i=0; i<accountNameInSource.length; i++) {
	            Column obj = acctCols.get(i);
	            obj.setNameInSource(accountNameInSource[i]);
	        }
	        
	        
	        Schema personViewSchema = RealMetadataFactory.createVirtualModel("Person_View_Model", store);
	        
	        String query = "SELECT t.PersonId, t.PersonName, t.Email FROM " +
	        		"Persons_Object_Model.Persons_Cache, OBJECTTABLE('x' PASSING Persons_Object_Model.Persons_Cache.PersonObject AS x COLUMNS PersonId integer 'teiid_row.id', PersonName string 'teiid_row.name', Email string 'teiid_row.email') AS t";
	        QueryNode qn = new QueryNode(query);
	        
	        // Create Contact group
	        Table viewTable = RealMetadataFactory.createVirtualGroup("Person_View", personViewSchema, qn); //$NON-NLS-1$
	       
	        viewTable.setNameInSource("Persons_View"); //$NON-NLS-1$    
	        viewTable.setProperty("Supports Query", Boolean.TRUE.toString()); //$NON-NLS-1$
	        // Create Columns
	        String[] elemNames = new String[] {
	            "PersonID", "PersonName", "Email"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	        };
	        String[] elemTypes = new String[] {  
	            DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING
	        };
	        // List<Column> personViewCols = 
	        RealMetadataFactory.createElements(viewTable, elemNames, elemTypes);

	        
	        query = "SELECT t.PersonId, t.PersonName, t.Email, t.PhoneNumber, t.PhoneType FROM Persons_Cache";
	        QueryNode phqn = new QueryNode(query);
	        
	        // Create Contact group
	        Table phoneviewTable = RealMetadataFactory.createVirtualGroup("PersonPhone_View", personViewSchema, phqn); //$NON-NLS-1$
	       
	        phoneviewTable.setNameInSource("PersonsPhones_View"); //$NON-NLS-1$    
	        phoneviewTable.setProperty("Supports Query", Boolean.TRUE.toString()); //$NON-NLS-1$
	        // Create Columns
	        String[] pvNames = new String[] {
	            "PersonID", "PersonName", "Email", "PhoneNumber", "PhoneType"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	        };
	        String[] pvTypes = new String[] {  
	            DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING
	        };
	        
	        //List<Column> viewCols = 
	        RealMetadataFactory.createElements(phoneviewTable, pvNames, pvTypes);
       
	        return new TransformationMetadata(null, new CompositeMetadataStore(store), null, RealMetadataFactory.SFM.getSystemFunctions(), null);
	    }    

		public static TranslationUtility TRANSLATION_UTILITY = new TranslationUtility(exampleTrades());
		
		public static RuntimeMetadata RUNTIME_METADATA = TRANSLATION_UTILITY.createRuntimeMetadata();


}
