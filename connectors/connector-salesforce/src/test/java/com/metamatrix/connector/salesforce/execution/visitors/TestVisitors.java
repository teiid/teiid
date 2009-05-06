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
package com.metamatrix.connector.salesforce.execution.visitors;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.teiid.connector.language.IQuery;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;

public class TestVisitors {

    public static FakeMetadataFacade exampleSalesforce() { 
        // Create models
        FakeMetadataObject salesforceModel = FakeMetadataFactory.createPhysicalModel("SalesforceModel"); //$NON-NLS-1$
        
        // Create physical groups
        FakeMetadataObject table = FakeMetadataFactory.createPhysicalGroup("SalesforceModel.Account", salesforceModel); //$NON-NLS-1$
        table.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, "Account"); //$NON-NLS-1$
        table.setExtensionProp("Supports Query", Boolean.TRUE.toString()); //$NON-NLS-1$
        // Create physical elements
        String[] elemNames = new String[] {
            "AccountID", "Name", "Stuff"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        String[] elemTypes = new String[] {  
            DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING 
        };
        
        List<FakeMetadataObject> cols = FakeMetadataFactory.createElements(table, elemNames, elemTypes);
        cols.get(2).putProperty(FakeMetadataObject.Props.NATIVE_TYPE, "multipicklist"); //$NON-NLS-1$
        cols.get(2).putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, false);
        cols.get(2).putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, true);
        // Set name in source on each column
        String[] nameInSource = new String[] {
           "id", "AccountName", "Stuff"             //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  
        };
        for(int i=0; i<2; i++) {
            FakeMetadataObject obj = cols.get(i);
            obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, nameInSource[i]);
        }
        
        // Add all objects to the store
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(salesforceModel);
        store.addObject(table);     
        store.addObjects(cols);
        
        // Create the facade from the store
        return new FakeMetadataFacade(store);
    }    

	private static TranslationUtility translationUtility = new TranslationUtility(exampleSalesforce());

	@Test public void testOr() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("select * from Account where Name = 'foo' or Stuff = 'bar'"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT id, AccountName, Stuff FROM Account WHERE (AccountName = 'foo') OR (Stuff = 'bar')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testNot() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("select * from Account where not (Name = 'foo' and Stuff = 'bar')"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT id, AccountName, Stuff FROM Account WHERE NOT ((AccountName = 'foo') AND (Stuff = 'bar'))", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testCountStart() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("select count(*) from Account"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT count() FROM Account", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testNotLike() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("select * from Account where Name not like '%foo' or Stuff = 'bar'"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT id, AccountName, Stuff FROM Account WHERE (NOT (Account.AccountName LIKE '%foo')) OR (Stuff = 'bar')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}

}
