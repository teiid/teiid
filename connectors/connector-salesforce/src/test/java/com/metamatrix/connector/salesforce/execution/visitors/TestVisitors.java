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
       
        // Create Account group
        FakeMetadataObject accounTable = FakeMetadataFactory.createPhysicalGroup("SalesforceModel.Account", salesforceModel); //$NON-NLS-1$
        accounTable.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, "Account"); //$NON-NLS-1$
        accounTable.setExtensionProp("Supports Query", Boolean.TRUE.toString()); //$NON-NLS-1$
        // Create Account Columns
        String[] acctNames = new String[] {
            "ID", "Name", "Stuff", "Industry"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        };
        String[] acctTypes = new String[] {  
            DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING
        };
        
        List<FakeMetadataObject> acctCols = FakeMetadataFactory.createElements(accounTable, acctNames, acctTypes);
        acctCols.get(2).putProperty(FakeMetadataObject.Props.NATIVE_TYPE, "multipicklist"); //$NON-NLS-1$
        acctCols.get(2).putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, false);
        acctCols.get(2).putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, true);
        // Set name in source on each column
        String[] accountNameInSource = new String[] {
           "id", "AccountName", "Stuff", "Industry"             //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  
        };
        for(int i=0; i<2; i++) {
            FakeMetadataObject obj = acctCols.get(i);
            obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, accountNameInSource[i]);
        }
        
        // Add all Account to the store
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(salesforceModel);
        store.addObject(accounTable);     
        store.addObjects(acctCols);
        
        // Create Contact group
        FakeMetadataObject contactTable = FakeMetadataFactory.createPhysicalGroup("SalesforceModel.Contact", salesforceModel); //$NON-NLS-1$
        contactTable.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, "Contact"); //$NON-NLS-1$
        contactTable.setExtensionProp("Supports Query", Boolean.TRUE.toString()); //$NON-NLS-1$
        // Create Contact Columns
        String[] elemNames = new String[] {
            "ContactID", "Name", "AccountId"  //$NON-NLS-1$ //$NON-NLS-2$
        };
        String[] elemTypes = new String[] {  
            DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING 
        };
        
        List<FakeMetadataObject> contactCols = FakeMetadataFactory.createElements(contactTable, elemNames, elemTypes);
        // Set name in source on each column
        String[] contactNameInSource = new String[] {
           "id", "ContactName", "accountid"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        for(int i=0; i<2; i++) {
            FakeMetadataObject obj = contactCols.get(i);
            obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, contactNameInSource[i]);
        }
        
        // Add all Account to the store
        store.addObject(salesforceModel);
        store.addObject(contactTable);     
        store.addObjects(contactCols);
        
        // Create the facade from the store
        return new FakeMetadataFacade(store);
    }    

	private static TranslationUtility translationUtility = new TranslationUtility(exampleSalesforce());

	@Test public void testOr() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("select * from Account where Name = 'foo' or Stuff = 'bar'"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.id, Account.AccountName, Account.Stuff, Account.Industry FROM Account WHERE (Account.AccountName = 'foo') OR (Account.Stuff = 'bar')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testNot() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("select * from Account where not (Name = 'foo' and Stuff = 'bar')"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.id, Account.AccountName, Account.Stuff, Account.Industry FROM Account WHERE (Account.AccountName != 'foo') OR (Account.Stuff != 'bar')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
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
		assertEquals("SELECT Account.id, Account.AccountName, Account.Stuff, Account.Industry FROM Account WHERE (NOT (Account.AccountName LIKE '%foo')) OR (Account.Stuff = 'bar')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}

	
	@Test public void testIN() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("select * from Account where Industry IN (1,2,3)"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertFalse(visitor.hasOnlyIDCriteria());
		assertEquals("SELECT Account.id, Account.AccountName, Account.Stuff, Account.Industry FROM Account WHERE Industry IN('1','2','3')", visitor.getQuery().toString().trim()); //$NON-NLS-1$
		
	}

	@Test public void testOnlyIDsIN() throws Exception {
		// this can resolve to a better performing retrieve call
		IQuery command = (IQuery)translationUtility.parseCommand("select * from Account where ID IN (1,2,3)"); //$NON-NLS-1$
		SelectVisitor visitor = new SelectVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertTrue(visitor.hasOnlyIdInCriteria());
		assertEquals("Account", visitor.getTableName());
		assertEquals("Account.id, Account.AccountName, Account.Stuff, Account.Industry", visitor.getRetrieveFieldList());
		assertEquals(new String[]{"1", "2", "3"}, visitor.getIdInCriteria());	
	}
	
	@Test public void testJoin() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("SELECT Account.Name, Contact.Name FROM Contact LEFT OUTER JOIN Account ON Account.Id = Contact.AccountId"); //$NON-NLS-1$
		SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.AccountName, Contact.ContactName FROM Contact", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}
	
	@Test public void testJoin2() throws Exception {
		IQuery command = (IQuery)translationUtility.parseCommand("SELECT Account.Name, Contact.Name FROM Account LEFT OUTER JOIN Contact ON Account.Id = Contact.AccountId"); //$NON-NLS-1$
		SelectVisitor visitor = new JoinQueryVisitor(translationUtility.createRuntimeMetadata());
		visitor.visit(command);
		assertEquals("SELECT Account.AccountName, (SELECT Contact.ContactName FROM Contacts) FROM Account", visitor.getQuery().toString().trim()); //$NON-NLS-1$
	}

}
