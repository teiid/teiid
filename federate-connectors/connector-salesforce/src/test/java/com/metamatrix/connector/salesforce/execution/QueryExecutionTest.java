/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package com.metamatrix.connector.salesforce.execution;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.connector.salesforce.execution.BaseExecutionTest;
import com.metamatrix.data.exception.ConnectorException;

public class QueryExecutionTest extends BaseExecutionTest {

	public void testSelectAllFromAccount() {
		try {
			List result = host.executeCommand("select * from Account");
			assertEquals(12, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectNameFromAccount() {
		try {
			List result = host.executeCommand("select Name from Account");
			assertEquals(12, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	
	public void testSelectNameFromAccountToken() {
		try {
			List result = noCredHost.executeCommand("select Name from Account");
			assertEquals(12, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
			return;
		}
	}

	// test for JBEDSP-21
	public void testSelectNameIDFromAccount() {
		try {
			List result = host.executeCommand("select Name, Id from Account");
			assertEquals(12, result.size());
			List row = (List)result.get(0);
			assertEquals(2, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithStringCriteria() {
		try {
			List result = host.executeCommand("select * from Account where Name = 'GenePoint' and Website = 'www.genepoint.com'");
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithHugeINCriteria() {
		try {
			String quote = "'";
			StringBuffer params = new StringBuffer();
			params.append("'GenePoint'");
			String param = "param";
			for (int i = 0; i != 10000; i++) {
				params.append(",").append(quote).append(param).append(i).append(quote);
			}
			
			List result = host.executeCommand("select * from Account where Name IN (" + params.toString() + ")");
		} catch (ConnectorException e) {
			assertEquals("Queries cannot exceed 10,000 characters",e.getMessage());
		}
	}
	
	//TOD0: Turn this into a scripted test, the IN limitation does not seem to be enforced
	// by the test kit.
	// Tests that a query with too large a IN clause does not get past the connector.
/*	public void testSelectAllFromAccountWith600INLimit() {
		try {
			String quote = "'";
			StringBuffer params = new StringBuffer();
			params.append("'GenePoint'");
			String param = "param";
			for (int i = 0; i != 10000; i++) {
				params.append(",").append(quote).append(param).append(i).append(quote);
			}
			
			List result = IN600Host.executeCommand("select * from Account where Name IN (" + params.toString() + ")");
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
*/	
	public void testSelectAllFromAccountWithBooleanEqualsCriteria() {
		try {
			List result = host.executeCommand("select * from Account where IsDeleted = 'false'");
			assertEquals(12, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithBoolean2EqualsCriteria() {
		try {
			List result = host.executeCommand("select * from Account where IsDeleted = false");
			assertEquals(12, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithBooleanEqualsCriteriaAndLimit() {
		try {
			List result = host.executeCommand("select * from Account where IsDeleted = 'false' LIMIT 5");
			assertEquals(5, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithIntegerGTCriteria() {
		try {
			List result = host.executeCommand("select * from Account where NumberOfEmployees > 999");
			assertEquals(9, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithLIKEOnId() {
		try {
			List result = host.executeCommand("select * from Account where Id LIKE '%badness'");
		} catch (ConnectorException e) {
			assertEquals("LIKE criteria are not allowed on columns of native type Id",e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithIntegerLTCriteria() {
		try {
			List result = host.executeCommand("select * from Account where NumberOfEmployees < 999");
			assertEquals(2, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithReversedCriteria() {
		try {
			List result = host.executeCommand("select * from Account where 'GenePoint' = Name");
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromAccountWithInCriteria() {
		try {
			List result = host.executeCommand("select Name from Account where Name IN('genepoint', 'sforce')");
			assertEquals(2, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectNameFromContactWithInCriteriaMultiSelect() {
		try {
			List result = host.executeCommand("select Name from Contact where ContactBy__c IN('Email', 'Phone')");
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
			assertEquals("Brock Sampson", (String)row.get(0));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectNameFromContactMultiSelect() {
		try {
			List result = host.executeCommand("select Name from Contact where ContactBy__c IN('Email','Phone')");
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
			assertEquals("Brock Sampson", (String)row.get(0));
		} catch (ConnectorException e) {
			assertTrue(e.getMessage().startsWith("The IN criteria is not supported on multi-select columns"));
		}
	}
	
	public void testSelectAllFromAccountWithLikeCriteria() {
		try {
			List result = host.executeCommand("select * from Account where Name LIKE 'gene%'");
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllFromUnqueryableTable() {
		try {
			host.executeCommand("select * from ProcessInstanceHistory");
		} catch (ConnectorException e) {
			assertTrue(e instanceof ConnectorException);
			//assertTrue(e instanceof UnsupportedStatementException);
			return;
		}
		fail("Should have thrown an exception.  This table does not support Query");
	}
	
	public void testLikeAndNEOnColumn() {
		try {
			List result = host.executeCommand("Select BillingCountry from Account where BillingCountry LIKE ('US%') and BillingCountry != 'USA'");
			// optimized to where BillingCountry LIKE 'US%'  this is not an optimization, this is a semantic change.
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
			Iterator iter = row.iterator();
			while(iter.hasNext()) {
				assertEquals("US shoud be the result", "US", ((String)iter.next()));
			}
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectAllColumnMismatch() {
		try {
			List result = host.executeCommand("Select * from Account where BillingCountry LIKE ('US%') and BillingCountry != 'USA'");
			// optimized to where BillingCountry LIKE 'US%'  this is not an optimization, this is a semantic change.
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(38, row.size());
			Iterator iter = row.iterator();
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}

	public void testSelectNameFromContactWithCriteriaMultiSelect() {
		try {
			List result = host.executeCommand("select Name from Contact where ContactBy__c = 'Email ; Phone'");
			assertEquals(1, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
			assertEquals("Brock Sampson", (String)row.get(0));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectNameFromContactWithSingleCriteriaMultiSelect() {
		// This test demonstrates the issue in JBEDSP-196.  This single value IN is converted to = and matches nothing.
		try {
			List result = host.executeCommand("select Name from Contact where ContactBy__c IN('Email')");
			assertEquals(0, result.size());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	/*  ORDER_BY no longer supported by connector	
	public void testSelectNameFromContactWithInCriteriaMultiSelectOrderBy() {
		try {
			List result = host.executeCommand("select Name from Contact where ContactBy__c IN ('Email;Phone','Seance') ORDER BY NAME");
			assertEquals(2, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
			assertEquals("Brock Sampson", (String)row.get(0));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}

	
	public void testSelectNameFromContactWithInCriteriaMultiSelectOrderBy2() {
		try {
			List result = host.executeCommand("select Name, Department from Contact where ContactBy__c IN ('Email;Phone','Seance') ORDER BY Department");
			assertEquals(2, result.size());
			List row = (List)result.get(0);
			assertEquals(2, row.size());
			assertEquals("Jonas Venture", (String)row.get(0));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSelectNameFromContactWithNotInCriteriaMultiSelect() {
		try {
			List result = host.executeCommand("select Name from Contact where ContactBy__c NOT IN ('Email','Phone')");
			assertEquals(23, result.size());
			List row = (List)result.get(0);
			assertEquals(1, row.size());
			Iterator iter = row.iterator();
			while(iter.hasNext()) {
				assertFalse("Brock Sampson should not be in this result", ((String)iter.next()).equals("Brock Sampson"));
			}
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
*/
	// JIRA https://jira.jboss.org/jira/browse/JBEDSP-598
	public void testSelectAllFromAccountWithDateInStrings() {
		try {
			List result = host.executeCommand("SELECT * from Account where LastActivityDate IN ('2008-01-01', '2008-01-31')");
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
		// JIRA https://jira.jboss.org/jira/browse/JBEDSP-598
	public void testSelectAllFromAccountWithDateIn() {
		try {
			List result = host.executeCommand("SELECT * from Account where LastActivityDate IN ({d'2008-01-01'}, {d'2008-01-31'})");
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	// JIRA https://jira.jboss.org/jira/browse/JBEDSP-598
	public void testSelectAllFromAccountWithDate() {
		try {
			List result = host.executeCommand("SELECT * from Account where LastActivityDate = {d'2008-01-01'}");
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
}
