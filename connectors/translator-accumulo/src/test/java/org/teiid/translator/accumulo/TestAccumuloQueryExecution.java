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
package org.teiid.translator.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestAccumuloQueryExecution {
    private AccumuloExecutionFactory translator;
    private TranslationUtility utility;
    private AccumuloConnection connection;
    
    @Before
    public void setUp() throws Exception {
    	this.translator = new AccumuloExecutionFactory();
    	this.translator.start();

    	TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sampledb.ddl")), "sakila", "rental");
    	this.utility = new TranslationUtility(metadata);
    	
    	MockInstance instance = new MockInstance("teiid");
    	this.connection = Mockito.mock(AccumuloConnection.class);
    	Connector connector = instance.getConnector("root", new PasswordToken(""));
    	Mockito.stub(this.connection.getInstance()).toReturn(connector);
    }
    
	private Execution executeCmd(String sql) throws TranslatorException {
		Command cmd = this.utility.parseCommand(sql);
    	Execution exec =  this.translator.createExecution(cmd, Mockito.mock(ExecutionContext.class), this.utility.createRuntimeMetadata(), this.connection);
    	exec.execute();
    	return exec;
	}

	
    @Test
    public void testExecution() throws Exception {
    	executeCmd("insert into customer (customer_id, name) values (2, 'Joe')");
    	executeCmd("insert into customer (customer_id, name) values (1, 'John')");
    	executeCmd("insert into customer (customer_id, name) values (3, 'Jack')");
    	
    	AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select * from customer");
    	assertEquals(Arrays.asList(1, "John"), exec.next());
    	assertEquals(Arrays.asList(2, "Joe"), exec.next());
    	assertEquals(Arrays.asList(3, "Jack"), exec.next());
    	assertNull(exec.next());
    	
    	
    	executeCmd("Update Customer set name = 'Jill' where customer_id = 2");
    	exec = (AccumuloQueryExecution)executeCmd("select * from customer where customer_id = 2");
    	assertEquals(Arrays.asList(2, "Jill"), exec.next());
    	assertNull(exec.next());
    	
    	executeCmd("delete from Customer where customer_id = 2");

    	exec = (AccumuloQueryExecution)executeCmd("select * from customer");
    	assertEquals(Arrays.asList(1, "John"), exec.next());
    	assertEquals(Arrays.asList(3, "Jack"), exec.next());
    	assertNull(exec.next());
    }
    
    @Test
    public void testValueInCQ() throws Exception {
    	executeCmd("insert into rental (rental_id, amount, customer_id) values (1, 3.99, 5)");
    	executeCmd("insert into rental (rental_id, amount, customer_id) values (2, 5.99, 2)");
    	executeCmd("insert into rental (rental_id, amount, customer_id) values (3, 11.99, 1)");
    	
    	AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select * from rental");
    	assertEquals(Arrays.asList(1, new BigDecimal("3.99"), 5), exec.next());
    	assertEquals(Arrays.asList(2, new BigDecimal("5.99"), 2), exec.next());
    	assertEquals(Arrays.asList(3, new BigDecimal("11.99"), 1), exec.next());
    	assertNull(exec.next());    	
    }
    
}
