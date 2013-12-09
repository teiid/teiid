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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.accumulo.core.data.Range;
import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.metadata.Column;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;


@SuppressWarnings("nls")
public class TestAccumuloQueryVisitor {
    private AccumuloExecutionFactory translator;
    private TranslationUtility utility;
   
    
    @Before
    public void setUp() throws Exception {
    	this.translator = new AccumuloExecutionFactory();
    	this.translator.start();

    	TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sampledb.ddl")), "sakila", "rental");
    	this.utility = new TranslationUtility(metadata);
    }

	@Test
	public void testSelectStar() {
		Command cmd = this.utility.parseCommand("select * from Customer");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertTrue(visitor.getRanges().isEmpty());
		assertNotNull(visitor.projectedColumns());
		
		ArrayList<Column> columns = visitor.projectedColumns();
		assertEquals("customer", visitor.getScanTable().getName());
		
		assertEquals(2, columns.size());
		Column rowid = columns.get(0);
		Column name = columns.get(1);
		
		assertEquals("customer_id", rowid.getName());
		assertEquals("rowid", rowid.getNameInSource());
		
		assertEquals("name", name.getName());
		assertEquals("customer", name.getProperty(AccumuloMetadataProcessor.CF, false));
		assertEquals("nameAttribute", name.getProperty(AccumuloMetadataProcessor.CQ, false));
		
	}

	
	@Test
	public void testSelectColumn() {
		Command cmd = this.utility.parseCommand("select name from Customer");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertTrue(visitor.getRanges().isEmpty());
		assertNotNull(visitor.projectedColumns());
		ArrayList<Column> columns = visitor.projectedColumns();
		assertEquals(1, columns.size());
		Column name = columns.get(0);
		assertEquals("name", name.getName());
		assertEquals("customer", name.getProperty(AccumuloMetadataProcessor.CF, false));
		assertEquals("nameAttribute", name.getProperty(AccumuloMetadataProcessor.CQ, false));
	}	
	
	@Test
	public void testSelectEquality() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id = 1");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(1, visitor.getRanges().size());
		Range range = visitor.getRanges().get(0);
		assertEquals(Range.exact("1"), range);
	}
	
	@Test
	public void testWhereIN() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id IN (1,2)");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(2, visitor.getRanges().size());
		assertEquals(Range.exact("2"), visitor.getRanges().get(0));
		assertEquals(Range.exact("1"), visitor.getRanges().get(1));
	}	
	
	@Test
	public void testWhereNOT_IN() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id NOT IN (1,2)");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(3, visitor.getRanges().size());
		assertEquals(new Range("2", false, null, true), visitor.getRanges().get(0));
		assertEquals(new Range("1", false, "2", false), visitor.getRanges().get(1));
		assertEquals(new Range(null, true, "1", false), visitor.getRanges().get(2));
	}
	
	@Test
	public void testWhereComapreLE() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id < 2");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(1, visitor.getRanges().size());
		assertEquals(new Range(null, true, "2", false), visitor.getRanges().get(0));
	}
	
	@Test
	public void testWhereComapreLEEQ() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id <= 2");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(1, visitor.getRanges().size());
		assertEquals(new Range(null, true, "2", true), visitor.getRanges().get(0));
	}		
	

	@Test
	public void testWhereComapreGT() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id > 2");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(1, visitor.getRanges().size());
		assertEquals(new Range("2", false, null, true), visitor.getRanges().get(0));
	}
	
	@Test
	public void testWhereComapreGTEQ() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id >= 2");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(1, visitor.getRanges().size());
		assertEquals(new Range("2", true, null, true), visitor.getRanges().get(0));
	}
	
	@Test
	public void testWhereComapreNOTEQ() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id <> 2");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(2, visitor.getRanges().size());
		assertEquals(new Range(null, true, "2", false), visitor.getRanges().get(0));
		assertEquals(new Range("2", false, null, true), visitor.getRanges().get(1));
	}	

	@Test
	public void testWhereComapreAND1() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id < 2 and customer_id > 4");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(2, visitor.getRanges().size());
		Range r1 = new Range(null, true, "2", false);
		Range r2 = new Range("4", false, null, true);
		assertEquals(Range.mergeOverlapping(Arrays.asList(r1, r2)), visitor.getRanges());
	}
	
	@Test
	public void testWhereComapreAND2() {
		Command cmd = this.utility.parseCommand("select name from Customer where customer_id < 2 and customer_id != 4");
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor();
		visitor.visitNode(cmd);
		
		assertEquals("customer", visitor.getScanTable().getName());
		assertEquals(2, visitor.getRanges().size());
		Range r1 = new Range(null, true, "2", false);
		Range r2 = new Range(null, true, "4", false);
		Range r3 = new Range("4", false, null, true);
		assertEquals(Range.mergeOverlapping(Arrays.asList(r1, r2, r3)), visitor.getRanges());
	}	
	
}
