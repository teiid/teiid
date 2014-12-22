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
package org.teiid.translator.hbase;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;

public class TestSQLConversionVisitor {
	
	@Test
	public void testInsert() throws TranslatorException {
		String sql = "INSERT INTO Customer VALUES('106', 'Beijing', 'Kylin Soong', '$8000.00', 'Crystal Orange')";
		String expected = "UPSERT INTO \"Customer\" (\"ROW_ID\", \"city\", \"name\", \"amount\", \"product\") VALUES ('106', 'Beijing', 'Kylin Soong', '$8000.00', 'Crystal Orange')";
		helpTest(sql, expected);
		
		sql = "INSERT INTO Customer(PK, city, name) VALUES ('109', 'Beijing', 'Kylin Soong')";
		expected = "UPSERT INTO \"Customer\" (\"ROW_ID\", \"city\", \"name\") VALUES ('109', 'Beijing', 'Kylin Soong')";
		helpTest(sql, expected);
	}
	
	@Test
	public void testBatchedInsert() throws TranslatorException {
		
	}
	
	@Test
	public void testUpdate() throws TranslatorException {
		
	}
	
	@Test
	public void testSelect() throws TranslatorException {
		
		String sql = "SELECT * FROM Customer";
		String expected = "SELECT Customer.\"ROW_ID\", Customer.\"city\", Customer.\"name\", Customer.\"amount\", Customer.\"product\" FROM \"Customer\" AS Customer";
		helpTest(sql, expected);
		
		sql = "SELECT city, amount FROM Customer";
		expected = "SELECT Customer.\"city\", Customer.\"amount\" FROM \"Customer\" AS Customer";
		helpTest(sql, expected);
		
		sql = "SELECT DISTINCT city FROM Customer";
		expected = "SELECT DISTINCT Customer.\"city\" FROM \"Customer\" AS Customer";
		helpTest(sql, expected);
		
		sql = "SELECT city, amount FROM Customer WHERE PK='105'";
		expected = "SELECT Customer.\"city\", Customer.\"amount\" FROM \"Customer\" AS Customer WHERE Customer.\"ROW_ID\" = '105'";
		helpTest(sql, expected);
		
		sql = "SELECT city, amount FROM Customer WHERE PK='105' OR name='John White'";
		expected = "SELECT Customer.\"city\", Customer.\"amount\" FROM \"Customer\" AS Customer WHERE Customer.\"ROW_ID\" = '105' OR Customer.\"name\" = 'John White'";
		helpTest(sql, expected);
		
		sql = "SELECT city, amount FROM Customer WHERE PK='105' AND name='John White'";
		expected = "SELECT Customer.\"city\", Customer.\"amount\" FROM \"Customer\" AS Customer WHERE Customer.\"ROW_ID\" = '105' AND Customer.\"name\" = 'John White'";
		helpTest(sql, expected);
	}
	
	@Test
	public void testSelectOrderBy() throws TranslatorException {
		
		String sql = "SELECT * FROM Customer ORDER BY PK";
		String expected = "SELECT Customer.\"ROW_ID\", Customer.\"city\", Customer.\"name\", Customer.\"amount\", Customer.\"product\" FROM \"Customer\" AS Customer ORDER BY Customer.\"ROW_ID\"";
		helpTest(sql, expected);
		
		sql = "SELECT * FROM Customer ORDER BY PK ASC";
		expected = "SELECT Customer.\"ROW_ID\", Customer.\"city\", Customer.\"name\", Customer.\"amount\", Customer.\"product\" FROM \"Customer\" AS Customer ORDER BY Customer.\"ROW_ID\"";
		helpTest(sql, expected);
		
		sql = "SELECT * FROM Customer ORDER BY PK DESC";
		expected = "SELECT Customer.\"ROW_ID\", Customer.\"city\", Customer.\"name\", Customer.\"amount\", Customer.\"product\" FROM \"Customer\" AS Customer ORDER BY Customer.\"ROW_ID\" DESC";
		helpTest(sql, expected);
		
		sql = "SELECT * FROM Customer ORDER BY name, city DESC";
		expected = "SELECT Customer.\"ROW_ID\", Customer.\"city\", Customer.\"name\", Customer.\"amount\", Customer.\"product\" FROM \"Customer\" AS Customer ORDER BY Customer.\"name\", Customer.\"city\" DESC";
		helpTest(sql, expected);
	}
	
	@Test
	public void testSelectGroupBy() throws TranslatorException{
		
		String sql = "SELECT COUNT(PK) FROM Customer WHERE name='John White'";
		String expected = "SELECT COUNT(Customer.\"ROW_ID\") FROM \"Customer\" AS Customer WHERE Customer.\"name\" = 'John White'";
		helpTest(sql, expected);
		
		sql = "SELECT name, COUNT(PK) FROM Customer GROUP BY name";
		expected = "SELECT Customer.\"name\", COUNT(Customer.\"ROW_ID\") FROM \"Customer\" AS Customer GROUP BY Customer.\"name\"";
		helpTest(sql, expected);
		
		sql = "SELECT name, COUNT(PK) FROM Customer GROUP BY name HAVING COUNT(PK) > 1";
		expected = "SELECT Customer.\"name\", COUNT(Customer.\"ROW_ID\") FROM \"Customer\" AS Customer GROUP BY Customer.\"name\" HAVING COUNT(Customer.\"ROW_ID\") > 1";
		helpTest(sql, expected);
		
		sql = "SELECT name, city, COUNT(PK) FROM Customer GROUP BY name, city";
		expected = "SELECT Customer.\"name\", Customer.\"city\", COUNT(Customer.\"ROW_ID\") FROM \"Customer\" AS Customer GROUP BY Customer.\"name\", Customer.\"city\"";
		helpTest(sql, expected);
		
		sql = "SELECT name, city, COUNT(PK) FROM Customer GROUP BY name, city HAVING COUNT(PK) > 1";
		expected = "SELECT Customer.\"name\", Customer.\"city\", COUNT(Customer.\"ROW_ID\") FROM \"Customer\" AS Customer GROUP BY Customer.\"name\", Customer.\"city\" HAVING COUNT(Customer.\"ROW_ID\") > 1";
		helpTest(sql, expected);
	}
	
	@Test
	public void testSelectLimit() throws TranslatorException {
		String sql = "SELECT * FROM Customer LIMIT 3";
		String expected = "SELECT Customer.\"ROW_ID\", Customer.\"city\", Customer.\"name\", Customer.\"amount\", Customer.\"product\" FROM \"Customer\" AS Customer LIMIT 3";
		helpTest(sql, expected);
		
		sql = "SELECT * FROM Customer ORDER BY PK LIMIT 3";
		expected = "SELECT Customer.\"ROW_ID\", Customer.\"city\", Customer.\"name\", Customer.\"amount\", Customer.\"product\" FROM \"Customer\" AS Customer ORDER BY Customer.\"ROW_ID\" LIMIT 3";
		helpTest(sql, expected);
	}
	
	private static TranslationUtility translationUtility = new TranslationUtility(TestHBaseUtil.queryMetadataInterface());
	
	private void helpTest(String sql, String expected) throws TranslatorException  {
		
		Command command = translationUtility.parseCommand(sql);
		
		HBaseExecutionFactory ef = new HBaseExecutionFactory();
		ef.start();
		
		SQLConversionVisitor vistor = new SQLConversionVisitor(ef);
		vistor.visitNode(command);
		
		System.out.println(vistor.getSQL());
		
		assertEquals(expected, vistor.getSQL());
		
	}

}
