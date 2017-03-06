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
package org.teiid.translator.solr;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URLDecoder;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestTeiidLanguageGroupingToSolr {

	private TransformationMetadata metadata;
	private SolrExecutionFactory translator;
	private TranslationUtility utility;
	
	private QueryMetadataInterface setUp(String ddl, String vdbName,
			String modelName) throws Exception {

		this.translator = new SolrExecutionFactory();
		this.translator.start();

		metadata = RealMetadataFactory.fromDDL(ddl, vdbName, modelName);
		this.utility = new TranslationUtility(metadata);

		return metadata;
	}

	private String getSolrTranslation(String sql) throws IOException, Exception {
		Select select = (Select) getCommand(sql);
		SolrSQLHierarchyVistor visitor = new SolrSQLHierarchyVistor(this.utility.createRuntimeMetadata(), this.translator);
		visitor.visit(select);
		String cmd =  visitor.getSolrQuery().toString();
		return cmd;
	}

	public Command getCommand(String sql) throws IOException, Exception {
		CommandBuilder builder = new CommandBuilder(setUp(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("exampleTBL.ddl")), "exampleVDB","exampleModel"));
		return builder.getCommand(sql);

	}

	@Test
	public void testGroupBySingleField() throws Exception {
		assertEquals("fl=name,1&rows=0&facet=true&facet.pivot=name&facet.missing=true&q=*:*",
				getSolrTranslation("select name, count(*)  from example group by name"));		
	}
	
	@Test
	public void testGroupByMultipleFieldsInOrder() throws Exception {
		assertEquals("fl=name,popularity,1&rows=0&facet=true&facet.pivot=name,popularity&facet.missing=true&q=*:*",
				getSolrTranslation("select name, popularity, count(*) from example group by name, popularity"));		
	}
	
	@Test
	public void testGroupByMultipleFieldsOutOfOrder() throws Exception {
		assertEquals("fl=popularity,name,1&rows=0&facet=true&facet.pivot=name,popularity&facet.missing=true&q=*:*",
				getSolrTranslation("select popularity, name, count(*) from example group by name, popularity"));		
	}
	
	@Test
	public void testGroupByDateRangeFunctionWithMinuteGap() throws Exception {
		assertEquals(
					"fl=purchasets,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1MINUTE&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))" 
					,
		getSolrTranslation(
					"Select PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM-dd HH:mm') ,'yyyy-MM-dd HH:mm'), count(*) from example " + 
					"where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' " + 
					"group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM-dd HH:mm') ,'yyyy-MM-dd HH:mm');"
					));
	
	}
	
	@Test
	public void testGroupByDateRangeFunctionWithHourGap() throws Exception {
		assertEquals(
					"fl=purchasets,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1HOUR&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))" 
					,
		getSolrTranslation(
					"Select PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM-dd HH') ,'yyyy-MM-dd HH'), count(*) from example " + 
					"where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' " + 
					"group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM-dd HH') ,'yyyy-MM-dd HH');"
					));
	
	}
	
	@Test
	public void testGroupByDateRangeFunctionWithDayGap() throws Exception {
		assertEquals(
					"fl=purchasets,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1DAY&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))" 
					,
		getSolrTranslation(
					"Select PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM-dd') ,'yyyy-MM-dd'), count(*) from example " + 
					"where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' " + 
					"group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM-dd') ,'yyyy-MM-dd');"
					));
	
	}
	
	@Test
	public void testGroupByDateRangeFunctionWithMonthGap() throws Exception {
		assertEquals(
					"fl=purchasets,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1MONTH&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))" 
					,
		getSolrTranslation(
					"Select PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), count(*) from example " + 
					"where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' " + 
					"group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM');"
					));
	
	}
	
	@Test
	public void testGroupByDateRangeFunctionWithYearGap() throws Exception {
		assertEquals(
					"fl=purchasets,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1YEAR&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))" 
					,
		getSolrTranslation(
					"Select PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy') ,'yyyy'), count(*) from example " + 
					"where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' " + 
					"group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy') ,'yyyy');"
					));
	
	}
	
	@Test
	public void testGroupByDateRangeOnSingleFieldInOrder() throws Exception {
		assertEquals(
					"fl=purchasets,name,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1MONTH&"
					+ "facet.pivot={!range%3Dr1}name&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))"
					,
		getSolrTranslation(
				"Select PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), name, count(*) from example " + 
			 	"where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' " + 
			 	"group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), name;"
			 	));
	}
	
	@Test
	public void testGroupByDateRangeOnSingleFieldOutOfOrder() throws Exception {
		assertEquals(
					"fl=purchasets,name,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1MONTH&"
					+ "facet.pivot={!range%3Dr1}name&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))"
					,
		getSolrTranslation(
				"Select PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), name, count(*) from example " 
			 	+ "where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' "
			 	+ "group by name, PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM');"
			 	));
	}
	
	@Test
	public void testGroupByDateRangeOnMultipleFieldsInOrder() throws Exception {
		assertEquals(
					"fl=purchasets,name,popularity,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1MONTH&"
					+ "facet.pivot={!range%3Dr1}name,popularity&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))"
					,
		getSolrTranslation(
				"Select PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), name, popularity, count(*) from example "
			 	+ "where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' " 
			 	+ "group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), name, popularity;"
			 	));
	}
	
	@Test
	public void testGroupByDateRangeOnMultipleFieldsOutOfOrder() throws Exception {
		assertEquals(
					"fl=name,popularity,purchasets,1&rows=0&facet=true&"
					+ "facet.range={!tag%3Dr1}purchasets&"
					+ "facet.range.start=2015-08-01T04:00:00:000Z&"
					+ "facet.range.end=2015-10-31T15:13:32:536Z&"
					+ "facet.range.gap=%2B1MONTH&"
					+ "facet.pivot={!range%3Dr1}name,popularity&"
					+ "facet.missing=true&"
					+ "q=((purchasets:[2015\\-08\\-01T04\\:00\\:00\\:000Z+TO+*])+AND+(purchasets:[*+TO+2015\\-10\\-31T15\\:13\\:32\\:536Z]))"
					,
		getSolrTranslation(
				"Select name, popularity, PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), count(*) from example " 
			 	+ "where purchasets between '2015-08-01 04:00:00' and '2015-10-31 15:13:32.536' "
			 	+ "group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), name, popularity;"
			 	));
	}
	
	@Test(expected=TranslatorException.class)
	public void testGroupByDateRangeWithoutDateBoundaries() throws Exception {
		getSolrTranslation(
				"Select name, popularity, PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), count(*) from example " 
			 	+ "group by PARSETIMESTAMP(FORMATTIMESTAMP(purchasets,'yyyy-MM') ,'yyyy-MM'), name, popularity;");
	}
	
	@Before public void setUp() { 
		TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT")); //$NON-NLS-1$ 
	}
	
	@After public void tearDown() { 
		TimestampWithTimezone.resetCalendar(null);
	}

}
