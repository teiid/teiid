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
package org.teiid.translator.odata.sap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.junit.Test;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings("nls")
public class TestSAPODataMetadataProcessor {
	private SAPODataExecutionFactory translator;
	
	@Test
	public void testSchema() throws Exception {
    	translator = new SAPODataExecutionFactory();
    	translator.start();

		String csdl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sap-metadata.xml"));
		SAPMetadataProcessor processor = new SAPMetadataProcessor();
		Properties props = new Properties();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "flight", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
		processor.getMetadata(mf, new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new InputStreamReader(new ByteArrayInputStream(csdl.getBytes())))));
		
		TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "flight", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
    	ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
    	if (report.hasItems()) {
    		throw new RuntimeException(report.getFailureMessage());
    	}		
		
//		String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
//		System.out.println(ddl);	
//		
//		MetadataFactory mf2 = new MetadataFactory(null, 1, "flight", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null); 
//		QueryParser.getQueryParser().parseDDL(mf2, ddl);
    	
    	TranslationUtility utility = new TranslationUtility(metadata);
    	RuntimeMetadata rm = utility.createRuntimeMetadata();
    	
    	Table t = rm.getTable("flight", "SubscriptionCollection");
    	assertNotNull(t);
    	
    	// check the label name
    	assertNotNull(t.getColumnByName("persistNotifications"));
    	assertTrue(!t.getColumnByName("ID").isUpdatable());
    	assertEquals("Persist Notification", t.getColumnByName("persistNotifications").getAnnotation());
    	// check filterable
    	assertEquals(SearchType.Unsearchable, t.getColumnByName("persistNotifications").getSearchType());
    	// check sortable
    	assertEquals(SearchType.Unsearchable, t.getColumnByName("filter").getSearchType());
    	// check visible
    	assertEquals(false, t.getColumnByName("filter").isSelectable());
    	//check required-in-filter
    	assertEquals(1, t.getAccessPatterns().size());
    	assertEquals(2, t.getAccessPatterns().get(0).getColumns().size());
	}
}
