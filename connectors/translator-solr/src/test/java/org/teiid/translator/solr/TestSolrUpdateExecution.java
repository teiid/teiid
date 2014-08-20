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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.CommandContext;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

@SuppressWarnings("nls")
public class TestSolrUpdateExecution {
    private SolrExecutionFactory translator;
    private TranslationUtility utility;

    @Before
    public void setUp() throws Exception {
    	this.translator = new SolrExecutionFactory();
    	this.translator.start();

    	TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("exampleTBL.ddl")), "example", "solr");
    	this.utility = new TranslationUtility(metadata);
    }
    
	private UpdateRequest helpUpdate(String query, QueryResponse... responseDocs) throws TranslatorException {
		
		Command cmd = this.utility.parseCommand(query);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		Mockito.stub(context.getCommandContext()).toReturn(Mockito.mock(CommandContext.class));
		
		SolrConnection connection = Mockito.mock(SolrConnection.class);
		UpdateResponse response = Mockito.mock(UpdateResponse.class);

		ArgumentCaptor<UpdateRequest> argument = ArgumentCaptor.forClass(UpdateRequest.class);
		
		
		Mockito.when(connection.query(Mockito.any(SolrQuery.class))).thenReturn(responseDocs[0], responseDocs[1]);
		Mockito.stub(connection.update(Mockito.any(UpdateRequest.class))).toReturn(response);
		
		UpdateExecution execution = this.translator.createUpdateExecution(cmd, context, this.utility.createRuntimeMetadata(), connection);
		execution.execute();
		
		Mockito.verify(connection).update(argument.capture());
		
		return argument.getValue();
	}    

	@Test
	public void testSimpleInsert() throws Exception {
		String query = "insert into example (price, weight, popularity, name, field) "
				+"VALUES ('1.10', '2.23', 5, 'teiid', 'any')";
		
		SolrInputDocument insert = new SolrInputDocument();
		insert.addField("price", 1.10f);
		insert.addField("weight", 2.23f);
		insert.addField("popularity", 5);
		insert.addField("name", "teiid");
		insert.addField("nis", "any");
		
		QueryResponse queryResponse = Mockito.mock(QueryResponse.class); 
		Mockito.stub(queryResponse.getResults()).toReturn(new SolrDocumentList());

		QueryResponse queryResponse2 = Mockito.mock(QueryResponse.class); 
		Mockito.stub(queryResponse2.getResults()).toReturn(new SolrDocumentList());
		
		UpdateRequest request = helpUpdate(query, queryResponse, queryResponse2);
		
		List<SolrInputDocument> docs = request.getDocuments();
		assertEquals(1, docs.size());
		assertEquals(insert.toString(), docs.get(0).toString());
	}
	
	
	@Test
	public void testSimpleUpdate() throws Exception {
		String query = "Update example set field = 'some' where name = 'teiid'";
		
		SolrDocument doc = new SolrDocument();
		doc.addField("price", 1.10f);
		doc.addField("weight", 2.23f);
		doc.addField("popularity", 5);
		doc.addField("name", "teiid");
		doc.addField("nis", "any");

		SolrDocumentList list = new SolrDocumentList();
		list.add(doc);
		
		QueryResponse queryResponse = Mockito.mock(QueryResponse.class); 
		Mockito.stub(queryResponse.getResults()).toReturn(list);

		QueryResponse queryResponse2 = Mockito.mock(QueryResponse.class); 
		Mockito.stub(queryResponse2.getResults()).toReturn(new SolrDocumentList());
		
		UpdateRequest request = helpUpdate(query, queryResponse, queryResponse2);
		List<SolrInputDocument> docs = request.getDocuments();
		assertEquals(1, docs.size());
		
		SolrInputDocument update = new SolrInputDocument();
		update.addField("price", 1.10f);
		update.addField("weight", 2.23f);
		update.addField("popularity", 5);
		update.addField("name", "teiid");
		update.addField("nis", "some");		
		assertEquals(update.toString(), docs.get(0).toString());
	}	

	@Test
	public void testSimpleDelete() throws Exception {
		String query = "Delete from example where name = 'teiid'";
		
		SolrDocument doc = new SolrDocument();
		doc.addField("price", 1.10f);
		doc.addField("weight", 2.23f);
		doc.addField("popularity", 5);
		doc.addField("name", "teiid");
		doc.addField("nis", "any");

		SolrDocumentList list = new SolrDocumentList();
		list.add(doc);
		
		QueryResponse queryResponse = Mockito.mock(QueryResponse.class); 
		Mockito.stub(queryResponse.getResults()).toReturn(list);

		QueryResponse queryResponse2 = Mockito.mock(QueryResponse.class); 
		Mockito.stub(queryResponse2.getResults()).toReturn(new SolrDocumentList());
		
		UpdateRequest request = helpUpdate(query, queryResponse, queryResponse2);
		List<SolrInputDocument> docs = request.getDocuments();

		UpdateRequest expected = new UpdateRequest();
		expected.deleteById("teiid");
		
		assertEquals(expected.getDeleteById().toString(), request.getDeleteById().toString());
	}	
}
