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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Properties;

import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.LukeResponse.FieldInfo;
import org.apache.solr.common.luke.FieldFlag;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestSolrMetadataProcessor {

	private FieldInfo buildField(String name, String type, EnumSet<FieldFlag> flags) {
		FieldInfo info = Mockito.mock(FieldInfo.class);
		Mockito.stub(info.getName()).toReturn(name);
		Mockito.stub(info.getType()).toReturn(type);
		Mockito.stub(info.getFlags()).toReturn(flags);
		return info;
	}
	
    @Test
    public void testMetadata() throws TranslatorException {
        SolrMetadataProcessor mp = new SolrMetadataProcessor();
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "solr", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        SolrConnection conn = Mockito.mock(SolrConnection.class);
        Mockito.stub(conn.getCoreName()).toReturn("SomeTable");
        
        
        HashMap<String, FieldInfo> fields = new HashMap<String, LukeResponse.FieldInfo>();
        fields.put("col1", buildField("col1", "string", EnumSet.of(FieldFlag.STORED, FieldFlag.INDEXED)));
        fields.put("col2", buildField("col2", "int", EnumSet.of(FieldFlag.STORED, FieldFlag.INDEXED)));
        fields.put("col3", buildField("col3", "int", EnumSet.of(FieldFlag.STORED, FieldFlag.INDEXED, FieldFlag.MULTI_VALUED)));
        fields.put("id", buildField("id", "long", EnumSet.of(FieldFlag.STORED, FieldFlag.INDEXED)));
        
        LukeResponse response = Mockito.mock(LukeResponse.class);;
        Mockito.stub(response.getFieldInfo()).toReturn(fields);
        
        Mockito.stub(conn.metadata(Mockito.any(LukeRequest.class))).toReturn(response);
        
        mp.process(mf, conn);

        String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        String expected = "CREATE FOREIGN TABLE SomeTable (\n" + 
        		"	id long OPTIONS (SEARCHABLE 'Searchable'),\n" + 
        		"	col1 string OPTIONS (SEARCHABLE 'Searchable'),\n" + 
        		"	col3 integer[] OPTIONS (SEARCHABLE 'Searchable'),\n" + 
        		"	col2 integer OPTIONS (SEARCHABLE 'Searchable'),\n" + 
        		"	CONSTRAINT PK0 PRIMARY KEY(id)\n" + 
        		") OPTIONS (UPDATABLE TRUE);";
        assertEquals(expected, metadataDDL);
    }
}
