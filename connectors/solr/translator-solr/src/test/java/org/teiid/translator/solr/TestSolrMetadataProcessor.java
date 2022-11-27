/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.solr;

import static org.junit.Assert.*;

import java.util.EnumSet;
import java.util.LinkedHashMap;
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


        LinkedHashMap<String, FieldInfo> fields = new LinkedHashMap<String, LukeResponse.FieldInfo>();
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
                "	col1 string OPTIONS (SEARCHABLE 'Searchable'),\n" +
                "	col2 integer OPTIONS (SEARCHABLE 'Searchable'),\n" +
                "	col3 integer[] OPTIONS (SEARCHABLE 'Searchable'),\n" +
                "	id long OPTIONS (SEARCHABLE 'Searchable'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (UPDATABLE TRUE);";
        assertEquals(expected, metadataDDL);
    }
}
