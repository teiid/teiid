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
package org.teiid.olingo.service;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.teiid.odata.api.ComplexResponse;
import org.teiid.olingo.ComplexReturnType;
import org.teiid.olingo.TeiidODataJsonSerializer;

public class CrossJoinResult implements ComplexResponse {
    private String nextToken;
    private CrossJoinNode documentNode;
    private List<List<ComplexReturnType>> out = new ArrayList<List<ComplexReturnType>>();
    private String baseURL;

    public CrossJoinResult(String baseURL, CrossJoinNode context) {
        this.baseURL = baseURL;
        this.documentNode = context;
    }

    @Override
    public void addRow(ResultSet rs) throws SQLException {

        ArrayList<ComplexReturnType> row = new ArrayList<ComplexReturnType>();

        Entity entity = EntityCollectionResponse.createEntity(rs,
                this.documentNode, this.baseURL, null);

        row.add(new ComplexReturnType(this.documentNode.getName(),
                this.documentNode.getEdmStructuredType(), entity, this.documentNode
                        .hasExpand()));

        for (DocumentNode node : this.documentNode.getSiblings()) {
            Entity sibiling = EntityCollectionResponse.createEntity(rs, node,
                    this.baseURL, null);

            row.add(new ComplexReturnType(node.getName(),
                    node.getEdmStructuredType(), sibiling,
                    ((CrossJoinNode) node).hasExpand()));
        }
        this.out.add(row);
    }

    public CrossJoinNode getResource() {
        return this.documentNode;
    }

    public List<List<ComplexReturnType>> getResults(){
        return this.out;
    }

    @Override
    public long size() {
        return this.out.size();
    }

    @Override
    public void setCount(long count) {
    }

    @Override
    public void setNextToken(String token) {
        this.nextToken = token;
    }

    @Override
    public String getNextToken() {
        return this.nextToken;
    }

    @Override
    public void serialize(ODataResponse response,
            TeiidODataJsonSerializer serializer, ServiceMetadata metadata,
            ContextURL contextURL, URI next) throws SerializerException {
        response.setContent(serializer.complexCollection(metadata,
                getResults(), contextURL, next)
                .getContent());
    }
}
