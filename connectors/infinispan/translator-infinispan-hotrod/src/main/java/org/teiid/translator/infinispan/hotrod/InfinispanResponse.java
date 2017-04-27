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
package org.teiid.translator.infinispan.hotrod;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.infinispan.api.InfinispanDocument;
import org.teiid.translator.document.DocumentNode;

public class InfinispanResponse {
    private Query query;
    private int batchSize;
    private Integer offset;
    private Integer limit;
    private boolean lastBatch = false;
    private Iterator<Object> responseIter;
    private List<String> projected;
    private DocumentNode documentNode;
    private List<Map<String, Object>> currentDocumentRows;

    public InfinispanResponse(RemoteCache<Object, Object> cache, String queryStr, int batchSize, Integer limit,
            Integer offset, List<String> projected, DocumentNode documentNode) {
        this.batchSize = batchSize;
        this.offset = offset == null?0:offset;
        this.limit = limit;
        this.projected = projected;
        this.documentNode = documentNode;

        QueryFactory qf = Search.getQueryFactory(cache);
        this.query = qf.create(queryStr);
    }

    private void fetchNextBatch() {
        query.startOffset(offset);

        int nextBatch = this.batchSize;
        if (this.limit != null) {
            if (this.limit > nextBatch) {
                this.limit = this.limit - nextBatch;
            } else {
                nextBatch = this.limit;
                this.limit = 0;
                this.lastBatch = true;
            }
        }
        query.maxResults(nextBatch);
        List<Object> values = query.list();

        if (query.getResultSize() < nextBatch) {
            this.lastBatch = true;
        }

        this.responseIter = values.iterator();
        offset = offset + nextBatch;
    }

    public List<Object> getNextRow(){
        if (this.currentDocumentRows != null && !this.currentDocumentRows.isEmpty()) {
            return buildRow(this.currentDocumentRows.remove(0));
        }

        if (responseIter == null) {
            fetchNextBatch();
        }

        if (responseIter != null && responseIter.hasNext()){
            Object row = this.responseIter.next();
            if (row instanceof Object[]) {
                return Arrays.asList((Object[])row);
            }
            this.currentDocumentRows = this.documentNode.tuples((InfinispanDocument)row);
        } else {
            if (lastBatch) {
                return null;
            } else {
                fetchNextBatch();
                Object row = this.responseIter.next();
                if (row instanceof Object[]) {
                    return Arrays.asList((Object[])row);
                }
                this.currentDocumentRows = this.documentNode.tuples((InfinispanDocument)row);
            }
        }
        return getNextRow();
    }

    private List<Object> buildRow(Map<String, Object> row) {
        ArrayList<Object> result = new ArrayList<>();
        for (String attr : this.projected) {
            result.add(row.get(attr));
        }
        return result;
    }
}