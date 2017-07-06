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