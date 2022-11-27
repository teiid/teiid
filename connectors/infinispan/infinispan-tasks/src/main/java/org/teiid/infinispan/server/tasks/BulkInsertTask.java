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
package org.teiid.infinispan.server.tasks;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

public class BulkInsertTask implements ServerTask<Long> {

    private TaskContext ctx;

    @Override
    public void setTaskContext(TaskContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public String getName() {
        return "teiid-bulk-insert";
    }

    @Override
    public Long call() throws Exception {
        Cache<Object, Object> cache = getCache();

        Map<String, ?> params = null;
        if (ctx.getParameters().isPresent()) {
            params = ctx.getParameters().get();
        }

        if (params == null) {
            return -1L;
        }

        boolean upsert = Boolean.parseBoolean((String) params.get("upsert"));
        int rowCount = (Integer)params.get("row-count");

        long updateCount = 0;
        for (int i = 0; i < rowCount; i++) {
            Object key = params.get("row-key-" + i);
            Object value = params.get("row-" + i);
            Object previous = cache.get(key);
            if (previous == null) {
                cache.put(key, value);
            } else {
                if (upsert) {
                    // TODO:merge
                    throw new Exception("not currently supported");
                } else {
                    throw new Exception("row already exists");
                }
            }
            updateCount++;
        }
        return updateCount;
    }

    @Override
    public TaskExecutionMode getExecutionMode() {
        return TaskExecutionMode.ONE_NODE;
    }

    @SuppressWarnings("unchecked")
    private <K, V> Cache<K, V> getCache() {
        return (Cache<K, V>) ctx.getCache().get();
    }
}