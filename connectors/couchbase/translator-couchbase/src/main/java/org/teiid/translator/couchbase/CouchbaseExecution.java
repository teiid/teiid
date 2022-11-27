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
package org.teiid.translator.couchbase;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;

public class CouchbaseExecution {

    protected ExecutionContext executionContext;
    protected RuntimeMetadata metadata;
    protected CouchbaseConnection connection;
    protected CouchbaseExecutionFactory executionFactory;

    protected CouchbaseExecution(CouchbaseExecutionFactory executionFactory, ExecutionContext executionContext, RuntimeMetadata metadata, CouchbaseConnection connection) {
        this.executionFactory = executionFactory;
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;
    }
}
