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

package org.teiid.resource.adapter.mongodb;

import java.util.List;

import javax.resource.ResourceException;
import javax.resource.cci.LocalTransaction;

import org.teiid.core.BundleUtil;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.resource.spi.BasicConnection;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;


public class MongoDBConnectionImpl extends BasicConnection implements MongoDBConnection {
    static final BundleUtil UTIL = BundleUtil.getBundleUtil(MongoDBConnectionImpl.class);

    private MongoClient client;
    private String database;

    public MongoDBConnectionImpl(String database, List<ServerAddress> servers,
            MongoCredential credential, MongoClientOptions options) {
        if (credential == null) {
            this.client = new MongoClient(servers, options);
        }
        else {
            this.client = new MongoClient(servers, credential, options);
        }
        this.database = database;
    }

    public MongoDBConnectionImpl(MongoClientURI uri) {
        this.database = uri.getDatabase();
        this.client = new MongoClient(uri);
    }

    @Override
    public DB getDatabase() {
        return this.client.getDB(this.database);
    }

    @Override
    public void close() throws ResourceException {
        if (this.client != null) {
            this.client.close();
        }
    }

    @Override
    public LocalTransaction getLocalTransaction() throws ResourceException {
        try {
            ClientSession session = this.client.startSession();

            return new LocalTransaction() {

                @Override
                public void rollback() throws ResourceException {
                    try {
                        session.abortTransaction();
                    } catch (MongoException e) {
                        throw new ResourceException(e.getMessage(), e);
                    }
                }

                @Override
                public void commit() throws ResourceException {
                    try {
                       session.commitTransaction();
                    } catch (MongoException e) {
                        throw new ResourceException(e.getMessage(), e);
                    }
                }

                @Override
                public void begin() throws ResourceException {
                    try {
                       session.startTransaction();
                    } catch (MongoException e) {
                        throw new ResourceException(e.getMessage(), e);
                    }
                }
            };
        } catch (MongoException e) {
            throw new ResourceException(e.getMessage(), e);
        }
    }
}
