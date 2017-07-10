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

package org.teiid.resource.adapter.couchbase;

import java.util.concurrent.TimeUnit;

import javax.resource.ResourceException;

import org.teiid.core.BundleUtil;
import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.resource.spi.BasicConnection;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;

public class CouchbaseConnectionImpl extends BasicConnection implements CouchbaseConnection {
    
	static final BundleUtil UTIL = BundleUtil.getBundleUtil(CouchbaseConnectionImpl.class);

	private Cluster cluster;
	private Bucket bucket;
	
	private String namespace; // map to namespaces
	
	public CouchbaseConnectionImpl(CouchbaseEnvironment environment, String connectionString, String keyspace, String password, TimeUnit timeUnit, String namespace){
	    
	    this.cluster = CouchbaseCluster.create(environment, connectionString);  
	    if(password != null) {
            this.bucket = this.cluster.openBucket(keyspace, password, environment.connectTimeout(), timeUnit);
        } else {
            this.bucket = this.cluster.openBucket(keyspace, environment.connectTimeout(), timeUnit);
        }
	    this.namespace = namespace;
	}

    @Override
    public N1qlQueryResult execute(String statement) throws ResourceException {
        N1qlQueryResult result = this.bucket.query(N1qlQuery.simple(statement));
        if (!result.finalSuccess()) {
            throw new ResourceException(UTIL.gs("query_error", result.errors()), result.status()); //$NON-NLS-1$
        }
        return result;
    }
    
    @Override
    public void close() throws ResourceException {
        if(this.bucket != null) {
            this.bucket.close();
        }
        this.cluster.disconnect();
    }

    @Override
    public String getNamespace() {
        return this.namespace;
    }

}
