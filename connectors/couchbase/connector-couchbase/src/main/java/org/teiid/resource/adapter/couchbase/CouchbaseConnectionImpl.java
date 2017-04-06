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
    public N1qlQueryResult executeQuery(String query) {
        return executeQuery(N1qlQuery.simple(query));
    }

    @Override
    public N1qlQueryResult executeQuery(N1qlQuery query) {
        return this.bucket.query(query); 
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
