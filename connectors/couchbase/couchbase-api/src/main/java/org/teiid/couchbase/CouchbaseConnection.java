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
package org.teiid.couchbase;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;

import com.couchbase.client.java.query.N1qlQueryResult;

/**
 * The Logical Hierarchy of a Couchbase cluster looks
 * <pre>
 *   Namespaces
 *       └── Keyspaces
 *              └──Documents
 * </pre>
 * A Keyspace is a set of JSON documents that may vary in structure, use a 
 * self-describing format, flexible Data Model, dynamic schemas. 
 * 
 * A {@code CouchbaseConnection} is a connection to a specific Couchbase Namespace,
 * build upon Couchbase N1QL, used to handle application-level operations
 * (SELECT/UPDATE/INSERT/DELETE) against the documents under a specific 
 * Couchbase Namespace.
 * 
 * @author kylin
 *
 */
public interface CouchbaseConnection extends Connection {
    
    /**
     * Returns the name of the  Namespace
     * @return
     */
    String getNamespace();
    
    /**
     * Executes the given N1QL statement, which returns a single <code>N1qlQueryResult</code> 
     * object.
     * 
     * @param statement Any N1QL statement, like Insert, Select, Update, Delete, etc.
     * @return
     */
    N1qlQueryResult execute(String statement) throws ResourceException;
}
