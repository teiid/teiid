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
