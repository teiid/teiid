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

package org.teiid.translator.cassandra;

import java.util.List;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.VersionNumber;

/**
 * Connection to Cassandra NoSql database.
 * */
public interface CassandraConnection extends Connection{
	
	/**
	 * Executes a CQL query.
	 * */
	public ResultSetFuture executeQuery(String query);
	
	/**
	 * Returns metadata about Cassandra keyspace (column families, columns metadata etc.)
	 * @throws KeyspaceNotDefinedException 
	 * */
	public KeyspaceMetadata keyspaceInfo() throws ResourceException;

	/**
	 * Execute a batch of updates
	 * @param updates
	 * @return
	 */
	ResultSetFuture executeBatch(List<String> updates);

	/**
	 * Execute a bulk update
	 * @param update
	 * @param values
	 * @return
	 * @throws ResourceException 
	 */
	ResultSetFuture executeBatch(String update, List<Object[]> values);

	/**
	 * Get the version in use for this connection
	 * @return
	 */
	VersionNumber getVersion();
	
}
