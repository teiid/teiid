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

package org.postgresql.core.v3;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.postgresql.core.Logger;
import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.ResultHandler;

/**
 * Allows for simple query executions against an implied cursor portal
 * 
 * This is hack to test cursoring through jdbc
 *
 */
@SuppressWarnings({"rawtypes", "nls"})
public class ExtendedQueryExectutorImpl extends QueryExecutorImpl {

	public static String simplePortal;
	
	public PGStream stream;
	public List pendingExecute = new ArrayList();
	public List pendingDescribe = new ArrayList();
	
	public ExtendedQueryExectutorImpl(ProtocolConnectionImpl protoConnection,
			PGStream pgStream, Properties info, Logger logger) {
		super(protoConnection, pgStream, info, logger);
		this.stream = pgStream;
		try {
			Field f = QueryExecutorImpl.class.getDeclaredField("pendingExecuteQueue");
			f.setAccessible(true);
			pendingExecute = (List) f.get(this);
			f = QueryExecutorImpl.class.getDeclaredField("pendingDescribePortalQueue");
			f.setAccessible(true);
			pendingDescribe = (List) f.get(this);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public synchronized void execute(Query query, ParameterList parameters,
			ResultHandler handler, int maxRows, int fetchSize, int flags)
			throws SQLException {
		if (simplePortal != null) {
			try {
				byte[] bytes = query.toString().getBytes("UTF-8");
				
				stream.SendChar('Q');
				stream.SendInteger4(bytes.length + 6);
				stream.Send(bytes);
				stream.SendInteger2(0);
				stream.flush();
				if (pendingExecute.isEmpty()) {
					pendingExecute.add(new Object[] { query, new Portal((SimpleQuery)query, simplePortal) });
				}
				if (pendingDescribe.isEmpty()) {
					pendingDescribe.add(query);
				}
				processResults(handler, flags);
				handler.handleCompletion();
				return;
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}
		super.execute(query, parameters, handler, maxRows, fetchSize, flags);
	}

}
