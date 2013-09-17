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
