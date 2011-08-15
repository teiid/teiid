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
package org.teiid.odbc;

import java.util.Properties;

import org.teiid.transport.PgFrontendProtocol.NullTerminatedStringDataInputStream;

public interface ODBCServerRemote {
	
	void initialize(Properties props);
	
	void logon(String databaseName, String userid, NullTerminatedStringDataInputStream data);
	
	void prepare(String prepareName, String sql, int[] paramType);

	void bindParameters(String bindName, String prepareName, int paramCount, Object[] paramdata, int resultCodeCount, int[] resultColumnFormat);
	
	void execute(String bindName, int maxrows);
	
	void getParameterDescription(String prepareName);
	
	void getResultSetMetaDataDescription(String bindName);
	
	void sync();
	
	void executeQuery(String sql);
	
	void terminate();
	
	void cancel();
	
	void closePreparedStatement(String preparedName);
	
	void closeBoundStatement(String bindName);
	
	void unsupportedOperation(String msg);

	void flush();

	void functionCall(int oid);
	
	void sslRequest();
	
	//  unimplemented frontend messages
	//	CopyData (F & B)
	//	CopyDone (F & B)
	//	CopyFail (F)
}


