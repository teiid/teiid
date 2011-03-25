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

import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.util.Properties;

import org.teiid.jdbc.ResultSetImpl;

public interface ODBCClientRemote {
	
	void initialized(Properties props);
	
	void setEncoding(String value);
	
	//	AuthenticationCleartextPassword (B)
	void useClearTextAuthentication();

	//	AuthenticationOk (B)
	//	BackendKeyData (B)
	//	ParameterStatus (B)
	void authenticationSucess(int processId, int screctKey);
	
	//	ParseComplete (B)
	void prepareCompleted(String preparedName);
	
	//	ErrorResponse (B)
	void errorOccurred(String msg);
	
	//	ErrorResponse (B)	
	void errorOccurred(Throwable e);
	
	void terminated();
	
	//	ParameterDescription (B)
	void sendParameterDescription(ParameterMetaData parameterMetaData, int[] paramType);

	//	BindComplete (B)
	void bindComplete();

	//	RowDescription (B)
	//	NoData (B)
	void sendResultSetDescription(ResultSetMetaData metaData);
	
	//	DataRow (B)
	//	CommandComplete (B)
	void sendResults(String sql, ResultSetImpl rs, boolean describeRows);

	//	CommandComplete (B)
	void sendUpdateCount(String sql, int updateCount);

	//	ReadyForQuery (B)
	void ready(boolean inTransaction, boolean failedTransaction);

	void statementClosed();

	//	EmptyQueryResponse (B)
	void emptyQueryReceived();

	void flush();
	
	// FunctionCallResponse (B)
	void functionCallResponse(byte[] data);
	void functionCallResponse(int data);
	
	void sslDenied();
	
	// unimplemented backend messages
	
	//	AuthenticationKerberosV5 (B)
	//	AuthenticationMD5Password (B)
	//	AuthenticationSCMCredential (B)
	//	AuthenticationGSS (B)
	//	AuthenticationSSPI (B)
	//	AuthenticationGSSContinue (B)
	
	//	CloseComplete (B)

	//	CopyData (F & B)
	//	CopyDone (F & B)
	//	CopyInResponse (B)
	//	CopyOutResponse (B)
	
	//	NoticeResponse (B)
	//	NotificationResponse (B)
	
	//	PortalSuspended (B)
	
		

}
