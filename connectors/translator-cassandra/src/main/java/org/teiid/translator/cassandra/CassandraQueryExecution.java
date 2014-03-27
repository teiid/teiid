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

package org.teiid.translator.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CassandraQueryExecution implements ResultSetExecution {

	private Command query;
	private CassandraConnection connection;
	private ResultSet resultSet = null;
	private ExecutionContext executionContext;
	protected boolean returnsArray;
	
	public CassandraQueryExecution(Command query, CassandraConnection connection, ExecutionContext context){
		this.query = query;
		this.connection = connection;
		this.executionContext = context;
	}

	@Override
	public void close() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, CassandraExecutionFactory.UTIL.getString("close_query")); //$NON-NLS-1$
		
	}

	@Override
	public void cancel() throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, CassandraExecutionFactory.UTIL.getString("cancel_query")); //$NON-NLS-1$
		
	}

	@Override
	public void execute() throws TranslatorException {
		CassandraSQLVisitor visitor = new CassandraSQLVisitor();
		visitor.translateSQL(query);
		String cql = visitor.getTranslatedSQL();
		execute(cql);
	}

	protected void execute(String cql) throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Source-Query:", cql); //$NON-NLS-1$
		try {
			resultSet = connection.executeQuery(cql);
		} catch(Throwable t) {
			throw new TranslatorException(t);
		}
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		return getRow(resultSet.one());
	}
	
	/**
	 * Iterates through all columns in the {@code row}. For each column, returns its value as Java type
	 * that matches the CQL type in switch part. Otherwise returns the value as bytes composing the value.
	 * @param row the row returned by the ResultSet
	 * @return list of values in {@code row}
	 */
	private List<Object> getRow(Row row) {
		if(row == null){
			return null;
		}
		final List<Object> values = new ArrayList<Object>(row.getColumnDefinitions().size());
		for(int i = 0; i < row.getColumnDefinitions().size(); i++){
			switch(row.getColumnDefinitions().getType(i).getName()){
			case ASCII:
				values.add(row.getString(i));
				break;
			case BIGINT:
				values.add(Long.valueOf(row.getLong(i)));
				break;
			case BLOB:
				values.add(row.getBytes(i));
				break;
			case BOOLEAN:
				values.add(Boolean.valueOf(row.getBool(i)));
				break;
			case COUNTER:
				values.add(Long.valueOf(row.getLong(i)));
				break;
			case DECIMAL:
				values.add(row.getDecimal(i));
				break;
			case DOUBLE:
				values.add(Double.valueOf(row.getDouble(i)));
				break;
			case FLOAT:
				values.add(Float.valueOf(row.getFloat(i)));
				break;
			case INET:
				values.add(row.getInet(i));
				break;
			case INT:
				values.add(Integer.valueOf(row.getInt(i)));
				break;
			case LIST:
				values.add(row.getList(i, row.getColumnDefinitions().getType(i).getTypeArguments().get(0).asJavaClass()));
				break;
			case MAP:
				values.add(row.getMap(i, row.getColumnDefinitions().getType(i).getTypeArguments().get(0).asJavaClass(),
										 row.getColumnDefinitions().getType(i).getTypeArguments().get(1).asJavaClass()));
				break;
			case SET:
				values.add(row.getSet(i, row.getColumnDefinitions().getType(i).getTypeArguments().get(0).asJavaClass()));
				break;
			case TEXT:
				values.add(row.getString(i));
				break;
			case TIMESTAMP:
				values.add(row.getDate(i));
				break;
			case TIMEUUID:
				values.add(row.getUUID(i));
				break;
			case UUID:
				values.add(row.getUUID(i));
				break;
			case VARCHAR:
				values.add(row.getString(i));
				break;
			case VARINT:
				values.add(row.getVarint(i));
				break;
			default:
				//read as a varbinary
				ByteBuffer bytesUnsafe = row.getBytesUnsafe(i);
				byte[] b = new byte[bytesUnsafe.remaining()];
				bytesUnsafe.get(b);
				values.add(b);
				break;
			}
			
		}
		if (returnsArray) {
			return Collections.singletonList((Object)values.toArray());
		}
		return values;
	}

}
