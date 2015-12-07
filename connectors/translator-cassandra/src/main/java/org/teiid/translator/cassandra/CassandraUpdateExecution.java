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
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.types.BinaryType;
import org.teiid.language.BatchedCommand;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.MoreExecutors;

public class CassandraUpdateExecution implements UpdateExecution {
	
	private CassandraConnection connection;
	private ExecutionContext executionContext;
	private RuntimeMetadata metadata;
	private Command command;
	private int updateCount = 1;
	private ResultSetFuture resultSetFuture;
	
	public CassandraUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			CassandraConnection connection) {
		this.command = command;
		this.executionContext = executionContext;
		this.metadata = metadata;
		this.connection = connection;
	}

	@Override
	public void close() {
		this.resultSetFuture = null;
	}

	@Override
	public void cancel() throws TranslatorException {
		if (this.resultSetFuture != null) {
			this.resultSetFuture.cancel(true);
		}
	}

	@Override
	public void execute() throws TranslatorException {
		internalExecute();
		resultSetFuture.addListener(new Runnable() {
			@Override
			public void run() {
				executionContext.dataAvailable();
			}
		}, MoreExecutors.sameThreadExecutor());
	}

	private void internalExecute() throws TranslatorException {
		if (this.command instanceof BatchedUpdates) {
			handleBatchedUpdates();
			return;
		}
		CassandraSQLVisitor visitor = new CassandraSQLVisitor();
		visitor.translateSQL(this.command);
		String cql = visitor.getTranslatedSQL();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Source-Query:", cql); //$NON-NLS-1$
		if (this.command instanceof BatchedCommand) {
			BatchedCommand bc = (BatchedCommand)this.command;
			if (bc.getParameterValues() != null) {
				int count = 0;
				List<Object[]> newValues = new ArrayList<Object[]>();
				Iterator<? extends List<?>> values = bc.getParameterValues();
				while (values.hasNext()) {
					Object[] bindValues = values.next().toArray();
					for (int i = 0; i < bindValues.length; i++) {
						if (bindValues[i] instanceof Blob) {
							Blob blob = (Blob)bindValues[i];
							try {
								if (blob.length() > Integer.MAX_VALUE) {
									throw new AssertionError("Blob is too large"); //$NON-NLS-1$
								}
								byte[] bytes = ((Blob)bindValues[i]).getBytes(0, (int) blob.length());
								bindValues[i] = ByteBuffer.wrap(bytes);
							} catch (SQLException e) {
								throw new TranslatorException(e);
							}
						} else if (bindValues[i] instanceof BinaryType) {
							bindValues[i] = ByteBuffer.wrap(((BinaryType)bindValues[i]).getBytesDirect());
						}
					}
					newValues.add(bindValues);
					count++;
				}
				updateCount = count;
				resultSetFuture = connection.executeBatch(cql, newValues);
				return;
			}
		}
		resultSetFuture = connection.executeQuery(cql);
	}

	private void handleBatchedUpdates() {
		BatchedUpdates updates = (BatchedUpdates)this.command;
		List<String> cqlUpdates = new ArrayList<String>();
		for (Command update : updates.getUpdateCommands()) {
			CassandraSQLVisitor visitor = new CassandraSQLVisitor();
			visitor.translateSQL(update);
			String cql = visitor.getTranslatedSQL();
			cqlUpdates.add(cql);
		}
		this.updateCount = cqlUpdates.size();
		resultSetFuture = connection.executeBatch(cqlUpdates);
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			TranslatorException {
		if (!resultSetFuture.isDone()) {
			throw DataNotAvailableException.NO_POLLING;
		}
		resultSetFuture.getUninterruptibly();
		return new int[] {this.updateCount};
	}

}
