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
 
package org.teiid.jdbc;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidRuntimeException;

/**
 * Handles the future processing logic and makes the appropriate calls to the callback
 */
public class NonBlockingRowProcessor implements
		ResultsFuture.CompletionListener<Boolean> {

	private static Logger logger = Logger.getLogger(NonBlockingRowProcessor.class.getName());
	private StatementImpl stmt;
	private StatementCallback callback;

	public NonBlockingRowProcessor(StatementImpl stmt, StatementCallback callback) {
		this.stmt = stmt;
		this.callback = callback;
	}

	@Override
	public void onCompletion(ResultsFuture<Boolean> future) {
		try {
			boolean hasResultSet = future.get(); 
			if (!hasResultSet) {
				callback.onComplete(stmt);
				return;
			}
			final ResultSetImpl resultSet = stmt.getResultSet();
			resultSet.asynch = true;
			Runnable rowProcessor = new Runnable() {
				@Override
				public void run() {
					while (true) {
						try {
							if (stmt.isClosed()) {
								callback.onComplete(stmt);
								break;
							}
							ResultsFuture<Boolean> hasNext = resultSet.submitNext();
							synchronized (hasNext) {
								if (!hasNext.isDone()) {
									hasNext.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
	
												@Override
												public void onCompletion(
														ResultsFuture<Boolean> f) {
													if (processRow(f)) {
														run();
													}
												}
											});
									break; // will be resumed by onCompletion above
								}
							}
							if (!processRow(hasNext)) {
								break;
							}
						} catch (Exception e) {
							onException(e);
							break;
						}
					}
				}
			};
			rowProcessor.run();
		} catch (Exception e) {
			onException(e);
		}
	}

	/**
	 * return true to continue processing
	 */
	boolean processRow(ResultsFuture<Boolean> hasNext) {
		try {
			if (!hasNext.get()) {
				callback.onComplete(stmt);
				return false;
			}

			List<?> row = stmt.getResultSet().getCurrentRecord();
			if (row == null) {
				if (callback instanceof ContinuousStatementCallback) {
					((ContinuousStatementCallback)callback).beforeNextExecution(this.stmt);
				}
				return true;
			}
			
			callback.onRow(stmt, stmt.getResultSet());

			return true;
		} catch (Exception e) {
			onException(e);
			return false;
		} catch (Throwable t) {
			onException(new TeiidRuntimeException(t));
			return false;	
		}
	}

	private void onException(Exception e) {
		if (e instanceof ExecutionException) {
			ExecutionException ee = (ExecutionException)e;
			if (ee.getCause() instanceof Exception) {
				e = (Exception) ee.getCause();
			}
		}
		try {
			callback.onException(stmt, e);
		} catch (Exception e1) {
			logger.log(Level.WARNING, "Unhandled exception from StatementCallback", e); //$NON-NLS-1$
		}
	}

}
