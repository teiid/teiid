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

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * A callback for non-blocking statement result processing.
 * {@link Statement#close()} must still be called to release
 * statement resources.
 * 
 * Statement methods, such as cancel, are perfectly valid
 * even when using a callback.
 */
public interface StatementCallback {
	
	/**
	 * Process the current row of the {@link ResultSet}.
	 * Any call that retrieves non-lob values from the current row
	 * will be performed without blocking on more data from sources.
	 * Calls outside of the current row, such as next(), may block.
	 * @param rs
	 * @throws Exception
	 */
	void onRow(Statement s, ResultSet rs) throws Exception;

	/**
	 * Called when an exception occurs.  No further rows will
	 * be processed by this callback.
	 * @param e
	 */
	void onException(Statement s, Exception e) throws Exception;

	/**
	 * Called when processing has completed normally.
	 * @param rs
	 */
	void onComplete(Statement s) throws Exception;

}
