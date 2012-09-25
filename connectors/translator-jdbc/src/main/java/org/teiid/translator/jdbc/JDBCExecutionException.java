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

package org.teiid.translator.jdbc;

import java.sql.SQLException;
import java.util.Arrays;

import org.teiid.core.BundleUtil;
import org.teiid.translator.TranslatorException;


public class JDBCExecutionException extends TranslatorException {

	private static final long serialVersionUID = 1758087499488916573L;

	public JDBCExecutionException(BundleUtil.Event event, SQLException error,TranslatedCommand... commands) {
		super(error, commands == null || commands.length == 0 ? event.toString()+":"+error.getMessage() : event.toString()+":"+JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11004, Arrays.toString(commands))); //$NON-NLS-1$ //$NON-NLS-2$ 
		setCode(String.valueOf(error.getErrorCode()));
	}
	
	public JDBCExecutionException(BundleUtil.Event event, SQLException error, String command) {
		super(error, event.toString()+":"+JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11004, command)); //$NON-NLS-1$
		setCode(String.valueOf(error.getErrorCode()));
	}
}
