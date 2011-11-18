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

import java.sql.CallableStatement;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.teiid.core.types.JDBCSQLTypeInfo;


/**
 * Note: this is currently only accurate for {@link PreparedStatement}s.
 * Only the basic type information will be accurate for {@link CallableStatement}s. 
 */
public class ParameterMetaDataImpl extends WrapperImpl implements ParameterMetaData {
	
	private ResultSetMetaDataImpl metadata;
	
	public ParameterMetaDataImpl(ResultSetMetaDataImpl metadata) {
		this.metadata = metadata;
	}

	@Override
	public String getParameterClassName(int param) throws SQLException {
		return JDBCSQLTypeInfo.getJavaClassName(getParameterType(param));
	}

	@Override
	public int getParameterCount() throws SQLException {
		return metadata.getColumnCount();
	}

	@Override
	public int getParameterMode(int param) throws SQLException {
		return parameterModeUnknown;
	}

	@Override
	public int getParameterType(int param) throws SQLException {
		return metadata.getColumnType(param);
	}

	@Override
	public String getParameterTypeName(int param) throws SQLException {
		return metadata.getColumnTypeName(param);
	}

	@Override
	public int getPrecision(int param) throws SQLException {
		return metadata.getPrecision(param);
	}

	@Override
	public int getScale(int param) throws SQLException {
		return metadata.getScale(param);
	}

	@Override
	public int isNullable(int param) throws SQLException {
		return metadata.isNullable(param);
	}

	@Override
	public boolean isSigned(int param) throws SQLException {
		return metadata.isSigned(param);
	}
	
	public String getParameterName(int param) throws SQLException {
		return metadata.getColumnName(param);
	}

}
