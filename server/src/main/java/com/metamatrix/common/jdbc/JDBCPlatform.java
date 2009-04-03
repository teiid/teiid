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

package com.metamatrix.common.jdbc;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.common.util.ErrorMessageKeys;

public class JDBCPlatform {

	/**
	 * These are the platforms supported
	 */
	public enum Supported {
		ORACLE,
		SYBASE,
		DB2,
		MSSQL,
		INFORMIX,
		METAMATRIX,
		MM_ORACLE,
		MYSQL,
		POSTGRES,       
		DEFAULT,
		DERBY
	}

	/**
	 * The use of platforms is a secondary search option in case the supported
	 * platforms don't match to the product name
	 */
	protected interface Protocol {
		public static final String MSSQL = "mssql"; //$NON-NLS-1$
		public static final String SQLSERVER = "sqlserver"; //$NON-NLS-1$
		public static final String ORACLE = "oracle"; //$NON-NLS-1$
		public static final String DB2 = "db2"; //$NON-NLS-1$
		public static final String SYBASE = "sybase"; //$NON-NLS-1$
		public static final String INFORMIX = "informix-sqli"; //$NON-NLS-1$
		public static final String METAMATRIX = "metamatrix"; //$NON-NLS-1$
		public static final String MM_ORACLE = "mmx:oracle"; //$NON-NLS-1$
		public static final String DERBY = "derby"; //$NON-NLS-1$
		public static final String MYSQL = "mysql"; //$NON-NLS-1$
		public static final String POSTGRES = "postgres"; //$NON-NLS-1$

	}

	public static Supported getSupportedByProtocol(String value) {
		// System.out.println("==== Look for platform by product " + value);
		String lower = value.toLowerCase();

		if (lower.indexOf(Protocol.METAMATRIX) >= 0) {
			return Supported.METAMATRIX;
		} else if (lower.indexOf(Protocol.MM_ORACLE) >= 0) {
			return Supported.MM_ORACLE;
		} else if (lower.indexOf(Protocol.MSSQL) >= 0
				|| lower.indexOf(Protocol.SQLSERVER) >= 0) {
			return Supported.MSSQL;
		} else if (lower.indexOf(Protocol.DB2) >= 0) {
			return Supported.DB2;
		} else if (lower.indexOf(Protocol.ORACLE) >= 0) {
			return Supported.ORACLE;
		} else if (lower.indexOf(Protocol.SYBASE) >= 0) {
			return Supported.SYBASE;
		} else if (lower.indexOf(Protocol.INFORMIX) >= 0) {
			return Supported.INFORMIX;
		} else if (lower.indexOf(Protocol.DERBY) >= 0) {
			return Supported.DERBY;
		} else if (lower.indexOf(Protocol.MYSQL) >= 0) {
			return Supported.MYSQL;
		} else if (lower.indexOf(Protocol.POSTGRES) >= 0) {
			return Supported.POSTGRES;
		}

		return null;
	}

	public static byte[] convertToByteArray(Object sourceObject)
			throws TransformationException {
		if (sourceObject instanceof byte[]) {
			return (byte[]) sourceObject;
		} else if (sourceObject instanceof java.sql.Clob) {
			return convertToByteArray((java.sql.Clob) sourceObject);
		} else if (sourceObject instanceof java.sql.Blob) {
			return convertToByteArray((java.sql.Blob) sourceObject);
		} else {
			throw new TransformationException(ErrorMessageKeys.JDBC_ERR_0001,
					CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0001,
							sourceObject.getClass().getName()));
		}
	}

	protected static byte[] convertToByteArray(java.sql.Blob sourceObject)
			throws TransformationException {

		try {

			// long size = sourceObject.length();
			// System.out.println("@@@@@@@@@@ Blob to bytes: " + size);

			// Open a stream to read the BLOB data
			InputStream l_blobStream = sourceObject.getBinaryStream();

			// Open a file stream to save the BLOB data
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			BufferedOutputStream bos = new BufferedOutputStream(out);

			// Read from the BLOB data input stream, and write to the file
			// output
			// stream
			byte[] l_buffer = new byte[1024]; // buffer holding bytes to be
			// transferred
			int l_nbytes = 0; // Number of bytes read
			while ((l_nbytes = l_blobStream.read(l_buffer)) != -1)
				// Read from BLOB stream
				bos.write(l_buffer, 0, l_nbytes); // Write to file stream

			// Flush and close the streams
			bos.flush();
			bos.close();
			l_blobStream.close();

			return out.toByteArray();

		} catch (IOException ioe) {
			throw new TransformationException(ioe,
					ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util
							.getString(ErrorMessageKeys.JDBC_ERR_0002,
									sourceObject.getClass().getName()));
		} catch (SQLException sqe) {
			throw new TransformationException(sqe,
					ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util
							.getString(ErrorMessageKeys.JDBC_ERR_0002,
									sourceObject.getClass().getName()));

		}

	}

	protected static byte[] convertToByteArray(java.sql.Clob sourceObject)
			throws TransformationException {

		try {

			// long size = sourceObject.length();
			// System.out.println("@@@@@@@@@@ Blob to bytes: " + size);

			// Open a stream to read the BLOB data
			InputStream l_clobStream = sourceObject.getAsciiStream();

			// Open a file stream to save the BLOB data
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			BufferedOutputStream bos = new BufferedOutputStream(out);

			// Read from the BLOB data input stream, and write to the file
			// output
			// stream
			byte[] l_buffer = new byte[1024]; // buffer holding bytes to be
			// transferred
			int l_nbytes = 0; // Number of bytes read
			while ((l_nbytes = l_clobStream.read(l_buffer)) != -1)
				// Read from BLOB stream
				bos.write(l_buffer, 0, l_nbytes); // Write to file stream

			// Flush and close the streams
			bos.flush();
			bos.close();
			l_clobStream.close();

			return out.toByteArray();

		} catch (IOException ioe) {
			throw new TransformationException(ioe,
					ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util
							.getString(ErrorMessageKeys.JDBC_ERR_0002,
									sourceObject.getClass().getName()));
		} catch (SQLException sqe) {
			throw new TransformationException(sqe,
					ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util
							.getString(ErrorMessageKeys.JDBC_ERR_0002,
									sourceObject.getClass().getName()));

		}
	}

	public static int getDatabaseColumnSize(String tableName,
			String columnName, Connection jdbcConnection) throws SQLException {
		DatabaseMetaData dbMetadata = jdbcConnection.getMetaData();
		String catalogName = jdbcConnection.getCatalog();

		ResultSet columns = dbMetadata.getColumns(catalogName, null, tableName,
				"%"); //$NON-NLS-1$
		int s = -1;
		while (columns.next()) {
			String nis = columns.getString(4);
			if (columnName.equals(nis)) {
				s = columns.getInt(7);
			}

		}
		return s;
	}

	public static String getIdentifierQuoteString(Connection connection)
			throws SQLException {
		String quote = connection.getMetaData().getIdentifierQuoteString();
		if (quote == null || quote.equals(" ")) { //$NON-NLS-1$
			return ""; //$NON-NLS-1$
		}
		return quote;
	}

}
