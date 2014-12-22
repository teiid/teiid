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

package org.teiid.resource.adapter.hbase;

import java.sql.Connection;
import java.sql.DriverManager;

import javax.resource.ResourceException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.HBaseConnection;


public class HBaseConnectionImpl extends BasicConnection implements HBaseConnection {
	
	static final BundleUtil UTIL = BundleUtil.getBundleUtil(HBaseConnectionImpl.class);

	private Connection connection;
	
	private String zkQuorum;

	public HBaseConnectionImpl(String zkQuorum) {
		
		this.zkQuorum = zkQuorum;
		
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			String connectionURL = "jdbc:phoenix:" + zkQuorum;
			connection = DriverManager.getConnection(connectionURL);
		}  catch (Exception e) {
			throw new IllegalArgumentException("Create phoenix connection(" + zkQuorum + ") throw exception", e);
		}
		
	}
	
	public Connection getConnection() {
		return this.connection;
	}


	@Override
	public void close() throws ResourceException {
		if(null != connection) {
			try {
				connection.close();
			} catch (Exception e) {
				throw new IllegalArgumentException("Close phoenix connection(" + zkQuorum + ") throw exception", e);
			}
		}
	}
}
