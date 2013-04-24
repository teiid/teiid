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

package org.teiid.resource.adapter.mongodb;

import java.util.Arrays;
import java.util.List;

import javax.resource.ResourceException;

import org.teiid.core.BundleUtil;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.resource.spi.BasicConnection;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;


public class MongoDBConnectionImpl extends BasicConnection implements MongoDBConnection {
	static final BundleUtil UTIL = BundleUtil.getBundleUtil(MongoDBConnectionImpl.class);

	private MongoClient client;
	private String database;

	public MongoDBConnectionImpl(String database, List<ServerAddress> servers,
			MongoCredential credential, MongoClientOptions options) {
		if (credential == null) {
			this.client = new MongoClient(servers, options);
		}
		else {
			this.client = new MongoClient(servers, Arrays.asList(credential), options);
		}
		this.database = database;
	}

	@Override
	public DB getDatabase() {
		return this.client.getDB(this.database);
	}

	@Override
	public void close() throws ResourceException {
		if (this.client != null) {
			this.client.close();
		}
	}
}
