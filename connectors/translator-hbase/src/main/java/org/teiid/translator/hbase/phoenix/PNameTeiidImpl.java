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
package org.teiid.translator.hbase.phoenix;

import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PName;

public class PNameTeiidImpl implements PName {

	private String name;

	public static PNameTeiidImpl makePName(String name) {
		return new PNameTeiidImpl("\"" +name + "\"");
	}
	
	public static PNameTeiidImpl makePNameWithoutQuote(String name) {
		return new PNameTeiidImpl(name);
	}

	public PNameTeiidImpl(String name) {
		this.name = name;
	}

	@Override
	public String getString() {
		return name;
	}

	@Override
	public byte[] getBytes() {
		return name.getBytes();
	}

	@Override
	public ImmutableBytesPtr getBytesPtr() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getEstimatedSize() {
		// TODO Auto-generated method stub
		return 0;
	}

}
