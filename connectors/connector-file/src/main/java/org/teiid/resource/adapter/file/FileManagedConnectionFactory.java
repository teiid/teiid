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
package org.teiid.resource.adapter.file;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class FileManagedConnectionFactory extends BasicManagedConnectionFactory{

	private static final long serialVersionUID = -1495488034205703625L;
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(FileManagedConnectionFactory.class);

	private String parentDirectory;
	
	@Override
	public BasicConnectionFactory createConnectionFactory() throws ResourceException {
		if (this.parentDirectory == null) {
			throw new InvalidPropertyException(UTIL.getString("parentdirectory_not_set")); //$NON-NLS-1$
		}
		return new BasicConnectionFactory() {
			
			@Override
			public BasicConnection getConnection() throws ResourceException {
				return new FileConnectionImpl(parentDirectory);
			}
		};
	}
	
	public String getParentDirectory() {
		return parentDirectory;
	}
	
	public void setParentDirectory(String parentDirectory) {
		this.parentDirectory = parentDirectory;
	}
	
}
