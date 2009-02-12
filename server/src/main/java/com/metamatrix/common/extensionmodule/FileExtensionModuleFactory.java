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

package com.metamatrix.common.extensionmodule;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.connection.BaseTransaction;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.SimpleManagedConnection;
import com.metamatrix.common.connection.TransactionFactory;
import com.metamatrix.common.connection.TransactionInterface;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleOrderingException;
import com.metamatrix.common.extensionmodule.spi.ExtensionModuleTransaction;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.FileUtils;

public class FileExtensionModuleFactory implements TransactionFactory {
	Properties env;
	
	
	public ManagedConnection createConnection(Properties env, String userName)
		throws ManagedConnectionException {
		this.env = env;
		return new SimpleManagedConnection(env, userName);
	}

	public TransactionInterface createTransaction(ManagedConnection connection, boolean readonly) 
		throws ManagedConnectionException {

		return new FileExtensionModuletransaction(env, connection, readonly);
	}

	static class FileExtensionModuletransaction extends BaseTransaction implements ExtensionModuleTransaction{
	
		File extensionDir;
		Map<String, ExtensionModuleDescriptor> extMap = new HashMap<String, ExtensionModuleDescriptor>();
		
		FileExtensionModuletransaction(Properties props, ManagedConnection connection, boolean readonly) throws ManagedConnectionException {
			super(connection, readonly);
			
			extensionDir = new File(props.getProperty("extension.dir", "./extensions"));
			if (!extensionDir.exists()) {
				this.extensionDir.mkdirs();
			}
			
			int i = 0;
			for (File f:extensionDir.listFiles()) {
				extMap.put(f.getName(), new ExtensionModuleDescriptor(f.getName(), getType(f.getName()), i++, true, f.getAbsolutePath(), "system", "now", "system", "now", i));
			}
		}
		
		private String getType(String source) {
			if (source.endsWith(".jar")) {
				return ExtensionModuleTypes.JAR_FILE_TYPE;
			} else if (source.endsWith(".vdb")) {
				return ExtensionModuleTypes.VDB_FILE_TYPE;
			} else if (source.endsWith(".xmi")) {
				return ExtensionModuleTypes.FUNCTION_DEFINITION_TYPE;
			}
			return ExtensionModuleTypes.MISC_FILE_TYPE;
		}
		
		public ExtensionModuleDescriptor addSource(String principalName, String type, String sourceName, byte[] source, long checksum, String description, boolean enabled)
				throws DuplicateExtensionModuleException, MetaMatrixComponentException {
			try {
				FileUtils.write(source, new File(extensionDir, sourceName));
			} catch (IOException e) {
				throw new MetaMatrixComponentException(e);
			}
			return new ExtensionModuleDescriptor(sourceName, type, 1, enabled, description, principalName, "now", principalName, "now", checksum);
		}
	
		public byte[] getSource(String sourceName) throws ExtensionModuleNotFoundException, MetaMatrixComponentException {
			
			File f = new File (extensionDir, sourceName);
			if (f.exists()) {
				FileUtil util = new FileUtil(f);
				return util.readBytes();
			}
			// ~~~little hack to get files in the classpath~~~
			InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(sourceName);
			if (in != null) {
				return readBytes(in);
			}
			throw new ExtensionModuleNotFoundException();
		}
		
	    private byte[] readBytes(InputStream input) throws ExtensionModuleNotFoundException{
	        ByteArrayOutputStream result = new ByteArrayOutputStream();
	        try {
				try {
				    byte[] buffer = new byte[1024];
				    int readCount = input.read(buffer);
				    while (readCount > 0) {
				        result.write(buffer, 0, readCount);
				        readCount = input.read(buffer);
				    }            
				    return result.toByteArray();
				} finally {
				    if (input != null) {
				        input.close();
				    }
				}
			} catch (IOException e) {
	        	throw new ExtensionModuleNotFoundException();
			}
	    }		
	
		public ExtensionModuleDescriptor getSourceDescriptor(String sourceName) throws ExtensionModuleNotFoundException, MetaMatrixComponentException {
			ExtensionModuleDescriptor m =  this.extMap.get(sourceName);
			if (m == null) {
				throw new ExtensionModuleNotFoundException();
			}
			return m;
		}
	
		public List getSourceDescriptors() throws MetaMatrixComponentException {
			return new ArrayList(this.extMap.values());
		}
	
		public List getSourceDescriptors(String type, boolean includeDisabled) throws MetaMatrixComponentException {
			ArrayList result = new ArrayList();
			for (Iterator i = getSourceDescriptors().iterator(); i.hasNext();) {
				ExtensionModuleDescriptor desc = (ExtensionModuleDescriptor) i.next();
				if (desc.getType().equals(type)) {
					if (includeDisabled || desc.isEnabled()) {
						result.add(new ExtensionModuleDescriptor(desc));
					}
				}
			}
			return result;
		}
	
		public List getSourceNames() throws MetaMatrixComponentException {
			ArrayList result = new ArrayList();
			for (Iterator i = getSourceDescriptors().iterator(); i.hasNext();) {
				ExtensionModuleDescriptor desc = (ExtensionModuleDescriptor) i.next();
				result.add(desc.getName());
			}
			return result;
		}
	
		public boolean isNameInUse(String sourceName) throws MetaMatrixComponentException {
			return true;
		}
	
		public boolean needsRefresh() throws MetaMatrixComponentException, UnsupportedOperationException {
			return false;
		}
	
		public void removeSource(String principalName, String sourceName)  
			throws ExtensionModuleNotFoundException, MetaMatrixComponentException {
			
			ExtensionModuleDescriptor m = this.extMap.remove(sourceName);
			
			File f = new File(extensionDir, sourceName);
			if (f.exists()) {
				f.delete();
			}
			
			if ( m == null) {
				throw new ExtensionModuleNotFoundException();
			}
		}
	
		public void setEnabled(String principalName, Collection sourceNames, boolean enabled) 
			throws ExtensionModuleNotFoundException, MetaMatrixComponentException {
			// ignore
		}
	
		public void setSearchOrder(String principalName, List sourceNames)
				throws ExtensionModuleOrderingException, MetaMatrixComponentException {
			// ignore..
		}
	
		public ExtensionModuleDescriptor setSource(String principalName, String sourceName, byte[] source, long checksum)
				throws ExtensionModuleNotFoundException, MetaMatrixComponentException {
			
			ExtensionModuleDescriptor m = this.extMap.get(sourceName);
			
			File f = new File(extensionDir, sourceName);
			if (f.exists()) {
				f.delete();
			}
			
			FileUtil file = new FileUtil(new File(this.extensionDir, sourceName));
			file.writeBytes(source);
			
			if (m == null) {
				this.extMap.put(sourceName, new ExtensionModuleDescriptor(sourceName, getType(sourceName), 1, true, sourceName, principalName, "now", principalName, "now", checksum));
			}
			return m;
		}
	
		public ExtensionModuleDescriptor setSourceDescription(String principalName, String sourceName, String description)
				throws ExtensionModuleNotFoundException, MetaMatrixComponentException {
			ExtensionModuleDescriptor m = this.extMap.get(sourceName);
			if ( m == null) {
				throw new ExtensionModuleNotFoundException();
			}
			m.setDescription(description);
			return m;
		}
	
		public ExtensionModuleDescriptor setSourceName(String principalName, String sourceName, String newName)
				throws ExtensionModuleNotFoundException, MetaMatrixComponentException {

			File f = new File(extensionDir, sourceName);
			if (f.exists()) {
				f.renameTo( new File(extensionDir, newName));
			}
			
			ExtensionModuleDescriptor m = this.extMap.remove(sourceName);
			if ( m == null) {
				throw new ExtensionModuleNotFoundException();
			}
			
			m.setName(newName);
		    m.setCreatedBy(principalName);
			this.extMap.put(newName, m);
			return m;
		}
	
	}
}