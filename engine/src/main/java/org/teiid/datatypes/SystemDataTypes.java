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
package org.teiid.datatypes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataTypes;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.metadata.Datatype;

public class SystemDataTypes {
	
	private static SystemDataTypes INSTANCE = new SystemDataTypes();
	
	public static SystemDataTypes getInstance() {
		return INSTANCE;
	}
	
	private List<Datatype> dataTypes = new ArrayList<Datatype>();
	private Map<String, Datatype> typeMap = new HashMap<String, Datatype>();
	
	public SystemDataTypes() {
		InputStream is = SystemDataTypes.class.getClassLoader().getResourceAsStream("org/teiid/metadata/types.dat"); //$NON-NLS-1$
		try {
			InputStreamReader isr = new InputStreamReader(is, Charset.forName("UTF-8")); //$NON-NLS-1$
			BufferedReader br = new BufferedReader(isr);
			String s = br.readLine();
			String[] props = s.split("\\|"); //$NON-NLS-1$
			while ((s = br.readLine()) != null) {
				Datatype dt = new Datatype();
				String[] vals = s.split("\\|"); //$NON-NLS-1$
				Properties p = new Properties();
				for (int i = 0; i < props.length; i++) {
					if (vals[i].length() != 0) {
						p.setProperty(props[i], new String(vals[i]));
					}
				}
				PropertiesUtils.setBeanProperties(dt, p, null);
				dataTypes.add(dt);
				if (dt.isBuiltin()) {
					typeMap.put(dt.getRuntimeTypeName(), dt);
				}
			}
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				throw new TeiidRuntimeException(e);
			}
		}
		addAliasType(DataTypeManager.DataTypeAliases.BIGINT);
		addAliasType(DataTypeManager.DataTypeAliases.DECIMAL);
		addAliasType(DataTypeManager.DataTypeAliases.REAL);
		addAliasType(DataTypeManager.DataTypeAliases.SMALLINT);
		addAliasType(DataTypeManager.DataTypeAliases.TINYINT);
		addAliasType(DataTypeManager.DataTypeAliases.VARCHAR);
		for (String name : DataTypeManager.getAllDataTypeNames()) {
			if (!name.equals(DefaultDataTypes.NULL)) {
				Assertion.isNotNull(typeMap.get(name), name);
			}
		}
	}
	
	private void addAliasType(String alias) {
		Class<?> typeClass = DataTypeManager.getDataTypeClass(alias);
		String primaryType = DataTypeManager.getDataTypeName(typeClass);
		Datatype dt = typeMap.get(primaryType);
		Assertion.isNotNull(dt, alias);
		typeMap.put(alias, dt);
	}
	
	public List<Datatype> getDataTypes() {
		return dataTypes;
	}
	
	public Map<String, Datatype> getBuiltinTypeMap() {
		return typeMap;
	}
}
