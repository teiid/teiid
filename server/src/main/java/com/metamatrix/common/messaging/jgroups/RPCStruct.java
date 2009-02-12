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
package com.metamatrix.common.messaging.jgroups;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

import org.jgroups.Address;

import com.metamatrix.core.MetaMatrixRuntimeException;

public class RPCStruct implements Serializable {
	private static final long serialVersionUID = -7372264971977481565L;
	
	private ArrayList<String> targetClasses = new ArrayList<String>();
	Address address;
	UUID objectId;
	transient Object actualObj;
	
	public RPCStruct(Address address, UUID objectId, Class[] targetClasses, Object original ) {
		this.address = address;
		this.objectId = objectId;
		
		for (int i = 0; i < targetClasses.length; i++) {
			this.targetClasses.add(targetClasses[i].getCanonicalName());
		}
		
		this.actualObj = original;
	}	
	
	public Class[] getTargetClasses() {
		Class[] clazzes = new Class[targetClasses.size()];
		int i = 0;
		try {
			for(String clazz:targetClasses) {
				clazzes[i++] = Class.forName(clazz);
			}
		} catch (ClassNotFoundException e) {
			throw new MetaMatrixRuntimeException(e);
		}
		return clazzes;
	}
	
}