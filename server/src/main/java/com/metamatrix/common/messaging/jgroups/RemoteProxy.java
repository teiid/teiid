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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.UUID;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.RemoteMessagingException;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;

public class RemoteProxy {
	private Map<UUID, RPCStruct> rpcStructs;

	public RemoteProxy(Map<UUID, RPCStruct> remoteObjects) {
		this.rpcStructs = remoteObjects;
	}
	
	public Object invokeRemoteMethod(UUID classId, String methodName, Class[] parameterTypes, Object[] args) throws Throwable {
		RPCStruct struct = rpcStructs.get(classId);
		try {
			if (struct != null) {
				try {
					Method m = struct.actualObj.getClass().getMethod(methodName, parameterTypes);
					return m.invoke(struct.actualObj, args);
				} catch (NoSuchMethodException e) {
					throw new RemoteMessagingException(e);
				} catch (InvocationTargetException e) {
					throw e.getTargetException();
				}
			}
			throw new ServiceNotFoundException(PlatformPlugin.Util.getString("RemoteProxy.localCallFailed", methodName, classId)); //$NON-NLS-1$
		} catch (Throwable t) {
			LogManager.logWarning(LogCommonConstants.CTX_PROXY, t, PlatformPlugin.Util.getString("RemoteProxy.localCallFailed", methodName, classId)); //$NON-NLS-1$
			throw t;
		}
	}		
	
	public static Method getInvokeMethod() {
		try {
			return RemoteProxy.class.getMethod("invokeRemoteMethod", UUID.class, String.class, Class[].class, Object[].class); //$NON-NLS-1$		
		} catch (NoSuchMethodException e) {
			throw new MetaMatrixRuntimeException(e);
		}			
	}
}
