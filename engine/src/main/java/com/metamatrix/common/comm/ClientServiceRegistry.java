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

package com.metamatrix.common.comm;

import java.util.concurrent.ConcurrentHashMap;

public class ClientServiceRegistry {
	
    private ConcurrentHashMap<String, Object> localServices = new ConcurrentHashMap<String, Object>();
    private ConcurrentHashMap<String, String> loggingContext = new ConcurrentHashMap<String, String>();
    
    public ClientServiceRegistry() {
    }
    
	public <T> T getClientService(Class<T> iface) {
		return (T)this.localServices.get(iface.getName());
	}
	
	public Object getClientService(String iface) {
		return localServices.get(iface);
	}

	public void registerClientService(Class<?> iface, Object instance, String loggingContext) {
		this.localServices.put(iface.getName(), instance);
		this.loggingContext.put(iface.getName(), loggingContext);
	}	
	
	public String getLoggingContextForService(String iface) {
		return this.loggingContext.get(iface);
	}

}
