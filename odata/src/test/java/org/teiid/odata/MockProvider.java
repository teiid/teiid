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
package org.teiid.odata;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.odata4j.producer.ODataProducer;
import org.teiid.core.TeiidRuntimeException;

@Provider
public class MockProvider implements ContextResolver<ODataProducer> {
	public static Client CLIENT = null;
	
	@Context
	protected UriInfo uriInfo;	
	
	@Override
	public ODataProducer getContext(Class<?> arg0) {
		if (!arg0.equals(ODataProducer.class)) {
			throw new TeiidRuntimeException("Invalid Context Type requested in OData Server");
		}
		
		return new TeiidProducer(CLIENT);
	}
}