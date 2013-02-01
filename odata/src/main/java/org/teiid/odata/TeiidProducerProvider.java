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

import java.util.HashMap;
import java.util.Timer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.odata4j.producer.ODataProducer;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.vdb.runtime.VDBKey;

@Provider
public class TeiidProducerProvider implements ContextResolver<ODataProducer> {

	@Context
	protected UriInfo uriInfo;	
	@Context
	protected HttpServletRequest request;
	protected HashMap<VDBKey, LocalClient> clientMap = new HashMap<VDBKey, LocalClient>();
	private Timer timer =  new Timer("Teiid OData", true);
	
	@Override
	public ODataProducer getContext(Class<?> arg0) {
		if (!arg0.equals(ODataProducer.class)) {
			throw new TeiidRuntimeException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16007));
		}
		
		String vdbName = null;
		int version = 1;
		String uri = uriInfo.getBaseUri().getRawPath();
		int idx = uri.indexOf("/odata/");
		int endIdx = uri.indexOf('/', idx+7);
		if (endIdx == -1) {
			vdbName = uri.substring(idx+7);
		}
		else {
			vdbName = uri.substring(idx+7, endIdx);
		}
		
		int versionIdx = vdbName.indexOf('.');
		if (versionIdx != -1) {
			version = Integer.parseInt(vdbName.substring(versionIdx+1));
			vdbName = vdbName.substring(0, versionIdx);
		}
		
		vdbName = vdbName.trim();
		if (vdbName.isEmpty()) {
			throw new TeiidRuntimeException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16008));
		}
		
		VDBKey key = new VDBKey(vdbName, version);
		LocalClient client = this.clientMap.get(key);
		if (client == null) {
			client = new LocalClient(vdbName, version);
			this.timer.schedule(client.getTimerTask(), 5*60*1000); // TODO: make this 100 configurable
			this.clientMap.put(key, client);
		}		
		return new TeiidProducer(client);
	}
}