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

import java.lang.ref.SoftReference;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.odata4j.producer.ODataProducer;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.LRUCache;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.transport.LocalServerConnection;
import org.teiid.vdb.runtime.VDBKey;

@Provider
public class TeiidProducerProvider implements ContextResolver<ODataProducer>, VDBLifeCycleListener {

	@Context
	protected UriInfo uriInfo;	
	@Context
	protected javax.servlet.ServletContext context;
	protected Map<VDBKey, SoftReference<LocalClient>> clientMap = Collections.synchronizedMap(new LRUCache<VDBKey, SoftReference<LocalClient>>());
	private volatile boolean listenerRegistered = false;
		
	@Override
	public ODataProducer getContext(Class<?> arg0) {
		if (!arg0.equals(ODataProducer.class)) {
			throw new TeiidRuntimeException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16007));
		}
		
		String vdbName = null;
		int version = 1;
		String uri = uriInfo.getBaseUri().getRawPath();
		int idx = uri.indexOf("/odata/"); //$NON-NLS-1$
		if (idx != -1) {
			int endIdx = uri.indexOf('/', idx+7);
			if (endIdx == -1) {
				vdbName = uri.substring(idx+7);
			}
			else {
				vdbName = uri.substring(idx+7, endIdx);
			}
		} 
		else {
			vdbName = getInitParameters().getProperty("allow-vdb"); //$NON-NLS-1$		
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
		SoftReference<LocalClient> ref = this.clientMap.get(key);
		LocalClient client = null;
		if (ref != null) {
			client = ref.get();
		}
		if (client == null) {
			client = new LocalClient(vdbName, version, getInitParameters());
			if (!this.listenerRegistered) {
				synchronized(this) {
					if (!this.listenerRegistered) {
						ConnectionImpl connection = null;
						try {
							connection = client.getConnection();
							LocalServerConnection lsc = (LocalServerConnection)connection.getServerConnection();
							lsc.addListener(this);
							this.listenerRegistered = true;
						} catch (SQLException e) {
							LogManager.logWarning(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16014)); 
						} finally {
							if (connection != null) {
								try {
									connection.close();
								} catch (SQLException e) {
								}
							}
						}
					}
				}
			}
			ref = new SoftReference<LocalClient>(client);
			this.clientMap.put(key, ref);
		}		
		return new TeiidProducer(client);
	}
	
	Properties getInitParameters() {
		Properties props = new Properties();
		Enumeration<String> en = this.context.getInitParameterNames();
		while(en.hasMoreElements()) {
			String key = en.nextElement(); 
			props.setProperty(key, this.context.getInitParameter(key));
		}
		return props;
	}
	
	@Override
	public void removed(String name, int version, CompositeVDB vdb) {
	}
	
	@Override
	public void finishedDeployment(String name, int version, CompositeVDB vdb,boolean reloading) {
	}
	
	@Override
	public void beforeRemove(String name, int version, CompositeVDB vdb) {
		this.clientMap.remove(new VDBKey(name, version));
	}
	
	@Override
	public void added(String name, int version, CompositeVDB vdb,boolean reloading) {
	}
}