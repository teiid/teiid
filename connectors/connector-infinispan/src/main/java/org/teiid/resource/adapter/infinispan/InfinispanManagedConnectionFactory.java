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
package org.teiid.resource.adapter.infinispan;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.teiid.resource.adapter.custom.spi.BasicConnectionFactory;
import org.teiid.resource.adapter.custom.spi.BasicManagedConnectionFactory;

@SuppressWarnings("serial")
public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory {

	private String remoteServerList;
	private RemoteCacheManager cacheContainer;


	@Override
	public BasicConnectionFactory createConnectionFactory() throws ResourceException {
		

	
		return new BasicConnectionFactory() {

			private static final long serialVersionUID = 1L;
			
			private Object lock = new Object();


			@Override
			public InfinispanConnectionImpl getConnection() throws ResourceException {
				synchronized(lock) {
					
					RemoteCacheManager cc = getOrCreateCacheContainer();
					if (cc == null) {
			            throw new ResourceException("Unable to create Infinispan CacheContainer" );
					}

				}
				return new InfinispanConnectionImpl(InfinispanManagedConnectionFactory.this);
			}
		};
	}	
    
	
   public String getRemoteServerList() {
        return remoteServerList;
    }

    /**
     * Set the list of remote servers that make up the Infinispan cluster. The servers must be Infinispan HotRod servers. The list
     * must be in the appropriate format of <code>host:port[;host:port...]</code> that would be used when defining an Infinispan
     * {@link RemoteCacheManager} instance. If the value is missing, <code>localhost:11311</code> is assumed.
     * 
     * @param remoteServerList the server list in appropriate <code>server:port;server2:port2</code> format.
     */
    public synchronized void setRemoteServerList( String remoteServerList ) {
        if (this.remoteServerList == remoteServerList || this.remoteServerList != null
            && this.remoteServerList.equals(remoteServerList)) return; // unchanged
        this.remoteServerList = remoteServerList;
    }
    
    
    protected RemoteCacheManager getRemoteCacheManager() {
    	return this.cacheContainer;
    }
	private RemoteCacheManager getOrCreateCacheContainer() {
		if (this.cacheContainer != null) {
			return this.cacheContainer;
		}
        if (getRemoteServerList() == null || getRemoteServerList().equals("")) {
        	this.cacheContainer = new RemoteCacheManager();
        	return this.cacheContainer;
        }
        this.cacheContainer = new RemoteCacheManager(getRemoteServerList());
        
        return this.cacheContainer;

    }

  
	
}
