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
package com.metamatrix.cache.jboss;

import org.jboss.cache.Fqn;
import org.jboss.cache.notifications.annotation.NodeCreated;
import org.jboss.cache.notifications.annotation.NodeEvicted;
import org.jboss.cache.notifications.annotation.NodeLoaded;
import org.jboss.cache.notifications.annotation.NodeModified;
import org.jboss.cache.notifications.annotation.NodeMoved;
import org.jboss.cache.notifications.annotation.NodeRemoved;
import org.jboss.cache.notifications.event.NodeEvent;

import com.metamatrix.cache.CacheListener;

@org.jboss.cache.notifications.annotation.CacheListener
public class JBossCacheListener {

	private CacheListener listener;
	private Fqn rootFqn;

	public JBossCacheListener(Fqn fqn, CacheListener listener) {
		this.rootFqn = fqn;
		this.listener = listener;
	}
	
    @NodeCreated
	@NodeRemoved
	@NodeModified
	@NodeMoved
	@NodeLoaded
	@NodeEvicted 
	public synchronized void cacheChanged(NodeEvent ne) {
    	Fqn fqn = ne.getFqn();
    	if (fqn.isChildOrEquals(rootFqn)) {
    		listener.cacheChanged();
    	}
	}
}
