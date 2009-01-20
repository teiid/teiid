/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.server;

import java.util.HashMap;
import java.util.Map;

import org.jgroups.Channel;
import org.jgroups.mux.Multiplexer;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.metamatrix.core.MetaMatrixRuntimeException;

@Singleton
public class ChannelProvider {
	public enum ChannelID{ CACHE, RPC};
	
	@Inject
	Multiplexer mux;
	
	private Map<ChannelID, Channel> channelMap = new HashMap<ChannelID, Channel>();
	
	public Channel get(ChannelID id) {
		Channel c = this.channelMap.get(id);
		if (c == null) {
			try {
				c = this.mux.createMuxChannel(id.toString(), "Federate"); //$NON-NLS-1$
				this.channelMap.put(id, c);
			} catch (Exception e) {
				throw new MetaMatrixRuntimeException("Failed to create a Channel"); //$NON-NLS-1$
			}
		}
		return c;
	}
}
