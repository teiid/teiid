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

package com.metamatrix.dqp;


import com.google.inject.Injector;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.NoOpMessageBus;

public class ResourceFinder {
	private static Injector injector;
	
	public static MessageBus getMessageBus() {
		if (injector == null) {
			return new NoOpMessageBus();
		}
		return injector.getInstance(MessageBus.class);
	}
	
	public static CacheFactory getCacheFactory() {
		if (injector == null) {
			throw new IllegalStateException();
		}
		return injector.getInstance(CacheFactory.class);
	}	
	
	public static void setInjector(Injector i) {
		injector = i;
	}
}
