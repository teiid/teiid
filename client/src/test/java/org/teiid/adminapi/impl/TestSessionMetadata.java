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

package org.teiid.adminapi.impl;

import static org.junit.Assert.*;

import org.jboss.metatype.api.values.MetaValue;
import org.junit.Test;

@SuppressWarnings("nls")
public class TestSessionMetadata {
	
	@Test public void testMapping() {
		SessionMetadata session = new SessionMetadata();
		session.setSessionId("test");
		session.setApplicationName("foo");
		SessionMetadataMapper smm = new SessionMetadataMapper();
		MetaValue mv = smm.createMetaValue(smm.getMetaType(), session);
		
		SessionMetadata session1 = smm.unwrapMetaValue(mv);
		
		assertEquals(session.getSessionId(), session1.getSessionId());
		assertEquals(session.getApplicationName(), session1.getApplicationName());
	}

}
