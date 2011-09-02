/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
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
package org.teiid.adminapi.impl;

import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;

public class PermissionMap extends ListOverMap<PermissionMetaData> {
	
	private static final long serialVersionUID = -1170556665834875267L;

	public PermissionMap(KeyBuilder<PermissionMetaData> builder) {
		super(builder);
	}

	@Override
	public void add(int index, PermissionMetaData element) {
		PermissionMetaData previous = this.map.get(builder.getKey(element));
		if (previous != null) {
			if (element.allowCreate != null) {
				previous.setAllowCreate(element.allowCreate);
			}
			if (element.allowRead != null) {
				previous.setAllowRead(element.allowRead);
			}
			if (element.allowUpdate != null) {
				previous.setAllowUpdate(element.allowUpdate);
			}
			if (element.allowDelete != null) {
				previous.setAllowDelete(element.allowDelete);
			}
		}
		else {
			super.add(index, element);
		}
	}

}
