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

import java.util.Arrays;

import org.junit.Test;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;

public class TestDataPolicyMetaData {

	@Test
	public void testAllowed() {
		DataPolicyMetadata policy = new DataPolicyMetadata();
		policy.setName("readOnly"); //$NON-NLS-1$
		policy.setDescription("Only has read only permissions"); //$NON-NLS-1$
		policy.setMappedRoleNames(Arrays.asList("jack", "susan")); //$NON-NLS-1$ //$NON-NLS-2$
		
		
		PermissionMetaData perm1 = new PermissionMetaData();
		perm1.setResourceName("catalog.schema.Table1"); //$NON-NLS-1$
		perm1.setAllowRead(true);
		
		PermissionMetaData perm2 = new PermissionMetaData();
		perm2.setResourceName("catalog.schema.Table2"); //$NON-NLS-1$
		perm2.setAllowRead(false);
		
		PermissionMetaData perm3 = new PermissionMetaData();
		perm3.setResourceName("catalog.schema.Table3"); //$NON-NLS-1$
		perm3.setAllowRead(true);
		
		PermissionMetaData perm4 = new PermissionMetaData();
		perm4.setResourceName("catalog.schema.Table4"); //$NON-NLS-1$
		perm4.setAllowRead(true);
		
		PermissionMetaData perm5 = new PermissionMetaData();
		perm5.setResourceName("catalog.schema.Table5.column1"); //$NON-NLS-1$
		perm5.setAllowRead(true);	
		
		policy.addPermission(perm1, perm2, perm3, perm4, perm5);
		
		assertTrue(policy.allows("catalog.schema.Table1".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
		assertNull(policy.allows("catalog.schema.Table1".toLowerCase(), PermissionType.CREATE)); //$NON-NLS-1$
		
		assertNull(policy.allows("catalog.schema", PermissionType.READ)); //$NON-NLS-1$
		
		assertNull(policy.allows("catalog.schema.Table2.column".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
		assertFalse(policy.allows("catalog.schema.Table2".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
		
		assertNull(policy.allows("catalog.schema.Table3.column".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
		assertTrue(policy.allows("catalog.schema.Table3".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
		
		assertTrue(policy.allows("catalog.schema.Table4".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
		assertNull(policy.allows("catalog.schema.Table4".toLowerCase(), PermissionType.DELETE)); //$NON-NLS-1$
		
		assertTrue(policy.allows("catalog.schema.Table5.column1".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
		assertNull(policy.allows("catalog.schema.Table5.column2".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
		assertNull(policy.allows("catalog.schema.Table5".toLowerCase(), PermissionType.READ)); //$NON-NLS-1$
	}
}
