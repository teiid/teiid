/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.adminapi.impl;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.DataPolicy.ResourceType;
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

        assertTrue(policy.allows("catalog.schema.Table1".toLowerCase(), ResourceType.TABLE, PermissionType.READ)); //$NON-NLS-1$
        assertNull(policy.allows("catalog.schema.Table1".toLowerCase(), ResourceType.TABLE, PermissionType.CREATE)); //$NON-NLS-1$

        assertNull(policy.allows("catalog.schema", ResourceType.SCHEMA, PermissionType.READ)); //$NON-NLS-1$

        assertNull(policy.allows("catalog.schema.Table2.column".toLowerCase(), ResourceType.COLUMN, PermissionType.READ)); //$NON-NLS-1$
        assertFalse(policy.allows("catalog.schema.Table2".toLowerCase(), ResourceType.TABLE, PermissionType.READ)); //$NON-NLS-1$

        assertNull(policy.allows("catalog.schema.Table3.column".toLowerCase(), ResourceType.COLUMN, PermissionType.READ)); //$NON-NLS-1$
        assertTrue(policy.allows("catalog.schema.Table3".toLowerCase(), ResourceType.TABLE, PermissionType.READ)); //$NON-NLS-1$

        assertTrue(policy.allows("catalog.schema.Table4".toLowerCase(), ResourceType.TABLE, PermissionType.READ)); //$NON-NLS-1$
        assertNull(policy.allows("catalog.schema.Table4".toLowerCase(), ResourceType.TABLE, PermissionType.DELETE)); //$NON-NLS-1$

        assertTrue(policy.allows("catalog.schema.Table5.column1".toLowerCase(), ResourceType.COLUMN, PermissionType.READ)); //$NON-NLS-1$
        assertNull(policy.allows("catalog.schema.Table5.column2".toLowerCase(), ResourceType.COLUMN, PermissionType.READ)); //$NON-NLS-1$
        assertNull(policy.allows("catalog.schema.Table5".toLowerCase(), ResourceType.TABLE, PermissionType.READ)); //$NON-NLS-1$
    }
}
