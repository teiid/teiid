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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDBImport;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;

@SuppressWarnings("nls")
public class TestVDBUtility {
    public static void validateVDB(VDBMetaData vdb) {
        ModelMetaData modelOne;
        ModelMetaData modelTwo;
        assertEquals("myVDB", vdb.getName()); //$NON-NLS-1$
        assertEquals("vdb description", vdb.getDescription()); //$NON-NLS-1$
        assertEquals("connection-type", "NONE", vdb.getConnectionType().name());
        assertEquals("1", vdb.getVersion());
        assertEquals("vdb-value", vdb.getPropertyValue("vdb-property")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("vdb-value2", vdb.getPropertyValue("vdb-property2")); //$NON-NLS-1$ //$NON-NLS-2$

        assertNotNull(vdb.getModel("model-one")); //$NON-NLS-1$
        assertNotNull(vdb.getModel("model-two")); //$NON-NLS-1$
        assertNull(vdb.getModel("model-unknown")); //$NON-NLS-1$

        assertEquals(1, vdb.getVDBImports().size());
        VDBImport vdbImport = vdb.getVDBImports().get(0);
        assertEquals("x", vdbImport.getName());
        assertEquals("2", vdbImport.getVersion());

        modelOne = vdb.getModel("model-one"); //$NON-NLS-1$
        assertEquals("model-one", modelOne.getName()); //$NON-NLS-1$
        assertEquals("s1", modelOne.getSourceNames().get(0)); //$NON-NLS-1$
        assertEquals(Model.Type.PHYSICAL, modelOne.getModelType());
        assertEquals("model-value-override", modelOne.getPropertyValue("model-prop")); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse(modelOne.isVisible());
        assertEquals("model description", modelOne.getDescription());
        assertEquals("DDL", modelOne.getSourceMetadataType().get(0));
        assertEquals("DDL Here", modelOne.getSourceMetadataText().get(0));
        assertEquals("OTHER", modelOne.getSourceMetadataType().get(1));
        assertEquals("other text", modelOne.getSourceMetadataText().get(1));

        modelTwo = vdb.getModel("model-two"); //$NON-NLS-1$
        assertEquals("model-two", modelTwo.getName()); //$NON-NLS-1$
        assertTrue(modelTwo.getSourceNames().contains("s1")); //$NON-NLS-1$
        assertTrue(modelTwo.getSourceNames().contains("s2")); //$NON-NLS-1$
        assertEquals(Model.Type.VIRTUAL, modelTwo.getModelType()); // this is not persisted in the XML
        assertEquals("model-value", modelTwo.getPropertyValue("model-prop")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("DDL", modelTwo.getSourceMetadataType().get(0));
        assertEquals("DDL Here", modelTwo.getSourceMetadataText().get(0));


        assertTrue(vdb.getValidityErrors().contains("There is an error in VDB")); //$NON-NLS-1$

        List<Translator> translators = vdb.getOverrideTranslators();
        assertTrue(translators.size() == 1);

        Translator translator = translators.get(0);
        assertEquals("oracleOverride", translator.getName());
        assertEquals("oracle", translator.getType());
        assertEquals("my-value", translator.getPropertyValue("my-property"));
        assertEquals("hello world", translator.getDescription());
        List<DataPolicy> roles = vdb.getDataPolicies();

        assertTrue(roles.size() == 1);

        DataPolicyMetadata role = vdb.getDataPolicyMap().get("roleOne"); //$NON-NLS-1$
        assertTrue(role.isGrantAll());
        assertTrue(role.isAllowCreateTemporaryTables());
        assertEquals("roleOne described", role.getDescription()); //$NON-NLS-1$
        assertNotNull(role.getMappedRoleNames());
        assertTrue(role.getMappedRoleNames().contains("ROLE1")); //$NON-NLS-1$
        assertTrue(role.getMappedRoleNames().contains("ROLE2")); //$NON-NLS-1$

        List<DataPolicy.DataPermission> permissions = role.getPermissions();
        assertEquals(4, permissions.size());

        boolean lang = false;
        for (DataPolicy.DataPermission p: permissions) {
            if (p.getAllowLanguage() != null) {
                assertTrue(p.getAllowLanguage());
                assertEquals("javascript", p.getResourceName());
                lang = true;
                continue;
            }
            if (p.getResourceName().equalsIgnoreCase("myTable.T1")) { //$NON-NLS-1$
                assertTrue(p.getAllowRead());
                assertNull(p.getAllowDelete());
                assertFalse(p.getConstraint());
                continue;
            }
            if (p.getResourceName().equalsIgnoreCase("myTable.T2.col1")) { //$NON-NLS-1$
                assertEquals("col2", p.getMask());
                assertEquals(1, p.getOrder().intValue());
                continue;
            }
            assertFalse(p.getAllowRead());
            assertTrue(p.getAllowDelete());
            assertEquals("col1 = user()", p.getCondition());
            assertTrue(p.getConstraint());
        }
        assertTrue(lang);
    }

    public static VDBMetaData buildVDB() {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("myVDB"); //$NON-NLS-1$
        vdb.setDescription("vdb description"); //$NON-NLS-1$
        vdb.setConnectionType("NONE");
        vdb.setVersion(1);
        vdb.addProperty("vdb-property", "vdb-value"); //$NON-NLS-1$ //$NON-NLS-2$
        vdb.addProperty("vdb-property2", "vdb-value2"); //$NON-NLS-1$ //$NON-NLS-2$

        VDBImportMetadata vdbImport = new VDBImportMetadata();
        vdbImport.setName("x");
        vdbImport.setVersion("2");
        vdb.getVDBImports().add(vdbImport);

        ModelMetaData modelOne = new ModelMetaData();
        modelOne.setName("model-one"); //$NON-NLS-1$
        modelOne.addSourceMapping("s1", "translator", "java:mybinding"); //$NON-NLS-1$ //$NON-NLS-2$
        modelOne.setModelType(Model.Type.PHYSICAL); //$NON-NLS-1$
        modelOne.addProperty("model-prop", "model-value"); //$NON-NLS-1$ //$NON-NLS-2$
        modelOne.addProperty("model-prop", "model-value-override"); //$NON-NLS-1$ //$NON-NLS-2$
        modelOne.setVisible(false);
        modelOne.addMessage("ERROR", "There is an error in VDB"); //$NON-NLS-1$ //$NON-NLS-2$
        modelOne.addMessage("INFO", "Nothing to see here"); //$NON-NLS-1$ //$NON-NLS-2$
        modelOne.setDescription("model description");
        modelOne.addSourceMetadata("DDL", "DDL Here");
        modelOne.addSourceMetadata("OTHER", "other text");

        vdb.addModel(modelOne);

        ModelMetaData modelTwo = new ModelMetaData();
        modelTwo.setName("model-two"); //$NON-NLS-1$
        modelTwo.addSourceMapping("s1", "translator", "java:binding-one"); //$NON-NLS-1$ //$NON-NLS-2$
        modelTwo.addSourceMapping("s2", "translator", "java:binding-two"); //$NON-NLS-1$ //$NON-NLS-2$
        modelTwo.setModelType(Model.Type.VIRTUAL); //$NON-NLS-1$
        modelTwo.addProperty("model-prop", "model-value"); //$NON-NLS-1$ //$NON-NLS-2$
        modelTwo.addSourceMetadata("DDL", "DDL Here");

        vdb.addModel(modelTwo);

        VDBTranslatorMetaData t1 = new VDBTranslatorMetaData();
        t1.setName("oracleOverride");
        t1.setType("oracle");
        t1.setDescription("hello world");
        t1.addProperty("my-property", "my-value");
        List<Translator> list = new ArrayList<Translator>();
        list.add(t1);
        vdb.setOverrideTranslators(list);

        DataPolicyMetadata roleOne = new DataPolicyMetadata();
        roleOne.setName("roleOne"); //$NON-NLS-1$
        roleOne.setDescription("roleOne described"); //$NON-NLS-1$
        roleOne.setAllowCreateTemporaryTables(true);
        roleOne.setGrantAll(true);

        PermissionMetaData perm1 = new PermissionMetaData();
        perm1.setResourceName("myTable.T1"); //$NON-NLS-1$
        perm1.setAllowRead(true);
        perm1.setCondition("col1 = user()");
        perm1.setConstraint(false);
        roleOne.addPermission(perm1);

        PermissionMetaData perm2 = new PermissionMetaData();
        perm2.setResourceName("myTable.T2"); //$NON-NLS-1$
        perm2.setAllowRead(false);
        perm2.setAllowDelete(true);
        perm2.setCondition("col1 = user()");
        perm2.setConstraint(true);
        roleOne.addPermission(perm2);

        PermissionMetaData perm3 = new PermissionMetaData();
        perm3.setResourceName("javascript"); //$NON-NLS-1$
        perm3.setAllowLanguage(true);
        roleOne.addPermission(perm3);

        PermissionMetaData perm4 = new PermissionMetaData();
        perm4.setResourceName("myTable.T2.col1"); //$NON-NLS-1$
        perm4.setMask("col2");
        perm4.setOrder(1);
        roleOne.addPermission(perm4);

        roleOne.setMappedRoleNames(Arrays.asList("ROLE1", "ROLE2")); //$NON-NLS-1$ //$NON-NLS-2$

        vdb.addDataPolicy(roleOne);

        EntryMetaData em = new EntryMetaData();
        em.setPath("/path-one");
        em.setDescription("entry one");
        em.addProperty("entryone", "1");
        vdb.getEntries().add(em);

        EntryMetaData em2 = new EntryMetaData();
        em2.setPath("/path-two");
        vdb.getEntries().add(em2);
        return vdb;
    }
}
