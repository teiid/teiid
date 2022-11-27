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

package org.teiid.translator.jdbc.modeshape;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.CoreConstants;
import org.teiid.language.Command;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.TranslationHelper;

/**
 */
@SuppressWarnings("nls")
public class TestModeShapeSqlTranslator {

    private static ModeShapeExecutionFactory TRANSLATOR;
    private static TranslationUtility UTIL;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new ModeShapeExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
        UTIL = new TranslationUtility(getMetadata());
        UTIL.addUDF(CoreConstants.SYSTEM_MODEL, TRANSLATOR.getPushDownFunctions());
    }

    public static TransformationMetadata getMetadata() {
        MetadataStore store = new MetadataStore();
        Schema modeshape = RealMetadataFactory.createPhysicalModel("modeshape", store);
        Table nt_base = RealMetadataFactory.createPhysicalGroup("nt_base", modeshape);
        nt_base.setNameInSource("\"nt:base\"");
        List<Column> cols = RealMetadataFactory.createElements(nt_base, new String[] { "jcr_path",
                "mode_properties", "jcr_primaryType", "prop" }, new String[] {
                TypeFacility.RUNTIME_NAMES.STRING,
                TypeFacility.RUNTIME_NAMES.STRING,
                TypeFacility.RUNTIME_NAMES.STRING,
                TypeFacility.RUNTIME_NAMES.STRING });
        cols.get(0).setNameInSource("\"jcr:path\"");
        cols.get(1).setNameInSource("\"mode:properties\"");
        cols.get(2).setNameInSource("\"jcr:primaryType\"");

        Table nt_version = RealMetadataFactory.createPhysicalGroup("nt_version", modeshape);
        nt_version.setNameInSource("\"nt:version\"");
        List<Column> cols2 = RealMetadataFactory.createElements(nt_version, new String[] { "jcr_path",
                "mode_properties", "jcr_primaryType", "prop" }, new String[] {
                TypeFacility.RUNTIME_NAMES.STRING,
                TypeFacility.RUNTIME_NAMES.STRING,
                TypeFacility.RUNTIME_NAMES.STRING,
                TypeFacility.RUNTIME_NAMES.STRING });
        cols2.get(0).setNameInSource("\"jcr:path\"");
        cols2.get(1).setNameInSource("\"mode:properties\"");
        cols2.get(2).setNameInSource("\"jcr:primaryType\"");
        return RealMetadataFactory.createTransformationMetadata(store, "modeshape");
    }

    public void helpTestVisitor(String input, String expectedOutput) throws TranslatorException {
        Command obj = UTIL.parseCommand(input, true, true);
        TranslationHelper.helpTestVisitor(expectedOutput, TRANSLATOR, obj);
    }

    @Test
    public void testSelectAllFromBase() throws Exception {
        String input = "select * from nt_base"; //$NON-NLS-1$
        String output = "SELECT g_0.\"jcr:path\", g_0.\"mode:properties\", g_0.\"jcr:primaryType\", g_0.prop FROM \"nt:base\" AS g_0"; //$NON-NLS-1$

        helpTestVisitor(input, output);

    }

    @Test
    public void testPredicate() throws Exception {

        String input = "SELECT x.jcr_primaryType from nt_base inner join nt_base as x on jcr_issamenode(nt_base.jcr_path, x.jcr_path) = true where jcr_isdescendantnode(nt_base.jcr_path, 'x/y/z') = true and jcr_reference(nt_base.mode_properties) = 'x'"; //$NON-NLS-1$
        String output = "SELECT g_1.\"jcr:primaryType\" FROM \"nt:base\" AS g_0 INNER JOIN \"nt:base\" AS g_1 ON ISSAMENODE(g_0, g_1) WHERE ISDESCENDANTNODE(g_0, 'x/y/z') AND REFERENCE(g_0.*) = 'x'"; //$NON-NLS-1$

        helpTestVisitor(input, output);

    }

    @Test
    public void testOrderBy() throws Exception {

        String input = "SELECT jcr_primaryType from nt_base ORDER BY jcr_primaryType"; //$NON-NLS-1$
        String output = "SELECT g_0.\"jcr:primaryType\" AS c_0 FROM \"nt:base\" AS g_0 ORDER BY c_0"; //$NON-NLS-1$

        helpTestVisitor(input, output);

    }

    @Test
    public void testUsingLike() throws Exception {

        String input = "SELECT jcr_primaryType from nt_base WHERE jcr_primaryType LIKE '%relational%'"; //$NON-NLS-1$
        String output = "SELECT g_0.\"jcr:primaryType\" FROM \"nt:base\" AS g_0 WHERE g_0.\"jcr:primaryType\" LIKE '%relational%'"; //$NON-NLS-1$

        helpTestVisitor(input, output);
    }

    /**
     * TEIID-3102
     * @throws Exception
     */
    @Test
    public void testSelectJoin() throws Exception {

        String input = "select nt_base.jcr_path from nt_base join nt_version  ON JCR_ISCHILDNODE(nt_base.jcr_path, nt_version.jcr_path)"; //$NON-NLS-1$
        String output = "SELECT g_0.\"jcr:path\" FROM \"nt:base\" AS g_0 INNER JOIN \"nt:version\" AS g_1 ON ISCHILDNODE(g_0, g_1)"; //$NON-NLS-1$

        helpTestVisitor(input, output);
    }

    @Test
    public void testOnCondition() throws Exception {
        String input = "select nt_base.jcr_path from nt_base join nt_version  ON JCR_ISCHILDNODE(nt_base.jcr_path, nt_version.jcr_path) and nt_base.jcr_path = nt_version.jcr_path"; //$NON-NLS-1$
        String output = "SELECT g_0.\"jcr:path\" FROM \"nt:base\" AS g_0 INNER JOIN \"nt:version\" AS g_1 ON g_0.\"jcr:path\" = g_1.\"jcr:path\" WHERE ISCHILDNODE(g_0, g_1)"; //$NON-NLS-1$

        helpTestVisitor(input, output);

        input = "select nt_base.jcr_path from nt_base join nt_version  ON JCR_ISCHILDNODE(nt_base.jcr_path, nt_version.jcr_path) or nt_base.jcr_path = nt_version.jcr_path"; //$NON-NLS-1$
        output = "SELECT g_0.\"jcr:path\" FROM \"nt:base\" AS g_0 INNER JOIN \"nt:version\" AS g_1 ON ISCHILDNODE(g_0, g_1) OR g_0.\"jcr:path\" = g_1.\"jcr:path\""; //$NON-NLS-1$

        helpTestVisitor(input, output);
    }

}
