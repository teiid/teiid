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

package org.teiid.translator.jdbc.modeshape;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
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
    private static String UDF = "/JCRFunctions.xmi"; //$NON-NLS-1$;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new ModeShapeExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
        UTIL = new TranslationUtility(getMetadata());
        TranslationHelper.loadUDFs(UDF, UTIL);
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
	
}
