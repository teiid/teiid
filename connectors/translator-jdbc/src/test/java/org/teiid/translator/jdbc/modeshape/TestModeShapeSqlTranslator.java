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

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslatedCommand;

/**
 */
@SuppressWarnings("nls")
public class TestModeShapeSqlTranslator {

	private static ModeShapeExecutionFactory TRANSLATOR;

	private static String MODESHAPE_VDB = (UnitTestUtil.getTestDataPath() != null ? UnitTestUtil
			.getTestDataPath()
			: "src/test/resources")
			+ "/ModeShape.vdb";
	
    
    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new ModeShapeExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();

    }

	public void helpTestVisitor(TranslationUtility util, String input,
			String expectedOutput) throws TranslatorException {
		// Convert from sql to objects
		Command obj = util.parseCommand(input);

		TranslatedCommand tc = new TranslatedCommand(Mockito
				.mock(ExecutionContext.class), TRANSLATOR);
		tc.translateCommand(obj);
		assertEquals("Did not get correct sql", expectedOutput, tc.getSql()); //$NON-NLS-1$
	}
	
	@Test
	public void testSelectAllFromBase() throws Exception {
		String input = "select * from nt_base"; //$NON-NLS-1$
		String output = "SELECT [jcr:primaryType] FROM [nt:base]"; //$NON-NLS-1$

		helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);

	}
	
	@Test
	public void testSelectColumnFromBase() throws Exception {
		String input = "select jcr_primaryType from nt_base"; //$NON-NLS-1$
		String output = "SELECT [jcr:primaryType] FROM [nt:base]"; //$NON-NLS-1$

		helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);

	}	

	@Test
	public void testWhereClause() throws Exception {

		String input = "SELECT jcr_primaryType from nt_base WHERE jcr_primaryType = 'relational:column'"; //$NON-NLS-1$
		String output = "SELECT [jcr:primaryType] FROM [nt:base] WHERE [jcr:primaryType] = 'relational:column'"; //$NON-NLS-1$

		helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);

	}

	@Test
	public void testOrderBy() throws Exception {

		String input = "SELECT jcr_primaryType from nt_base ORDER BY jcr_primaryType"; //$NON-NLS-1$
		String output = "SELECT [jcr:primaryType] FROM [nt:base] ORDER BY [jcr:primaryType] ASC"; //$NON-NLS-1$

		helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);

	}

	@Test
	public void testUsingLike() throws Exception {

		String input = "SELECT jcr_primaryType from nt_base WHERE jcr_primaryType LIKE '%relational%'"; //$NON-NLS-1$
		String output = "SELECT [jcr:primaryType] FROM [nt:base] WHERE [jcr:primaryType] LIKE '%relational%'"; //$NON-NLS-1$

		helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);

	}
	


}
