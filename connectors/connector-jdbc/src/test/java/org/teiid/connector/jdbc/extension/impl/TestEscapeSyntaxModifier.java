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

package org.teiid.connector.jdbc.extension.impl;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.jdbc.translator.EscapeSyntaxModifier;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.visitor.util.SQLReservedWords;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.query.unittest.TimestampUtil;

/**
 */
public class TestEscapeSyntaxModifier extends TestCase {

    /**
     * Constructor for TestDropFunctionModifier.
     * @param name
     */
    public TestEscapeSyntaxModifier(String name) {
        super(name);
    }

    public void testEscape() {
        ILiteral arg1 = CommandBuilder.getLanuageFactory().createLiteral("arg1", String.class); //$NON-NLS-1$
        ILiteral arg2 = CommandBuilder.getLanuageFactory().createLiteral("arg2", String.class);//$NON-NLS-1$
        IFunction func = CommandBuilder.getLanuageFactory().createFunction("concat", Arrays.asList( arg1, arg2), Integer.class); //$NON-NLS-1$
                
        helpTest(func, "{fn concat('arg1', 'arg2')}");
    }
    
    public void testTimestampAdd() {
        ILiteral arg1 = CommandBuilder.getLanuageFactory().createLiteral(SQLReservedWords.SQL_TSI_HOUR, String.class); //$NON-NLS-1$
        ILiteral arg2 = CommandBuilder.getLanuageFactory().createLiteral(Integer.valueOf(1), Integer.class);//$NON-NLS-1$
        ILiteral arg3 = CommandBuilder.getLanuageFactory().createLiteral(TimestampUtil.createTimestamp(0, 0, 0, 0, 0, 0, 0), Timestamp.class);//$NON-NLS-1$
        IFunction func = CommandBuilder.getLanuageFactory().createFunction("timestampadd", Arrays.asList( arg1, arg2, arg3), Timestamp.class); //$NON-NLS-1$
                
        helpTest(func, "{fn timestampadd(SQL_TSI_HOUR, 1, {ts'1899-12-31 00:00:00.0'})}");
    }

	private void helpTest(IFunction func, String expected) {
        EscapeSyntaxModifier mod = new EscapeSyntaxModifier();

		func = (IFunction) mod.modify(func);
        List parts = mod.translate(func);
        StringBuffer sb = new StringBuffer();
        for (Object object : parts) {
			sb.append(object);
		}
        assertEquals(expected, sb.toString());
	}
    
}
