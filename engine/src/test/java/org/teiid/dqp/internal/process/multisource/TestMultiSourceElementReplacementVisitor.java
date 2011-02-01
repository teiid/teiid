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

package org.teiid.dqp.internal.process.multisource;

import java.util.HashSet;
import java.util.Set;

import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.unittest.FakeMetadataFactory;

import junit.framework.TestCase;



/** 
 * @since 4.2
 */
public class TestMultiSourceElementReplacementVisitor extends TestCase {

    public QueryMetadataInterface getMetadata() throws Exception {
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMultiBinding();
        Set<String> multiSourceModels = new HashSet<String>();
        multiSourceModels.add("MultiModel"); //$NON-NLS-1$
        MultiSourceMetadataWrapper wrapper = new MultiSourceMetadataWrapper(metadata, multiSourceModels);  
        
        return wrapper;
    }
    
    public void helpTest(String sql, QueryMetadataInterface metadata, String expected) throws Exception {
        QueryParser parser = new QueryParser();
        Command command = parser.parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);        
        
        MultiSourceElementReplacementVisitor visitor = new MultiSourceElementReplacementVisitor("x"); //$NON-NLS-1$
        DeepPostOrderNavigator.doVisit(command, visitor);
        
        assertEquals(expected, command.toString());
    }
    
    public void testCommon() throws Exception {
        helpTest("SELECT a, b, SOURCE_NAME FROM MultiModel.Phys WHERE SOURCE_NAME = SOURCE_NAME", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT a, b, 'x' FROM MultiModel.Phys WHERE 'x' = 'x'"); //$NON-NLS-1$
    }

    public void testLike() throws Exception {
        helpTest("SELECT a, b FROM MultiModel.Phys WHERE SOURCE_NAME LIKE SOURCE_NAME", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT a, b FROM MultiModel.Phys WHERE 'x' LIKE 'x'"); //$NON-NLS-1$
    }

    public void testIn() throws Exception {
        helpTest("SELECT a, b FROM MultiModel.Phys WHERE SOURCE_NAME IN ('a', 'b', SOURCE_NAME)", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT a, b FROM MultiModel.Phys WHERE 'x' IN ('a', 'b', 'x')"); //$NON-NLS-1$
    }

    public void testNot() throws Exception {
        helpTest("SELECT a, b FROM MultiModel.Phys WHERE NOT (SOURCE_NAME IN ('a', 'b', SOURCE_NAME))", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT a, b FROM MultiModel.Phys WHERE NOT ('x' IN ('a', 'b', 'x'))"); //$NON-NLS-1$
    }

    public void testCompound() throws Exception {
        helpTest("SELECT a, b FROM MultiModel.Phys WHERE ('x' IN ('a', 'b', SOURCE_NAME)) AND (SOURCE_NAME = 'y')", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT a, b FROM MultiModel.Phys WHERE ('x' IN ('a', 'b', 'x')) AND ('x' = 'y')"); //$NON-NLS-1$
    }

    public void testFunction() throws Exception {
        helpTest("SELECT length(concat(SOURCE_NAME, 'a')) FROM MultiModel.Phys", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT length(concat('x', 'a')) FROM MultiModel.Phys"); //$NON-NLS-1$
    }

    public void testBetween() throws Exception {
        helpTest("SELECT SOURCE_NAME FROM MultiModel.Phys WHERE SOURCE_NAME BETWEEN SOURCE_NAME AND SOURCE_NAME", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT 'x' FROM MultiModel.Phys WHERE 'x' BETWEEN 'x' AND 'x'"); //$NON-NLS-1$
    }

    public void testIsNull() throws Exception {
        helpTest("SELECT a FROM MultiModel.Phys WHERE SOURCE_NAME IS NULL", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT a FROM MultiModel.Phys WHERE 'x' IS NULL"); //$NON-NLS-1$
    }

    public void testInSubquery() throws Exception {
        helpTest("SELECT a FROM MultiModel.Phys WHERE SOURCE_NAME IN (SELECT b FROM MultiModel.Phys WHERE SOURCE_NAME IN ('x'))", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT a FROM MultiModel.Phys WHERE 'x' IN (SELECT b FROM MultiModel.Phys WHERE 'x' IN ('x'))"); //$NON-NLS-1$
    }

    public void testCompareSubquery() throws Exception {
        helpTest("SELECT a FROM MultiModel.Phys WHERE SOURCE_NAME = (SELECT b FROM MultiModel.Phys WHERE SOURCE_NAME IN ('x'))", //$NON-NLS-1$
                 getMetadata(),
                 "SELECT a FROM MultiModel.Phys WHERE 'x' = (SELECT b FROM MultiModel.Phys WHERE 'x' IN ('x'))"); //$NON-NLS-1$
    }
    
}
