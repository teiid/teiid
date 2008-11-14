/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
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

package com.metamatrix.common.extensionmodule;

import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;


public class TestExtensionModuleInstallUtil extends TestCase {

    private static final String PRINCIPAL = "TestPrincipal"; //$NON-NLS-1$
    private static final String PARENT_DIRECTORY = UnitTestUtil.getTestDataPath() + "/extensionmodule"; //$NON-NLS-1$

    private static ExtensionModuleInstallUtil installUtil;
    
	public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestExtensionModuleInstallUtil.class);
		//return suite;
        return new TestSetup(suite){
            protected void setUp() throws Exception{
                TestExtensionModuleManager.setUpOnce();
                installUtil = new ExtensionModuleInstallUtil(System.getProperties());
            }
            protected void tearDown() throws Exception{
            }
        };
	}
    
	// ################################## FRAMEWORK ################################

	public TestExtensionModuleInstallUtil(String name) {
		super(name);
	}

    /**
     * One of the tests jars will be removed here, which clears the cache
     * of ExtensionModuleManager between each test
     */
	public void tearDown() throws Exception{
        //ExtensionModuleManager.getInstance().removeSource( PRINCIPAL, FakeData.TestJar1.SOURCE_NAME);
        FakeData.TestJar1.data = null;
	}

    public void testAddSource() throws Exception {
        ExtensionModuleDescriptor desc = installUtil.installExtensionModule(FakeData.TestJar1.SOURCE_NAME, PARENT_DIRECTORY, FakeData.TestJar1.TYPE, PRINCIPAL, FakeData.TestJar1.DESCRIPTION, FakeData.TestJar1.SOURCE_NAME);
        assertNotNull(desc);

        Checksum algorithm = new CRC32();
        algorithm.update(FakeData.TestJar1.data, 0, FakeData.TestJar1.data.length);
        long thisChecksum = algorithm.getValue();
        long thatChecksum = desc.getChecksum();
        assertTrue(thisChecksum == thatChecksum);

        ExtensionModuleManager manager = ExtensionModuleManager.getInstance(); 

        byte[] data = manager.getSource( FakeData.TestJar1.SOURCE_NAME);
        assertNotNull(data);
        assertTrue(thisChecksum == thatChecksum);

        if (data.length != FakeData.TestJar1.data.length){
            fail("Source size returned for " + FakeData.TestJar1.SOURCE_NAME + " not equal to test data size."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        if (!Arrays.equals(data, FakeData.TestJar1.data)){
            fail("Source returned for " + FakeData.TestJar1.SOURCE_NAME + " not equal to test data."); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    
    /**
     * This will fail if the source doesn't already exists.  As for confirming the update, the add
     * essentially did that above. 
     * 
     * @since 4.3
     */
    public void testUpdateSource(){
        try{
            String fullpath = FileUtils.buildDirectoryPath(new String[] {PARENT_DIRECTORY, FakeData.TestJar1.SOURCE_NAME});
            installUtil.updateExtensionModule(fullpath , FakeData.TestJar1.SOURCE_NAME, PRINCIPAL);
            
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " + FakeData.TestJar1.SOURCE_NAME + " does not exists: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
    }
}
