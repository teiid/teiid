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

package com.metamatrix.common.extensionmodule;

import static org.junit.Assert.*;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleOrderingException;
import com.metamatrix.common.extensionmodule.exception.InvalidExtensionModuleTypeException;
import com.metamatrix.common.extensionmodule.spi.InMemoryExtensionModuleTransactionFactory;


public class TestExtensionModuleManager {

    private static final String PRINCIPAL = "TestPrincipal"; //$NON-NLS-1$

    private static ExtensionModuleManager manager;
    
    /**
     * Loads most of the extension sources into the ExtensionModuleManager
     * up front, one time, to avoid overhead - this is used by
     * {@link #suite} method
     */
    @BeforeClass public static void setUpOnce() throws Exception{
		manager = new ExtensionModuleManager(getExtensionModuleProperties());
        FakeData.init();
    }

    private static Properties getExtensionModuleProperties() {
        Properties BASE_PROPERTIES = new Properties();
        
        BASE_PROPERTIES.setProperty(ExtensionModulePropertyNames.CONNECTION_FACTORY,InMemoryExtensionModuleTransactionFactory.class.getName()); 

        return BASE_PROPERTIES;
    }	
	
    /**
     * All remaining extension sources are removed - this is used by
     * {@link #suite} method
     */
    @AfterClass public static void tearDownOnce() throws Exception{
        Iterator i = manager.getSourceNames().iterator();
        while (i.hasNext()){
            manager.removeSource( PRINCIPAL, i.next().toString());
        }
    }

    /**
     * One of the tests jars will be added here, which clears the cache
     * of ExtensionModuleManager between each test
     */
	@Before public void setUp() throws Exception{
		try {
			manager.removeSource(PRINCIPAL, FakeData.TestJar1.SOURCE_NAME);
		} catch (ExtensionModuleNotFoundException e) {
			
		}
		try {
			manager.removeSource(PRINCIPAL, FakeData.TestTextFile.SOURCE_NAME);
		} catch (ExtensionModuleNotFoundException e) {
			
		}
		try {
			manager.removeSource(PRINCIPAL, FakeData.TestJar2.SOURCE_NAME);
		} catch (ExtensionModuleNotFoundException e) {
			
		}
		try {
			manager.removeSource(PRINCIPAL, FakeData.TestJar7.SOURCE_NAME);
		} catch (ExtensionModuleNotFoundException e) {
			
		}
        manager.addSource( PRINCIPAL, FakeData.TestJar1.TYPE, FakeData.TestJar1.SOURCE_NAME, FakeData.TestJar1.data, FakeData.TestJar1.DESCRIPTION, true);
        manager.addSource( PRINCIPAL, FakeData.TestTextFile.TYPE, FakeData.TestTextFile.SOURCE_NAME, FakeData.TestTextFile.data, FakeData.TestTextFile.DESCRIPTION, true);
        manager.addSource( PRINCIPAL, FakeData.TestJar2.TYPE, FakeData.TestJar2.SOURCE_NAME, FakeData.TestJar2.data, FakeData.TestJar2.DESCRIPTION, true);
        manager.addSource( PRINCIPAL, FakeData.TestJar7.TYPE, FakeData.TestJar7.SOURCE_NAME, FakeData.TestJar7.data, FakeData.TestJar7.DESCRIPTION, true);
	}

	static void printDescriptor(ExtensionModuleDescriptor desc, PrintStream ps){
        ps.println("<!><!><!><!>------------------------------------"); //$NON-NLS-1$
        ps.println("ExtensionModuleDescriptor:"); //$NON-NLS-1$
        ps.println("Name:       " + desc.getName()); //$NON-NLS-1$
        ps.println("Desc:       " + desc.getDescription()); //$NON-NLS-1$
        ps.println("Type:       " + desc.getType()); //$NON-NLS-1$
        ps.println("Search Pos: " + desc.getPosition()); //$NON-NLS-1$
        ps.println("Enabled:    " + desc.isEnabled()); //$NON-NLS-1$
        ps.println("Created by: " + desc.getCreatedBy()); //$NON-NLS-1$
        ps.println("Created:    " + desc.getCreationDate()); //$NON-NLS-1$
        ps.println("Updated by: " + desc.getLastUpdatedBy()); //$NON-NLS-1$
        ps.println("Updated:    " + desc.getLastUpdatedDate()); //$NON-NLS-1$
        ps.println("checksum:   " + desc.getChecksum()); //$NON-NLS-1$
        ps.println("toString(): " + desc.toString()); //$NON-NLS-1$
        ps.println("<!><!><!><!>------------------------------------"); //$NON-NLS-1$
    }

    static void printDescriptors(Collection descs, PrintStream ps){
        ps.println("Printing " + descs.size() + " descriptor(s)..."); //$NON-NLS-1$ //$NON-NLS-2$
        for (Iterator i = descs.iterator(); i.hasNext(); ){
            printDescriptor((ExtensionModuleDescriptor)i.next(), ps);
        }
    }

    //===================================================================
    //ACTUAL TESTS
    //===================================================================

	// ################################## addSource ################################

    /**
     * @see ExtensionModuleManager@addSource
     */
    @Test public void testAddSource() throws Exception {
        try{
            manager.removeSource( PRINCIPAL, FakeData.TestJar1.SOURCE_NAME);
        } catch (ExtensionModuleNotFoundException e){
            //ignore
        }

        ExtensionModuleDescriptor desc = manager.addSource( PRINCIPAL, FakeData.TestJar1.TYPE, FakeData.TestJar1.SOURCE_NAME, FakeData.TestJar1.data, FakeData.TestJar1.DESCRIPTION, true);

        //check checksum
        assertTrue(desc.getName().equals(FakeData.TestJar1.SOURCE_NAME));
        assertTrue(desc.getType().equals(FakeData.TestJar1.TYPE));
        assertTrue(desc.getDescription().equals(FakeData.TestJar1.DESCRIPTION));
        CRC32 algorithm = new CRC32();
        algorithm.update(FakeData.TestJar1.data, 0, FakeData.TestJar1.data.length);
        assertTrue(desc.getChecksum() == algorithm.getValue());
    }

    /**
     * Tests that an DuplicateExtensionModuleException is (correctly) triggered
     * @see ExtensionModuleManager@addSource
     */
    @Test public void testAddDuplicateSource(){
        DuplicateExtensionModuleException exception = null;
        try{
            manager.addSource( PRINCIPAL, FakeData.TestJar1.TYPE, FakeData.TestJar1.SOURCE_NAME, FakeData.TestJar1.data, FakeData.TestJar1.DESCRIPTION, true);
        } catch (DuplicateExtensionModuleException e){
            exception = e;
        } catch (InvalidExtensionModuleTypeException e){
            fail(e.getMessage());
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(exception);
    }

	// ################################## removeSource #############################

    /**
     * Tests that a source can be removed, and that it is really gone if
     * it is asked for after that
     * @see ExtensionModuleManager@removeSource
     */
    @Test public void testRemoveSource() {
        try{
            manager.removeSource( PRINCIPAL, FakeData.TestJar2.SOURCE_NAME);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  FakeData.TestJar2.SOURCE_NAME + " not found: " + e); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }

        //try to get source just removed, should trigger exception
        ExtensionModuleNotFoundException exception = null;
        try{
            manager.getSource( FakeData.TestJar2.SOURCE_NAME);
        } catch (ExtensionModuleNotFoundException e){
            exception = e;
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(exception);
    }

	// ################################## getSourceNames ###########################



	// ################################## getSourceDescriptors #####################

    /**
     * Just tests that collection comes back as non-null
     * @see ExtensionModuleManager@getSourceDescriptors
     */
    @Test public void testGetDescriptors(){
        List descriptors = null;
        try{
            descriptors = manager.getSourceDescriptors( );
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(descriptors);
        //printDescriptors(descriptors, System.out);
    }

	// ################################## getSourceTypes ###########################
	// ################################## getSourceDescriptors(String type) ########

    /**
     * Just tests that collections come back as non-null
     * @see ExtensionModuleManager@getSourceTypes
     * @see ExtensionModuleManager@getSourceDescriptors
     */
    @Test public void testGetTypesAndDescriptors(){
        Collection types = null;
        try{
            types = manager.getSourceTypes();
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(types);

        Iterator i = types.iterator();
        List descriptors = null;
        try{
            while (i.hasNext()){
                descriptors = manager.getSourceDescriptors(i.next().toString());
                assertNotNull(descriptors);
                //printDescriptors(descriptors, System.out);
                descriptors = null;
            }
        } catch (InvalidExtensionModuleTypeException e){
            fail(e.getMessage());
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
    }

    /**
     * Tests that an InvalidExtensionTypeException is (correctly) triggered
     * @see ExtensionModuleManager@getSourceDescriptors
     */
    @Test public void testGetDescriptorsOfInvalidType(){
        InvalidExtensionModuleTypeException exception = null;
        try{
            manager.getSourceDescriptors( "!!BOGUS TYPE!!"); //$NON-NLS-1$
        } catch (InvalidExtensionModuleTypeException e){
            exception = e;
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(exception);
    }

	// ################################## getSourceDescriptor ######################

    /**
     * Tests that the descriptor is not null and has the correct source name
     * @see ExtensionModuleManager@getSourceDescriptor
     */
    @Test public void testGetDescriptor(){
        ExtensionModuleDescriptor result = null;
        try{
            result = manager.getSourceDescriptor( FakeData.TestJar2.SOURCE_NAME);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source descriptor for " +  FakeData.TestJar2.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(result);
        assertTrue(result.getName().equals(FakeData.TestJar2.SOURCE_NAME));
        /*
        if ( equals(result, FakeData.TestJar6.data))
            fail("Source returned for " + FakeData.TestJar1.SOURCE_NAME + " not equal to test data.");
        }
        */

        //printDescriptor(result, System.out);
    }

	// ################################## setSearchOrder ###########################

    /**
     * @see ExtensionModuleManager@setSearchOrder
     */
    @Test public void testShuffleSources(){
        List sourceNames = null;
        try{
            sourceNames = manager.getSourceNames();
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }

        Collections.shuffle(sourceNames);

        List test = null;
        try{
            manager.setSearchOrder(PRINCIPAL, sourceNames);
            test = manager.getSourceNames();
        } catch (ExtensionModuleOrderingException e){
            fail(e.getMessage());
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }

        assertEquals(sourceNames, test);
    }

    /**
     * Tries three things:
     * <ul>
     * <li>Tries to order with a shuffled list of source names that is missing
     * one - catches the expected exception
     * <li>Tries to order with a shuffled list of source names that includes a
     * bogus nonexistant one - catches the expected exception
     * <li>Verifies that the list on the back end hasn't changed through all
     * of this
     * </ul>
     * @see ExtensionModuleManager@setSearchOrder
     */
    @Test public void testInvalidOrdering(){
        List sourceNames = null;
        List oneMissing = null;
        List oneTooMany = null;
        List oneWrong = null;

        try{
            sourceNames = manager.getSourceNames();
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }

        oneWrong = new ArrayList(sourceNames);
        oneMissing = new ArrayList(sourceNames);
        Collections.shuffle(oneMissing);

        oneTooMany = new ArrayList(oneMissing);
        //remove one
        oneMissing.remove(0);
        //add one bogus
        oneTooMany.add("BOGUS##!!.txt"); //$NON-NLS-1$
        oneWrong.set(0,"BOGUS##!!.txt"); //$NON-NLS-1$

        //PART A - make sure if the parameter has too few
        //of the source names, an exception is caught
        List test = null;
        ExtensionModuleOrderingException exception = null;
        try{
            manager.setSearchOrder(PRINCIPAL, oneMissing);
            test = manager.getSourceNames();
        } catch (ExtensionModuleOrderingException e){
            exception = e;
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(exception);

        //PART B - make sure if the parameter has any bogus
        //source names, an exception is caught
        test = null;
        exception = null;
        try{
            manager.setSearchOrder(PRINCIPAL, oneTooMany);
            test = manager.getSourceNames();
        } catch (ExtensionModuleOrderingException e){
            exception = e;
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(exception);

        //PART C - make sure if the parameter has any bogus
        //source names, an exception is caught, even if the
        //# of sources passed in happens to equal the # of
        //sources stored in the DB
        test = null;
        exception = null;
        try{
            manager.setSearchOrder(PRINCIPAL, oneWrong);
            test = manager.getSourceNames();
        } catch (ExtensionModuleOrderingException e){
            exception = e;
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(exception);

        //FINALLY - make sure the order in the backend hasn't changed
        //through all of this
        test = null;
        try{
            test = manager.getSourceNames();
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertEquals(sourceNames, test);
    }


	// ################################## setEnabled ###############################

    /**
     * @see ExtensionModuleManager@setSearchOrder
     */
    @Test public void testSetEnabled() throws Exception {
        List sourceNames = new ArrayList(2);
        sourceNames.add(FakeData.TestJar1.SOURCE_NAME);
        sourceNames.add(FakeData.TestJar2.SOURCE_NAME);
        List descriptors = manager.setEnabled(PRINCIPAL, sourceNames, false);

        ExtensionModuleDescriptor descriptor = null;
        for (Iterator i = descriptors.iterator(); i.hasNext(); ){
            descriptor = (ExtensionModuleDescriptor)i.next();
            assertTrue(!descriptor.isEnabled());
        }

        descriptors = manager.setEnabled(PRINCIPAL, sourceNames, true);

        for (Iterator i = descriptors.iterator(); i.hasNext(); ){
            descriptor = (ExtensionModuleDescriptor)i.next();
            assertTrue(descriptor.isEnabled());
        }
    }

	// ################################## getSource ################################

    /**
     * @see ExtensionModuleManager@getSource
     */
    @Test public void testGetSource(){
        byte[] source = null;
        try{
            source = manager.getSource( FakeData.TestJar1.SOURCE_NAME);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  FakeData.TestJar1.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }

        if (!Arrays.equals(source, FakeData.TestJar1.data)){
            fail("Source returned for " + FakeData.TestJar1.SOURCE_NAME + " not equal to test data."); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

	// ################################## setSource ################################

    /**
     * @see ExtensionModuleManager@setSource
     * @see ExtensionModuleManager@getSource
     */
    @Test public void testSetSource(){
        ExtensionModuleDescriptor descriptor = null;
        try{
            descriptor = manager.setSource(PRINCIPAL, FakeData.TestTextFile.SOURCE_NAME, FakeData.TestTextFile2.data);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  FakeData.TestTextFile.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(descriptor);
        //printDescriptor(descriptor, System.out);

        byte[] source = null;
        try{
            source = manager.getSource( FakeData.TestTextFile.SOURCE_NAME);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  FakeData.TestJar1.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }

        if (!Arrays.equals(source, FakeData.TestTextFile2.data)){
            fail("Source returned for " + FakeData.TestJar1.SOURCE_NAME + " not equal to test data."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        source = null;

        //reset and check
        try{
            descriptor = manager.setSource(PRINCIPAL, FakeData.TestTextFile.SOURCE_NAME, FakeData.TestTextFile.data);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  FakeData.TestTextFile.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(descriptor);
        //printDescriptor(descriptor, System.out);

        try{
            source = manager.getSource( FakeData.TestTextFile.SOURCE_NAME);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  FakeData.TestJar1.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }

        if (!Arrays.equals(source, FakeData.TestTextFile.data)){
            fail("Source returned for " + FakeData.TestJar1.SOURCE_NAME + " not equal to test data."); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

	// ################################## setSourceName ############################

    /**
     * @see ExtensionModuleManager@setSourceName
     */
    @Test public void testSetSourceName(){
        ExtensionModuleDescriptor descriptor = null;
        String newName = "BOGUS NAME"; //$NON-NLS-1$
        try{
            descriptor = manager.setSourceName(PRINCIPAL, FakeData.TestJar2.SOURCE_NAME, newName);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  FakeData.TestJar2.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(descriptor);
        //printDescriptor(descriptor, System.out);
        assertEquals(descriptor.getName(), newName);

        //reset
        try{
            descriptor = manager.setSourceName(PRINCIPAL, newName, FakeData.TestJar2.SOURCE_NAME);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  newName + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(descriptor);
        //printDescriptor(descriptor, System.out);
        assertEquals(descriptor.getName(), FakeData.TestJar2.SOURCE_NAME);

    }

	// ################################## setSourceDescription #####################

    /**
     * @see ExtensionModuleManager@setSourceDescription
     */
    @Test public void testSetSourceDescription(){
        ExtensionModuleDescriptor descriptor = null;
        String newDesc = "BOGUS DESCRIPTION"; //$NON-NLS-1$
        try{
            descriptor = manager.setSourceDescription(PRINCIPAL, FakeData.TestJar2.SOURCE_NAME, newDesc);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source " +  FakeData.TestJar2.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
        assertNotNull(descriptor);
        //printDescriptor(descriptor, System.out);
        assertEquals(descriptor.getDescription(), newDesc);
    }

	// ################################## getInstance ##############################


	// ################################## checksum #################################

    /**
     * Tests that the long checksum returned with an ExtensionModuleDescriptor
     * is the same number as one generated by scratch using the binary
     * data and a java.util.zip.CRC32 instance.
     */
    @Test public void testChecksum(){
        Checksum algorithm = new CRC32();
        algorithm.update(FakeData.TestJar1.data, 0, FakeData.TestJar1.data.length);
        long thisChecksum = algorithm.getValue();

        ExtensionModuleDescriptor desc = null;
        try{
            desc = manager.getSourceDescriptor( FakeData.TestJar1.SOURCE_NAME);
            assertNotNull(desc);
            long thatChecksum = desc.getChecksum();
            assertTrue(thisChecksum == thatChecksum);
        } catch (ExtensionModuleNotFoundException e){
            fail("Source descriptor for " +  FakeData.TestJar1.SOURCE_NAME + " not found: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MetaMatrixComponentException e){
            fail(e.getMessage());
        }
    }

}
