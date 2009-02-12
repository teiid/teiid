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

package com.metamatrix.core.id;

import java.util.Collection;
import java.util.Iterator;

import junit.framework.TestCase;

/**
 * TestIDGenerator
 */
public class TestIDGenerator extends TestCase {

    private static final String DEFAULT_FACTORY_PROTOCOL = UUID.PROTOCOL;

    private IDGenerator generator;
    private IDGenerator generatorWithDefault;
    private IDGenerator generatorWithBuiltIns;

    /**
     * Constructor for TestIDGenerator.
     * @param name
     */
    public TestIDGenerator(String name) {
        super(name);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        generator = new IDGenerator();
        generatorWithDefault = new IDGenerator();
        generatorWithDefault.addFactory( new IntegerIDFactory() );
        generatorWithDefault.setDefaultFactory( IntegerID.PROTOCOL );

        generatorWithBuiltIns = new IDGenerator();
        generatorWithBuiltIns.addBuiltInFactories();
    }

    /*
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        generator = null;
        generatorWithDefault = null;
        generatorWithBuiltIns = null;
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    public void helpTestDefaultFactoryAccessors( final IDGenerator gen ) {
        if ( gen.hasDefaultFactory() != (gen.getDefaultFactory() != null) ){
            fail("The hasDefaultFactory returned " + gen.hasDefaultFactory() + //$NON-NLS-1$
                              " but getDefaultFactory returned " + gen.getDefaultFactory() ); //$NON-NLS-1$
        }
    }

    public void helpTestGetFactory( final IDGenerator gen, final String protocol, final boolean shouldSucceed ) {
        final ObjectIDFactory factory = gen.getFactory(protocol);
        if ( factory == null && shouldSucceed ) {
            fail("Unable to find factory for protocol " + protocol); //$NON-NLS-1$
        }
        if ( factory != null && !shouldSucceed ) {
            fail("Unexpectedly found factory for protocol " + protocol); //$NON-NLS-1$
        }
    }

    public void helpTestGetDefaultFactory( final IDGenerator gen, final String protocolOrNull ) {
        final ObjectIDFactory factory = gen.getDefaultFactory();
        if ( factory == null && protocolOrNull != null ) {
            fail("Unable to find default factory"); //$NON-NLS-1$
        }
        if ( factory != null && protocolOrNull == null ) {
            fail("Unexpectedly found default factory"); //$NON-NLS-1$
        }
        if ( factory != null && !factory.getProtocol().equals(protocolOrNull) ) {
            fail("Default factory had protocol " + factory.getProtocol() + //$NON-NLS-1$
                              " but should have been " + protocolOrNull); //$NON-NLS-1$
        }
    }

    public void helpTestRemoveFactory( final IDGenerator gen, final String protocol, final boolean shouldSucceed ) {
        // Determine if the factory is really there ...
        boolean shouldFind = false;
        final Collection factories = gen.getFactories();
        final Iterator iter = factories.iterator();
        while (iter.hasNext()) {
            final ObjectIDFactory factory = (ObjectIDFactory)iter.next();
            if ( factory.getProtocol().equals(protocol) ) {
                shouldFind = true;
            }
        }

        final boolean actuallyRemoved = gen.removeFactory(protocol);
        if ( shouldFind != actuallyRemoved ) {
            if ( shouldFind ) {
                fail("Found factory via iteration but not by removal"); //$NON-NLS-1$
            } else {
                fail("Didn't find factory via iteration but one was removed"); //$NON-NLS-1$
            }
        }
        if ( !actuallyRemoved && shouldSucceed ) {
            fail("Unable to remove factory for protocol " + protocol); //$NON-NLS-1$
        }
        if ( actuallyRemoved && !shouldSucceed ) {
            fail("Unexpectedly removed factory for protocol " + protocol); //$NON-NLS-1$
        }
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    public void testDefaultFactoryUponConstruction() {
        if ( generator.getDefaultFactory() == null ){
            fail("The IDGenerator should have a default factory upon construction"); //$NON-NLS-1$
        }
    }

    public void testDefaultFactoryAccessorsUponConstruction() {
        helpTestDefaultFactoryAccessors(generator);
    }

    public void testDefaultFactoryAccessorsAfterSettingDefault() {
        helpTestDefaultFactoryAccessors(generatorWithDefault);
    }

    public void testBuiltInFactoriesUponConstruction() {
        final Collection factories = generator.getFactories();
        if ( factories.size() != 1 ) {
            fail("Found " + factories.size() + " factories, but expected 1"); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void testBuiltInFactories() {
        final Collection factories = generatorWithBuiltIns.getFactories();
        if ( factories.size() != 4 ) {
            fail("Found " + factories.size() + " factories, but expected 4"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        generatorWithBuiltIns.addBuiltInFactories();
        generatorWithBuiltIns.addBuiltInFactories();
        final Collection factories2 = generatorWithBuiltIns.getFactories();
        if ( factories2.size() != 4 ) {
            fail("Found " + factories2.size() + " factories, but expected 4"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        helpTestGetFactory(generatorWithBuiltIns,IntegerID.PROTOCOL,true);
        helpTestGetFactory(generatorWithBuiltIns,StringID.PROTOCOL,true);
        helpTestGetFactory(generatorWithBuiltIns,LongID.PROTOCOL,true);
        helpTestGetFactory(generatorWithBuiltIns,UUID.PROTOCOL,true);
        helpTestGetFactory(generatorWithBuiltIns,"bogus id",false); //$NON-NLS-1$
    }

    public void testCreatingIDWithoutDefaultFactory() {
        try {
            generator.create();
        } catch ( IllegalArgumentException e ) {
            fail("Should have been able to create an ObjectID using a generator (there is a default factory)"); //$NON-NLS-1$
        }
    }

    public void testCreatingIDWithDefaultFactory() {
        generatorWithDefault.create();
    }

    public void testSerializingIDWithStandardDelim() throws InvalidIDException {
        final ObjectID guid = generatorWithDefault.create();
        final String str1 = generatorWithDefault.toString(guid);
        final String str2 = generatorWithDefault.toString(guid,ObjectID.DELIMITER);
        final String str3 = generatorWithDefault.toString(guid,'/');
        assertEquals(str1, str2);
        final ObjectID guid1 = generatorWithDefault.stringToObject(str1);
        final ObjectID guid2 = generatorWithDefault.stringToObject(str2);
        final ObjectID guid3 = generatorWithDefault.stringToObject(str3);
        final ObjectID guid1a = generatorWithDefault.stringToObject(str1,ObjectID.DELIMITER);
        final ObjectID guid2a = generatorWithDefault.stringToObject(str2,ObjectID.DELIMITER);
        final ObjectID guid3a = generatorWithDefault.stringToObject(str3,'/');
        assertEquals(guid, guid1);
		assertEquals(guid, guid2);
		assertEquals(guid, guid3);
		assertEquals(guid, guid1a);
		assertEquals(guid, guid2a);
		assertEquals(guid, guid3a);
    }

    public void testCreatingIDWithDefaultBuiltInFactory() {
        generatorWithBuiltIns.create();
    }

    public void testCreatingIDWithDefaultBuiltInFactoryAndSpecifyingProtocol() {
        generatorWithBuiltIns.create(StringID.PROTOCOL);
    }

    public void testCreatingIDWithDefaultBuiltInFactoryAndSpecifyingInvalidProtocol() {
        try {
            generator.create("This is a protocol that won't match"); //$NON-NLS-1$
            fail("Should have failed to create an ObjectID using an unknown protocol"); //$NON-NLS-1$
        } catch ( IllegalArgumentException e ) {
            // expected
        }
    }

    public void testCreatingIDWithDefaultBuiltInFactoryAndSpecifyingNullProtocol() {
        try {
            generator.create(null);
            fail("Should have failed to create an ObjectID using a null protocol"); //$NON-NLS-1$
        } catch ( IllegalArgumentException e ) {
            // expected
        }
    }

    public void testCreatingIDWithDefaultBuiltInFactoryAndSpecifyingZeroLengthProtocol() {
        try {
            generator.create(""); //$NON-NLS-1$
            fail("Should have failed to create an ObjectID using a zero-length protocol"); //$NON-NLS-1$
        } catch ( IllegalArgumentException e ) {
            // expected
        }
    }

    public void testRemovingFactories() {
        final Collection factories = generatorWithBuiltIns.getFactories();
        if ( factories.size() != 4 ) {
            fail("Found " + factories.size() + " factories, but expected 4"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        helpTestRemoveFactory(generatorWithBuiltIns,IntegerID.PROTOCOL,true);
        helpTestRemoveFactory(generatorWithBuiltIns,StringID.PROTOCOL,true);
        helpTestRemoveFactory(generatorWithBuiltIns,LongID.PROTOCOL,true);
        helpTestRemoveFactory(generatorWithBuiltIns,IntegerID.PROTOCOL,false);  // already removed
        helpTestRemoveFactory(generatorWithBuiltIns,UUID.PROTOCOL,true);
        helpTestRemoveFactory(generatorWithBuiltIns,"bogus id",false); //$NON-NLS-1$
    }

    public void testSettingDefaultFactoryByProtocol() {
        final IDGenerator gen = new IDGenerator();
        final ObjectIDFactory factory = new IntegerIDFactory();
        helpTestGetDefaultFactory(gen,DEFAULT_FACTORY_PROTOCOL);
        gen.addFactory(factory);
        helpTestGetDefaultFactory(gen,DEFAULT_FACTORY_PROTOCOL);
        gen.setDefaultFactory(IntegerID.PROTOCOL);
        helpTestGetDefaultFactory(gen,IntegerID.PROTOCOL);
    }

    public void testSettingDefaultFactoryByReference() {
        final IDGenerator gen = new IDGenerator();
        final ObjectIDFactory factory = new IntegerIDFactory();
        helpTestGetDefaultFactory(gen,DEFAULT_FACTORY_PROTOCOL);
        gen.addFactory(factory);
        helpTestGetDefaultFactory(gen,DEFAULT_FACTORY_PROTOCOL);
        gen.setDefaultFactory(factory);
        helpTestGetDefaultFactory(gen,IntegerID.PROTOCOL);
    }

}
