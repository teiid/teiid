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

package org.teiid.core.id;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.teiid.core.CorePlugin;


/**
 * IDGenerator
 */
public class IDGenerator {
    
    private static final IDGenerator INSTANCE = new IDGenerator();

    /**
     * Obtain the shared instance of this class.
     * @return the statically shared instance of this class.
     */
    public static IDGenerator getInstance() {
        return INSTANCE;
    }

    /** ObjectIDFactory instances keyed by protocol */
    private final Map factories;
    private final Set protocols;
    
    private ObjectIDFactory defaultFactory;

    public IDGenerator() {
        this.factories = new HashMap();
        this.protocols = new HashSet();
        
        // Initialize default factory ...
        final ObjectIDFactory newDefaultFactory = new UUIDFactory();
        addFactory( newDefaultFactory );
        if ( !this.hasDefaultFactory() ) {
            this.setDefaultFactory(newDefaultFactory);
        }
    }
    
    /**
     * Method that creates and adds to this generator all the built-in factories, and if there is no default
     * factory, set the default factory to the {@link UUIDFactory}. 
     * This method may be called multiple times without side effect.
     */
    public void addBuiltInFactories() {
        // Add the UUID factory as the default ...
        final ObjectIDFactory newDefaultFactory = new UUIDFactory();
        addFactory( newDefaultFactory );
        if ( !this.hasDefaultFactory() ) {
            this.setDefaultFactory(newDefaultFactory);
        }
        addFactory( new IntegerIDFactory() );
        addFactory( new LongIDFactory() );
        addFactory( new StringIDFactory() );
    }
    
    /**
     * Supply to this generator a new factory for a type of {@link ObjectID}.
     * This method has no effect if the factory is null, or if this generator already knows
     * about the factory.
     * @param factory the new factory
     */
    public void addFactory( final ObjectIDFactory factory ) {
        if ( factory == null ) {
            return;
        }
        final String protocol = factory.getProtocol();
        
        // See if the factory is already known ...
        if ( !this.factories.containsKey(protocol) ) {
            this.factories.put(protocol,factory);
        }
        this.protocols.add(protocol);
    }
    
    /**
     * Remove a factory from this generator.
     * This method has no effect if the supplied protocol doesn't match the protocol of any descriptor
     * known to this generator.
     * @param protocol the protocol for which the factory is to be removed
     * @return whether a factory was found and removed for the supplied protocol
     */
    public boolean removeFactory(String protocol) {
        if ( protocol == null ) {
            return false;
        }
        final Object previous = this.factories.remove(protocol);
        if ( previous != null ) {
            this.protocols.remove(protocol);
            return true;
        }
        return false;
    }

    /**
     * Method to obtain the collection of {@link ObjectIDFactory} instances that each describe
     * one of the types of {@link ObjectID}s that are available to this generator.
     * @return the collection of {@link ObjectIDFactory} instances.
     */
    public Collection getFactories() {
        // Currently, we get the collection of values from the 'factories' map.  This is okay,
        // because although not terribly efficient, this method is not intended to be called very
        // frequently
        return factories.values();
    }
    
    /**
     * Method to obtain the collection of {@link ObjectIDFactory} instances that each describe
     * one of the types of {@link ObjectID}s that are available to this generator.
     * @return the collection of {@link ObjectIDFactory} instances.
     */
    public ObjectIDFactory getFactory( final String protocol ) {
        return (ObjectIDFactory) this.factories.get(protocol);
    }
    
    /**
     * Method to obtain the set of {@link java.lang.String String} protocols.  This is a utility that merely obtains the
     * protocols from the factories.
     * @return the Set of String protocols; never null
     */
    public Set getProtocols() {
        return factories.keySet();
    }
    
    /**
     * Return whether there is a factory that is used by default for the {@link #create()} method is invoked.
     * @return true if there is a default factory, or false otherwise
     */
    public boolean hasDefaultFactory() {
        return defaultFactory != null;
    }

    /**
     * Get the factory that is used by default for the {@link #create()} method is invoked.
     * @return the default factory, or null if there is no default factory
     */
    public ObjectIDFactory getDefaultFactory() {
        return defaultFactory;
    }

    /**
     * Set the factory that should be used by default for the {@link #create()} method is invoked.
     * @param factory the factory that should be used by default; may be null if no default is to be allowed.
     */
    public void setDefaultFactory(final ObjectIDFactory factory) {
        defaultFactory = factory;
    }

    /**
     * Set the factory that should be used by default for the {@link #create()} method is invoked.
     * @param protocol the protocol for factory that should be used by default; may be null if no default
     * is to be allowed.
     */
    public void setDefaultFactory(final String protocol) {
        final ObjectIDFactory factory = getFactory(protocol);   // null if protocol doesn't match
        defaultFactory = factory;
    }

    /**
     * Create a new {@link ObjectID} using the default factory
     * @return the new ObjectID
     */
    public ObjectID create() {
        if ( this.defaultFactory != null ) {
            return this.defaultFactory.create();
        }
        // Invalid protocol
        throw new IllegalArgumentException(CorePlugin.Util.getString("IDGenerator.No_default_id_factory_has_been_defined")); //$NON-NLS-1$
    }
    
    /**
     * Create a new {@link ObjectID} for the type specified by the protocol
     * @param protocol the protocol of the type of {@link ObjectID} to be created; may not be null
     * @return the new ObjectID
     */
    public ObjectID create( final String protocol ) {
        if (protocol == null) {
            final String msg = CorePlugin.Util.getString("IDGenerator.The_protocol_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }
        // Get the factory ...
        final ObjectIDFactory factory = (ObjectIDFactory) this.factories.get(protocol);
        if ( factory != null ) {
            // Create the new ObjectID ...
            return factory.create();
        }
        // Invalid protocol
        throw new IllegalArgumentException(CorePlugin.Util.getString("IDGenerator.The_specified_ObjectID_protocol___8",protocol)); //$NON-NLS-1$ 
    }
    
    /**
     * Convenience method for obtaining the stringified form of an ObjectID.
     * @param id
     * @return
     */
    public String toString( final ObjectID id ) {
        return id.toString();
    }
    
    /**
     * Convenience method for obtaining the stringified form of an ObjectID.
     * @param id
     * @return
     */
    public String toString( final ObjectID id, final char delim ) {
        return id.toString(delim);
    }
    
    /**
     * Attempt to convert the specified string to the appropriate ObjectID instance.
     * @param id the stringified id of the form <code>protocol:value</code>, where
     * <code>protocol</code> defines the protocol of the ID, and <code>value</code
     * contains the global identifier value; may never be null
     * @return the ObjectID instance for the stringified ID
     * @throws InvalidIDException if the specified string does not contain a valid ObjectID
     * or if the protocol is unknown
     */
    public ObjectID stringToObject(String id) throws InvalidIDException {
        if (id == null) {
            final String msg = CorePlugin.Util.getString("IDGenerator.The_stringified_ID_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }

        // Parse the string
        final ParsedObjectID parsedID = ParsedObjectID.parsedStringifiedObjectID(id,this.protocols);

        // Find the appropriate factory and parse the id ...
        final ObjectIDFactory factory = (ObjectIDFactory) this.factories.get(parsedID.getProtocol());
        ObjectID result = null;
        if ( factory != null ) {
            result = factory.stringWithoutProtocolToObject(parsedID.getRemainder());
        }
        if ( result == null ) {
            throw new InvalidIDException(CorePlugin.Util.getString("IDGenerator.The_stringified_ObjectID_has_an_unknown_protocol___16") + parsedID.getProtocol()); //$NON-NLS-1$
        }
        return result;
    }
    
    /**
     * Attempt to convert the specified string to the appropriate ObjectID instance.
     * @param id the stringified id of the form <code>protocol:value</code>, where
     * <code>protocol</code> defines the protocol of the ID, and <code>value</code
     * contains the global identifier value; may never be null
     * @return the ObjectID instance for the stringified ID
     * @throws InvalidIDException if the specified string does not contain a valid ObjectID
     * or if the protocol is unknown
     */
    public ObjectID stringToObject(String id, String protocol) throws InvalidIDException {
        if (id == null) {
            final String msg = CorePlugin.Util.getString("IDGenerator.The_stringified_ID_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }

        // Parse the string
        final ParsedObjectID parsedID = ParsedObjectID.parsedStringifiedObjectID(id,protocol);

        // Find the appropriate factory and parse the id ...
        final ObjectIDFactory factory = (ObjectIDFactory) this.factories.get(parsedID.getProtocol());
        ObjectID result = null;
        if ( factory != null ) {
            result = factory.stringWithoutProtocolToObject(parsedID.getRemainder());
        }
        if ( result == null ) {
            throw new InvalidIDException(CorePlugin.Util.getString("IDGenerator.The_stringified_ObjectID_has_an_unknown_protocol___16") + parsedID.getProtocol()); //$NON-NLS-1$
        }
        return result;
    }
    
    /**
     * Attempt to convert the specified string to the appropriate ObjectID instance.
     * @param id the stringified id of the form <code>protocol:value</code>, where
     * <code>protocol</code> defines the protocol of the ID, and <code>value</code
     * contains the global identifier value; may never be null
     * @return the ObjectID instance for the stringified ID
     * @throws InvalidIDException if the specified string does not contain a valid ObjectID
     * or if the protocol is unknown
     */
    public ObjectID stringToObject(String id, char delim) throws InvalidIDException {
        if (id == null) {
            final String msg = CorePlugin.Util.getString("IDGenerator.The_stringified_ID_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }

        // Parse the string
        final ParsedObjectID parsedID = ParsedObjectID.parsedStringifiedObjectID(id,delim);

        // Find the appropriate factory and parse the id ...
        final ObjectIDFactory factory = (ObjectIDFactory) this.factories.get(parsedID.getProtocol());
        ObjectID result = null;
        if ( factory != null ) {
            result = factory.stringWithoutProtocolToObject(parsedID.getRemainder());
        }
        if ( result == null ) {
            throw new InvalidIDException(CorePlugin.Util.getString("IDGenerator.The_stringified_ObjectID_has_an_unknown_protocol___16") + parsedID.getProtocol()); //$NON-NLS-1$
        }
        return result;
    }
    
}

