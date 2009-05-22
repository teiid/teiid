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

package com.metamatrix.metadata.runtime.api;

import java.util.Properties;

import com.metamatrix.metadata.runtime.exception.VirtualDatabaseException;
import com.metamatrix.modeler.core.metadata.runtime.MetadataConstants;

/**
 * Interface that allows a {@link VirtualDatabaseLoader} implementation to
 * create runtime metadata objects for a virtual database.
 * @since 3.1
 */
public interface VirtualDatabaseFactory {
    
    //############################################################################################################################
    //# Constants: Default values for Element creation                                                                                                              #
    //############################################################################################################################

    public static final int DEFAULT_POSITION_IN_GROUP         = 0;
    public static final String DEFAULT_ALIAS                   = null;
    public static final String DEFAULT_DESC                    = null;
    public static final String DEFAULT_LABEL                   = null;
    public static final int DEFAULT_LENGTH                     = 0;
    public static final String DEFAULT_VALUE                   = null;
    public static final String DEFAULT_FORMAT                  = null;
    public static final String DEFAULT_MAX_RANGE               = null;
    public static final String DEFAULT_MIN_RANGE               = null;
    public static final short DEFAULT_NULL_TYPE               = MetadataConstants.NULL_TYPES.NULLABLE;
    public static final short DEFAULT_SEARCH_TYPE             = MetadataConstants.SEARCH_TYPES.SEARCHABLE;
    public static final int DEFAULT_RADIX                      = 0;
    public static final int DEFAULT_SCALE                      = 0;
    public static final int DEFAULT_PRECISION_LENGTH           = 0;
    public static final int DEFAULT_CHAR_OCTET_LENGTH          = 0;
    public static final boolean DEFAULT_IS_AUTO_INCREMENTED   = false;
    public static final boolean DEFAULT_IS_CASE_SENSITIVE     = false;
    public static final boolean DEFAULT_IS_CURRENCY           = false;
    public static final boolean DEFAULT_IS_FIXED_LENGTH       = false;
    public static final boolean DEFAULT_IS_SIGNED             = false;
    public static final boolean DEFAULT_SUPPORTS_SELECT       = true;
    public static final boolean DEFAULT_SUPPORTS_SET          = false;
    public static final boolean DEFAULT_SUPPORTS_SUBSCRIPTION = false;
    public static final boolean DEFAULT_SUPPORTS_UPDATE       = true;
    public static final boolean DEFAULT_IS_NULLABLE           = false;
    public static final Properties DEFAULT_PROPERTIES          = null;
  
    //############################################################################################################################
	//# Methods                                                                                                                  #
	//############################################################################################################################
    
    /**
     * @since 3.1
     */    
    public Key createAccessPattern(final GroupID groupId,
                                   final String name)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public Element createElement(final GroupID groupId,
                                 final String name,
                                 final int positionInGroup, 
                                 final String dataTypeName, 
                                 final String alias, 
                                 final String desc, 
                                 final String label, 
                                 final int length, 
                                 final String defaultValue, 
                                 final String format, 
                                 final String maxRange, 
                                 final String minRange, 
                                 final short nullType, 
                                 final short searchType, 
                                 final int radix, 
                                 final int scale, 
                                 final int precisionLength, 
                                 final int charOctetLength, 
                                 final boolean isAutoIncremented, 
                                 final boolean isCaseSensitive, 
                                 final boolean isCurrency, 
                                 final boolean isLengthFixed, 
                                 final boolean isSigned, 
                                 final boolean supportsSelect, 
                                 final boolean supportsSet, 
                                 final boolean supportsSubscription, 
                                 final boolean supportsUpdate, 
                                 final Properties props)
    throws VirtualDatabaseException;
    /**
     * @since 3.1
     */    
    public Key createUniqueKey(final GroupID groupId,
                               final String name,
                               final boolean isPrimary)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public Key createForeignKey(final GroupID groupId,
                                final String name, 
                                final Key primaryKey)
    throws VirtualDatabaseException;
    
    /**
     * Create a Key instance of the specified type
     * @param groupId the identifier for the parent Group
     * @param name the name of the key
     * @param keyType one of the types defined in 
     * <code>com.metamatrix.metadata.runtime.api.MetadataConstants.KEY_TYPES</code>
     * @since 3.1
     */    
    public Key createKey(final GroupID groupId,
                         final String name, 
                         final short keyType)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void addElementsInKey(final Key key,
                                 final Element[] elementsInKey)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public VirtualDatabaseLoaderProperties createLoaderProperties()
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public Group createGroup(final String fullnameWithinModel)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setDescription(final Group group, final String description)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public Procedure createProcedure();
    
    /**
     * @since 3.1
     */    
    public String getDelimiter();
    
    /**
     * @since 3.1
     */    
    public Element createElement(final GroupID groupId, final String name, final String dataTypeName)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void setPositionInGroup(final Element element, final int positionInGroup)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void setDataTypeName(final Element element, final String dataTypeName)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void setAlias(final Element element, final String alias)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setDescription(final Element element, final String description)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setLabel(final Element element, final String label)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setLength(final Element element, final int length)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void setDefaultValue(final Element element, final String defaultValue)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void setFormat(final Element element, final String format)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void setMaxRange(final Element element, final String maxRange)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void setMinRange(final Element element, final String minRange)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setNullType(final Element element, final short nullType)
    throws VirtualDatabaseException;
    
    /**
     * @since 3.1
     */    
    public void setSearchType(final Element element, final short searchType)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setRadix(final Element element, final int radix)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setScale(final Element element, final int scale)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setPrecisionLength(final Element element, final int precisionLength)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setCharOctetLength(final Element element, final int charOctetLength)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setIsAutoIncremented(final Element element, final boolean isAutoIncremented)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setIsCaseSensitive(final Element element, final boolean isCaseSensitive)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setIsCurrency(final Element element, final boolean isCurrency)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setIsLengthFixed(final Element element, final boolean isLengthFixed)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setIsSigned(final Element element, final boolean isSigned)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setSupportsSelect(final Element element, final boolean supportsSelect)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setSupportsSet(final Element element, final boolean supportsSet)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setSupportsSubscription(final Element element, final boolean supportsSubscription)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setSupportsUpdate(final Element element, final boolean supportsUpdate)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setProperties(final Element element, final Properties props)
    throws VirtualDatabaseException;

    /**
     * @since 3.1
     */    
    public void setAlias(final Group group, final String alias)
    throws VirtualDatabaseException;
    
}
