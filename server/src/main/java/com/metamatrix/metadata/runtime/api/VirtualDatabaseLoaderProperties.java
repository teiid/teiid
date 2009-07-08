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


/**
 * Interface that represents the properties associated with a {@link VirtualDatabaseLoader}
 * implementation.
 * @since 3.1
 */
public interface VirtualDatabaseLoaderProperties {
    
    //############################################################################################################################
    //# Constants: Default property values                                                                                                           #
    //############################################################################################################################

    public static final boolean DEFAULT_SUPPORTS_JOIN      = true;
    public static final boolean DEFAULT_SUPPORTS_AND       = true;
    public static final boolean DEFAULT_SUPPORTS_OR        = true;
    public static final boolean DEFAULT_SUPPORTS_SET       = true;
    public static final boolean DEFAULT_SUPPORTS_WHERE     = true;
    public static final boolean DEFAULT_SUPPORTS_SELECT    = true;
    public static final boolean DEFAULT_SUPPORTS_ORDER_BY  = true;
    public static final boolean DEFAULT_SUPPORTS_GROUP_BY  = true;
    public static final boolean DEFAULT_SUPPORTS_TRANS     = true;
    public static final boolean DEFAULT_SUPPORTS_DISTINCT  = true;
    public static final boolean DEFAULT_SUPPORTS_OUTERJOIN = false;
    public static final boolean DEFAULT_SUPPORTS_SUBSCR    = false;
    public static final boolean DEFAULT_SUPPORTS_AGGREGATE = true;
    public static final boolean DEFAULT_SUPPORTS_SINGLE_GROUP_SELECT = true;
    public static final boolean DEFAULT_SUPPORTS_LEAF_SELECT         = true;
    public static final boolean DEFAULT_SUPPORTS_BLACK_BOX_JOIN      = true;

    
    boolean supportsAnd();
    void setSupportsAnd(boolean sup_AND);
    
    boolean supportsJoin();
    void setSupportsJoin(boolean sup_JOIN);
    
    boolean supportsOr();
    void setSupportsOr(boolean sup_OR);
    
    boolean supportsSet();
    void setSupportsSet(boolean sup_SET);
    
    boolean supportsWhereAll();
    void setSupportsWhereAll(boolean sup_WHERE_ALL);
    
    boolean supportsSelectAll();
    void setSupportsSelectAll(boolean sup_SELECT_ALL);
    
    boolean supportsOrderBy();
    void setSupportsOrderBy(boolean sup_ORDER_BY);
    
    boolean supportsGroupBy();
    void setSupportsGroupBy(boolean sup_GROUP_BY);

    boolean supportsTransaction();
    void setSupportsTransaction(boolean sup_TRANS);
    
    boolean supportsDistinct();
    void setSupportsDistinct(boolean sup_DISTINCT);
    
    boolean supportsOuterJoin();
    void setSupportsOuterJoin(boolean sup_OUTERJOIN);
    
    boolean supportsSubscription();
    void setSupportsSubscription(boolean sup_SUP);
    
    boolean supportsAggregate();
    void setSupportsAggregate(boolean sup_AGGREGATE);
    
    boolean supportsSingleGroupSelect();
    void setSupportsSingleGroupSelect(boolean sup_SINGLE_GROUP_SELECT);
    
    boolean supportsLeafSelect();
    void setSupportsLeafSelect(boolean sup_LEAF_SELECT);
    
    boolean supportsBlackBoxJoin();
    void setSupportsBlackBoxJoin(boolean sup_BLACK_BOX_JOIN);

}
