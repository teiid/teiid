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

package com.metamatrix.metadata.runtime.model;

import com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties;

/**
 * @author dfuglsang
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class BasicVirtualDatabaseLoaderProperties implements VirtualDatabaseLoaderProperties {
    
    private boolean sup_JOIN      = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_JOIN;
    private boolean sup_AND       = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_AND;
    private boolean sup_OR        = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_OR;
    private boolean sup_SET       = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_SET;
    private boolean sup_WHERE     = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_WHERE;
    private boolean sup_SELECT    = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_SELECT;
    private boolean sup_ORDER_BY  = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_ORDER_BY;
    private boolean sup_GROUP_BY  = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_GROUP_BY;
    private boolean sup_TRANS     = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_TRANS;
    private boolean sup_DISTINCT  = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_DISTINCT;
    private boolean sup_OUTERJOIN = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_OUTERJOIN;
    private boolean sup_SUBSCR    = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_SUBSCR;
    private boolean sup_AGGREGATE = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_AGGREGATE;
    private boolean sup_SINGLE_GROUP_SELECT = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_SINGLE_GROUP_SELECT;
    private boolean sup_LEAF_SELECT         = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_LEAF_SELECT;
    private boolean sup_BLACK_BOX_JOIN      = VirtualDatabaseLoaderProperties.DEFAULT_SUPPORTS_BLACK_BOX_JOIN;

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsAggregate(boolean)
     */
    public void setSupportsAggregate(boolean sup_AGGREGATE) {
        this.sup_AGGREGATE = sup_AGGREGATE;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsAnd(boolean)
     */
    public void setSupportsAnd(boolean sup_AND) {
        this.sup_AND = sup_AND;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsDistinct(boolean)
     */
    public void setSupportsDistinct(boolean sup_DISTINCT) {
        this.sup_DISTINCT = sup_DISTINCT;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsGroupBy(boolean)
     */
    public void setSupportsGroupBy(boolean sup_GROUP_BY) {
        this.sup_GROUP_BY = sup_GROUP_BY;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsJoin(boolean)
     */
    public void setSupportsJoin(boolean sup_JOIN) {
        this.sup_JOIN = sup_JOIN;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsOr(boolean)
     */
    public void setSupportsOr(boolean sup_OR) {
        this.sup_OR = sup_OR;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsOrderBy(boolean)
     */
    public void setSupportsOrderBy(boolean sup_ORDER_BY) {
        this.sup_ORDER_BY = sup_ORDER_BY;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsOuterJoin(boolean)
     */
    public void setSupportsOuterJoin(boolean sup_OUTERJOIN) {
        this.sup_OUTERJOIN = sup_OUTERJOIN;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsSelectAll(boolean)
     */
    public void setSupportsSelectAll(boolean sup_SELECT_ALL) {
        this.sup_SELECT = sup_SELECT_ALL;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsSet(boolean)
     */
    public void setSupportsSet(boolean sup_SET) {
        this.sup_SET = sup_SET;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsSubscription(boolean)
     */
    public void setSupportsSubscription(boolean sup_SUP) {
        this.sup_SUBSCR = sup_SUP;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsTransaction(boolean)
     */
    public void setSupportsTransaction(boolean sup_TRANS) {
        this.sup_TRANS = sup_TRANS;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsWhereAll(boolean)
     */
    public void setSupportsWhereAll(boolean sup_WHERE_ALL) {
        this.sup_WHERE = sup_WHERE_ALL;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsBlackBoxJoin(boolean)
     */
    public void setSupportsBlackBoxJoin(boolean sup_BLACK_BOX_JOIN) {
        this.sup_BLACK_BOX_JOIN = sup_BLACK_BOX_JOIN;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsLeafSelect(boolean)
     */
    public void setSupportsLeafSelect(boolean sup_LEAF_SELECT) {
        this.sup_LEAF_SELECT = sup_LEAF_SELECT;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#setSupportsSingleGroupSelect(boolean)
     */
    public void setSupportsSingleGroupSelect(boolean sup_SINGLE_GROUP_SELECT) {
        this.sup_SINGLE_GROUP_SELECT = sup_SINGLE_GROUP_SELECT;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsAggregate()
     */
    public boolean supportsAggregate() {
        return this.sup_AGGREGATE;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsAnd()
     */
    public boolean supportsAnd() {
        return this.sup_AND;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsDistinct()
     */
    public boolean supportsDistinct() {
        return this.sup_DISTINCT;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsGroupBy()
     */
    public boolean supportsGroupBy() {
        return this.sup_GROUP_BY;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsJoin()
     */
    public boolean supportsJoin() {
        return this.sup_JOIN;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsOr()
     */
    public boolean supportsOr() {
        return this.sup_OR;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsOrderBy()
     */
    public boolean supportsOrderBy() {
        return this.sup_ORDER_BY;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsOuterJoin()
     */
    public boolean supportsOuterJoin() {
        return this.sup_OUTERJOIN;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsSelectAll()
     */
    public boolean supportsSelectAll() {
        return this.sup_SELECT;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsSet()
     */
    public boolean supportsSet() {
        return this.sup_SET;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsSubscription()
     */
    public boolean supportsSubscription() {
        return this.sup_SUBSCR;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsTransaction()
     */
    public boolean supportsTransaction() {
        return this.sup_TRANS;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsWhereAll()
     */
    public boolean supportsWhereAll() {
        return this.sup_WHERE;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsBlackBoxJoin()
     */
    public boolean supportsBlackBoxJoin() {
        return this.sup_BLACK_BOX_JOIN;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsLeafSelect()
     */
    public boolean supportsLeafSelect() {
        return this.sup_LEAF_SELECT;
    }

    /**
     * @see com.metamatrix.metadata.runtime.api.VirtualDatabaseLoaderProperties#supportsSingleGroupSelect()
     */
    public boolean supportsSingleGroupSelect() {
        return this.sup_SINGLE_GROUP_SELECT;
    }

}
