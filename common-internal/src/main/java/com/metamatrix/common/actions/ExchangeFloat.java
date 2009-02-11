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

package com.metamatrix.common.actions;

public class ExchangeFloat extends ExchangePrimitive {
    private float previousValue;
    private float newValue;
    public ExchangeFloat(Object target, AttributeDefinition attribute, float previousValue, float newValue) {
        super(target,attribute);
    	this.previousValue = previousValue;
    	this.newValue = newValue;
    }
    private ExchangeFloat(Object target, Integer attributeCode, float previousValue, float newValue) {
        super(target,attributeCode);
    	this.previousValue = previousValue;
    	this.newValue = newValue;
    }
    private ExchangeFloat(ExchangeFloat rhs) {
        super(rhs);
    	this.previousValue = rhs.previousValue;
    	this.newValue = rhs.newValue;
    }

    public synchronized float getPreviousValue(){
            return previousValue;
        }
    public synchronized float getNewValue(){
            return newValue;
        }
    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString() {
        return getActionDescription() + "; new value = " + newValue + ", previous value = " + previousValue; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     */
    public synchronized Object clone() {
    	return new ExchangeFloat(this);
    }
    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public synchronized boolean equals(Object obj) {
        // Check if instances are identical ...
        if ( this == obj ) return true;

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if ( obj instanceof ExchangeFloat ) {
        	ExchangeFloat that = (ExchangeFloat) obj;
        	return ( this.getNewValue() == that.getNewValue() && this.getPreviousValue() == that.getPreviousValue() );
        }

        // Otherwise not comparable ...
        return false;
    }
    /**
     * Obtain the definition of the action that undoes this action definition.  If a modification action with the
     * returned action definition is applied to the same target (when the state is such as that left by
     * the original modification action), the resulting target will be left in a state that is identical to the target
     * before either action were applied.
     * @return the action definition that undoes this action definition.
     */
    public synchronized ActionDefinition getUndoActionDefinition() {
        return new ExchangeFloat(this.getTarget(),this.getAttributeCode(),this.newValue, this.previousValue);
    }
}

