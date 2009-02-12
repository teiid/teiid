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

package com.metamatrix.common.actions;

import java.util.Collection;
import java.util.Iterator;

/**
 * This action definition specifies that the value of an object reference in the target should be changed
 * from the current object to a new object.
 */
public class ExchangeObject extends TargetedActionDefinition {

    public ExchangeObject(Object target, AttributeDefinition attribute, Object previousValue, Object newValue) {
        super(target,attribute, new Object[]{previousValue,newValue});
    }
    protected ExchangeObject(Object target, Integer attributeCode, Object[] args) {
        super(target,attributeCode, args );
    }
    protected ExchangeObject(ExchangeObject rhs) {
        super(rhs);
    }
    public synchronized Object getPreviousValue() {
        return this.getArguments()[0];
    }

    public synchronized Object getNewValue() {
        return this.getArguments()[1];
    }
    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer( getActionDescription() );
        for (int i=0; i!=2; ++i ) {
            Object newValue = null;
            if ( i==0 ) {
                sb.append("; new value = "); //$NON-NLS-1$
                newValue = getNewValue();
            } else {
                sb.append(", previous value = "); //$NON-NLS-1$
                newValue = getPreviousValue();
            }
            if ( newValue instanceof Collection ) {
                Iterator iter = ((Collection)newValue).iterator();
                sb.append('[');
                if ( iter.hasNext() ) {
                    sb.append( iter.next() );
                }
                while ( iter.hasNext() ) {
                    sb.append(',');
                    sb.append( iter.next() );
                }
                sb.append(']');
            } else {
                sb.append(newValue);
            }
        }
        return sb.toString();
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     */
    public synchronized Object clone() {
    	return new ExchangeObject(this);
    }
    /**
     * Obtain the definition of the action that undoes this action definition.  If a modification action with the
     * returned action definition is applied to the same target (when the state is such as that left by
     * the original modification action), the resulting target will be left in a state that is identical to the target
     * before either action were applied.
     * @return the action definition that undoes this action definition.
     */
    public synchronized ActionDefinition getUndoActionDefinition() {
        Object args[] = this.getArguments();
        return new ExchangeObject(this.getTarget(),this.getAttributeCode(),
                                  new Object[]{args[1],args[0]});
    }
    public synchronized String getActionDescription() {
        StringBuffer buff = new StringBuffer();
        Object target = this.getTarget();
        String targetClassName = target.getClass().getName();
        String attrDesc = this.getAttributeDescription();

        buff.append("Set "); //$NON-NLS-1$
        if ( attrDesc.length() > 0 ) {
            buff.append(this.getAttributeDescription() + " on "); //$NON-NLS-1$
        }
        buff.append(targetClassName.substring(targetClassName.lastIndexOf('.') + 1) +
                    " " + target.toString()); //$NON-NLS-1$
        return buff.toString();
    }
}


