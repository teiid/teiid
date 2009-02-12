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



abstract public class ExchangePrimitive extends TargetedActionDefinition {

    /**
     * Create a new instance of an action definition by specifying the target.
     * @param target the object (or identifier for the object) that is the target of this action.
     * @param arguments the objects that define the arguments for this action.
     * @throws IllegalArgumentException if the target is null or not serializable.
     */
    public ExchangePrimitive(Object target, AttributeDefinition attribute ) {
        super(target,attribute,null);
    }

    protected ExchangePrimitive(Object target, Integer code ) {
        super(target,code,null);
    }

    protected ExchangePrimitive(ExchangePrimitive rhs ) {
        super(rhs);
    }

    /**
     * Get the description (i.e., verb) for this type of action.
     * @return the string description for this type of action.
     */
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





