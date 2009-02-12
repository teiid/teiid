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

/**
 * This action definition specifies that an object is to be removed from the target.
 */
public class RemoveObject extends TargetedActionDefinition  {

    public RemoveObject(Object target, AttributeDefinition attribute, Object object) {
        super(target,attribute, new Object[] {object} );
    }

    RemoveObject(Object target, Integer attributeCode, Object[] args) {
        super(target,attributeCode, args );
    }

    RemoveObject(RemoveObject rhs) {
        super( rhs );
    }

    public synchronized String getActionDescription() {
        StringBuffer buff = new StringBuffer();
        Object target = this.getTarget();
        String targetClassName = target.getClass().getName();
        String attrDesc = this.getAttributeDescription();

        buff.append("Remove "); //$NON-NLS-1$
        if ( attrDesc.length() > 0 ) {
            buff.append(this.getAttributeDescription() + " from "); //$NON-NLS-1$
        }
        buff.append(targetClassName.substring(targetClassName.lastIndexOf('.') + 1) +
                    " " + target.toString()); //$NON-NLS-1$
        return buff.toString();
    }

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString() {
        String removed = null;
        if ( this.getArguments().length > 0 ) {
            removed = this.getArguments()[0].toString();
        }
        return getActionDescription() + "; remove " + removed; //$NON-NLS-1$
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     */
    public synchronized Object clone() {
        return new RemoveObject(this);
    }

    /**
     * Obtain the definition of the action that undoes this action definition.  If a modification action with the
     * returned action definition is applied to the same target (when the state is such as that left by
     * the original modification action), the resulting target will be left in a state that is identical to the target
     * before either action were applied.
     * @return the action definition that undoes this action definition.
     */
    public synchronized ActionDefinition getUndoActionDefinition() {
        return new AddObject(this.getTarget(),this.getAttributeCode(),this.getArguments());
    }
}




