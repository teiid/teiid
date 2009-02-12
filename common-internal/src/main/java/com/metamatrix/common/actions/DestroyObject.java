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
 * This action definition specifies that an object is to be added to the target.
 */
public class DestroyObject extends TargetedActionDefinition  {

    public DestroyObject(Object target) {
        super(target,null);
    }

    public DestroyObject(Object target, Object object) {
        super(target,(AttributeDefinition)null,new Object[]{object});
    }

    DestroyObject(DestroyObject rhs) {
        super(rhs);
    }

    public synchronized String getActionDescription(){
        Object target = this.getArguments()[0];
        String targetClassName = target.getClass().getName();

        return "Destroy " //$NON-NLS-1$
               + targetClassName.substring(targetClassName.lastIndexOf('.') + 1) +
               " " + target.toString(); //$NON-NLS-1$
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     */
    public synchronized Object clone() {
        return new DestroyObject(this);
    }

    /**
     * Obtain the definition of the action that undoes this action definition.  If a modification action with the
     * returned action definition is applied to the same target (when the state is such as that left by
     * the original modification action), the resulting target will be left in a state that is identical to the target
     * before either action were applied.
     * @return the action definition that undoes this action definition.
     */
    public synchronized ActionDefinition getUndoActionDefinition() {
        return new CreateObject(this.getTarget(),this.getArguments()[0]);
    }
}



