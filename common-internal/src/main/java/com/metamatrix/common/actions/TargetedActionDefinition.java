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

import java.io.Serializable;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

abstract public class TargetedActionDefinition extends ActionDefinition {

    /**
     * Create a new instance of an action definition by specifying the target.
     * @param target the object (or identifier for the object) that is the target of this action.
     * @param arguments the objects that define the arguments for this action.
     * @throws IllegalArgumentException if the target is null or not serializable.
     */
    public TargetedActionDefinition(Object target, AttributeDefinition attribute, Object[] arguments ) {
        super(target,attribute,arguments);
        if (target == null) {

           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0012));

        }
        if ( ! ( target instanceof Serializable ) ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0013));

        }
    }

    public TargetedActionDefinition(Object target, Integer attributeCode ) {
        super(target,attributeCode);
    }

    public TargetedActionDefinition(Object target, Integer attributeCode, Object[] arguments ) {
        super(target,attributeCode,arguments);
    }

    protected TargetedActionDefinition( TargetedActionDefinition rhs ) {
        super( rhs );
        if (rhs == null) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0014));

        }
    }

}

