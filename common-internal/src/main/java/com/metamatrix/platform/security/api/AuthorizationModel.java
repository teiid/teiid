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

package com.metamatrix.platform.security.api;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.common.actions.AttributeDefinition;
import com.metamatrix.common.actions.ClassDefinition;

public class AuthorizationModel {

    public static class Class {
        public static final ClassDefinition POLICY              = new ClassDefinition( AuthorizationPolicy.class );
        public static final ClassDefinition POLICYID            = new ClassDefinition( AuthorizationPolicyID.class );
    }

    public static class Attribute {
        // AuthorizationPolicy
        public static final AttributeDefinition DESCRIPTION     = new AttributeDefinition(0,"DESCRIPTION"); //$NON-NLS-1$
        public static final AttributeDefinition PRINCIPAL_NAME  = new AttributeDefinition(1,"PRINCIPAL_NAME"); //$NON-NLS-1$
        public static final AttributeDefinition PRINCIPAL_SET   = new AttributeDefinition(2,"PRINCIPAL_SET"); //$NON-NLS-1$
        public static final AttributeDefinition PERMISSION      = new AttributeDefinition(3,"PERMISSION"); //$NON-NLS-1$
        public static final AttributeDefinition PERMISSIONS     = new AttributeDefinition(4,"PERMISSIONS"); //$NON-NLS-1$
        public static final AttributeDefinition PERMISSION_SET  = new AttributeDefinition(5,"PERMISSION_SET"); //$NON-NLS-1$
    }

    private static int ATTRIBUTE_DEFINITION_COUNT = 6;
    private static Map classLookup = new HashMap(3);
    private static Map classLookupByIDClass = new HashMap();
    private static AttributeDefinition[] attributeDefinitions = new AttributeDefinition[ATTRIBUTE_DEFINITION_COUNT];

    static {
        // AuthorizationPolicy
        classLookup.put( Class.POLICY.getClassObject(), Class.POLICY );

        classLookupByIDClass.put( Class.POLICYID.getClassObject(), AuthorizationPolicyID.class );

        addAttributeToLookupMap( Attribute.DESCRIPTION );
        addAttributeToLookupMap( Attribute.PRINCIPAL_NAME );
        addAttributeToLookupMap( Attribute.PRINCIPAL_SET );
        addAttributeToLookupMap( Attribute.PERMISSION );
        addAttributeToLookupMap( Attribute.PERMISSIONS );
        addAttributeToLookupMap( Attribute.PERMISSION_SET );

    }

    public static ClassDefinition getClassDefinition( Class c ) {
        return (ClassDefinition) classLookup.get(c);
    }
    public static ClassDefinition getClassDefinition( Object obj ) {
        return (ClassDefinition) classLookup.get(obj.getClass());
    }
    public static ClassDefinition getClassDefinition( AuthorizationPolicyID policyID ) {
        return (ClassDefinition) classLookupByIDClass.get(policyID.getClass());
    }
    public static AttributeDefinition getAttributeDefinition( int code ) {
        if ( code < 0 || code >= ATTRIBUTE_DEFINITION_COUNT ) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0009, code));
        }
        return attributeDefinitions[ code ];
    }

    private static void addAttributeToLookupMap( AttributeDefinition attribute ) {
        attributeDefinitions[ attribute.getCode() ] = attribute;
    }
}

