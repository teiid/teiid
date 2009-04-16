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

package com.metamatrix.common.config.model;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.common.actions.AttributeDefinition;
import com.metamatrix.common.actions.ClassDefinition;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.namedobject.BaseID;

public class ConfigurationModel {

    public static class Class {
        public static final ClassDefinition COMPONENT           = new ClassDefinition( ComponentDefn.class );
        public static final ClassDefinition DEPLOYED_COMPONENT  = new ClassDefinition( DeployedComponent.class );
        public static final ClassDefinition CONFIGURATION       = new ClassDefinition( Configuration.class );
        public static final ClassDefinition HOST                = new ClassDefinition( Host.class );
        public static final ClassDefinition COMPONENT_TYPE_DEFN = new ClassDefinition( ComponentTypeDefn.class );
        public static final ClassDefinition COMPONENT_TYPE      = new ClassDefinition( ComponentType.class );

    }

    public static class Attribute {
        public static final AttributeDefinition ID                  = new AttributeDefinition(0,"ID"); //$NON-NLS-1$
        public static final AttributeDefinition IS_RELEASED         = new AttributeDefinition(1,"Released"); //$NON-NLS-1$
        public static final AttributeDefinition IS_DEPLOYED         = new AttributeDefinition(2,"Deployed"); //$NON-NLS-1$
        public static final AttributeDefinition PROPERTIES          = new AttributeDefinition(3,"Properties"); //$NON-NLS-1$
        public static final AttributeDefinition PROPERTY            = new AttributeDefinition(4,"Property"); //$NON-NLS-1$
        public static final AttributeDefinition NAME                = new AttributeDefinition(5,"Name"); //$NON-NLS-1$

        // ComponentType
        public static final AttributeDefinition COMPONENT_TYPEID            = new AttributeDefinition(7,"Component Type ID"); //$NON-NLS-1$
        public static final AttributeDefinition TYPE_DEFINITIONS            = new AttributeDefinition(8,"Component Type Defns"); //$NON-NLS-1$
        public static final AttributeDefinition PARENT_COMPONENT_TYPEID     = new AttributeDefinition(9,"Parent Component Type ID"); //$NON-NLS-1$
        public static final AttributeDefinition SUPER_COMPONENT_TYPEID      = new AttributeDefinition(10,"Super Component Type ID"); //$NON-NLS-1$
        public static final AttributeDefinition IS_DEPLOYABLE               = new AttributeDefinition(11,"Is Deployable"); //$NON-NLS-1$
        public static final AttributeDefinition IS_MONITORED                = new AttributeDefinition(12,"Is Monitored"); //$NON-NLS-1$
        public static final AttributeDefinition IS_DEPRECATED               = new AttributeDefinition(13,"Is Deprecated"); //$NON-NLS-1$
        public static final AttributeDefinition PSC_NAME                    = new AttributeDefinition(15,"PSC Name"); //$NON-NLS-1$
        public static final AttributeDefinition IS_ENABLED                  = new AttributeDefinition(18,"Is Enabled"); //$NON-NLS-1$
        public static final AttributeDefinition ROUTING_UUID                = new AttributeDefinition(19,"Routing UUID"); //$NON-NLS-1$

      // ComponentTypeDefn
      // only the name of ComponentTypeDefn is used because the whole object is sent back and updated,
      // not column by column
        public static final AttributeDefinition COMPONENT_TYPE_DEFN         = new AttributeDefinition(14,"Component Type Defn"); //$NON-NLS-1$

        // System Configurations
        public static final AttributeDefinition CURRENT_CONFIGURATION   = new AttributeDefinition(6,"Current Configuation"); //$NON-NLS-1$
        public static final AttributeDefinition NEXT_STARTUP_CONFIGURATION   = new AttributeDefinition(16,"Next Startup Configuation"); //$NON-NLS-1$
 //       public static final AttributeDefinition STARTUP_CONFIGURATION   = new AttributeDefinition(17,"Startup Configuation"); //$NON-NLS-1$
    

        public static final AttributeDefinition UPDATE_PSC   = new AttributeDefinition(20,"Update PSC"); //$NON-NLS-1$
        public static final AttributeDefinition UPDATE_COMPONENT_TYPE   = new AttributeDefinition(21,"Update ComponentType"); //$NON-NLS-1$
    
    }

    private static int ATTRIBUTE_DEFINITION_COUNT = 15;
    private static Map classLookup = new HashMap(15);
    private static Map classLookupByIDClass = new HashMap();
    private static AttributeDefinition[] attributeDefinitions = new AttributeDefinition[ATTRIBUTE_DEFINITION_COUNT];

    static {
        classLookup.put( Class.COMPONENT.getClassObject(), Class.COMPONENT );
        classLookup.put( Class.DEPLOYED_COMPONENT.getClassObject(), Class.DEPLOYED_COMPONENT );
        classLookup.put( Class.CONFIGURATION.getClassObject(), Class.CONFIGURATION );

        classLookupByIDClass.put( Class.COMPONENT.getClassObject(), ComponentDefnID.class );
        classLookupByIDClass.put( Class.DEPLOYED_COMPONENT.getClassObject(), DeployedComponentID.class );
        classLookupByIDClass.put( Class.CONFIGURATION.getClassObject(), ConfigurationID.class );
        classLookupByIDClass.put( Class.HOST.getClassObject(), HostID.class );


        addAttributeToLookupMap( Attribute.ID );
        addAttributeToLookupMap( Attribute.IS_RELEASED );
        addAttributeToLookupMap( Attribute.PROPERTIES );
        addAttributeToLookupMap( Attribute.PROPERTY );
        addAttributeToLookupMap( Attribute.NAME );

        // Component
     /*   addAttributeToLookupMap( Attribute.COMPONENT_TYPE_UID );

        // DeployedComponent
        addAttributeToLookupMap( Attribute.CONFIGURATION_UID );
        addAttributeToLookupMap( Attribute.HOST_UID );
        addAttributeToLookupMap( Attribute.COMPONENT_UID );
        addAttributeToLookupMap( Attribute.DEPLOYED_COMPONENT_UID );
    */
    }

    public static ClassDefinition getClassDefinition( Class c ) {
        return (ClassDefinition) classLookup.get(c);
    }
    public static ClassDefinition getClassDefinition( Object obj ) {
        return (ClassDefinition) classLookup.get(obj.getClass());
    }
    public static ClassDefinition getClassDefinition( BaseID id ) {
        return (ClassDefinition) classLookupByIDClass.get(id.getClass());
    }
/*
    public static AttributeDefinition getAttributeDefinition( int code ) {
        if ( code < 0 || code >= ATTRIBUTE_DEFINITION_COUNT ) {
            throw new IllegalArgumentException("The specified code (" + code + ") is invalid");
        }
        return attributeDefinitions[ code ];
    }
*/
    private static void addAttributeToLookupMap( AttributeDefinition attribute ) {
        attributeDefinitions[ attribute.getCode() ] = attribute;
    }
}

