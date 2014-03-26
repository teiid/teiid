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
package org.teiid.metadata;

import java.lang.annotation.*;

/**
 * Annotates a property that defines a extension metadata property  
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface ExtensionMetadataProperty {
    public static final String EMPTY_STRING = ""; //$NON-NLS-1$
        
    /**
     * Kind of metadata record this property is applicable for  
     */
    Class[] applicable() default {Table.class};
    
    /**
     * Display Name 
     */
    String display() default EMPTY_STRING;
    
    /**
     * Description of the Property
     */
    String description() default EMPTY_STRING;
    
    /**
     * Is this a required property 
     */
    boolean required() default false;
    
    /**
     * Data type of the property
     * @return
     */
    Class datatype() default java.lang.String.class; 
    
    /**
     * If only takes predefined values, this describes comma separated values 
     */
    String allowed() default EMPTY_STRING;
}
