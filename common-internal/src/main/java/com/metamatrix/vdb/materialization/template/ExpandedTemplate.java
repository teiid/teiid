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

package com.metamatrix.vdb.materialization.template;

/**
 * Multi-valued return value used to hold the results of expanding templates with data values.
 * 
 * Name would typically correspond to a generated file name.
 * 
 * Contents would typically correspond to the contents of a generated file.
 * 
 * @since 4.2
 */
public class ExpandedTemplate {

    public String name;
    public String contents;
    public String type;

    public ExpandedTemplate(String name, String contents, String type) {
        super();
        this.name = name;
        this.contents = contents;
        this.type = type;
    }
}
