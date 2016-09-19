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

package org.teiid.translator.swagger;

import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.FileProperty;
import io.swagger.models.properties.MapProperty;
import io.swagger.models.properties.ObjectProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;

public abstract class PropertyVisitor {
    void visit(String name, RefProperty property) {
    }
    void visit(String name, ObjectProperty property) {
    }
    void visit(String name, MapProperty property) {
    }
    void visit(String name, FileProperty property) {
    }
    void visit(String name, ArrayProperty property) {
    }
    void visit(String name, Property property) {
    }
    
    void accept(String name, Property property) {
        if (property instanceof ArrayProperty) {
            visit(name, (ArrayProperty)property);
        } else if (property instanceof RefProperty) {
            visit(name, (RefProperty)property);
        } else if (property instanceof MapProperty) {
            visit(name, (MapProperty)property);
        } else if (property instanceof ObjectProperty) {
            visit(name, (ObjectProperty)property);
        } else if (property instanceof FileProperty) {
            visit(name, (FileProperty)property);
        } else {
            visit(name, property);
        }        
    }
}
