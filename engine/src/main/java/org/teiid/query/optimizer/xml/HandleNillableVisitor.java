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

package org.teiid.query.optimizer.xml;

import java.util.Properties;

import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Namespace;
import org.teiid.query.mapping.xml.Navigator;



/** 
 * Removes the all the "excluded" nodes from the Mapping XML node tree 
 */
public class HandleNillableVisitor extends MappingVisitor{

    public void visit(MappingElement element) {
        visitNode(element);
    }

    public void visit(MappingRecursiveElement element) {
        visitNode(element);
    }
    
    void visitNode(MappingElement element) {
        if (element.isNillable()) {
            MappingDocument doc = element.getDocument();

            // get the first visual node on the document.
            MappingElement rootElement = doc.getTagRootElement();
            
            Properties namespaces = rootElement.getNamespacesAsProperties();
            if (namespaces == null || !namespaces.containsKey(MappingNodeConstants.INSTANCES_NAMESPACE_PREFIX)) {
                rootElement.addNamespace(new Namespace(MappingNodeConstants.INSTANCES_NAMESPACE_PREFIX, MappingNodeConstants.INSTANCES_NAMESPACE));
            } 
            
            // now exit; we are done
            setAbort(true);
        }
    }
    
    public static MappingDocument execute(MappingDocument doc) {
        HandleNillableVisitor visitor = new HandleNillableVisitor();
        doc.acceptVisitor(new Navigator(true, visitor));
        return doc;
    }    
}
