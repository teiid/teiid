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

package org.teiid.query.mapping.xml;

import java.util.HashMap;


/** 
 * This is mapping visitor which can make the intercepting of the
 * Mapping Nodes possible.
 */
public class InterceptingVisitor extends MappingVisitor {
    MappingInterceptor interceptor;
    HashMap context = new HashMap();
    
    public InterceptingVisitor(MappingInterceptor interceptor) {
        this.interceptor = interceptor;
    }
    
    public void visit(MappingAllNode all) {
        this.interceptor.start(all, context);
        walkChildNodes(all);
        this.interceptor.end(all, context);
    }

    public void visit(MappingAttribute attribute) {
        this.interceptor.start(attribute, context);
        this.interceptor.end(attribute, context);
    }

    public void visit(MappingChoiceNode choice) {
        this.interceptor.start(choice, context);
        walkChildNodes(choice);
        this.interceptor.end(choice, context);
    }

    public void visit(MappingCommentNode comment) {
        this.interceptor.start(comment, context);
        this.interceptor.end(comment, context);
    }

    public void visit(MappingCriteriaNode node) {
        this.interceptor.start(node, context);
        walkChildNodes(node);      
        this.interceptor.end(node, context);        
    }

    public void visit(MappingDocument doc) {
        this.interceptor.start(doc, context);
        doc.getRootNode().acceptVisitor(this);
        this.interceptor.end(doc, context);        
    }

    public void visit(MappingElement element) {
        this.interceptor.start(element, context);
        walkAttributes(element);
        walkChildNodes(element);        
        this.interceptor.end(element, context);        
    }

    public void visit(MappingRecursiveElement element) {
        this.interceptor.start(element, context);
        this.interceptor.end(element, context);
    }

    public void visit(MappingSequenceNode sequence) {
        this.interceptor.start(sequence, context);
        walkChildNodes(sequence);
        this.interceptor.end(sequence, context);
    }
    
    public void visit(MappingSourceNode node) {
        this.interceptor.start(node, context);
        walkChildNodes(node);
        this.interceptor.end(node, context);

    }   
}
