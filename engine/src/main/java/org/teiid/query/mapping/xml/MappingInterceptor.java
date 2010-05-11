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

import java.util.Map;


/** 
 * This defines a intercepting interface for the mapping nodes where for each
 * node in the MappingNode can be called at the begining and end of the node
 * occurence.
 */
public interface MappingInterceptor {
    void start(MappingDocument doc, Map context);
    void end(MappingDocument doc, Map context);
    
    void start(MappingAllNode all, Map context);
    void end(MappingAllNode all, Map context);
    
    void start(MappingAttribute attribute, Map context);
    void end(MappingAttribute attribute, Map context);
    
    void start(MappingChoiceNode choice, Map context);
    void end(MappingChoiceNode choice, Map context);
    
    void start(MappingCommentNode comment, Map context);
    void end(MappingCommentNode comment, Map context);
    
    void start(MappingCriteriaNode element, Map context);
    void end(MappingCriteriaNode element, Map context);
    
    void start(MappingElement element, Map context);
    void end(MappingElement element, Map context);
    
    void start(MappingRecursiveElement element, Map context);
    void end(MappingRecursiveElement element, Map context);
    
    void start(MappingSequenceNode sequence, Map context);
    void end(MappingSequenceNode sequence, Map context);    
    
    void start(MappingSourceNode sequence, Map context);
    void end(MappingSourceNode sequence, Map context);        
}
