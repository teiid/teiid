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


/** 
 * This node describes a <b>comment</b> in XML Mapping document 
 */
public class MappingCommentNode extends MappingNode {

    public MappingCommentNode(String comment) {
        setProperty(MappingNodeConstants.Properties.NODE_TYPE, MappingNodeConstants.COMMENT);
        if (comment != null) {
            setProperty(MappingNodeConstants.Properties.COMMENT_TEXT, comment);
        }
    }
    
    public void acceptVisitor(MappingVisitor visitor) {
        visitor.visit(this);
    }
    
    public MappingElement getParentNode() {
        return (MappingElement)getParent();
    }    
    
    public String getComment() {
        return (String)getProperty(MappingNodeConstants.Properties.COMMENT_TEXT);
    }
}
