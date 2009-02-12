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

package com.metamatrix.query.mapping.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.QueryPlugin;


/** 
 * A Mapping Node document object.
 */
public class MappingDocument extends MappingBaseNode {
    
    MappingBaseNode root;
    boolean formatted;
    String encoding;
    String name;
    
    public MappingDocument(boolean formatted) {
        this(MappingNodeConstants.Defaults.DEFAULT_DOCUMENT_ENCODING, formatted);
    }
            
    public MappingDocument(String encoding, boolean formatted) {
        if (encoding == null) {
            encoding = MappingNodeConstants.Defaults.DEFAULT_DOCUMENT_ENCODING;
        }
        setDocumentEncoding(encoding);
        setFormatted(formatted);
    }
    
    public void acceptVisitor(MappingVisitor visitor) {
        visitor.visit(this);
    }

    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getFullyQualifiedName() {
        return name;
    }
    
    public String getCanonicalName() {
        return name.toUpperCase();
    }
        
    public MappingBaseNode getRootNode() {
        return root;
    }
    
    /**
     * A tag root is the first visual node on the document. A document can contain a "source" node
     * at root, but what ever is the first maping element that is the tag root.
     * @return
     */
    public MappingElement getTagRootElement() {
        if (this.root instanceof MappingSourceNode) {
            return (MappingElement)this.root.getNodeChildren().get(0);
        }
        return (MappingElement)this.root;
    }
    
    void setRoot(MappingBaseNode root) {
        if (root != null) {
            this.root = root;
            this.getChildren().clear();
            this.addChild(root);
        }
    }    
          
    public String getDocumentEncoding() {
        return this.encoding;
    }
    
    public boolean isFormatted() {
        return this.formatted;
    }
    
    public boolean isDocumentNode() {
        return true;
    }
    
    public void setDocumentEncoding(String encoding) {
        this.encoding = encoding;
        setProperty(MappingNodeConstants.Properties.DOCUMENT_ENCODING, this.encoding);
    }
    
    public void setFormatted(boolean formatted) {
        this.formatted = formatted;
        setProperty(MappingNodeConstants.Properties.FORMATTED_DOCUMENT, Boolean.valueOf(this.formatted));
    }    
    
    /**
     * Make sure the cardinality is set correctly
     */
    private void fixCardinality(MappingElement root) {
        root.setMaxOccurrs(1);
        root.setMinOccurrs(1);
    }     
    
    public MappingAllNode addAllNode(MappingAllNode elem) {
        throw new MetaMatrixRuntimeException(QueryPlugin.Util.getString("WrongTypeChild")); //$NON-NLS-1$
    }

    public MappingChoiceNode addChoiceNode(MappingChoiceNode elem) {
        throw new MetaMatrixRuntimeException(QueryPlugin.Util.getString("WrongTypeChild")); //$NON-NLS-1$
    }

    public MappingSequenceNode addSequenceNode(MappingSequenceNode elem) {
        throw new MetaMatrixRuntimeException(QueryPlugin.Util.getString("WrongTypeChild")); //$NON-NLS-1$
    }
    
    public MappingElement addChildElement(MappingElement elem) {
        if (elem == null) {
            throw new MetaMatrixRuntimeException(QueryPlugin.Util.getString("root_cannotbe_null")); //$NON-NLS-1$
        }        
        fixCardinality(elem);
        setRoot(elem);
        return elem;
    }    
    
    public MappingSourceNode addSourceNode(MappingSourceNode elem) {
        if (elem == null) {
            throw new MetaMatrixRuntimeException(QueryPlugin.Util.getString("root_cannotbe_null")); //$NON-NLS-1$
        }
        setRoot(elem);
        return elem;
    }
    
    /** 
     * @see com.metamatrix.query.mapping.xml.MappingNode#clone()
     */
    public Object clone() {
        // I found this as cheap way of cloneing for now fast, may be we will
        // do all the object cloneing later..
        try {
            MappingOutputter out = new MappingOutputter();
            StringWriter stream = new StringWriter();
            out.write(this, new PrintWriter(stream));
            MappingLoader loader = new MappingLoader();
            MappingDocument doc =  loader.loadDocument(new ByteArrayInputStream(stream.toString().getBytes()));

            // Copy the values of the instance variables.
            doc.formatted = this.formatted;
            doc.encoding = this.encoding;
            doc.name = this.name;
            
            return doc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch(MappingException e) {
            throw new RuntimeException(e);
        }
    }    
}
