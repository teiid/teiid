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

package org.teiid.query.processor.xml;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.transform.sax.TransformerHandler;

import org.teiid.core.util.Assertion;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;


class Element{
	static final String DEFAULT_ATTRIBUTE_TYPE = "CDATA"; //$NON-NLS-1$
    static final AttributesImpl EMPTY_ATTRIBUTES = new AttributesImpl();
	
	private NodeDescriptor descriptor;
    
	private AttributesImpl attributes;
	private String content;
	private String comment;
    private boolean isOptional;
	private final boolean wasOptional;
	private boolean elementStarted;
	private boolean elementEnded;
    private Element parent;
    private List children = new LinkedList();
    private NodeDescriptor nillableDescriptor;
    private TransformerHandler handler;
    
	Element(NodeDescriptor descripter, TransformerHandler handler){
		this.descriptor = descripter;
        this.handler = handler;
        this.isOptional = descripter.isOptional();
        this.wasOptional = descripter.isOptional();
	}
	
	void setAttribute(NodeDescriptor descriptor, String value){
        if (this.elementStarted) {
            Assertion.failed("Attributes must not be added to Element after Element is started."); //$NON-NLS-1$
        }
		if(attributes == null) {
            attributes = new AttributesImpl();
        }
        attributes.addAttribute(descriptor.getNamespaceURI(), descriptor.getName(), descriptor.getQName(), DEFAULT_ATTRIBUTE_TYPE, value);
	}
	
	void setParent(Element parent){
		this.parent = parent;
	}
	
	Element getParent(){
		return this.parent;
	}
	
	void startElement() throws SAXException {
		if(elementStarted){
			return;
		}

		//start namespace prefix mapping
        Properties namespaceURIs = descriptor.getNamespaceURIs();
		if(namespaceURIs != null){
			Iterator iter = namespaceURIs.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry namespaceURI = (Map.Entry)iter.next();
                String prefix = (String)namespaceURI.getKey();
                String uri = (String)namespaceURI.getValue();
                //start name space mapping if not started
                //by its parent yet
                if(parent != null && uri.equals(parent.getNamespaceURI(prefix))) {
                    continue;
                }
                if(uri.length() > 0) {
                    handler.startPrefixMapping(prefix, uri);
                }
			}
		}
		//start and output element with its attributes
        if(attributes == null) {
            attributes = EMPTY_ATTRIBUTES;
        }
		handler.startElement(descriptor.getNamespaceURI(), descriptor.getName(), descriptor.getQName(), attributes);
		
		//output comment
		if(comment != null){
			handler.comment(comment.toCharArray(),0, comment.length());
		}
		
		//output content
		if(content != null){
			handler.characters(content.toCharArray(),0, content.length());
		}
		
		//we do not need attributes and content any more
		attributes = null;
		content = null;
		isOptional = false;
		elementStarted = true;
	}
	
	void setContent(String content){
		this.content = content;
	}
	
	void endElement() throws SAXException{
		if(elementEnded){
			return;
		}

		//end element (output end tag)
		handler.endElement(descriptor.getNamespaceURI(), descriptor.getName(), descriptor.getQName());
		
		//end namespace prefix mapping
        Properties namespaceURIs = descriptor.getNamespaceURIs();
		if(namespaceURIs != null){
            Iterator iter = namespaceURIs.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry namespaceURI = (Map.Entry)iter.next();
                String prefix = (String)namespaceURI.getKey();
                String uri = (String)namespaceURI.getValue();
                if(parent != null && uri.equals(parent.getNamespaceURI(prefix))) {
                    continue;
                }
                handler.endPrefixMapping(prefix);
			}
		}

		elementEnded = true;
	}
	
	void setComment(String comment){
        if (this.elementStarted) {
            Assertion.failed("Comment must not be added to Element after Element is started."); //$NON-NLS-1$
        }
		this.comment = comment;
	}

	String getNamespaceURI(String namespacePrefix){
        Properties namespaceURIs = descriptor.getNamespaceURIs();
        if(namespaceURIs != null) {
    		String uri = (String)namespaceURIs.get(namespacePrefix);
    		if(uri != null){
    			return uri;
    		}
        }
		if(parent != null){
			//look for namespace prefix in its parent
			return parent.getNamespaceURI(namespacePrefix);
		}
		return null;
	}

    boolean hadOptionalParent() {
        if (parent != null) {
            if (parent.wasOptional()) {
                return true;
            }
            return parent.hadOptionalParent();
        }
        return false;
    }
    
	boolean isOptional() {
		return isOptional;
	}

	void setOptional(boolean b) {
		isOptional = b;
	}
	
	public String toString(){
		return descriptor.getQName();
	}

	boolean isChildOf(Element elementToRemove) {
		boolean isChild = false;
		Element parentObj = parent;
		while(parentObj != null){
			if(parentObj == elementToRemove){
				isChild = true;
				break;
			}
			parentObj = parentObj.getParent();
		}
		return isChild;
	}

    
    /** 
     * @return Returns the nillableDescriptor.
     * @since 5.0
     */
    public NodeDescriptor getNillableDescriptor() {
        return this.nillableDescriptor;
    }

    
    /** 
     * @param nillableDescriptor The nillableDescriptor to set.
     * @since 5.0
     */
    public void setNillableDescriptor(NodeDescriptor nillableDescriptor) {
        this.nillableDescriptor = nillableDescriptor;
    }

    
    /** 
     * @return Returns the elementStarted.
     * @since 5.0
     */
    public boolean isElementStarted() {
        return this.elementStarted;
    }

    
    /** 
     * @return Returns the children.
     * @since 5.0
     */
    public List getChildren() {
        return this.children;
    }
    
    public void addChild(Element child) {
        this.children.add(child);
    }

    
    /** 
     * @return Returns the wasOptional.
     * @since 5.0
     */
    public boolean wasOptional() {
        return this.wasOptional;
    }

}
