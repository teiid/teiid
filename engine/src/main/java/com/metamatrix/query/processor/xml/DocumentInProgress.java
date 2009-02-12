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

package com.metamatrix.query.processor.xml;

import org.xml.sax.SAXException;

import com.metamatrix.api.exception.MetaMatrixComponentException;

/**
 * <p>This represents a document in construction.  It maintains a reference 
 * to the current XML element being worked on, although the nature of that 
 * reference is completely hidden from the user.  This allows the nastiness
 * of creating the document using some document model to be hidden from the 
 * user.</p>  
 */
public interface DocumentInProgress {

    // Methods for initializing the document

    /**
     * Sets the document encoding property of the XML document,
     * typically something like <code>UTF-8</code>
     * @param document encoding value
     */
    public void setDocumentEncoding(String documentEncoding);
    
    /**
     * Sets whether the document will be formatted in human-readable
     * form (multi-line, with tabs) or compact form (no line breaks
     * or tabs).
     * @param isFormatted true for human-readable form, false for
     * compact form
     */
    public void setDocumentFormat(boolean isFormatted);

	// Methods for moving the cursor

	public boolean moveToParent() throws SAXException;
	
	public boolean moveToLastChild();
	
	// Methods for adding data
	
    /**
     * Add an element for the given NodeDescriptor.
     * @param descriptor NodeDescriptor of the element
     * @return whether operation was a success or not
     */
    boolean addElement(NodeDescriptor descriptor, NodeDescriptor nillableDescriptor);

    /**
     * Add an element with content for the given NodeDescriptor. 
     * If the content is either a null or an empty String, use the other 
     * {@link #addElement(NodeDescriptor) addElement}
     * method.
     * @param descriptor NodeDescriptor of the element
     * @param content Content of the element
     * @return whether operation was a success or not
     */
    public boolean addElement(NodeDescriptor descriptor, String content);

    /**
     * Add an attribute with content for the given NodeDescriptor.
     * @param descriptor NodeDescriptor of the attribute
     * @param attributeValue String content of the attribute, this must be a
     * non-null, non-empty String.  Otherwise, no attribute will be added.
     * @return whether operation was a success or not
     */
    public boolean addAttribute(NodeDescriptor descriptor, String attributeValue);

    /**
     * Adds a comment to the current document node
     * @param commentText text of the comment
     * @return whether operation was a success or not
     */
    public boolean addComment(String commentText);


    /**
     * This flag indicates the document is finished and requires no more processing.
     * @see #markAsFinished
     */
    public boolean isFinished();

    /**
     * This marks the document as finished, requiring no more processing.  (It will not,
     * however, prevent any more processing from being done.)
     * @throws MetaMatrixComponentException if there is any problem ending the document
     * @see #isFinished
     */
    public void markAsFinished() throws MetaMatrixComponentException;
    
    /**
     * Retrieve the next chunk of document.
     * @param sizeInBytes size of the chunk in bytes. No limit if it is 0.
     * @return character array containing the specfied number of characters, or less if 
     * it has reached the end of the document. Return null if there is not enough characters
     * and it has not reached the end of the document. 
     */	
    public char[] getNextChunk(int sizeInBytes);
		
}
