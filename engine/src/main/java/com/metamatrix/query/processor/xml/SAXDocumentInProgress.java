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

import java.io.CharArrayWriter;
import java.util.Iterator;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import net.sf.saxon.TransformerFactoryImpl;

import org.xml.sax.SAXException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.query.mapping.xml.MappingNodeConstants;
import com.metamatrix.query.util.LogConstants;

/**
 * This class is used to build XML document and stream the output as
 * chunks. The class holds one chunk of the document in memory at one time. 
 */
public class SAXDocumentInProgress implements DocumentInProgress {
	private TransformerHandler handler;
    private Transformer transformer;
    private Element currentParent;
    private Element currentObject;
    private boolean finished;
    private String documentEncoding = MappingNodeConstants.Defaults.DEFAULT_DOCUMENT_ENCODING;
    private boolean isFormatted = MappingNodeConstants.Defaults.DEFAULT_FORMATTED_DOCUMENT.booleanValue();
    private CharArrayWriter streamResultHolder = new CharArrayWriter();
    
    
    public SAXDocumentInProgress() throws MetaMatrixComponentException{
        SAXTransformerFactory factory = new TransformerFactoryImpl();
    	//TODO use standard jaxp to create factory 
        //SAXTransformerFactory factory = (SAXTransformerFactory)TransformerFactory.newInstance();
        try {
			//SAX2.0 ContentHandler
			handler = factory.newTransformerHandler();
			handler.setResult(new StreamResult(streamResultHolder));
		} catch (Exception e) {
			throw new MetaMatrixComponentException(e);
		}
        transformer = handler.getTransformer();
    }
    
	/**
	 * @see com.metamatrix.query.processor.xml.DocumentInProgress#setDocumentEncoding(java.lang.String)
	 */
	public void setDocumentEncoding(String documentEncoding) {
		this.documentEncoding = documentEncoding;
	}

	/**
	 * @see com.metamatrix.query.processor.xml.DocumentInProgress#setDocumentFormat(boolean)
	 */
	public void setDocumentFormat(boolean isFormatted) {
		this.isFormatted = isFormatted;
	}

	/**
	 * Move to the parent of this element. The parent of this element becomes
	 * the current object. Need to process the current and child object before moving 
	 * to the parent.
	 * @throws SAXException 
	 * @see com.metamatrix.query.processor.xml.DocumentInProgress#moveToParent()
	 */
	public boolean moveToParent() throws SAXException {
        showState( "moveToParent - TOP" );  //$NON-NLS-1$
        
        endElement(currentObject);
   
        //move to parent - if parent is null, then stop processing here
		if(currentParent == null){
			return false;
		}
        
        showState( "moveToParent - before processWorkingElements, second time" );  //$NON-NLS-1$        
		currentObject = currentParent;
		currentParent = currentParent.getParent();
        showState( "moveToParent - BOT" );  //$NON-NLS-1$
		return true;
	}
	
	/**
	 * @see com.metamatrix.query.processor.xml.DocumentInProgress#moveToLastChild()
	 */
	public boolean moveToLastChild() {
        showState( "moveToLastChild - TOP" );  //$NON-NLS-1$
		currentParent = currentObject;
		currentObject = null;
        showState( "moveToLastChild - BOT" );  //$NON-NLS-1$
		return true;
	}
    
	public boolean addElement(NodeDescriptor descriptor, NodeDescriptor nillableDescriptor){			
        return addElement(descriptor, null, nillableDescriptor);
	}
    
    public boolean addElement(NodeDescriptor descriptor, String content){
        return addElement(descriptor, content, null);
    }
    
    private boolean addElement(NodeDescriptor descriptor, String content, NodeDescriptor nillableDescriptor){
        showState( "addElement(2) - TOP" );  //$NON-NLS-1$
        
        try{
            if(currentParent == null){
                //this is the root element, start document first
                showState( "addElement(2) - before StartDocument()" ); //$NON-NLS-1$

                startDocument();
            }
            Element element = makeElement(descriptor);
            if (element != null){
                if (content != null) {
                    element.setContent(normalizeText(content, descriptor.getTextNormalizationMode()));
                    //mark the element and its parents to be mandatory
                    markAsNonOptional(element);
                } else {
                    element.setNillableDescriptor(nillableDescriptor);
                }
                
                showState( "addElement(2) - before markAsNonOptional()" );  //$NON-NLS-1$
                
                endElement(currentObject);
                currentObject = element;
                return true;
            }
        } catch (SAXException e) {
            LogManager.logError(LogConstants.CTX_XML_PLAN, e, e.getMessage());
            return false;
        }
        showState( "addElement(2) - BOT" );  //$NON-NLS-1$
        return false;
    }

    private void endElement(Element element) throws SAXException {
        showState( "endElement(2) - TOP" );  //$NON-NLS-1$
        
        if (element == null) {
            return;
        }
        
        if (element.isOptional()) {
            if (element.getParent() != null) {
                element.getParent().getChildren().remove(element);
            }
            return;
        }
        
        NodeDescriptor nillableDescriptor = element.getNillableDescriptor();
                
        if (nillableDescriptor != null) {
            addAttribute(nillableDescriptor, nillableDescriptor.getDefaultValue(), element);
        }
                
        // Optional parents are in control of when their children are emitted.
        if (element.hadOptionalParent()) {
            return;
        }
                        
        startElement(element);
        processChildren(element);
        element.endElement();
        
        if (element.getParent() != null) {
            element.getParent().getChildren().remove(element);
        }
        showState( "endElement(2) - BOT" );  //$NON-NLS-1$
    }
    
    private void startElement(Element element) throws SAXException {
        Element parent = element.getParent();
        while (parent != null && !parent.isElementStarted()) {
            parent.setNillableDescriptor(null);
            startElement(parent);
        }
        element.startElement();
    }
    
    private void processChildren(Element element) throws SAXException {
        
        for (Iterator i = element.getChildren().iterator(); i.hasNext();) {
            Element child = (Element)i.next();
            i.remove();
            
            child.startElement();
            processChildren(child);
            child.endElement();
        }
    }

    private void markAsNonOptional(Element element) {

        while(element != null){
            element.setOptional(false);
            element = element.getParent();
        }
    }
    
    public boolean addAttribute(NodeDescriptor descriptor, String attributeValue, Element element){
        element.setAttribute(descriptor, normalizeText(attributeValue,descriptor.getTextNormalizationMode()));
        
        if (!descriptor.isOptional()){
            //mark the element and its parents to be mandatory
            markAsNonOptional(element);
        }        
        return true;
    }

    /**
     * @see com.metamatrix.query.processor.xml.DocumentInProgress#addAttribute(java.lang.String, java.lang.String, java.lang.String, boolean)
     */
	public boolean addAttribute(NodeDescriptor descriptor, String attributeValue){
        return addAttribute(descriptor, attributeValue, currentParent);
	}

	/**
	 * @throws MetaMatrixComponentException
	 * @see com.metamatrix.query.processor.xml.DocumentInProgress#addComment(java.lang.String)
	 */
	public boolean addComment(String commentText) {
        currentParent.setComment(commentText);
		return true;
	}

	/**
	 * @see com.metamatrix.query.processor.xml.DocumentInProgress#isFinished()
	 */
	public boolean isFinished() {
		return finished;
	}

	/**
	 * @see com.metamatrix.query.processor.xml.DocumentInProgress#markAsFinished()
	 */
	public void markAsFinished() throws MetaMatrixComponentException{
		try {
			endDocument();
		} catch (SAXException e) {
			throw new MetaMatrixComponentException(e);
		}
		finished = true;
	}

	/**
	 * @see com.metamatrix.query.processor.xml.DocumentInProgress#getNextChunk(int)
	 */
	public char[] getNextChunk(int sizeInBytes) {
        if(sizeInBytes == 0 && !finished) {
            return null;
        }
        
		//the unit of the size is Kb
		if(finished || streamResultHolder.size() >= sizeInBytes){
			char[] chunk = streamResultHolder.toCharArray();
			if(finished){
				streamResultHolder.close();
			}else{
				streamResultHolder.reset();
			}
			return chunk;
		}
		return null;
	}
	
	private Element makeElement(NodeDescriptor descripter) {
        showState( "makeElement - TOP" );   //$NON-NLS-1$
        Element element = new Element(descripter, handler);
        element.setParent(currentParent);
        if (currentParent != null) {
            currentParent.addChild(element);
        }
        showState( "makeElement - BOT" );  //$NON-NLS-1$
		return element;
	}
	
	private void startDocument() throws SAXException{
        showState( "startDocument - TOP" );  //$NON-NLS-1$
		transformer.setOutputProperty(OutputKeys.ENCODING, documentEncoding);
		if(isFormatted){
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");//$NON-NLS-1$
		}
		handler.startDocument();
        showState( "startDocument - BOT" );  //$NON-NLS-1$
	}
	
	private void endDocument() throws SAXException{
		//special case: only one root element
		endElement(currentObject);
		handler.endDocument();
	}
    
    /** 
     * @param content
     * @param textNormalizationMode
     *              preserve     No normalization is done, the value is not changed for element content
     *              replace      All occurrences of #x9 (tab), #xA (line feed) and #xD (carriage return) are replaced with #x20 (space)
     *              collapse     After the processing implied by replace, contiguous sequences of #x20's are collapsed to a single #x20, and leading and trailing #x20's are removed.
     *
     * @return
     * @since 4.3
     
     */
    public static String normalizeText(String content, String textNormalizationMode) {
        String result = content;
        String singleSpace = " "; //$NON-NLS-1$
        if(textNormalizationMode.equalsIgnoreCase(MappingNodeConstants.NORMALIZE_TEXT_REPLACE)) {
            result = result.replaceAll("\\s", singleSpace); //$NON-NLS-1$
        }else if(textNormalizationMode.equalsIgnoreCase(MappingNodeConstants.NORMALIZE_TEXT_COLLAPSE)){
            result = result.replaceAll("\\s+", singleSpace); //$NON-NLS-1$
            result = result.trim();
        }
        return result;
    }
      

    private void showState( String sOccasion ) {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_XML_PLAN, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"\n [showState] State Vars at: " + sOccasion} ); //$NON-NLS-1$ 
            LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"[showState] currentParent: " + currentParent} ); //$NON-NLS-1$
            LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"[showState] currentObject: " + currentObject} ); //$NON-NLS-1$

            if ( currentObject != null ) { 
                LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"[showState] currentObject.getNillableDescriptor(): " + currentObject.getNillableDescriptor()}); //$NON-NLS-1$ 
                LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"[showState] workingElements: " + currentObject.getChildren()}); //$NON-NLS-1$ 
            }    
            if ( currentParent != null ) { 
                LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"[showState] currentParent.getParent(): " + currentParent.getParent()}); //$NON-NLS-1$
            } else {
                LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"[showState] currentParent.getParent(): is NULL "}); //$NON-NLS-1$ 
            }
        }
    }

}
