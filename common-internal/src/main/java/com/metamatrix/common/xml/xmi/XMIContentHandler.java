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

package com.metamatrix.common.xml.xmi;

import java.io.IOException;
import java.io.InputStream;
import java.util.EmptyStackException;
import java.util.Stack;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.metamatrix.api.core.message.DefaultMessage;
import com.metamatrix.api.core.message.MessageList;
import com.metamatrix.api.core.message.MessageTypes;
import com.metamatrix.api.core.xmi.EntityInfo;
import com.metamatrix.api.core.xmi.FeatureInfo;
import com.metamatrix.api.core.xmi.XMIConstants;
import com.metamatrix.api.core.xmi.XMIHeader;
import com.metamatrix.api.core.xmi.XMIReaderAdapter;
import com.metamatrix.common.log.LogManager;

public class XMIContentHandler extends DefaultHandler {
	
	public static final String XMI_CONTENT_HANDLER = "XMI Content Handler"; //$NON-NLS-1$
	public static final String XMI_UUID = XMIConstants.AttributeName.XMI_UUID;
	public static final String XMI_ID = XMIConstants.AttributeName.XMI_ID;// "xmi.id";
	public static final String NAME = "name"; //$NON-NLS-1$
	public static final String HREF = XMIConstants.AttributeName.HREF; // "href";
	public static final String XMI_LABEL = XMIConstants.AttributeName.XMI_LABEL; //"xmi.label";
	public static final String XMI_ID_REF = XMIConstants.AttributeName.XMI_IDREF; //"xmi.idref";
	public static final String XMI_CONTENT = XMIConstants.ElementName.CONTENT; //"XMI.content";
	
	private HeaderHandler headerHandler;
	private XMIReaderAdapter readerAdapter;
	private ContentHandler currentHandler;
	private int depth = 1;
	private Stack entityInfo;
	private Stack objectInfo;
    private EntityInfo previousEntityInfo;
    private FeatureInfo previousFeatureInfo;

    protected BodyHandler bodyHandler;

    // Temporary holder for Text and CDATA
    private StringBuffer textBuffer = new StringBuffer(4096);
    // Whether to ignore ignorable whitespace 
    private boolean ignoringWhite = true;
	
	/**
	 * Inner class to wrap objectInfo stack objects
	 */
	private static class ObjectInfoWrapper {
	    final private Object result;
	    final private boolean isEntity;
        final private String id;
        final private String parentId;
	    
	    public ObjectInfoWrapper(final Object result, final boolean isEntity, final String id, final String parentId){
	        this.result = result;
	        this.isEntity = isEntity;
            this.id = id;
            this.parentId = parentId;
	    }
	    
	    public Object getResult(){
	        return result;
	    }
	    
	    public boolean isEntity(){
	        return isEntity;
	    }
        /**
         * @return
         */
        public String getId() {
            return id;
        }

        /**
         * @return
         */
        public String getParentId() {
            return parentId;
        }

	}
	     
	
	/**
	 * Inner class to handle the XMI Header
	 */
	private class HeaderHandler extends XMIHeaderContentHandler{
		/**
		 * Default contstructor
		 */ 
		public HeaderHandler() throws IOException{
			super();
		}		
		
		/**
		 * Called when the XMIHeaderContentHandler reaches a close tag on the header
		 */ 
		public void complete(){
			//Set the XMIHeader on the containing class
			XMIContentHandler.this.setXMIHeader(this.getHeader() );
		}
	}//End HeaderHandler inner class
	
	/**
	 * Inner class to handle the XMI Body
	 */ 
	protected class BodyHandler extends DefaultHandler{
		private XMIReaderAdapter readerAdapter;
		public final String BODY_HANDLER = "BODY Handler"; //$NON-NLS-1$
		
		public BodyHandler(XMIReaderAdapter adapter){
			readerAdapter = adapter;
		}		
		
		// **************************************************************
		// Overridden methods of DefaultHandler... event listener methods
		// **************************************************************
		
		public void characters(char[] characters, int start, int length){
		    //DEBUG
		    //System.out.println("Processing Characters");
		    
		    if(objectInfo.isEmpty() ){
		        //System.out.println("NO PARENT INFO OBJECT");
		        return;
		    }
    
            // Append the characters to a buffer until we hit an endElement tag
            textBuffer.append(characters, start, length);

//		    String value = new String(characters, start, length).trim();
//		    if(value.equals("") ){
//		        //System.out.println("NO TEXT");
//		        return;
//		    }
//		    
//            // If there is a feature (for what contains this entity) on the stack, process it ...
//            ObjectInfoWrapper parent = (ObjectInfoWrapper)entityInfo.peek();
//            ObjectInfoWrapper featureWrapper = (ObjectInfoWrapper)objectInfo.peek();
//            FeatureInfo featureInfo = (FeatureInfo) featureWrapper.getResult();
//            featureInfo.setValue(value);
//            readerAdapter.createFeature(featureInfo, parent.getResult() );
		}

        public void ignorableWhitespace(char[] characters, int start, int length) throws SAXException {
            textBuffer.append(characters, start, length);
        }
		
		public void endElement(String uri, String name, String qName) throws SAXException {
		    //DEBUG
			//System.out.println("End Element " + name);
			
			if ("".equals(uri)){ //$NON-NLS-1$
				LogManager.logTrace(BODY_HANDLER, "End element: " + qName); //$NON-NLS-1$
			}else{
				LogManager.logTrace(BODY_HANDLER, "End element:   {" + uri + "}" + name); //$NON-NLS-1$ //$NON-NLS-2$
			}

            // If the character buffer is not empty, create the associated feature
            if (textBuffer.length() != 0) {
                String value = normalizeString(textBuffer.toString());
                textBuffer.setLength(0);
                
                // If there is a feature (for what contains this entity) on the stack, process it ...
                if (value.length() != 0) {
                    ObjectInfoWrapper parent = (ObjectInfoWrapper)entityInfo.peek();
                    ObjectInfoWrapper featureWrapper = (ObjectInfoWrapper)objectInfo.peek();
                    FeatureInfo featureInfo = (FeatureInfo) featureWrapper.getResult();
                    featureInfo.setValue(value);
                    readerAdapter.createFeature(featureInfo, parent.getResult() );
                }
            }

            // Pop the entity off the stack
			ObjectInfoWrapper result = null;
			try{
			    result = (ObjectInfoWrapper)objectInfo.pop();
			}catch(EmptyStackException e){
			    //Do nothing... the stack will be empty when the last item is popped
			}
			
			//result will equal null when we reach the close tag for xmi.content.  We're done.
			if(result == null){
			    return;
			}
			
			
			// If the Result is an entity ...
			if(result.isEntity() ){

                // update the entityInfo stack ...
			    try{
			        entityInfo.pop();
			    }catch(EmptyStackException e){
			    	//Do nothing... the stack will be empty when the last item is popped
				}

                // And notify the adapter that this entity is finished
                readerAdapter.finishEntity(result.getResult() );
                
                // If there is a feature (for what contains this entity) on the stack, process it ...
                try{
                    ObjectInfoWrapper featureWrapper = (ObjectInfoWrapper)objectInfo.peek();
                    if ( !featureWrapper.isEntity() ) {
                        FeatureInfo featureInfo = (FeatureInfo) featureWrapper.getResult();
                        featureInfo.setValue(result.getResult());
                        ObjectInfoWrapper parentWrapper = (ObjectInfoWrapper)entityInfo.peek();
                        readerAdapter.createFeature(featureInfo, parentWrapper.getResult() );
                    }
                }catch(EmptyStackException e){
                    //Do nothing... the stack will be empty when the last item is popped
                }
            } else {
                ObjectInfoWrapper parentWrapper = (ObjectInfoWrapper)entityInfo.peek();
                FeatureInfo featureInfo = (FeatureInfo) result.getResult();
                readerAdapter.finishFeature(featureInfo,parentWrapper.getResult());    
            }
			
			//process the Result
			depth--;
		}
		
		public void startElement(String uri, String name, String qName, Attributes atts) throws SAXException {
			//DEBUG
			//System.out.println("Starting Element " + name);
						
			if ("".equals(uri)){ //$NON-NLS-1$
				LogManager.logTrace(BODY_HANDLER, "Start element: " + qName); //$NON-NLS-1$
			}else{
				LogManager.logTrace(BODY_HANDLER, "Start element: {" + uri + "}" + name); //$NON-NLS-1$ //$NON-NLS-2$
			}
			
			ObjectInfoWrapper parent = null;
            
            // If the depth is an even number ...
			if( isEntity(uri, name, qName, atts) ){
                // then it is an entity ...
			    EntityInfo info = new EntityInfo(previousFeatureInfo, name, uri);
			    if(!entityInfo.isEmpty() ){
			    	parent = (ObjectInfoWrapper)entityInfo.peek();
			    }
                previousEntityInfo = info;
			    			    
			    //Set the entity info values
			    int length = atts.getLength();
            	for (int i = 0; i < length; i++){
            		String localName = atts.getLocalName(i);
            		String value = atts.getValue(i);
            		if(NAME.equals(localName) ){
            		    info.setName(value);
            		}else if(XMI_ID.equals(localName) ){
            		    info.setID(value);
            		}else if(XMI_UUID.equals(localName) ){
            		    info.setUUID(value);
            		}else if(HREF.equals(localName) ){
            		    info.setHref(value);
            		}else if(XMI_LABEL.equals(localName) ){
            		    info.setLabel(value);
            		}else if(XMI_ID_REF.equals(localName) ){
            		    info.setIDRef(value);
            		}
            	}
                
                //Add the attributes as well (required for MIMB ERwin processing)
                info.setAttributes(atts);
                
                Object result = null;
                String parentId = null;
                if(parent != null){
                    parentId = parent.getId();
                    info.setParentId(parentId);
					result = readerAdapter.createEntity(info, parent.getResult() );
                }else{
                    result = readerAdapter.createEntity(info, null );
                }
                
				ObjectInfoWrapper wrapper = new ObjectInfoWrapper(result, true, info.getID(), parentId);
				objectInfo.push(wrapper);
				entityInfo.push(wrapper);

				//Add a feature for each attribute
				length = atts.getLength();
            	for (int i = 0; i < length; i++){
            		String localName = atts.getLocalName(i);
                    
                    // Ignore the 'standard' XMI attributes already handled through the EntityInfo ...
                    if(XMI_ID.equals(localName) ){
                        continue;
                    }else if(XMI_UUID.equals(localName) ){
                        continue;
                    }else if(HREF.equals(localName) ){
                        continue;
                    }else if(XMI_LABEL.equals(localName) ){
                        continue;
                    }else if(XMI_ID_REF.equals(localName) ){
                        continue;
                    }
                    
            		String value = atts.getValue(i);
            		FeatureInfo feature = new FeatureInfo(previousEntityInfo,localName, name, uri);
            		feature.setValue(value);
                    // Do NOT do this next line, since we'll finish the feature immediately anyway
                    // previousFeatureInfo = info;
					readerAdapter.createFeature(feature, result);
                    readerAdapter.finishFeature(feature, result);    
            	}
				
			}else{
                // Otherwise, it is a feature
			    parent = (ObjectInfoWrapper)entityInfo.peek();

                FeatureInfo info = null;
                int index = name.indexOf(XMIConstants.DELIMITER_CHAR);
                if ( index > 0 ) {
                    String metaClassName = name.substring(0,index);
                    String featurename = name.substring(index+1);                
                    info = new FeatureInfo(previousEntityInfo, featurename, metaClassName, uri);
                } else {
                    // This really shouldn't happen ...
                    info = new FeatureInfo(previousEntityInfo, name, name, uri);
                }
                previousFeatureInfo = info;
                
				ObjectInfoWrapper wrapper = new ObjectInfoWrapper(info, false, null, null);
				objectInfo.push(wrapper);
			}
			
			++depth;	
		}
		
		public void endDocument() throws SAXException {
			LogManager.logTrace(BODY_HANDLER, "End document"); //$NON-NLS-1$
			
			readerAdapter.finishDocument();		
			//DEBUG
			//System.out.println("End Document");
		}
        
        protected boolean isEntity(String uri, String name, String qName, Attributes atts){
            return ( (depth + 2) % 2 == 0 );
        }
	}
	
	/**
	 * Constructor: initializes two inner classes
	 */
	public XMIContentHandler(XMIReaderAdapter adapter){
		readerAdapter = adapter;
		
		try{
			headerHandler = new HeaderHandler();
		}catch(Exception e){
			e.printStackTrace();
		}
		bodyHandler = new BodyHandler(adapter);
		
		//The currentHandler will start as the header handler and will be switched to the
		//body handler when the end tag for the header is reached.
		currentHandler = headerHandler;
		
		//Initialize the record object stacks
		entityInfo = new Stack();
		objectInfo = new Stack();
	}
	
	/**
	 * Method to set the XMIHeader attribute.
	 * This method will create an EntityInfo object for the model.
	 * Called when the HeaderContentHandler finishes processing the XMIHeader.
	 * @param XMIHeader header, the XMIHeader
	 */ 
	public void setXMIHeader(XMIHeader header){
	    //Debug
	    //System.out.println("Setting Header");
    	
    	readerAdapter.setHeader(header);
		
		//Switch the currentHandler to the body handler.
		currentHandler = bodyHandler;
	}
		
		 
	////////////////////////////////////////////////////////////////////
	// Event handlers.
	////////////////////////////////////////////////////////////////////
	
	/** 
	 * Called by the SAXParser when it encounters character data while
	 * processing an element.
	 * @param ch The characters.
     * @param start The start position in the character array.
     * @param length The number of characters to use from the
     *               character array.
     * @exception org.xml.sax.SAXException Any SAX exception, possibly
     *            wrapping another exception.
     */ 
	public void characters(char[] characters, int start, int length) throws SAXException{
	    LogManager.logTrace(XMI_CONTENT_HANDLER, "Process characters"); //$NON-NLS-1$
	    
	    currentHandler.characters(characters, start, length);
	}

    /**
     * <p>
     * Capture ignorable whitespace as text.  If
     * setIgnoringElementContentWhitespace(true) has been called then this
     * method does nothing.
     * </p>
     *
     * @param ch <code>[]</code> - char array of ignorable whitespace
     * @param start <code>int</code> - starting position within array
     * @param length <code>int</code> - length of whitespace after start
     * @throws SAXException when things go wrong
     */
    public void ignorableWhitespace(char[] characters, int start, int length) throws SAXException {
        LogManager.logTrace(XMI_CONTENT_HANDLER, "ignorableWhitespace"); //$NON-NLS-1$
        if (ignoringWhite) return;
        if (length == 0) return;

        currentHandler.ignorableWhitespace(characters, start, length);
    }

	/**
	 * Called by the SAXParser when it starts processing the Document
	 */ 
	public void startDocument() throws SAXException {
		LogManager.logTrace(XMI_CONTENT_HANDLER, "Start document"); //$NON-NLS-1$
		
		//DEBUG
		//System.out.println("Start Document");
	}
	
	/**
	 * Called by the SAXParser when it reaches the end of the Document
	 */ 
	public void endDocument() throws SAXException {
		LogManager.logTrace(XMI_CONTENT_HANDLER, "End document"); //$NON-NLS-1$
		
		currentHandler.endDocument();		
		//DEBUG
		//System.out.println("End Document");
	}
	
	/**
	 * Called by the SAXParser when it reaches an end Element tag
	 * @param String uri of the element
	 * @param String name of the element
	 * @param String qName of the element
	 */ 
	public void endElement(String uri, String name, String qName) throws SAXException {
		if ("".equals(uri)) { //$NON-NLS-1$
			LogManager.logTrace(XMI_CONTENT_HANDLER, "End element: " + qName); //$NON-NLS-1$
        } else {
			LogManager.logTrace(XMI_CONTENT_HANDLER, "End element:   {" + uri + "}" + name); //$NON-NLS-1$ //$NON-NLS-2$
        }
		
		if(XMI_CONTENT.equals(name) ){
		    depth--;
		    return;
		}
		
		currentHandler.endElement(uri, name, qName);
	}
	
	/**
	 * Called by the SAXParser when it reaches an start Element tag
	 * @param String uri of the element
	 * @param String name of the element
	 * @param String qName of the element
	 * @param Attributes atts of the elements
	 */ 
	public void startElement(String uri, String name, String qName, Attributes atts) throws SAXException {
		if ("".equals(uri)) //$NON-NLS-1$
			LogManager.logTrace(XMI_CONTENT_HANDLER, "Start element: " + qName); //$NON-NLS-1$
		else
			LogManager.logTrace(XMI_CONTENT_HANDLER, "Start element: {" + uri + "}" + name); //$NON-NLS-1$ //$NON-NLS-2$
		
		if(XMI_CONTENT.equals(name) ){
		    depth++;
		    return;
		}
		
		currentHandler.startElement(uri, name, qName, atts);
	}
	
    /**
     * Utility method to use SAX and this ContentHandler to read in XMI from the specified stream.
     */
    public static void read( InputStream stream, MessageList messages, XMIReaderAdapter adapter, boolean closeStream ) throws SAXException, IOException {
        try {

            //Create the SAX input source, parser and handler
            InputSource source = new InputSource(stream);
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            SAXParser parser = spf.newSAXParser();
            
            DefaultHandler handler = new XMIContentHandler(adapter);

            parser.parse(source, handler);
        } catch (SAXException e) {
            messages.add( new DefaultMessage("Unable to process XMI from the input stream",MessageTypes.ERROR_MESSAGE) ); //$NON-NLS-1$
            throw e;
        } catch (ParserConfigurationException e) {
        	messages.add( new DefaultMessage("Unable to process XMI from the input stream",MessageTypes.ERROR_MESSAGE) ); //$NON-NLS-1$
        	throw new SAXException(e);
		} finally {
            if (stream != null && closeStream) {
                try {
                    stream.close();
                } catch (IOException e) {
                    messages.add( new DefaultMessage("Failed to close FileInputStream",MessageTypes.ERROR_MESSAGE) ); //$NON-NLS-1$
                    throw e;
                }
            }
        }
    }
    
    // Support method for fast string normalization
    // Per XML 1.0 Production 3 whitespace includes: #x20, #x9, #xD, #xA
    private static String normalizeString(String value) {
        char[] c = value.toCharArray();
        char[] n = new char[c.length];
        boolean white = true;
        int pos = 0;
        for (int i = 0; i < c.length; i++) {
            if (" \t\n\r".indexOf(c[i]) != -1) { //$NON-NLS-1$
                if (!white) {
                    n[pos++] = ' ';
                    white = true;
                }
            }
            else {
                n[pos++] = c[i];
                white = false;
            }
        }
        if (white && pos > 0) {
            pos--;
        }
        return new String(n, 0, pos);
    }

}

