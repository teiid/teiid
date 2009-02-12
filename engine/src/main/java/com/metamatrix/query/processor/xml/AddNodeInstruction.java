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

import java.util.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.LogConstants;

/**
 */
public class AddNodeInstruction extends ProcessorInstruction {

    public static final boolean ELEMENT = true;
    public static final boolean ATTRIBUTE = false;

    private NodeDescriptor descriptor;
    
    private ElementSymbol symbol;
    private NodeDescriptor nillableDescriptor;

    /**
     * Constructor for AddElementInstruction.
     * @param tag Tag name
     * @param namespacePrefix String prefix that maps to a namespace (optional, may be null)
     * @param isElement indicates an element or an attribute
     * @param fixedValue
     * @param namespaceDeclarations
     * @param isOptional
     */
    public AddNodeInstruction(NodeDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /**
     * Constructor for AddElementInstruction.
     * @param tag Tag name
     * @param namespacePrefix String prefix that maps to a namespace (optional, may be null)
     * @param isElement indicates an element or an attribute
     * @param resultSetName
     * @param resultSetColumn
     * @param type
     * @param defaultValue
     * @param namespaceDeclarations
     * @param isOptional
     */
    public AddNodeInstruction(NodeDescriptor descriptor, ElementSymbol symbol) {
        this.descriptor = descriptor;
        this.symbol = symbol;
        
    }

    /**
     * Outputs an element or an attribute, or nothing, based on the state of the instruction.
     * @see ProcessorInstruction#process(ProcessorEnvironment)
     */
    public XMLContext process(XMLProcessorEnvironment env, XMLContext context)
    throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException{

        DocumentInProgress doc = env.getDocumentInProgress();
        boolean success = true;
        boolean isElement = descriptor.isElement();
        String defaultValue = descriptor.getDefaultValue();
        
        if(symbol == null) { 
            if (defaultValue != null){
                if(isElement) {
                    success = doc.addElement(descriptor, defaultValue);
                    LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"TAG elem",descriptor.getName(),"fixed value",defaultValue}); //$NON-NLS-1$ //$NON-NLS-2$
                } else {
                    success = doc.addAttribute(descriptor, defaultValue);    
                    LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"TAG attr",descriptor.getName(),"fixed value",defaultValue}); //$NON-NLS-1$ //$NON-NLS-2$
                }
            } else {
                if(isElement) {
                    success = doc.addElement(descriptor, nillableDescriptor);
                    LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"TAG elem",descriptor.getName()}); //$NON-NLS-1$
                } //else nothing gets outputted for attribute with no content
            } 
        } else {
            Object value = context.getVariableContext().getValue(symbol);
            String valueStr = XMLValueTranslator.translateToXMLValue(value, descriptor.getRuntimeType(), descriptor.getDocBuiltInType());

            if (valueStr != null){
                if(isElement) {
                    success = doc.addElement(descriptor, valueStr);         
                    LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"TAG elem",descriptor.getName(),"value",valueStr}); //$NON-NLS-1$ //$NON-NLS-2$
                } else {
                    success = doc.addAttribute(descriptor, valueStr);    
                    LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"TAG attr",descriptor.getName(),"value",valueStr}); //$NON-NLS-1$ //$NON-NLS-2$
                }
            } else {
                if (defaultValue != null){
                    if(isElement) {
                        success = doc.addElement(descriptor, defaultValue);         
	                    LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"TAG elem",descriptor.getName(),"default value",defaultValue}); //$NON-NLS-1$ //$NON-NLS-2$
                    } else {
                        success = doc.addAttribute(descriptor, defaultValue);    
                        LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"TAG attr",descriptor.getName(),"default value",defaultValue}); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                } else {
                    if(isElement) {
                        success = doc.addElement(descriptor, nillableDescriptor);
                        LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"TAG elem",descriptor.getName(),"no value"}); //$NON-NLS-1$ //$NON-NLS-2$
                    } //else nothing gets outputted for attribute with no content
                } 
            }
        }
        
        if (!success){
            String elem = (isElement ? QueryExecPlugin.Util.getString("AddNodeInstruction.element__1" ) : QueryExecPlugin.Util.getString("AddNodeInstruction.attribute__2")); //$NON-NLS-1$ //$NON-NLS-2$
            Object[] params = new Object[]{elem, this.descriptor.getQName(), this.descriptor.getNamespaceURI(), this.descriptor.getNamespaceURIs()};
            String msg = QueryExecPlugin.Util.getString("AddNodeInstruction.Unable_to_add_xml_{0}_{1},_namespace_{2},_namespace_declarations_{3}_3", params); //$NON-NLS-1$
            throw new MetaMatrixComponentException(msg);
        }
        
        env.incrementCurrentProgramCounter();
        return context;
    }
    
    public String toString() {
        StringBuffer str = new StringBuffer();
        if(descriptor.isElement()) { 
            str.append("ELEM "); //$NON-NLS-1$
        } else {
            str.append("ATTR "); //$NON-NLS-1$
        }

        str.append(descriptor.getQName());
        
        if(symbol != null) {             
            str.append(" "); //$NON-NLS-1$
            str.append(symbol);
        }
        
        if (descriptor.getDefaultValue() != null){
            str.append(" (default " + descriptor.getDefaultValue() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        } 

        Properties namespaceDeclarations = descriptor.getNamespaceURIs();
        if (namespaceDeclarations != null){
            str.append(" (namespaces "); //$NON-NLS-1$
            Enumeration e = namespaceDeclarations.propertyNames();
            while (e.hasMoreElements()){
                String prefix = (String)e.nextElement();
                str.append(prefix);
                str.append("=>"); //$NON-NLS-1$
                str.append(namespaceDeclarations.getProperty(prefix));
                if (e.hasMoreElements()) str.append(", "); //$NON-NLS-1$
            }
            str.append(")"); //$NON-NLS-1$
        }
    
        return str.toString();
    }
    
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        if(descriptor.isElement()) {
            props.put(PROP_TYPE, "ADD ELEMENT"); //$NON-NLS-1$                    
        } else {
            props.put(PROP_TYPE, "ADD ATTRIBUTE"); //$NON-NLS-1$                    
        }

        props.put(PROP_TAG, this.descriptor.getName());
        
        if(descriptor.isOptional()) {
            props.put(PROP_OPTIONAL, ""+descriptor.isOptional()); //$NON-NLS-1$            
        }
        
        if(this.symbol != null) {
            props.put(PROP_DATA_COL, this.symbol); 
        }

        if(descriptor.getNamespacePrefix() != null) {
            props.put(PROP_NAMESPACE, descriptor.getNamespacePrefix());
        }

        Properties namespaceDeclarations = descriptor.getNamespaceURIs();
        if(namespaceDeclarations != null) {
            List nsDecl = new ArrayList(namespaceDeclarations.size());
            Enumeration e = namespaceDeclarations.propertyNames();
            while (e.hasMoreElements()){
                String prefix = (String)e.nextElement();
                String ns = namespaceDeclarations.getProperty(prefix);
                nsDecl.add(prefix + "=\"" + ns + "\""); //$NON-NLS-1$ //$NON-NLS-2$
            }
            props.put(PROP_NAMESPACE_DECL, nsDecl);
        }

        if(descriptor.getDefaultValue() != null) {
            props.put(PROP_DEFAULT, descriptor.getDefaultValue()); 
        }

        return props;
    }
    
    /** 
     * @param nillableDescriptor The nillableDescriptor to set.
     * @since 4.3
     */
    public void setNillableDescriptor(NodeDescriptor nillableDescriptor) {
        this.nillableDescriptor = nillableDescriptor;
    }
    
}
