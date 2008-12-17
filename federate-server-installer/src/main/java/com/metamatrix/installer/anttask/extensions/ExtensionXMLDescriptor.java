/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package com.metamatrix.installer.anttask.extensions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.ExtensionModuleTypes;
import com.metamatrix.common.extensionmodule.spi.jdbc.JDBCExtensionDescriptor;
import com.metamatrix.common.xml.XMLReaderWriter;
import com.metamatrix.common.xml.XMLReaderWriterImpl;
import com.metamatrix.core.util.Assertion;

/**
 * The ExtensionXMLDescriptor is used for processing extension modules in bulk.
 * This is used to write out the descriptor information for "n" number
 * of ExtensionModuleDescriptor to an xml file and read that xml file in when the extensions
 * are being imported in bulk.
 * 
 * This use of this class should be done in one pass.
 * For export extension modules, do the following:
 * <li>first, call @see #initForExport(String, String)</li>
 * <li>next, call @see #addForExport(ExtensionModuleDescriptor) once for each extension to be exported</li>
 * <li>last, call @see #write() to complete the writing of the xml file</li>
 * 
 * To import extension modules, do the following:
 * <li> call @see #read(String, String)</li>
 * 
 * @since 4.3
 */
public class ExtensionXMLDescriptor {

    private String exportFileName;
    private Element root = null;
    private Document doc = null;
    
    
/*
 * Returns a Map
 * Key --> filename --> ExtensionModuleDescriptor
 * 
 * The filename will provide the mapping from the filesystem to the extension module.  Because when it was exported,
 * the filename was kept to track it.
 */
    public Map read(String xmlFile) {
        Assertion.isNotNull(xmlFile);
        // the xmlFile should be the full path to the file, if not then it looks in the path and that location becomes
        // the default directory to find the extensions.
        Map extensionModules = new HashMap();
        
        Document doc = null;
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(xmlFile);
            doc = getXMLReaderWriter().readDocument(fis);
            Element root = doc.getRootElement();
            
            List extMods = root.getChildren(ELEMENTS.Extension.ELEMENT);
            if (extMods == null) {
                
                return extensionModules;
            }
            Iterator iterator = extMods.iterator();
            while (iterator.hasNext()) {
                Element connElement = (Element)iterator.next();
                createExtensionDescriptor(extensionModules, connElement);
            
            }
            
        } catch (IOException err) {
        } catch (JDOMException err) {
        } catch (MetaMatrixException err) {
        }finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (Exception e) {
                    
                }
            }
        }    

        
        
        return extensionModules;

    }    
    
    public void initForExport(String descriptorXMLFileName) {
        Assertion.isNotNull(descriptorXMLFileName);
        this.exportFileName = descriptorXMLFileName;
        
        root = createRootElement();

        // create a new Document with a root element
        doc = new Document(root);

    }
    
    public String addForExport(ExtensionModuleDescriptor extension) {
        String fileName = null;
        if (extension.getType() == ExtensionModuleTypes.CONFIGURATION_MODEL_TYPE) {
            if (extension.getName().equalsIgnoreCase(Configuration.NEXT_STARTUP)) {
                fileName = "config_nextstartup.xml"; //$NON-NLS-1$
            } else {
                fileName = "config_startup.xml"; //$NON-NLS-1$
            }
        } else {
            fileName = extension.getName();
        }

        Element ext = createExtensionElement(extension, fileName);
        root.addContent(ext);
        
        return fileName;
    }
    


    public void write() {
        FileOutputStream fos = null;

        try {
            File f = new File(this.exportFileName);
            if (f.exists()) {
                f.delete();
            }
            fos = new FileOutputStream(f);
        
            getXMLReaderWriter().writeDocument(doc, fos);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException err) {
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (Exception e) {
                    
                }
            }
        }            
    }


    private XMLReaderWriter getXMLReaderWriter() {

        return new XMLReaderWriterImpl();
    }

    private Element createRootElement() {
        return new Element(ELEMENTS.ROOT_ELEMENT);

    }


    /**
     * @return a JDOM XML Element
     */
    private void createExtensionDescriptor(Map extensionMap, Element extension) throws MetaMatrixException {
        Assertion.isNotNull(extension);
                
        JDBCExtensionDescriptor ed = new JDBCExtensionDescriptor();
 
        String name = extension.getAttributeValue(ELEMENTS.Extension.Attributes.NAME);
        String type = extension.getAttributeValue(ELEMENTS.Extension.Attributes.TYPE);
        
        ed.setName(name);
        ed.setType(type);
        
        ExtensionModuleTypes.checkTypeIsValid(type);
        
        String extfileName = extension.getChildText(ELEMENTS.Extension.Properties.FILE_NAME);
        if (extfileName == null || extfileName.length() == 0) {
            extfileName = name;
        }
        String desc = extension.getChildText(ELEMENTS.Extension.Properties.DESCRIPTION);
        if (desc != null) {
            desc = name;
        }
        
        ed.setDescription(desc);

        addChangeHistoryFromElement(extension, ed);
        
        extensionMap.put(extfileName, ed);
    }
    
    /**
     * @return a JDOM XML Element
     */
    private Element createExtensionElement(ExtensionModuleDescriptor extension, String fileName) {
        Assertion.isNotNull(extension);

        Element extElement = new Element(ELEMENTS.Extension.ELEMENT);

        extElement.setAttribute(ELEMENTS.Extension.Attributes.NAME, extension.getName());
        extElement.setAttribute(ELEMENTS.Extension.Attributes.TYPE, extension.getType());
       
        addPropertyElement(extElement, ELEMENTS.Extension.Properties.FILE_NAME, fileName);

        addPropertyElement(extElement, ELEMENTS.Extension.Properties.DESCRIPTION, extension.getDescription());
        addChangeHistoryToElement(extension, extElement);

        return extElement;
    }

    private void addChangeHistoryFromElement(Element extension, JDBCExtensionDescriptor obj) {

        
        String lcby = extension.getChildText(ELEMENTS.Extension.Properties.LAST_CHANGED_BY);
        if (lcby != null) {obj.setLastUpdatedBy(lcby); }
        
        String lcbydate = extension.getChildText(ELEMENTS.Extension.Properties.LAST_CHANGED_DATE);
        if (lcbydate != null) {obj.setLastUpdatedDate(lcbydate); }

        String cby = extension.getChildText(ELEMENTS.Extension.Properties.CREATED_BY);
        if (cby != null) {obj.setCreatedBy(cby); }

        String cbydate = extension.getChildText(ELEMENTS.Extension.Properties.CREATION_DATE);
        if (cbydate != null) {obj.setCreationDate(cbydate); }

        
    }

    private void addChangeHistoryToElement(ExtensionModuleDescriptor obj,
                                         Element element) {

        String lastChangedBy = null;
        String lastChangedDate = null;
        String createdDate = null;
        String createdBy = null;

        lastChangedBy = obj.getLastUpdatedBy();
        lastChangedDate = obj.getLastUpdatedDate();

        createdBy = obj.getCreatedBy();
        createdDate = obj.getCreationDate();

        if (lastChangedBy == null || lastChangedBy.trim().length() == 0) {

        } else {
            addPropertyElement(element, ELEMENTS.Extension.Properties.LAST_CHANGED_BY, lastChangedBy);
        }

        if (lastChangedDate == null) {

        } else {
            addPropertyElement(element, ELEMENTS.Extension.Properties.LAST_CHANGED_DATE, lastChangedDate);

        }

        if (createdBy == null || createdBy.trim().length() == 0) {
        } else {
            addPropertyElement(element, ELEMENTS.Extension.Properties.CREATED_BY, createdBy);
        }

        if (createdDate == null) {
        } else {
            addPropertyElement(element, ELEMENTS.Extension.Properties.CREATION_DATE, createdDate);
        }

    }

    private void addPropertyElement(Element element,
                                    String propName,
                                    String propValue) {
        Element property = new Element(propName);
        property.setText(propValue);
        element.addContent(property);
    }

    private interface ELEMENTS {

        /**
         * This should be the root Element name for the XML Descriptor Document.
         */
        public static final String ROOT_ELEMENT = "ExtensionModuleDocument"; //$NON-NLS-1$

        public static class Extension {

            public static final String ELEMENT = "Extension"; //$NON-NLS-1$

            public static class Attributes {

                public static final String NAME = "Name"; //$NON-NLS-1$
                public static final String TYPE = "Type"; //$NON-NLS-1$
            }

            public static class Properties {

                public static final String FILE_NAME = "FileName"; //$NON-NLS-1$
                public static final String DESCRIPTION = "Description"; //$NON-NLS-1$
                public static final String LAST_CHANGED_DATE = "LastChangedDate"; //$NON-NLS-1$
                public static final String LAST_CHANGED_BY = "LastChangedBy"; //$NON-NLS-1$
                public static final String CREATION_DATE = "CreationDate"; //$NON-NLS-1$
                public static final String CREATED_BY = "CreatedBy"; //$NON-NLS-1$
            }
        }

    }

}
