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

package com.metamatrix.common.vdb.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.config.xml.XMLConfig_ElementNames;
import com.metamatrix.common.config.xml.XMLHelperImpl;
import com.metamatrix.common.xml.XMLReaderWriter;
import com.metamatrix.common.xml.XMLReaderWriterImpl;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.vdb.runtime.BasicModelInfo;
import com.metamatrix.vdb.runtime.BasicVDBDefn;


public class DEFReaderWriter {
    private static final String TRUE = "true"; //$NON-NLS-1$
	private final static String UNKNOWN = "Unknown"; //$NON-NLS-1$
    
    public BasicVDBDefn read(InputStream defStream) throws IOException {
    	BasicVDBDefn vdbDefn = null;
        try {
        	XMLReaderWriter reader = new XMLReaderWriterImpl();
            Document doc = reader.readDocument(defStream);
            Element root = doc.getRootElement();
            
            vdbDefn = new BasicVDBDefn(UNKNOWN);

            // load the Header section
            loadHeaderSection(vdbDefn, root);
            
            // load VDBInfo section
            loadVDBInfoSection(vdbDefn, root);
            
            // add Models section
            loadModelsSection(vdbDefn, root);
            
            // add Connector types
            loadConnectorTypes(vdbDefn, root);
            
            // load Connector Bindings
            loadConnectorBindings(vdbDefn, root);
            
        } catch (JDOMException e) {
            throw new IOException(CommonPlugin.Util.getString("VDBDefnXMLHelper.Unable_to_read_defn_file"));//$NON-NLS-1$
        } 
        return vdbDefn;
    }
    
    private void loadHeaderSection(BasicVDBDefn vdbDefn, Element root) {
    	Element headElement = root.getChild(Header.ELEMENT);
        if (headElement == null) {
        	headElement = new Element(Header.ELEMENT);
        }
    	
        Properties header = new Properties();
        String createdBy = headElement.getChildText(Header.APPLICATION_CREATED_BY);
        String applicationVersion = headElement.getChildText(Header.APPLICATION_VERSION);
        String systemVersion = headElement.getChildText(Header.SYSTEM_VERSION);
        String userCreatedBy = headElement.getChildText(Header.USER_CREATED_BY);
        String vdbExporterVersion = headElement.getChildText(Header.VDB_EXPORTER_VERSION);
        String modificationTime = headElement.getChildText(Header.MODIFICATION_TIME);
        
        header.setProperty(Header.APPLICATION_CREATED_BY, createdBy!=null?createdBy:UNKNOWN);
        header.setProperty(Header.APPLICATION_VERSION, applicationVersion!=null?applicationVersion:UNKNOWN);
        header.setProperty(Header.SYSTEM_VERSION, systemVersion!=null?systemVersion:UNKNOWN);
        header.setProperty(Header.USER_CREATED_BY, userCreatedBy!=null?userCreatedBy:UNKNOWN);
        header.setProperty(Header.VDB_EXPORTER_VERSION, vdbExporterVersion!=null?vdbExporterVersion:UNKNOWN);
        header.setProperty(Header.MODIFICATION_TIME, modificationTime!=null?modificationTime:UNKNOWN);
        vdbDefn.setHeaderProperties(header);
        
        // now place them in the defn too
        vdbDefn.setCreatedBy(headElement.getChildText(Header.USER_CREATED_BY));
        
        boolean useDefault = modificationTime == null; 
        if (!useDefault) {
	        try {
				vdbDefn.setDateCreated(new SimpleDateFormat().parse(modificationTime));
			} catch (ParseException e) {
				useDefault = true;
			}
        }
        if (useDefault) {
        	vdbDefn.setDateCreated(Calendar.getInstance().getTime());
        }
    }
    
    private void loadVDBInfoSection(BasicVDBDefn vdbDefn, Element root) throws IOException {
        
    	Element vdbInfoElement = root.getChild(VDBInfo.ELEMENT);
        if (vdbInfoElement == null) {
            throw new IOException("VDBDefnXMLHelper.Invalid_xml_section"); //$NON-NLS-1$
        }

        Properties vdbProps = getElementProperties(vdbInfoElement);
        if (vdbProps == null || vdbProps.isEmpty()) {
            throw new IOException("VDBDefnXMLHelper.No_properties_defined_to_create_defn"); //$NON-NLS-1$
        }
        
        vdbDefn.setName((String)vdbProps.remove(VDBInfo.NAME));
        String version = (String)vdbProps.remove(VDBInfo.VERSION);
        vdbDefn.setVersion(version!=null?version:"1");//$NON-NLS-1$
        vdbDefn.setFileName((String)vdbProps.remove(VDBInfo.ARCHIVE_NAME));
        vdbDefn.setDescription((String)vdbProps.remove(VDBInfo.DESCRIPTION));
        vdbDefn.setUUID((String)vdbProps.remove(VDBInfo.GUID)); 
        if (TRUE.equals(vdbProps.remove(VDBInfo.ACTIVE))) { 
        	vdbDefn.setStatus(VDBStatus.ACTIVE);
        }
        else {
        	vdbDefn.setStatus(VDBStatus.INACTIVE);
        }
        vdbDefn.setInfoProperties(vdbProps);
    }
              
    private void loadModelsSection(BasicVDBDefn vdbDefn, Element root) throws IOException {
    	Collection<Element> modelsElements = root.getChildren(Model.ELEMENT);
    	for(Element modelElement:modelsElements) {
    		vdbDefn.addModelInfo(loadModel(modelElement));
        }
    }
    
    private BasicModelInfo loadModel(Element modelElement) throws IOException {
    	Properties props = getElementProperties(modelElement);
    	
    	BasicModelInfo model = new BasicModelInfo(props.getProperty(Model.NAME));
    	String visibility = props.getProperty(Model.VISIBILITY, ModelInfo.PUBLIC_VISIBILITY);
    	props.remove(Model.VISIBILITY);
    	model.setVisibility(visibility.equalsIgnoreCase(ModelInfo.PRIVATE_VISIBILITY)?ModelInfo.PRIVATE:ModelInfo.PUBLIC);
    	model.enableMutliSourceBindings(Boolean.parseBoolean(props.getProperty(Model.MULTI_SOURCE_ENABLED)));
    	props.remove(Model.MULTI_SOURCE_ENABLED);
    	model.setProperties(props);
    	
    	Element cbElement = modelElement.getChild(Model.CONNECTOR_BINDINGS_ELEMENT);
    	if (cbElement != null) {
    		Collection<Element> bindingElements = cbElement.getChildren(Model.CONNECTOR);
    		for(Element bindingElement:bindingElements) {
    			model.addConnectorBindingByName(bindingElement.getAttributeValue(Model.CONNECTOR_ATTRIBUTE_NAME));
    		}
    	}
    	return model;
    }
    
    protected Properties getElementProperties(Element rootElement) {
        Properties properties = new Properties();
        if(rootElement == null) {
            return properties;
        }
        // obtain any defaults that are defined
        List propertyElements = rootElement.getChildren(Property.ELEMENT);
        if (propertyElements != null) {
            Iterator iterator = propertyElements.iterator();
            for (int i = 1; iterator.hasNext(); i++) {
                Element element = (Element)iterator.next();
                String name = element.getAttributeValue(Property.ATTRIBUTE_NAME);
                String value = element.getAttributeValue(Property.ATTRIBUTE_VALUE);
                if (name != null && name.length() > 0) {
                    properties.setProperty(name, value);
                }
            }
        }
        return properties;
    }


    private void loadConnectorTypes (BasicVDBDefn vdbDefn, Element root) throws IOException{
        Element components = root.getChild(XMLConfig_ElementNames.ComponentTypes.ELEMENT);
        if (components == null) {
        	return;
        }
        
        // TODO: eventually we need to get rid of this below class.
        XMLHelperImpl helper = new XMLHelperImpl();
        
        try {
			List<Element> connectorTypes= components.getChildren(XMLConfig_ElementNames.ComponentTypes.ComponentType.ELEMENT);
			for(Element connectorTypeElement:connectorTypes) {
				vdbDefn.addConnectorType(helper.loadComponentType(connectorTypeElement));
			}
		} catch (InvalidConfigurationElementException e) {
			IOException ex = new IOException();
			ex.initCause(e);
			throw ex;
		}
    }
    
    private void loadConnectorBindings (BasicVDBDefn vdbDefn, Element root) throws IOException{
        Element components = root.getChild(XMLConfig_ElementNames.Configuration.ConnectorComponents.ELEMENT);
        if (components == null) {
        	return;
        }
        
        // TODO: eventually we need to get rid of this below class.
        XMLHelperImpl helper = new XMLHelperImpl();
        
        try {
			List<Element> connectorBindings= components.getChildren(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.ELEMENT);
			for(Element bindingElement:connectorBindings) {
				vdbDefn.addConnectorBinding(helper.loadConnectorBinding(bindingElement));
			}
		} catch (InvalidConfigurationElementException e) {
			IOException ex = new IOException();
			ex.initCause(e);
			throw ex;
		}
    }    
  
   
   /**
    * Write the DEF contents into given output stream 
    * @param outstream
    * @param def
    * @param headerProperties
    * @throws IOException
    */
   public void write(OutputStream outstream, BasicVDBDefn def, Properties headerProperties) throws IOException{
       
	   Element rootElement = new Element(ROOT_ELEMENT);
       Document doc = new Document(rootElement);

       // write the header properties
       rootElement.addContent(createHeaderElement(headerProperties));
       
       // write vdbinfo
       rootElement.addContent(createVDBInfoElement(def, def.getStatus()));

       // write the model elements
       Collection<ModelInfo> models = def.getModels();
       for(ModelInfo model:models) {
    	   rootElement.addContent(createModel(model));
       }
       
       // write the connector types elements
       Element componentTypesElement = new Element(XMLConfig_ElementNames.ComponentTypes.ELEMENT);
       Collection<ComponentType> connectorTypes= def.getConnectorTypes().values();
       for(ComponentType connectorType:connectorTypes) {
    	   componentTypesElement.addContent(createConnectorType(connectorType));
       }
       rootElement.addContent(componentTypesElement);
       
       // write the connector bindings elements
       Element connectorBindingsElement = new Element(XMLConfig_ElementNames.Configuration.ConnectorComponents.ELEMENT);
       Collection<ConnectorBinding> connectorBindings= def.getConnectorBindings().values();
       for(ConnectorBinding connectorBinding:connectorBindings) {
    	   connectorBindingsElement.addContent(createConnectorBinding(connectorBinding));
       }
       rootElement.addContent(connectorBindingsElement);
       
       // write doc to the stream
       new XMLReaderWriterImpl().writeDocument(doc, outstream);
   }
   
   private Element createHeaderElement(Properties props) throws IOException {
       Element headerElement = new Element(Header.ELEMENT);
       headerElement.addContent(new Element(Header.VDB_EXPORTER_VERSION).addContent(props.getProperty(Header.VDB_EXPORTER_VERSION, UNKNOWN)));
       headerElement.addContent(new Element(Header.APPLICATION_CREATED_BY).addContent(props.getProperty(Header.APPLICATION_CREATED_BY, UNKNOWN)));
       headerElement.addContent(new Element(Header.APPLICATION_VERSION).addContent(props.getProperty(Header.APPLICATION_VERSION, UNKNOWN)));
       headerElement.addContent(new Element(Header.USER_CREATED_BY).addContent(props.getProperty(Header.USER_CREATED_BY, UNKNOWN)));
       headerElement.addContent(new Element(Header.SYSTEM_VERSION).addContent(props.getProperty(Header.SYSTEM_VERSION, UNKNOWN)));
       headerElement.addContent(new Element(Header.MODIFICATION_TIME).addContent(props.getProperty(Header.MODIFICATION_TIME, UNKNOWN)));       
       return headerElement;
   }
   
   
   private Element createVDBInfoElement(BasicVDBDefn info, int status) throws IOException{
       Element vdbInfoElement = new Element(VDBInfo.ELEMENT);
       boolean valid = addPropertyElement(vdbInfoElement, VDBInfo.NAME, info.getName());
       if (valid) {
           addPropertyElement(vdbInfoElement, VDBInfo.GUID, info.getUUID());
           addPropertyElement(vdbInfoElement, VDBInfo.VERSION, info.getVersion());
           addPropertyElement(vdbInfoElement, VDBInfo.ARCHIVE_NAME, info.getFileName());           
           addPropertyElement(vdbInfoElement, VDBInfo.DESCRIPTION, info.getDescription());
           if (status == VDBStatus.ACTIVE) {
        	   addPropertyElement(vdbInfoElement, VDBInfo.ACTIVE, TRUE); 
           }
           Properties p = info.getInfoProperties();
           addPropertyElements(vdbInfoElement, p);
       }
       else {
    	   throw new IOException("Invalid DEF, No name supplied"); //$NON-NLS-1$
       }
       return vdbInfoElement;
   }

	private void addPropertyElements(Element vdbInfoElement, Properties p) {
		if (p != null) {
			for (String key : (List<String>) Collections.list(p.propertyNames())) {
				addPropertyElement(vdbInfoElement, key, p.getProperty(key));
			}
		}
	}
   
   private Element createModel(ModelInfo model) throws IOException {
       Element modelElement = new Element(Model.ELEMENT);
       boolean valid = addPropertyElement(modelElement, Model.NAME, model.getName());
       if (valid) {
    	   addPropertyElement(modelElement, Model.VISIBILITY, model.getVisibility()==ModelInfo.PRIVATE?ModelInfo.PRIVATE_VISIBILITY:ModelInfo.PUBLIC_VISIBILITY);
    	   addPropertyElement(modelElement, Model.MULTI_SOURCE_ENABLED, Boolean.toString(model.isMultiSourceBindingEnabled()));
    	   addPropertyElements(modelElement, model.getProperties());
    	   List<String> bindings = model.getConnectorBindingNames();
    	   if (bindings != null && !bindings.isEmpty()) {
    		   Element cbsElement = new Element(Model.CONNECTOR_BINDINGS_ELEMENT);
    		   for(String cbName:bindings) {
    			   Element connector = new Element(Model.CONNECTOR);
    			   connector.setAttribute(Model.CONNECTOR_ATTRIBUTE_NAME, cbName);
    			   cbsElement.addContent(connector);
    		   }
    		   modelElement.addContent(cbsElement);
    	   }
       }
	   return modelElement;
   }

   private Element createConnectorType(com.metamatrix.common.config.api.ComponentType connectorType) {
       // TODO: eventually we need to get rid of this below class.
       XMLHelperImpl helper = new XMLHelperImpl();
       return helper.createComponentTypeElement(connectorType);
   }

	private Element createConnectorBinding(ConnectorBinding binding) {
		// TODO: eventually we need to get rid of this below class.
		XMLHelperImpl helper = new XMLHelperImpl();
		return helper.createConnectorBindingElement(binding, false);
	}
   
   private boolean addPropertyElement(Element element, String name, String value) {
		if (element == null || name == null || value == null) {
			return false;
		}
		Element propElement = new Element(Property.ELEMENT);
		propElement.setAttribute(Property.ATTRIBUTE_NAME, name);
		propElement.setAttribute(Property.ATTRIBUTE_VALUE, value);
		element.addContent(propElement);
		return true;
	}
   
   static final String ROOT_ELEMENT = "VDB"; //$NON-NLS-1$
   public static class VDBInfo {
	    public static final String ELEMENT = "VDBInfo"; //$NON-NLS-1$
        public static final String NAME = "Name"; //$NON-NLS-1$
        public static final String ARCHIVE_NAME = "VDBArchiveName"; //$NON-NLS-1$
        public static final String VERSION = "Version"; //$NON-NLS-1$
        
        // Optional - defaults to VDB Name
        public static final String DESCRIPTION = "Description"; //$NON-NLS-1$
        // Optional - defaults to VDB Name
        public static final String GUID = "GUID"; //$NON-NLS-1$
        public static final String ACTIVE = "Active"; //$NON-NLS-1$
	}   
   
   public static class Property {
       public static final String ELEMENT = "Property"; //$NON-NLS-1$
       public static final String ATTRIBUTE_NAME = "Name"; //$NON-NLS-1$
       public static final String ATTRIBUTE_VALUE = "Value"; //$NON-NLS-1$
   }
   
   public static interface Header {
	   public static final String ELEMENT = "Header"; //$NON-NLS-1$
	   public static final String VDB_EXPORTER_VERSION = "VDBExporterVersion"; //$NON-NLS-1$
	   public static final String APPLICATION_CREATED_BY = "ApplicationCreatedBy"; //$NON-NLS-1$
	   public static final String APPLICATION_VERSION = "ApplicationVersion"; //$NON-NLS-1$
	   public static final String USER_CREATED_BY = "UserCreatedBy"; //$NON-NLS-1$
	   public static final String SYSTEM_VERSION = "SystemVersion"; //$NON-NLS-1$
	   public static final String MODIFICATION_TIME = "Time"; //$NON-NLS-1$	   
   }
   
   public static class Model {
		public static final String ELEMENT = "Model"; //$NON-NLS-1$
		public static final String NAME = "Name"; //$NON-NLS-1$
		
		// Optional - Default - physical=false, virtual=true
		public static final String VISIBILITY = "Visibility"; //$NON-NLS-1$
		public static final String MULTI_SOURCE_ENABLED = "MultiSourceEnabled"; //$NON-NLS-1$
		
		// Optional - no binding set
		public static final String CONNECTOR_BINDINGS_ELEMENT = "ConnectorBindings"; //$NON-NLS-1$
		public static final String CONNECTOR = "Connector"; //$NON-NLS-1$
		public static final String CONNECTOR_ATTRIBUTE_NAME = "Name"; //$NON-NLS-1$
	}   
}

