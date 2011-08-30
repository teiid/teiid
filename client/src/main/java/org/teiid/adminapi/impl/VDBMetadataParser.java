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
package org.teiid.adminapi.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.adminapi.impl.ModelMetaData.ValidationError;
import org.teiid.adminapi.impl.VDBMetaData;

@SuppressWarnings("nls")
public class VDBMetadataParser {

	public static VDBMetaData unmarshell(InputStream content) throws XMLStreamException {
		 XMLInputFactory inputFactory=XMLInputFactory.newInstance();
		 XMLStreamReader reader = inputFactory.createXMLStreamReader(content);

        // elements
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
			case VDB:
				VDBMetaData vdb = new VDBMetaData();
				Properties props = getAttributes(reader);
				vdb.setName(props.getProperty(Element.NAME.getLocalName()));			
				vdb.setVersion(Integer.parseInt(props.getProperty(Element.VERSION.getLocalName())));
				parseVDB(reader, vdb);
				return vdb;
             default: 
                throw new XMLStreamException("Unexpected element '" + reader.getName() + "' encountered", reader.getLocation()); 
            }
        }
		return null;
	}

	private static void parseVDB(XMLStreamReader reader, VDBMetaData vdb) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
			case DESCRIPTION:
				vdb.setDescription(reader.getElementText());
				break;
			case PROPERTY:
		    	parseProperty(reader, vdb);
				break;
			case MODEL:
				ModelMetaData model = new ModelMetaData();
				parseModel(reader, model);
				vdb.addModel(model);
				break;
			case TRANSLATOR:
				VDBTranslatorMetaData translator = new VDBTranslatorMetaData();
				parseTranslator(reader, translator);
				vdb.addOverideTranslator(translator);
				break;
			case DATA_ROLE:
				DataPolicyMetadata policy = new DataPolicyMetadata();
				parseDataRole(reader, policy);
				vdb.addDataPolicy(policy);
				break;
			case ENTRY:
				// this is designer specific.
				break;
             default: 
                throw new XMLStreamException("Unexpected element '" + reader.getName() + "' encountered", reader.getLocation()); 
            }
        }		
	}

	private static void parseProperty(XMLStreamReader reader, AdminObjectImpl anObj)
			throws XMLStreamException {
		if (reader.getAttributeCount() > 0) {
			String key = null;
			String value = null;
			for(int i=0; i<reader.getAttributeCount(); i++) {
				String attrName = reader.getAttributeLocalName(i);
				String attrValue = reader.getAttributeValue(i);
				if (attrName.equals(Element.NAME.getLocalName())) {
					key = attrValue;
				}
				if (attrName.equals(Element.VALUE.getLocalName())) {
					value = attrValue;
				}		    			
			}
			anObj.addProperty(key, value);
		}
		while(reader.nextTag() != XMLStreamConstants.END_ELEMENT);
	}
	
	private static void parseDataRole(XMLStreamReader reader, DataPolicyMetadata policy) throws XMLStreamException {
		Properties props = getAttributes(reader);
		policy.setName(props.getProperty(Element.NAME.getLocalName()));
		policy.setAnyAuthenticated(Boolean.parseBoolean(props.getProperty(Element.DATA_ROLE_ANY_ATHENTICATED_ATTR.getLocalName())));
		policy.setAllowCreateTemporaryTables(Boolean.parseBoolean(props.getProperty(Element.DATA_ROLE_ALLOW_TEMP_TABLES_ATTR.getLocalName())));
		
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
			case DESCRIPTION:
				policy.setDescription(reader.getElementText());
				break;            
			case PERMISSION:
				PermissionMetaData permission = new PermissionMetaData();
				parsePermission(reader, permission);
				policy.addPermission(permission);
				break;
			case MAPPED_ROLE_NAME:
				policy.addMappedRoleName(reader.getElementText());
				break;
             default: 
                throw new XMLStreamException("Unexpected element '" + reader.getName() + "' encountered", reader.getLocation()); 
            }
        }		
	}	
	
	private static void parsePermission(XMLStreamReader reader, PermissionMetaData permission) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
			case RESOURCE_NAME:
				permission.setResourceName(reader.getElementText());
				break;            
			case ALLOW_ALTER:
				permission.setAllowAlter(Boolean.parseBoolean(reader.getElementText()));
				break;
			case ALLOW_CREATE:
				permission.setAllowCreate(Boolean.parseBoolean(reader.getElementText()));
				break;
			case ALLOW_DELETE:
				permission.setAllowDelete(Boolean.parseBoolean(reader.getElementText()));
				break;
			case ALLOW_EXECUTE:
				permission.setAllowExecute(Boolean.parseBoolean(reader.getElementText()));
				break;
			case ALLOW_READ:
				permission.setAllowRead(Boolean.parseBoolean(reader.getElementText()));
				break;
			case ALLOW_UPADTE:
				permission.setAllowUpdate(Boolean.parseBoolean(reader.getElementText()));
				break;				

             default: 
                throw new XMLStreamException("Unexpected element '" + reader.getName() + "' encountered", reader.getLocation()); 
            }
        }		
	}	
	
	private static void parseTranslator(XMLStreamReader reader, VDBTranslatorMetaData translator) throws XMLStreamException {
		Properties props = getAttributes(reader);
		translator.setName(props.getProperty(Element.NAME.getLocalName()));
		translator.setType(props.getProperty(Element.TYPE.getLocalName()));
		translator.setDescription(props.getProperty(Element.DESCRIPTION.getLocalName()));
		
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
			case PROPERTY:
				parseProperty(reader, translator);
				break;
             default: 
                throw new XMLStreamException("Unexpected element '" + reader.getName() + "' encountered", reader.getLocation()); 
            }
        }		
	}	
	
	private static void parseModel(XMLStreamReader reader, ModelMetaData model) throws XMLStreamException {
		Properties props = getAttributes(reader);
		model.setName(props.getProperty(Element.NAME.getLocalName()));
		model.setModelType(Model.Type.valueOf(props.getProperty(Element.TYPE.getLocalName(), "PHYSICAL")));
		model.setVisible(Boolean.parseBoolean(props.getProperty(Element.VISIBLE.getLocalName(), "true")));
		model.setPath(props.getProperty(Element.PATH.getLocalName()));
		
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
			case DESCRIPTION:
				model.setDescription(reader.getElementText());
				break;
			case PROPERTY:
				parseProperty(reader, model);
				break;
			case SOURCE:
				Properties sourceProps = getAttributes(reader);
				String name = sourceProps.getProperty(Element.NAME.getLocalName());
				String translatorName = sourceProps.getProperty(Element.SOURCE_TRANSLATOR_NAME_ATTR.getLocalName());
				String connectionName = sourceProps.getProperty(Element.SOURCE_CONNECTION_JNDI_NAME_ATTR.getLocalName());
				model.addSourceMapping(name, translatorName, connectionName);
				while(reader.nextTag() != XMLStreamConstants.END_ELEMENT);
				break;
			case VALIDATION_ERROR:
				Properties validationProps = getAttributes(reader);
				String msg =  reader.getElementText();
				String severity = validationProps.getProperty(Element.VALIDATION_SEVERITY_ATTR.getLocalName());
				String path = validationProps.getProperty(Element.PATH.getLocalName());
				ValidationError ve = new ValidationError(severity, msg);
				ve.setPath(path);
				model.addError(ve);
				break;
             default: 
                throw new XMLStreamException("Unexpected element '" + reader.getName() + "' encountered", reader.getLocation()); 
            }
        }		
	}	
	
	
	private static Properties getAttributes(XMLStreamReader reader) {
		Properties props = new Properties();
    	if (reader.getAttributeCount() > 0) {
    		for(int i=0; i<reader.getAttributeCount(); i++) {
    			String attrName = reader.getAttributeLocalName(i);
    			String attrValue = reader.getAttributeValue(i);
    			props.setProperty(attrName, attrValue);
    		}
    	}
    	return props;
	}	
	
	enum Element {
	    // must be first
	    UNKNOWN(null),
	    VDB("vdb"),
	    NAME("name"),
	    VERSION("version"),
	    DESCRIPTION("description"),
	    PROPERTY("property"),
	    VALUE("value"),
	    MODEL("model"),
	    TYPE("type"),
	    VISIBLE("visible"),
	    PATH("path"),
	    SOURCE("source"),
	    SOURCE_TRANSLATOR_NAME_ATTR("translator-name"),
	    SOURCE_CONNECTION_JNDI_NAME_ATTR("connection-jndi-name"),
	    VALIDATION_ERROR("validation-error"),
	    VALIDATION_SEVERITY_ATTR("severity"),
	    TRANSLATOR("translator"),
	    DATA_ROLE("data-role"),
	    DATA_ROLE_ANY_ATHENTICATED_ATTR("any-authenticated"),
	    DATA_ROLE_ALLOW_TEMP_TABLES_ATTR("allow-create-temporary-tables"),
	    PERMISSION("permission"),
	    RESOURCE_NAME("resource-name"),
	    ALLOW_CREATE("allow-create"),
	    ALLOW_READ("allow-read"),
	    ALLOW_UPADTE("allow-update"),
	    ALLOW_DELETE("allow-delete"),
	    ALLOW_EXECUTE("allow-execute"),
	    ALLOW_ALTER("allow-alyer"),
	    MAPPED_ROLE_NAME("mapped-role-name"),
	    ENTRY("entry");
	    
	    private final String name;

	    Element(final String name) {
	        this.name = name;
	    }

	    /**
	     * Get the local name of this element.
	     *
	     * @return the local name
	     */
	    public String getLocalName() {
	        return name;
	    }

	    private static final Map<String, Element> elements;

	    static {
	        final Map<String, Element> map = new HashMap<String, Element>();
	        for (Element element : values()) {
	            final String name = element.getLocalName();
	            if (name != null) map.put(name, element);
	        }
	        elements = map;
	    }

	    public static Element forName(String localName) {
	        final Element element = elements.get(localName);
	        return element == null ? UNKNOWN : element;
	    }	    
	}	
}
