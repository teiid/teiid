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

package com.metamatrix.common.config.xml;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jdom.Element;
import org.teiid.adminapi.PropertyDefinition.RestartType;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.model.BasicComponentObject;
import com.metamatrix.common.config.model.BasicComponentType;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.BasicUtil;
import com.metamatrix.common.config.model.BasicVMComponentDefn;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.util.InvalidConfigurationElementException;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyDefinitionImpl;
import com.metamatrix.common.object.PropertyType;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.EquivalenceUtil;
/**
* This helper class is used to create JDOM XML Elements from configuration objects
* and to create Configuration objects from JDOM XML Elements.
*
*
*  NOTE - The helper cannot have any calls to LogManager because the bootstrapping of
* 		 	the CurrentConfiguration
*           uses this class and the CurrentConfiguration has to come up before
*           logging is available.
*
*/
public class XMLHelperImpl implements  ConfigurationPropertyNames  {


     /**
    * This method is used to create a Configuration JDOM Element from a
    * Configuration object.
    *
    * @param configuration the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createConfigurationElement(Configuration configuration) {
        // first we set up the organizational structure of a configuration
        Assertion.isNotNull(configuration);

        Element configElement = createComponentObjectElement(XMLConfig_ElementNames.Configuration.ELEMENT, configuration, true);
        return configElement;
    }


    /**
    * This method is used to create a DeployedComponent JDOM Element from a
    * DeployedComponent object.
    *
    * @param deployedComponent the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createDeployedServiceElement(DeployedComponent deployedComponent) {
        Assertion.isNotNull(deployedComponent);
        
        Element deployedComponentElement = createComponentObjectElement(XMLConfig_ElementNames.Configuration.DeployedService.ELEMENT, deployedComponent, true);

        return deployedComponentElement;
    }

    /**
    * This method is used to create a VMComponentDefn JDOM Element from a
    * VMComponentDefn object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createProcessElement(VMComponentDefn defn) {
        Assertion.isNotNull(defn);

        Element vmComponentDefnElement = createComponentObjectElement(XMLConfig_ElementNames.Configuration.Process.ELEMENT, defn, true);
        return vmComponentDefnElement;
    }
   

    /**
    * This method is used to create a ServiceComponentDefn JDOM Element from a
    * ServiceComponentDefn object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createServiceComponentDefnElement(ServiceComponentDefn defn) {
        Assertion.isNotNull(defn);

        Element serviceComponentDefnElement = createComponentObjectElement(XMLConfig_ElementNames.Configuration.Services.Service.ELEMENT, defn, true);
        serviceComponentDefnElement.setAttribute(XMLConfig_ElementNames.Configuration.Services.Service.Attributes.ROUTING_UUID, defn.getRoutingUUID());
        return serviceComponentDefnElement;
    }

    
    /**
     * This method is used to create a ServiceComponentDefn JDOM Element from a
     * ServiceComponentDefn object.
     *
     * @param defn the Object to be converted to a JDOM XML Element
     * @return a JDOM XML Element
     */
     public Element createAuthenticationProviderElement(AuthenticationProvider defn) {
         Assertion.isNotNull(defn);

         Element serviceComponentDefnElement = createComponentObjectElement(XMLConfig_ElementNames.Configuration.AuthenticationProviders.Provider.ELEMENT, defn, true);
          return serviceComponentDefnElement;
     }    

    
  
    /**
    * This method is used to create a ComponentType JDOM Element from a
    * ComponentType object.
    *
    * @param type the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createComponentTypeElement(ComponentType type) {
        Assertion.isNotNull(type);

        Element componentTypeElement = new Element(XMLConfig_ElementNames.ComponentTypes.ComponentType.ELEMENT);
        Iterator iterator = type.getComponentTypeDefinitions().iterator();
        while (iterator.hasNext()) {
            ComponentTypeDefn defn = (ComponentTypeDefn)iterator.next();
//            Element componentTypeDefnElement = createComponentTypeDefnElement(defn);
            Element propertyDefinitionElement = createPropertyDefinitionElement(defn.getPropertyDefinition());
//            componentTypeDefnElement.addContent(propertyDefinitionElement);
            componentTypeElement.addContent(propertyDefinitionElement);
        }
        componentTypeElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.NAME, type.getName());

        if (type.getDescription() != null) {
            componentTypeElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.DESCRIPTION, type.getDescription()); 
        }
        
        componentTypeElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.COMPONENT_TYPE_CODE, new Integer(type.getComponentTypeCode()).toString());
        componentTypeElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.DEPLOYABLE, (Boolean.valueOf(type.isDeployable())).toString());
        componentTypeElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.DEPRECATED, (Boolean.valueOf(type.isDeprecated())).toString());
        componentTypeElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.MONITORABLE, (Boolean.valueOf(type.isMonitored())).toString());
        // we only add these if they are not null
        BaseID superID = type.getSuperComponentTypeID();
        String superIDString;
        if (superID != null) {
            superIDString = superID.getName();
            componentTypeElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.SUPER_COMPONENT_TYPE, superIDString);

        }

        BaseID parentID = type.getParentComponentTypeID();
        String parentIDString;
        if (parentID!=null)     {
            parentIDString = parentID.getName();
            componentTypeElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.PARENT_COMPONENT_TYPE, parentIDString);

        }

        addChangeHistoryElement(type, componentTypeElement);
//        componentTypeElement.addContent(chgHistoryElement);


        return componentTypeElement;
    }

    /**
    * This method is used to create a PropertyDefinition JDOM Element from a
    * PropertyDefinition object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createPropertyDefinitionElement(PropertyDefinition defn) {
        Assertion.isNotNull(defn);


        Element element = new Element(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.ELEMENT);

        String name = defn.getName();
        element.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.NAME, name);

        String displayName = defn.getDisplayName();
        element.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.DISPLAY_NAME,displayName);

        
        setAttributeString(element,
                           XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.SHORT_DESCRIPTION,
                           defn.getShortDescription(), PropertyDefinitionImpl.DEFAULT_SHORT_DESCRIPTION);    

        Object value = defn.getDefaultValue();
        if (value!=null) {
            setAttributeString(element,
                               XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.DEFAULT_VALUE,
                               value.toString(), PropertyDefinitionImpl.DEFAULT_DEFAULT_VALUE);
        }
        
        PropertyType type = defn.getPropertyType();
        if (type != null) {
            setAttributeString(element,
                               XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.PROPERTY_TYPE, 
                               type.getDisplayName(), PropertyDefinitionImpl.DEFAULT_TYPE.getDisplayName());
        }


        setAttributeBoolean(element,
                XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_REQUIRED, 
                defn.isRequired(), PropertyDefinitionImpl.DEFAULT_IS_REQUIRED);

        setAttributeBoolean(element, 
                            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_EXPERT,
                            defn.isExpert(), PropertyDefinitionImpl.DEFAULT_IS_EXPERT);

        setAttributeBoolean(element, 
                            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_MASKED,
                            defn.isMasked(), PropertyDefinitionImpl.DEFAULT_IS_MASKED);
        
        setAttributeBoolean(element, 
                            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_MODIFIABLE,
                            defn.isModifiable(), PropertyDefinitionImpl.DEFAULT_IS_MODIFIABLE);
        
        setAttributeString(element,
                            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.REQUIRES_RESTART,
                            defn.getRequiresRestart().toString(), PropertyDefinitionImpl.DEFAULT_REQUIRES_RESTART.toString());
        
        
        List allowedValues = defn.getAllowedValues();
        Iterator iterator = allowedValues.iterator();
        while (iterator.hasNext()) {
            Element allowedValueElement = new Element(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.AllowedValue.ELEMENT);
            allowedValueElement.addContent((iterator.next()).toString());
            element.addContent(allowedValueElement);
        }

        return element;

    }

    
    
    /**
     * Set the specified attribute on the on the specified element, only if it's not the default value. 
     * @param element Element to modify
     * @param attributeName name of Attribute to set
     * @param value Value to set
     * @param defaultValue If value==default value, don't set anything.
     * @since 4.3
     */
    private static void setAttributeString(Element element, String attributeName, String value, String defaultValue) {
        if (value != null && ! EquivalenceUtil.areEqual(value, defaultValue)) {
            element.setAttribute(attributeName, value);
        }
    }
    /**
     * Set the specified attribute on the on the specified element, only if it's not the default value. 
     * @param element Element to modify
     * @param attributeName name of Attribute to set
     * @param value Value to set
     * @param defaultValue If value==default value, don't set anything.
     * @since 4.3
     */
    private static void setAttributeBoolean(Element element, String attributeName, boolean value, boolean defaultValue) {
        if (! value == defaultValue) {
            String valueString = String.valueOf(value);
            element.setAttribute(attributeName, valueString);
        }
    }
    
    
    /**
    * This method is used to create a ComponentTypeDefn JDOM Element from a
    * ComponentTypeDefn object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createComponentTypeDefnElement(ComponentTypeDefn defn) {
        Assertion.isNotNull(defn);

        Element componentTypeDefnElement = new Element(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.ELEMENT);
        componentTypeDefnElement.setAttribute(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.Attributes.DEPRECATED, (Boolean.valueOf(defn.isDeprecated())).toString());
        return componentTypeDefnElement;
    }

    /**
    * This method is used to create a Host JDOM Element from a
    * Host object.
    *
    * @param host the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createHostElement(Host host) {
        Assertion.isNotNull(host);

        Element hostElement = createComponentObjectElement(XMLConfig_ElementNames.Configuration.Host.ELEMENT, host, true);
        return hostElement;
    }
    
    
    public Properties getHeaderProperties(Element element) throws InvalidConfigurationElementException{
        Properties props=new Properties();
        
        if (!element.getName().equals(XMLConfig_ElementNames.Header.ELEMENT)) {
            throw new InvalidConfigurationElementException("This is not the header element: " + element.getName() + "."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        List elements = element.getChildren();
        Iterator it = elements.iterator();
        while(it.hasNext()) {
            final Element e = (Element) it.next();
            props.setProperty(e.getName(), e.getText());
        }
               
        
        return props;
    }


    /**
    * <p>This method is used to create a Header JDOM Element from a
    * Properties object.  The properties object can contain any of the
    * following properties that will be included in the header:<p>
    * <pre>
    * XMLConfig_ElementNames.Header.ApplicationCreatedDate.ELEMENT
    * XMLConfig_ElementNames.Header.ApplicationVersionCreatedBy.ELEMENT
    * XMLConfig_ElementNames.Header.UserName.ELEMENT
    * XMLConfig_ElementNames.Header.DocumentTypeVersion.ELEMENT
    * XMLConfig_ElementNames.Header.MetaMatrixServerVersion.ELEMENT
    * XMLConfig_ElementNames.Header.Time.ELEMENT
    * <pre>
    * <p>Any of these properties that are not included in the properties object
    * will not be included in the header Element that is returned.
    *
    * @param props the properties object that contains the values for the Header
    * @return a JDOM XML Element
    */
    public Element createHeaderElement(Properties props) {
        Assertion.isNotNull(props);

        Element headerElement = new Element(XMLConfig_ElementNames.Header.ELEMENT);
        String applicationCreatedByContent = props.getProperty(XMLConfig_ElementNames.Header.ApplicationCreatedBy.ELEMENT);
        String applicationVersionCreatedByContent = props.getProperty(XMLConfig_ElementNames.Header.ApplicationVersionCreatedBy.ELEMENT);
        String userNameContent = props.getProperty(XMLConfig_ElementNames.Header.UserCreatedBy.ELEMENT);
        String configVersionContent = props.getProperty(XMLConfig_ElementNames.Header.ConfigurationVersion.ELEMENT);
        String serverVersionContent = props.getProperty(XMLConfig_ElementNames.Header.MetaMatrixSystemVersion.ELEMENT);
        String timeContent = props.getProperty(XMLConfig_ElementNames.Header.Time.ELEMENT);


        if (configVersionContent !=null) {
            Element configurationVersion = new Element(XMLConfig_ElementNames.Header.ConfigurationVersion.ELEMENT);
            configurationVersion.addContent(configVersionContent);
            headerElement.addContent(configurationVersion);
        }        
        
        if (applicationCreatedByContent !=null) {
            Element applicationCreatedBy = new Element(XMLConfig_ElementNames.Header.ApplicationCreatedBy.ELEMENT);
            applicationCreatedBy.addContent(applicationCreatedByContent);
            headerElement.addContent(applicationCreatedBy);
        }

        if (applicationVersionCreatedByContent != null) {
            Element applicationVersionCreatedBy = new Element(XMLConfig_ElementNames.Header.ApplicationVersionCreatedBy.ELEMENT);
            applicationVersionCreatedBy.addContent(applicationVersionCreatedByContent);
            headerElement.addContent(applicationVersionCreatedBy);
        }

        if (userNameContent != null) {
            Element userName = new Element(XMLConfig_ElementNames.Header.UserCreatedBy.ELEMENT);
            userName.addContent(userNameContent);
            headerElement.addContent(userName);
        }

        if (serverVersionContent != null) {
            Element serverVersion = new Element(XMLConfig_ElementNames.Header.MetaMatrixSystemVersion.ELEMENT);
            serverVersion.addContent(serverVersionContent);
            headerElement.addContent(serverVersion);
        }

        if (timeContent != null) {
            Element time = new Element(XMLConfig_ElementNames.Header.Time.ELEMENT);
            time.addContent(timeContent);
            headerElement.addContent(time);
        }
        return headerElement;

    }

    private ComponentObject setDateHistory(ComponentObject defn, Element element, ConfigurationObjectEditor editor) {

        String lastChangedBy=null;
        String lastChangedDate=null;
        String createdDate=null;
        String createdBy=null;

        Properties props = getChangeHistoryFromElement(element);

        if (props != null && props.size() > 0) {
            lastChangedBy = props.getProperty(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_BY);
            lastChangedDate = props.getProperty(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_DATE);
            createdBy = props.getProperty(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATED_BY);
            createdDate = props.getProperty(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATION_DATE);
        } else {
            
            lastChangedBy = element.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_BY);
            lastChangedDate = element.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_DATE);
            createdBy = element.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATED_BY);
            createdDate = element.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATION_DATE);
        }  
        
        lastChangedBy = (lastChangedBy!=null?lastChangedBy:""); //$NON-NLS-1$
        lastChangedDate = (lastChangedDate!=null?lastChangedDate:DateUtil.getCurrentDateAsString()); 
        createdBy = (createdBy!=null?createdBy:""); //$NON-NLS-1$
        createdDate = (createdDate!=null?createdDate:DateUtil.getCurrentDateAsString()); 
        

    	defn = editor.setCreationChangedHistory(defn, createdBy, createdDate);
    	defn = editor.setLastChangedHistory(defn, lastChangedBy, lastChangedDate);

    	return defn;

    }

    private ComponentType setDateHistory(ComponentType type, Element element, ConfigurationObjectEditor editor) {
        String lastChangedBy=null;
        String lastChangedDate=null;
        String createdDate=null;
        String createdBy=null;

        Properties props = getChangeHistoryFromElement(element);

        if (props != null && props.size() > 0) {
            lastChangedBy = props.getProperty(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_BY);
            lastChangedDate = props.getProperty(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_DATE);
            createdBy = props.getProperty(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATED_BY);
            createdDate = props.getProperty(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATION_DATE);
        } else {
            
            lastChangedBy = element.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_BY);
            lastChangedDate = element.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_DATE);
            createdBy = element.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATED_BY);
            createdDate = element.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATION_DATE);
        }  
        
        lastChangedBy = (lastChangedBy!=null?lastChangedBy:""); //$NON-NLS-1$
        lastChangedDate = (lastChangedDate!=null?lastChangedDate:DateUtil.getCurrentDateAsString()); 
        createdBy = (createdBy!=null?createdBy:""); //$NON-NLS-1$
        createdDate = (createdDate!=null?createdDate:DateUtil.getCurrentDateAsString()); 
        
    	type = editor.setCreationChangedHistory(type, createdBy, createdDate);
    	type = editor.setLastChangedHistory(type, lastChangedBy, lastChangedDate);

    	return type;

    }


     private Properties getChangeHistoryFromElement(Element parentElement) {
         
    	Element propertiesElement = parentElement.getChild(XMLConfig_ElementNames.ChangeHistory.ELEMENT);

    	if (propertiesElement == null ) {
        	return new Properties();
    	}

		Properties props = new Properties();

        List properties = propertiesElement.getChildren(XMLConfig_ElementNames.ChangeHistory.Property.ELEMENT);
		if (properties == null) {
			return new Properties();
		}
        Iterator iterator = properties.iterator();
        while (iterator.hasNext()) {
            Element propertyElement = (Element)iterator.next();
            String propertyName = propertyElement.getAttributeValue(XMLConfig_ElementNames.ChangeHistory.Property.Attributes.NAME);
            String propertyValue = propertyElement.getText();

            props.setProperty(propertyName, (propertyValue!=null?propertyValue:"")); //$NON-NLS-1$

        }
        return props;

    }


    private void addChangeHistoryElement(ComponentObject obj, Element element ) {
        
        String lastChangedBy=null;
        String lastChangedDate=null;
        String createdDate=null;
        String createdBy=null;

        lastChangedBy = obj.getLastChangedBy();
        lastChangedDate = ((BasicComponentObject) obj).getLastChangedDateString();

        createdBy = obj.getCreatedBy();
        createdDate = ((BasicComponentObject) obj).getCreatedDateString();


        if (lastChangedBy == null || lastChangedBy.trim().length() == 0) {

        } else {
            element.setAttribute(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_BY, lastChangedBy);
        }

        if (lastChangedDate == null) {

        } else {
            element.setAttribute(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_DATE, lastChangedDate);

        }

        if (createdBy == null || createdBy.trim().length() == 0) {
        } else {
            element.setAttribute(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATED_BY, createdBy);
        }

        if (createdDate == null) {
        } else {
            element.setAttribute(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATION_DATE, createdDate);
        }
        
    }
    
    private void addChangeHistoryElement(ComponentType obj, Element element ) {

//      call to create the structure for the properties
 
         String lastChangedBy=null;
         String lastChangedDate=null;
         String createdDate=null;
         String createdBy=null;

         lastChangedBy = obj.getLastChangedBy();
         lastChangedDate = ((BasicComponentType) obj).getLastChangedDateString();

         createdBy = obj.getCreatedBy();
         createdDate = ((BasicComponentType) obj).getCreatedDateString();


         if (lastChangedBy == null || lastChangedBy.trim().length() == 0) {
             
         } else {
             element.setAttribute(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_BY, lastChangedBy);
         }

         if (lastChangedDate == null) {
             lastChangedDate = DateUtil.getCurrentDateAsString();
         } 
             element.setAttribute(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.LAST_CHANGED_DATE, lastChangedDate);



         if (createdBy == null || createdBy.trim().length() == 0) {
         } else {
             element.setAttribute(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATED_BY, createdBy);
         }

         if (createdDate == null) {
             createdDate = DateUtil.getCurrentDateAsString();
         } 
             element.setAttribute(XMLConfig_ElementNames.ChangeHistory.Property.NAMES.CREATION_DATE, createdDate);

     }
   

    /**
    * This method is used to create a Properties JDOM Element from a
    * Properties object.
    *
    * @param props the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createPropertiesElement(Properties props) {
        Assertion.isNotNull(props);

        Properties sortprops = PropertiesUtils.sort(props);
        Element propertiesElement = new Element(XMLConfig_ElementNames.Properties.ELEMENT);
        Enumeration enumeration = sortprops.propertyNames();
        while (enumeration.hasMoreElements()) {
            String propName = (String)enumeration.nextElement();
            propertiesElement = addPropertyElement(propertiesElement, propName, props.getProperty(propName));
        }

        return propertiesElement;
    }


    private Element addPropertyElement(Element propertiesElement, String propName, String propValue) {
            Element property = new Element(XMLConfig_ElementNames.Properties.Property.ELEMENT);
            property.setAttribute(XMLConfig_ElementNames.Properties.Property.Attributes.NAME, propName);
            property.addContent(propValue);
            propertiesElement.addContent(property);
            return propertiesElement;

	}



    /**
    * This method is used to create a Configuration ID JDOM Element from a
    * Configuration ID object.
    *
    * @param type the ID type to be created. @see XMLConfig_ElementNames.Configuration.XXXID.ELEMENT for valid values
    * @param name the calue of the name attribute of the ID element to create.
    * @return a JDOM XML Element
    */
    public Element createIDElement(String type, String name) {

        Element idElement = new Element(type);
        idElement.setAttribute(XMLConfig_ElementNames.ID.Attributes.NAME, name);
        return idElement;
    }

    /**
    * This method is used to create a Configurations JDOM Element from a
    * Configuration ID object.  This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createConfigurationsElement() {
        throw new UnsupportedOperationException("Method createConfigurationsElement is unsupported in 4.2"); //$NON-NLS-1$
    }



    /**
    * This method is used to create a ServiceComponentDefns JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createServiceComponentDefnsElement() {
        return new Element(XMLConfig_ElementNames.Configuration.Services.ELEMENT);
    }

    /**
    * This method is used to create a ComponentTypes JDOM Element from a
    * Configuration ID object.  This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createComponentTypesElement() {
        return new Element(XMLConfig_ElementNames.ComponentTypes.ELEMENT);
    }


    public Element createConnectorBindingsElement() {
        return new Element(XMLConfig_ElementNames.Configuration.ConnectorComponents.ELEMENT);
    }


    /**
    * This method is used to create a root JDOM Element.
    * This element is for structural organization
    * only and does not represent any real configuration object.
    *
    * @return a JDOM XML Element
    */
    public Element createRootConfigurationDocumentElement() {
        return new Element(XMLConfig_ElementNames.ELEMENT);
    }


    /**
    * This method is used to create a ComponentObject JDOM Element from a
    * ComponentObject object.
    *
    * @param type The subclass type of the configuration object to be created.
    * @see XMLConfig_ElementNames.Configuration.XXXX.ELEMENT
    * @param componentObject the object to create the Element for.
    * @return a JDOM XML Element
    */
    private Element createComponentObjectElement(String type, ComponentObject componentObject, boolean addType) {
        Element componentObjectElement = new Element(type);
        componentObjectElement.setAttribute(XMLConfig_ElementNames.ComponentObject.Attributes.NAME, componentObject.getName());
        BaseID id = componentObject.getComponentTypeID();
        if (id !=null && addType) {
            componentObjectElement.setAttribute(XMLConfig_ElementNames.ComponentObject.Attributes.COMPONENT_TYPE, id.getName());
        }

			// this will add the changed history information
        if (componentObject instanceof DeployedComponent) {
            Properties props = componentObject.getProperties();
            if (props != null && ! props.isEmpty()) {
                Element properties = createPropertiesElement( props);
                componentObjectElement.addContent(properties);           	
            }
        } else {
            Element properties = createPropertiesElement( componentObject.getProperties());
            componentObjectElement.addContent(properties);
        }

        addChangeHistoryElement(componentObject, componentObjectElement);

        return componentObjectElement;
    }



// ##############################################################################
//
//              Configuration Object Creation Methods
//
// ##############################################################################


    /**
    * This method will create a Host configuration object from an XML element
    * that represents a Host.
    *
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as
    * the name of the object.
    * @return the Host configuration object
    * @throws InvalidConfigurationElementException if the element passed in
    * or its XML structure do not conform to the XML structure specfied in
    * the XMLConfig_ElementNames class.
    */
    public Host createHost(Element element, ConfigurationID configID, ConfigurationObjectEditor editor, String name) throws InvalidConfigurationElementException{
        Assertion.isNotNull(element);
        Assertion.isNotNull(editor);

        if (!element.getName().equals(XMLConfig_ElementNames.Configuration.Host.ELEMENT)) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0032, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0032, element.getName()));
        }
        if (name == null || name.trim().length() == 0) {
            name = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Host.Attributes.NAME);
        }

        Host host = editor.createHost(configID, name);

        host = (Host) setDateHistory(host, element, editor);

            // now we add the system properties to the configuration object
            host = (Host)addProperties(element, host, editor);



        return host;
    }

    
    public Element createAuthenticationProviderElement() {
        return new Element(XMLConfig_ElementNames.Configuration.AuthenticationProviders.ELEMENT);
     }    
    
    
    /**
     * This method will create a Resource configuration object from an XML element
     * that represents a Resource.
     *
     * @param element the JDOM element to convert to a configuration object
     * @param editor the editor to use to create the configuration object
     * @param name the name of the returned configuration object. Note this
     * name will override the name in the JDOM element.  If the name parameter
     * is null, the name of the object in the JDOM element will be used as
     * the name of the object.
     * @return the SharedResource configuration object
     * @throws InvalidConfigurationElementException if the element passed in
     * or its XML structure do not conform to the XML structure specfied in
     * the XMLConfig_ElementNames class.
     */
     public AuthenticationProvider createAuthenticationProvider(Element element, ConfigurationID configID, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException{
         Assertion.isNotNull(element);
         Assertion.isNotNull(editor);

         if (!element.getName().equals(XMLConfig_ElementNames.Configuration.AuthenticationProviders.Provider.ELEMENT)) {
             throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0033, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0033, element.getName()));
         }

         String name = element.getAttributeValue(XMLConfig_ElementNames.Configuration.AuthenticationProviders.Provider.Attributes.NAME);

         checkElementValue(name, null, ErrorMessageKeys.CONFIG_ERR_0053);

         String type = element.getAttributeValue(XMLConfig_ElementNames.Configuration.AuthenticationProviders.Provider.Attributes.COMPONENT_TYPE);

         checkElementValue(type, name, ErrorMessageKeys.CONFIG_ERR_0054);

         ComponentTypeID id = new ComponentTypeID(type);

         // create the descriptor used to get the resource
         AuthenticationProvider authProvider = editor.createAuthenticationProviderComponent(configID, id, name);

         Element propertiesElement = element.getChild(XMLConfig_ElementNames.Properties.ELEMENT);

         authProvider = (AuthenticationProvider) setDateHistory(authProvider, element, editor);

         if (propertiesElement != null) {
             // now we add the system properties to the configuration object
        	 authProvider = (AuthenticationProvider)addProperties(propertiesElement, authProvider, editor);

         }


         return authProvider;
     }    


    /**
    * This method is used to create a ServiceComponentDefn JDOM Element from a
    * ServiceComponentDefn object.
    *
    * @param defn the Object to be converted to a JDOM XML Element
    * @return a JDOM XML Element
    */
    public Element createSharedResourceElement(SharedResource resource) {
        Assertion.isNotNull(resource);

        Element resourceElement = createComponentObjectElement(XMLConfig_ElementNames.Configuration.Resources.Resource.ELEMENT, resource, true);
        return resourceElement;
    }


    /**
    * This method will create a Resource configuration object from an XML element
    * that represents a Resource.
    *
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as
    * the name of the object.
    * @return the SharedResource configuration object
    * @throws InvalidConfigurationElementException if the element passed in
    * or its XML structure do not conform to the XML structure specfied in
    * the XMLConfig_ElementNames class.
    */
    public SharedResource createSharedResource(Element element, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException{
        Assertion.isNotNull(element);
        Assertion.isNotNull(editor);

        if (!element.getName().equals(XMLConfig_ElementNames.Configuration.Resources.Resource.ELEMENT)) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0034, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0034, element.getName()));
        }

        String name = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Resources.Resource.Attributes.NAME);

        checkElementValue(name, null, ErrorMessageKeys.CONFIG_ERR_0055);

        String type = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Resources.Resource.Attributes.COMPONENT_TYPE);

        checkElementValue(type, name, ErrorMessageKeys.CONFIG_ERR_0056);

		ComponentTypeID id = new ComponentTypeID(type);

        // create the descriptor used to get the resource
        SharedResource descriptor = editor.createSharedResource(
                                            id,
                                            name);


        Element propertiesElement = element.getChild(XMLConfig_ElementNames.Properties.ELEMENT);

        descriptor = (SharedResource) setDateHistory(descriptor, element, editor);

        if (propertiesElement != null) {
            // now we add the system properties to the configuration object
            descriptor = (SharedResource)addProperties(propertiesElement, descriptor, editor);
        }

        return descriptor;
    }

    public Element createSharedResourcesElement() {
       return new Element(XMLConfig_ElementNames.Configuration.Resources.ELEMENT);
    }

    /**
    * This method will create a ComponentType configuration object from an XML element
    * that represents a ComponentType.
    *
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as
    * the name of the object.
    * @return the ComponentType configuration object
    * @throws InvalidConfigurationElementException if the element passed in
    * or its XML structure do not conform to the XML structure specfied in
    * the XMLConfig_ElementNames class.
    */
    public ComponentType createComponentType(Element element, ConfigurationObjectEditor editor, String name, boolean maintainParentID) throws InvalidConfigurationElementException{
        Assertion.isNotNull(element);
        Assertion.isNotNull(editor);

        if (!element.getName().equals(XMLConfig_ElementNames.ComponentTypes.ComponentType.ELEMENT)) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0035, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0035, element.getName()));
        }

        // retreive the attributes of this ComponentType from the JDOM element
        String parentType = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.PARENT_COMPONENT_TYPE);
        String superType = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.SUPER_COMPONENT_TYPE);
        String componentTypeCode = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.COMPONENT_TYPE_CODE);
        String deployable = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.DEPLOYABLE);
        String monitorable = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.MONITORABLE);
        String description = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.DESCRIPTION);



        // convert them into their proper data types
        int typeCode = Integer.parseInt(componentTypeCode);
        
        if (! BasicUtil.isValdComponentTypeCode(typeCode)) {
        	throw new RuntimeException("File error2, invalid component type code " + componentTypeCode + " for " + name + " super " + superType);
        }
        
        // we will use the passed in name unless it is null...
        if (name == null || name.trim().length() == 0) {
            name = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.NAME);
        }

        ComponentTypeID parentTypeID = null;
        ComponentTypeID superTypeID = null;

        if (parentType != null && parentType.length() > 0) {
           parentTypeID = new ComponentTypeID(parentType);
        }

        if (superType !=null && superType.length() > 0) {
            superTypeID = new ComponentTypeID(superType);
        }
        

        boolean isDeployable = (Boolean.valueOf(deployable)).booleanValue();
        boolean isMonitorable =  (Boolean.valueOf(monitorable)).booleanValue();

        // create the ComponentTypeObject
        BasicComponentType type = (BasicComponentType) editor.createComponentType(typeCode, name, parentTypeID, superTypeID, isDeployable, isMonitorable);

        
        if (description != null && description.length() > 0) {
            type.setDescription(description);
        }
        ComponentType t = setDateHistory(type, element, editor);
        

        // get the ComponentTypeDefn sub-Elements of this ComponentType
        // and create them also.
        List componentTypeDefnElements = element.getChildren(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.ELEMENT);

        if (componentTypeDefnElements == null || componentTypeDefnElements.size() == 0) {
            componentTypeDefnElements = element.getChildren(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.ELEMENT);
            return addPropertyDefns(componentTypeDefnElements, t, editor);
        } 

        return addComponentTypeDefns(componentTypeDefnElements, t, editor);

    }

    public ComponentType loadComponentType(Element rootElement) throws InvalidConfigurationElementException {
    	return createComponentType(rootElement, new BasicConfigurationObjectEditor(), null, true);
    }



    /**
    * This method will create a Configuration configuration object from an XML element
    * that represents a Configuration.
    *
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as
    * the name of the object.
    * @return the Configuration configuration object
    * @throws InvalidConfigurationElementException if the element passed in
    * or its XML structure do not conform to the XML structure specfied in
    * the XMLConfig_ElementNames class.
    */
    public Configuration createConfiguration(Element element, ConfigurationObjectEditor editor, String name) throws InvalidConfigurationElementException{

        Assertion.isNotNull(element);
        Assertion.isNotNull(editor);

        if (!element.getName().equals(XMLConfig_ElementNames.Configuration.ELEMENT)) {
            	throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0038, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0038, element.getName()));
        }

        if (name==null || name.trim().length() == 0) {
            name = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Attributes.NAME);
        }

        Configuration config = editor.createConfiguration(name);

        config = (Configuration) setDateHistory(config, element, editor);

        config = (Configuration)addProperties(element, config, editor);

        return config;
    }

    public Element createConnectorBindingElement(ConnectorBinding connector, boolean isExportConfig)  {
        
        Assertion.isNotNull(connector);
    
        Element connectorElement = createComponentObjectElement(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.ELEMENT, connector, true);

         connectorElement.setAttribute(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.Attributes.ROUTING_UUID, connector.getRoutingUUID());


        return connectorElement;
    
    }

    
    public ConnectorBinding createConnectorBinding(ConfigurationID configurationID, Element element, ConfigurationObjectEditor editor, String name, boolean isImportConfig)throws InvalidConfigurationElementException {

        Assertion.isNotNull(element);
        Assertion.isNotNull(editor);

        if (!element.getName().equals(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.ELEMENT)) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0041, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0041,element.getName()));
        }

        if (name==null || name.trim().length() == 0) {
            name = element.getAttributeValue(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.Attributes.NAME);
        }

        String componentType = element.getAttributeValue(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.Attributes.COMPONENT_TYPE);

        checkElementValue(componentType, name, ErrorMessageKeys.CONFIG_ERR_0057);

        ComponentTypeID id = new ComponentTypeID(componentType);

        String routingUUID = null;
        // vah - 09-24-2003
        // when importing a configuration use the routing uuid,
        // otherwise do not use it (which will cause the routingUUID to be regenerated)
        // This is done to help ensure there are no duplicate UUIDs
        if (isImportConfig) {  
            routingUUID = element.getAttributeValue(XMLConfig_ElementNames.Configuration.ConnectorComponents.ConnectorComponent.Attributes.ROUTING_UUID);
        }

        ConnectorBinding defn = null;
        defn = editor.createConnectorComponent(configurationID, id, name, routingUUID);

        defn = (ConnectorBinding) setDateHistory(defn, element, editor);

        // add the properties to this ComponentObject...
        Element propertiesElement = element.getChild(XMLConfig_ElementNames.Properties.ELEMENT);
        if (propertiesElement != null) {
            // now we add the system properties to the configuration object
            return (ConnectorBinding)addProperties(propertiesElement, defn, editor);
        }

        return defn;
    }

    public ConnectorBinding loadConnectorBinding(Element rootElement) throws InvalidConfigurationElementException {
    	return createConnectorBinding(Configuration.NEXT_STARTUP_ID, rootElement, new BasicConfigurationObjectEditor(), null, false);
    }
    
    /**
    * This method will create a ServiceComponentDefn configuration object from an XML element
    * that represents a ServiceComponentDefn.
    *
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as
    * the name of the object.
    * @return the ServiceComponentDefn configuration object
    * @throws InvalidConfigurationElementException if the element passed in
    * or its XML structure do not conform to the XML structure specfied in
    * the XMLConfig_ElementNames class.
    */
    public ComponentDefn createServiceComponentDefn(Element element, Configuration config, ConfigurationObjectEditor editor, String name) throws InvalidConfigurationElementException{
		ConfigurationID configID = null;
		if (config != null) {
			configID = (ConfigurationID) config.getID();
    	}
		return createServiceComponentDefn(element,  configID, editor, name);

    }

    public ComponentDefn createServiceComponentDefn(Element element, ConfigurationID configID, ConfigurationObjectEditor editor, String name)throws InvalidConfigurationElementException {

        Assertion.isNotNull(element);
        Assertion.isNotNull(editor);
        Assertion.isNotNull(configID);

        if (!element.getName().equals(XMLConfig_ElementNames.Configuration.Services.Service.ELEMENT)) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0042, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0042,element.getName()));
        }

        if (name==null || name.trim().length() == 0) {
            name = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Services.Service.Attributes.NAME);
        }

        String componentType = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Services.Service.Attributes.COMPONENT_TYPE);

        checkElementValue(componentType, name, ErrorMessageKeys.CONFIG_ERR_0058);

        ComponentTypeID id = new ComponentTypeID(componentType);

        String routingUUID = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Services.Service.Attributes.ROUTING_UUID);

        ComponentDefn defn = null;
        boolean isResourcePool = isResourcePool(componentType);

        if (configID == null) {

        }else {
            if (!isResourcePool) {

                if (routingUUID == null){
                    //allow the object editor to generate a UUID
                    defn = editor.createServiceComponentDefn(configID, id, name);
                } else {
                    //use the UUID specified in the XML file
                    defn = editor.createServiceComponentDefn(configID, id, name, routingUUID);
                }

            } 

        }

        defn = (ComponentDefn) setDateHistory(defn, element, editor);

        // add the properties to this ComponentObject...
        Element propertiesElement = element.getChild(XMLConfig_ElementNames.Properties.ELEMENT);
        if (propertiesElement != null) {
            // now we add the system properties to the configuration object
            return (ComponentDefn)addProperties(propertiesElement, defn, editor);
        }

        return defn;

    }

    private boolean isResourcePool(String componentTypeName) {
        boolean result = false;

         if (componentTypeName.equals(SharedResource.MISC_COMPONENT_TYPE_NAME) ) {
                return true;
            }


        return result;

    }



    public DeployedComponent createDeployedServiceComponent(Element element,
                                                     ConfigurationID configID, 
                                                     HostID hostID,
                                                     VMComponentDefnID vmID,
                                                     Map componentTypeMap,
                                                     ConfigurationObjectEditor editor) 
                                                     throws InvalidConfigurationElementException{
        Assertion.isNotNull(element);
        Assertion.isNotNull(editor);
        Assertion.isNotNull(configID);
        Assertion.isNotNull(hostID);
        Assertion.isNotNull(vmID);
        
        DeployedComponent component;
       
        if (!element.getName().equals(XMLConfig_ElementNames.Configuration.DeployedService.ELEMENT)) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0044, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0044,element.getName()));
        }
                             
        String name = element.getAttributeValue(XMLConfig_ElementNames.Configuration.DeployedService.Attributes.NAME);
        checkElementValue(name, "NAME", ErrorMessageKeys.CONFIG_ERR_0048); //$NON-NLS-1$

        String componentTypeIDString = element.getAttributeValue(XMLConfig_ElementNames.Configuration.DeployedService.Attributes.COMPONENT_TYPE);

        checkElementValue(componentTypeIDString, name, ErrorMessageKeys.CONFIG_ERR_0049);
      
        
        ComponentType type = null;
        Iterator it = componentTypeMap.keySet().iterator();
        while (it.hasNext() ) {
            ComponentTypeID id = (ComponentTypeID) it.next();
            if (id.getFullName().equals(componentTypeIDString)) {
                type = (ComponentType) componentTypeMap.get(id);
                break;
            }
        }

        if (type == null) {
            throw new InvalidConfigurationElementException(ErrorMessageKeys.CONFIG_ERR_0050, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0050, new Object[] {componentTypeIDString, name} ));
        }  
        
        ServiceComponentDefnID svcid = null;
        if (type.isOfTypeConnector()) {

            svcid = new ConnectorBindingID(configID, name);

        } else {


            svcid = new ServiceComponentDefnID(configID, name);

        }
        
        component = editor.createDeployedServiceComponent(name, configID, hostID,vmID, svcid, (ComponentTypeID) type.getID());
        component =  (DeployedComponent)addProperties(element, component, editor);
        
        component = (DeployedComponent) setDateHistory(component, element, editor);
        
        
      return component;  
      }

    
    /**
    * This method will create a VMComponentDefn configuration object from an XML element
    * that represents a VMComponentDefn.
    *
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as
    * the name of the object.
    * @return the VMComponentDefn configuration object
    * @throws InvalidConfigurationElementException if the element passed in
    * or its XML structure do not conform to the XML structure specfied in
    * the XMLConfig_ElementNames class.
    */
    public BasicVMComponentDefn createProcess(Element element, ConfigurationID configID, HostID hostID, ConfigurationObjectEditor editor, String name) throws InvalidConfigurationElementException{
        Assertion.isNotNull(element);
        Assertion.isNotNull(editor);
        Assertion.isNotNull(configID);

        if (!element.getName().equals(XMLConfig_ElementNames.Configuration.Process.ELEMENT)) {
            throw new InvalidConfigurationElementException("A Configuration object cannot be created from a JDOM Element type: " + element.getName() + "."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        if (name==null || name.trim().length() == 0) {
            name = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Process.Attributes.NAME);
        }

        String componentType = element.getAttributeValue(XMLConfig_ElementNames.Configuration.Process.Attributes.COMPONENT_TYPE);

        checkElementValue(componentType, name, ErrorMessageKeys.CONFIG_ERR_0060);

        ComponentTypeID id = new ComponentTypeID(componentType);

        VMComponentDefn defn = editor.createVMComponentDefn(configID, hostID, id, name);

    	defn = (VMComponentDefn) setDateHistory(defn, element, editor);

        defn =  (VMComponentDefn)addProperties(element, defn, editor);


        return (BasicVMComponentDefn) defn;
    }

    /**
    * This method is a helper method to create a PropertyDefinition object from
    * an XML element that represents same.
    *
    * @param element the XML element that represents a PropertyDefinition object
    * @throws InvalidConfigurationElementException if the element passed in
    * or its XML structure do not conform to the XML structure specfied in
    * the XMLConfig_ElementNames class.
    */
    public PropertyDefinition createPropertyDefinition(Element element) throws InvalidConfigurationElementException{

        if (!element.getName().equals(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.ELEMENT)) {
            throw new InvalidConfigurationElementException("A Configuration object cannot be created from a JDOM Element type: " + element.getName() + "."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        
        String nameString = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.NAME);
        
        String displayNameString = element.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.DISPLAY_NAME);
        

        String shortDescriptionString = getAttributeString(element,
            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.SHORT_DESCRIPTION,
            PropertyDefinitionImpl.DEFAULT_SHORT_DESCRIPTION);

        String defaultValueString = getAttributeString(element,
            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.DEFAULT_VALUE,
            PropertyDefinitionImpl.DEFAULT_DEFAULT_VALUE);
        
        String multiplicityString = getAttributeString(element,
            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.MULTIPLICITY,
            "0..1"); //$NON-NLS-1$
        
        String propertyTypeString = getAttributeString(element,
            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.PROPERTY_TYPE,
            PropertyDefinitionImpl.DEFAULT_TYPE.getDisplayName());
        PropertyType type = PropertyType.getInstance(propertyTypeString);

        boolean isExpert = getAttributeBoolean(element,
            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_EXPERT,
            PropertyDefinitionImpl.DEFAULT_IS_EXPERT);
        
        boolean isRequired = getAttributeBoolean(element,
                XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_REQUIRED,
                PropertyDefinitionImpl.DEFAULT_IS_REQUIRED);
        
        boolean isMasked = getAttributeBoolean(element,
            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_MASKED,
            PropertyDefinitionImpl.DEFAULT_IS_MASKED);

        boolean isModifiable = getAttributeBoolean(element,
            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_MODIFIABLE,
            PropertyDefinitionImpl.DEFAULT_IS_MODIFIABLE);

        boolean isHidden = getAttributeBoolean(element,
                XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.IS_HIDDEN,
                false);
        
        String requiresRestart = getAttributeString(element,
            XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.Attributes.REQUIRES_RESTART,
            PropertyDefinitionImpl.DEFAULT_REQUIRES_RESTART.toString());

        RestartType restartType = null;

        /*
         * -- backwards compatibility checks
         * requiresRestart="true" for requiresRestart="PROCESS"
         * isHidden="true" for isModifiable="false"
         * multiplicity="1" for isRequired="true" 
         */
        
        if ("true".equalsIgnoreCase(requiresRestart)) { //$NON-NLS-1$
        	restartType = RestartType.PROCESS;
        } else {
        	restartType = RestartType.valueOf(requiresRestart.toUpperCase());
        }

        if (isHidden) {
        	isModifiable = false;
        }

        if (!isRequired && !multiplicityString.startsWith("0")) { //$NON-NLS-1$
        	isRequired = true;
        }
        
        // we must retrieve all of the allowed values from the PropertyDefinition
        // element
        Collection allowedValuesElements = element.getChildren(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.AllowedValue.ELEMENT);
        ArrayList allowedValues = new ArrayList(allowedValuesElements.size());

        Iterator iterator = allowedValuesElements.iterator();
        while (iterator.hasNext()) {
            Element allowedValueElement = (Element)iterator.next();
            allowedValues.add(allowedValueElement.getText());
        }

        PropertyDefinitionImpl defn = new PropertyDefinitionImpl(nameString, displayNameString, type,
                        isRequired,  shortDescriptionString, defaultValueString,
                        allowedValues, isHidden,
                        isExpert, isModifiable);

                defn.setMasked(isMasked);
                defn.setRequiresRestart(restartType);
        return defn;

    }

    
    
    /**
     * Get the value of the specified attribute from the on the specified element.
     * If null, get the the default value. 
     * @param element
     * @param attributeName name of Attribute to get
     * @param defaultValue
     * @return the attribute value, or defaultValue if it is null.
     * @since 4.3
     */
    private static String getAttributeString(Element element, String attributeName, String defaultValue) {
        String stringValue = element.getAttributeValue(attributeName);
        if (stringValue == null) {
            return defaultValue;
        }
        
        return stringValue;               
    }
    /**
     * Get the value of the specified attribute from the on the specified element.
     * If null, get the the default value. 
     * @param element
     * @param attributeName name of Attribute to get
     * @param defaultValue
     * @return the attribute value, or defaultValue if it is null.
     * @since 4.3
     */
    private static boolean getAttributeBoolean(Element element, String attributeName, boolean defaultValue) {
        String stringValue = element.getAttributeValue(attributeName);
        if (stringValue == null) {
            return defaultValue;
        }
        
        return Boolean.valueOf(stringValue).booleanValue();                
    }
    
    

    /**
    * This method will create a ComponentObject configuration object from an XML element
    * that represents a ComponentObject.
    *
    * @param element the JDOM element to convert to a configuration object
    * @param editor the editor to use to create the configuration object
    * @param name the name of the returned configuration object. Note this
    * name will override the name in the JDOM element.  If the name parameter
    * is null, the name of the object in the JDOM element will be used as
    * the name of the object.
    * @return the ComponentObject configuration object
    * @throws InvalidConfigurationElementException if the element passed in
    * or its XML structure do not conform to the XML structure specfied in
    * the XMLConfig_ElementNames class.
    */
    public ComponentObject addProperties(Element propertiesElement, ComponentObject object, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException{

        if (!propertiesElement.getName().equals(XMLConfig_ElementNames.Properties.ELEMENT)) {
            propertiesElement = propertiesElement.getChild(XMLConfig_ElementNames.Properties.ELEMENT);
        } 
        
        Properties props = null;
        if (propertiesElement == null) {           
        	props = new Properties();
        }
        else {
        	props = getProperties(propertiesElement);
        }

        object = editor.modifyProperties(object, props, ConfigurationObjectEditor.ADD);
        return object;
    }


    private Properties getProperties(Element propertiesElement) {
		Properties props = new Properties();

        List properties = propertiesElement.getChildren(XMLConfig_ElementNames.Properties.Property.ELEMENT);
        Iterator iterator = properties.iterator();
        while (iterator.hasNext()) {
            Element propertyElement = (Element)iterator.next();
            String propertyName = propertyElement.getAttributeValue(XMLConfig_ElementNames.Properties.Property.Attributes.NAME);
            String propertyValue = propertyElement.getText();

            props.setProperty(propertyName, propertyValue);

        }
        return props;
    }

    /**
    * This is a helper method for ProductTypes and ComponentTypes.  this method
    * will add a list of Component Type Definitions to a ComponentType using
    * the passed in editor.  The Collection of XML elements passed in are
    * translated into ComponentTypeDefn objects and then set on the passed in
    * ComponentType.
    *
    * @param componentTypeDefnElements a collection of JDOM elements that
    * each represent a ComponentTypeDefn object.
    * @param type the ComponentType object to add the ComponentTypeDefns to
    * @param editor the editor to use to both create the ComponentTypeDefns
    * and to set them on the passed in ComponentType.
    * @return the ComponentType reference that now has the CompoenentTypeDefns
    * set on it.
    * @throws InvalidConfigurationElementException if the ComponentTypeDefn
    * JDOM elements do not adhere to the proper XML structure as defined by the
    * XMLConfig_ElementNames class.
    */
    private ComponentType addComponentTypeDefns(Collection componentTypeDefnElements, ComponentType type, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException{
        ArrayList componentTypeDefns = new ArrayList(componentTypeDefnElements.size());

        Iterator iterator = componentTypeDefnElements.iterator();
        while (iterator.hasNext()) {
            Element componentTypeDefnElement = (Element)iterator.next();
            Element propertyDefinitionElement = componentTypeDefnElement.getChild(XMLConfig_ElementNames.ComponentTypes.ComponentType.ComponentTypeDefn.PropertyDefinition.ELEMENT);
            PropertyDefinition propDefn = createPropertyDefinition(propertyDefinitionElement);
            componentTypeDefns.add(editor.createComponentTypeDefn(type, propDefn, false));
        }
        return editor.setComponentTypeDefinitions(type, componentTypeDefns);
    }
    
    private ComponentType addPropertyDefns(Collection propertyDefnElements, ComponentType type, ConfigurationObjectEditor editor) throws InvalidConfigurationElementException{
        ArrayList componentTypeDefns = new ArrayList(propertyDefnElements.size());

        Iterator iterator = propertyDefnElements.iterator();
        while (iterator.hasNext()) {
            Element propertyDefinitionElement = (Element)iterator.next();
            PropertyDefinition propDefn = createPropertyDefinition(propertyDefinitionElement);
            componentTypeDefns.add(editor.createComponentTypeDefn(type, propDefn, false));
        }
        return editor.setComponentTypeDefinitions(type, componentTypeDefns);
    }    

    public void orderComponentTypeElementList(List componentTypeElements) {
        ComponentTypeElementComparator comparator = new ComponentTypeElementComparator();
        Collections.sort(componentTypeElements, comparator);
    }


    class ComponentTypeElementComparator implements Comparator{

        /**
        * This compare to will determine whether the ComponentType element
        * represented by 'this' has a superComponentType that is the
        * passed in ComponentType element representation to be compared to. if so, the 'this' element
        * is considered to be greater than the passed in element.  If it is
        * determined that 'this' is the superCompoentType of the passed in
        * object then 'greater than' will be returned.  If it is determined
        * that the two ComponentTypeObjects are unrelated, then equals is
        * returned...Note that this is inconsistent with the equals() method.
        */
        public int compare(Object thisObject, Object thatObject) {
            if (thisObject instanceof Element) {
                Element thisElement = (Element)thisObject;
                if (thatObject instanceof Element) {
                    Element thatElement = (Element)thatObject;
                    String thatSuperID = getElementSuperID(thatElement);
                    String thisSuperID = getElementSuperID(thisElement);
                    String thatID = getElementID(thatElement);
                    String thisID = getElementID(thisElement);

                    if(thisSuperID!=null && thisSuperID.equals(thatID)) {
                        return 1;
                    }else if(thatSuperID!=null && thatSuperID.equals(thisID)) {
                        return -1;
                    }else {
                        return 0;
                    }
                }
            }
            return 0;
        }


        private String getElementSuperID(Element componentTypeElement) {
            return componentTypeElement.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.SUPER_COMPONENT_TYPE);
        }

        private String getElementID(Element componentTypeElement) {
            return componentTypeElement.getAttributeValue(XMLConfig_ElementNames.ComponentTypes.ComponentType.Attributes.NAME);
        }
    }

    // helper class to check that an element is not null and length is greater than zero
    // this should be used instead of Assertion when checking that
    // component ID's exist
    private void checkElementValue(String value, String name, String errorKey) throws InvalidConfigurationElementException {
    	if (value == null || value.trim().length() > 0) {
    		if (name != null) {
                if(value == null){
	                Assertion.isNotNull(value, CommonPlugin.Util.getString(errorKey, name));
                }
    		} else {
                if(value == null){
    			    Assertion.isNotNull(value, CommonPlugin.Util.getString(errorKey));
                }
    		}

    	}

    }


}
