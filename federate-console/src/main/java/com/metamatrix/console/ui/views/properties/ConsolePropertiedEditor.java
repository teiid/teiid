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

package com.metamatrix.console.ui.views.properties;

import java.util.*;

import javax.swing.JTabbedPane;
import javax.swing.event.*;

import com.metamatrix.common.config.model.PropertyValidations;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.*;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.console.models.PropertiesManager;
import com.metamatrix.console.models.PropertyDetail;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;



public class ConsolePropertiedEditor implements PropertiedObjectEditor, ChangeListener{

    public static final short NSUCONFIGINDICATOR = 2;
    public static final short STARTUPCONFIGINDICATOR = 3;
    private Collection allPropDefns, allNSUPropDefns, allSUPropDefns;
    private ArrayList currentPropDefns = new ArrayList();
    private Properties oraginalProperties, allProperties;
    private Properties oldNSUProperties, nsuProperties;
    private Properties currentProperties = new Properties();
    private String propertyValue;
    private HashMap objectIdMap, propHM, defnHM;
    private HashMap propChangeHM = new HashMap();
    private String currentTabName, tabName;
    private EventListenerList listeners = new EventListenerList();
    private boolean buttonState;
    private PropertiesManager manager;
    private ObjectDefinitionDisplayNameComparator comp = new ObjectDefinitionDisplayNameComparator();
    
    private PropertyValidations validations = new PropertyValidations();
    

    public ConsolePropertiedEditor (PropertiesManager mgr) {
        super();
        this.manager = mgr;
        init();
    }
    private void init() {
        oldNSUProperties = new Properties();
        getPropertiesAndDefn();
        this.allPropDefns = this.allNSUPropDefns;
        allProperties = nsuProperties;
    }

    public ConsolePropertyObjectId getPropertyObjectId(String gName, PropertyFilter pFilter) {
		if (objectIdMap == null)
            objectIdMap = new HashMap();

        if (objectIdMap.containsKey(gName)) {
//            objectId = (ConsolePropertyObjectId)objectIdMap.get(gName);
        } else {
            objectIdMap.put(gName, new ConsolePropertyObjectId(gName, pFilter));
        }
		return (ConsolePropertyObjectId)objectIdMap.get(gName);
    }

    public HashMap getChangeHM() {
        return propChangeHM;
    }

     String getCurrentTitle() {
        return currentTabName;
    }

    public void stateChanged(ChangeEvent e) {
        int iSelected = ((JTabbedPane)e.getSource()).getSelectedIndex();
        currentTabName = ((JTabbedPane)e.getSource()).getTitleAt(iSelected);
        if (currentTabName == PropertiesMasterPanel.NEXT_STARTUP) {
            if (allNSUPropDefns == null) {
                getNSUDefn();
                getNSUProperty();
            }
            this.allPropDefns = this.allNSUPropDefns;
            allProperties  =  nsuProperties;
        } else if (currentTabName == PropertiesMasterPanel.STARTUP) {

           if (allSUPropDefns == null) {
                getSUDefn();
                getSUProperty();
            } 
            this.allPropDefns = this.allSUPropDefns;
            allProperties =   oraginalProperties;//TODO: May be using stProperties instead
        }
        if (propHM != null)
            propHM.clear();
    }


    private void  getPropertiesAndDefn() {
		getNSUDefn();
        getNSUProperty();
    }

//!!!!!!!!!Warning if properties value = null, the data in table will be empty.


    private void getNSUDefn() {
            try{
                allNSUPropDefns = manager.getNextStartUpDefn();
            } catch (Exception ex) {
                ExceptionUtility.showMessage("Failed getting next startup defination", ex);
                LogManager.logError(LogContexts.PROPERTIES, ex,
                        "Error creating nextStartUp propertyDefination");
            }
            if (allNSUPropDefns == null) {
                allNSUPropDefns = new ArrayList(0);
            }
            //allPropDefns = allNSUPropDefns ;
    }

    private void getNSUProperty() {
        try{
            Properties p = manager.getNSUProperties();
            nsuProperties = new Properties();
            nsuProperties.putAll(p);
        } catch (Exception ex) {
                ExceptionUtility.showMessage("Failed getting next startup properties", ex);
                LogManager.logError(LogContexts.PROPERTIES, ex,
                        "Error creating nextStartUp property");
        }
        if (nsuProperties != null) {
           // allProperties = nsuProperties;
            oldNSUProperties.putAll(nsuProperties);
        }
        allProperties = nsuProperties;
    }

    public Properties getNSUProperties() {
        return nsuProperties;
    }

    public Collection getPropDefn() {
        return allNSUPropDefns;
    }

    private void getSUDefn() {
       	try{
            allSUPropDefns = manager.getStartUpDefn();
        } catch (Exception ex) {
            ExceptionUtility.showMessage("Failed getting start up definition", ex);
            LogManager.logError(LogContexts.PROPERTIES, ex,
              		"Error creating start up property definition");
        }
        if (allSUPropDefns == null) {
            allSUPropDefns = new ArrayList(0);
        }   
        allPropDefns = allSUPropDefns;
    }

    private void getSUProperty() {
     try{
        oraginalProperties = manager.getSUProperties();
     } catch (Exception ex) {
                ExceptionUtility.showMessage("Failed getting start up properties", ex);
                LogManager.logError(LogContexts.PROPERTIES, ex,
                        "Error creating start up property");
        }
        if (oraginalProperties !=null)
            allProperties = oraginalProperties;

      //TODO currect properties
    }         

    boolean getButtonState() {
        return buttonState;
    }
  //// used by applybutton
    void setChangePropValue(String name, HashMap cValue) {
        HashMap changeValue = cValue;
        Properties p = new Properties();
        p.clear();
        if (name != null) {
            tabName = name;
        } else {
            tabName = currentTabName;
        }
        if (tabName == PropertiesMasterPanel.NEXT_STARTUP) {
            oldNSUProperties.putAll(nsuProperties);
            nsuProperties.putAll(changeValue);
            if (name == null)
                allProperties  =  nsuProperties;
            p.putAll(changeValue);
            saveProperty(p, NSUCONFIGINDICATOR);
        }
        if (propHM != null)
            propHM.clear();
    }

    void resetPropValue() {
		if (currentTabName == PropertiesMasterPanel.NEXT_STARTUP) {
            getNSUProperty();
            allProperties  =  nsuProperties;
            //saveProperty(pDiff,NSUCONFIGINDICATOR);
        }
        if (propHM != null)
            propHM.clear();
    }


    private void saveProperty(Properties p, int i) {
       Iterator iter = p.keySet().iterator();
        try{
            while (iter.hasNext()) {
                String name = (String) iter.next();
                manager.setProperty(name, (String)p.get(name), i);
            }
        } catch (Exception ex) {
                ExceptionUtility.showMessage("Failed saving property", ex);
                LogManager.logError(LogContexts.PROPERTIES, ex,
                        "Error creating save property");
        }

    }
    
    public void setGroupDefn(String groupName, List groupPropDefns) {
        if (defnHM == null) {
            defnHM = new HashMap();
        }
        if (groupPropDefns != null) {
            defnHM.put(groupName, groupPropDefns);
        } else {
            defnHM.put(groupName, new ArrayList(0));
        }
    }

    private void filterPropDf(String gName) {
        Object propValue = null;
        Properties groupProperties = new Properties();
        if (propHM == null) {
            propHM = new HashMap();
        }
        if (gName !=null) {
            String groupName = gName;
            if (propHM.get(groupName) == null) {
                groupProperties.clear();
                List pd = (List)defnHM.get(groupName);
                if (pd != null) {
                    Iterator PropDefnsIter = pd.listIterator();
                    while (PropDefnsIter.hasNext()) {
                        PropertyDefinition propDefn =(PropertyDefinition) PropDefnsIter.next();
                        String propName = propDefn.getName();
                        propValue = this.allProperties.get(propName);
                        if (propValue != null) {
                            groupProperties.put(propName,propValue);
                        }
                    }
                }
                
                propHM.put(groupName, groupProperties);
            }                 
            currentPropDefns =(ArrayList)(defnHM.get(groupName));
            currentProperties =(Properties) (propHM.get(groupName));
        }
    }
    
    public List getNSUDefns(List propDefnsList) {
        return getDefnsFromAll(this.allNSUPropDefns, propDefnsList);
    }

    public List getSUDefns(List propDefnsList) {
        return getDefnsFromAll(this.allSUPropDefns, propDefnsList);
    }

    private List getDefnsFromAll(Collection allDefns, List defns) {
        List result = new ArrayList();
        Iterator iter = defns.listIterator();
        PropertyDefinition pDefn, pDefn1;
        while(iter.hasNext()) {
            pDefn = (PropertyDefinition)iter.next();
            Iterator iter1 = allDefns.iterator();
            while(iter1.hasNext()) {
                pDefn1 = (PropertyDefinition)iter1.next();
                if (pDefn1.getDisplayName().equalsIgnoreCase(pDefn.getDisplayName())) {
                    result.add(pDefn1);
                    break;
                }
            }
        }
        return result;
    }
    
    public void refreshData() {
        if (currentTabName == PropertiesMasterPanel.NEXT_STARTUP) {
            getNSUProperty();
        } else if (currentTabName == PropertiesMasterPanel.STARTUP) {
            getSUProperty();
        }
        if (propHM != null) {
            propHM = null;
        }
    }

    //********implements PropertiedObjectEditor

    public List getPropertyDefinitions(PropertiedObject obj) {
        ConsolePropertyObjectId objId = (ConsolePropertyObjectId)obj;
        if (!objId.getGroupName().equals(ConsolePropertyObjectId.ALL_SYS_PROPS)) {
                filterPropDf(objId.getGroupName());
                if (currentPropDefns != null) {
                    objId.setGroupProperties(currentProperties);
                    objId.setGroupPropDefn(currentPropDefns);
                }
        } else {
            objId.setGroupProperties(allProperties);
            if (allPropDefns == null) {

               return new ArrayList(0);

            }
            objId.setGroupPropDefn(allPropDefns);
        }

        List defns = objId.getFilteredPropDefn();
        Collections.sort(defns, comp);
        return defns;
    }
    
    /**
     * Return whether this editor may be used to set property values on
     * the specified PropertiedObject.
     * @param obj the propertied object; may not be null
     * @return true if the object may not be modified, or false otherwise.
     * @throws AssertionError if <code>obj</code> is null
     */
    public boolean isReadOnly(PropertiedObject obj)throws UnsupportedOperationException{return false;}

    /**
     * Obtain from the specified PropertiedObject the property value
     * that corresponds to the specified PropertyDefinition.  The return type and cardinality
     * (including whether the value may be null) depend upon the PropertyDefinition.
     * @param obj the propertied object whose property value is to be obtained;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the value for the property, which may be an empty collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0", or the NO_VALUE reference if the specified object
     * does not contain the specified PropertyDefinition
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public Object getValue(PropertiedObject obj, PropertyDefinition def) {
        Properties currentProperty;
        //object propValue = NO_VALUE;
        Object propValue = null;
        ConsolePropertyObjectId objId = (ConsolePropertyObjectId)obj;
        PropertyDefinition defn = def;
        if (objId.getGroupProperties() != null && defn != null) {
            currentProperty = objId.getGroupProperties();
            propValue = currentProperty.get(defn.getName());
            if ((propValue != null) && defn.getPropertyType().equals(PropertyType.BOOLEAN)) {
                propValue = new Boolean(currentProperty.get(defn.getName()).toString());
            }
        }
        return propValue;
    }

    /**
     * Return whether the specified value is considered valid.  The value is not
     * valid if the propertied object does not have the specified property definition,
     * or if it does but the value is inconsistent with the requirements of the
     * property definition.
     * @param obj the propertied object whose property value is to be validated;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be validated; may not be null
     * @param value the proposed value for the property, which may be a collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0"
     * @return true if the value is considered valid, or false otherwise.
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
    public boolean isValidValue(PropertiedObject obj, PropertyDefinition def, Object value) {
        
        if (value instanceof String) {
            try {
                validations.isPropertyValid(def.getName(), (String) value);
            } catch (Exception e) {
                return false;
            }
        }

        return true;
    }

    public String getPropertyValue() {
        PropertyDetail pDetail = new PropertyDetail();
        pDetail.setDisplayName(propertyValue);
        return propertyValue;
    }
    
    /**
     * Set on the specified PropertiedObject the value defined by the specified PropertyDefinition.
     * @param obj the propertied object whose property value is to be set;
     * may not be null
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be changed; may not be null
     * @param value the new value for the property; the cardinality and type
     * must conform PropertyDefinition
     * @throws IllegalArgumentException if the value does not correspond
     * to the PropertyDefinition requirements.
     * @throws AssertionError if either of <code>obj</code> or <code>def</code> is null
     */
   	public void setValue(PropertiedObject obj, PropertyDefinition def, Object value) {
        if (value != null) {
            propChangeHM.put(def.getName(), value.toString());
        } else {
        	Object defaultValue = def.getDefaultValue();
        	String strValue;
        	if (defaultValue == null) {
        		strValue = "";
        	} else {
        		strValue = defaultValue.toString();
        	}
			propChangeHM.put(def.getName(), strValue);
		}
    }

    public List getAllowedValues(PropertiedObject obj, PropertyDefinition def) {
        Assertion.isNotNull(def,"The PropertyDefinition reference may not be null");
        return def.getAllowedValues();    
    }

    //====
    public static final boolean DEFAULT_READ_ONLY_PRIVILEGE = false;


    public boolean isReadOnly(PropertiedObject obj, PropertyDefinition def)throws UnsupportedOperationException{return false;}

    public void setReadOnly(PropertiedObject obj,  PropertyDefinition def, boolean readOnly) {}
    public void setReadOnly(PropertiedObject obj, boolean readOnly) {}
    public void reset(PropertiedObject obj) {}
    
    public UserTransaction createReadTransaction() {
        return new FakeUserTransaction();
    }

    public UserTransaction createWriteTransaction() {
        return new FakeUserTransaction();
    }

    public UserTransaction createWriteTransaction(Object p0) {
        return new FakeUserTransaction();
    }
    
    public PropertyAccessPolicy getPolicy() throws UnsupportedOperationException{return null;}


    public void setPolicy(PropertyAccessPolicy policy) {}

     /**
     * Method to add any kind of a ChangeListener.
     *
     * @param listener   Listener to add
     */
    public void addChangeListener(ChangeListener listener) {
        getListeners().add(ChangeListener.class, listener);
    }

    /**
     * Method to remove a ChangeListener.
     * @param listener  Listener to remove
     */
    public void removeChangeListener(ChangeListener listener) {
        getListeners().remove(ChangeListener.class, listener);
    }
    
    public EventListenerList getListeners() {
        return listeners;
    }

    // ==========================
    //  Other supporting methods
    // ==========================
    /**
     * Call stateChanged() for each listener.
     * @param e     event to be fired
     */
    protected void fireChangedEvent(ChangeEvent e) {
        // Guaranteed to return a non-null array
        Object[] listeners = getListeners().getListenerList();

        // Process the listeners last to first, notifying
        // those ChangeListeners that are interested in this event
        for (int i = listeners.length - 2; i >= 0; i -= 2) {
            if (listeners[i] == ChangeListener.class) {
                ((ChangeListener)listeners[i+1]).stateChanged(e);
            }
        }
    }

}

