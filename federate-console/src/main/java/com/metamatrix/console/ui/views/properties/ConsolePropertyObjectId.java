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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertyDefinition;

public class ConsolePropertyObjectId implements PropertiedObject {
    public static final String ALL_SYS_PROPS = "System Property"; //should = root name
    private Properties groupProperties;
    private Collection propDefns;
    private List currentPropDefns = new ArrayList();
    private PropertyFilter propFilter;
    private HashMap defnHM, descriptionHM;
    private String groupName;

    public ConsolePropertyObjectId(String gn, PropertyFilter pFilter){
        groupName = gn;
        propFilter = pFilter;
    }

    public String getGroupName(){
        return groupName;
    }

    public PropertyFilter getPropertyFilter(){
        return propFilter;
    }

   /* public PropertyFilter setPropertyFilter(PropertyFilter filter){
        propFilter = filter;
        return propFilter;
    }  */

    public void setGroupProperties(Properties gp){
        groupProperties = gp;
    }
  //whether need it????
    public void setGroupPropDefn(Collection pd){
        propDefns = pd;
        Iterator iter = propDefns.iterator();
        while (iter.hasNext()){
//            PropertyDefinition pd1 = (PropertyDefinition)
            iter.next();
           // defnHM.put(pd1.getDisplayName(),pd1.getName());
        }
    }

    public Properties getGroupProperties(){
        return groupProperties;
    }

    public List getFilteredPropDefn(){
        defnHM = new HashMap();
        descriptionHM = new HashMap();
        filterPropDefns();
        Iterator currentPropDefnsIter = currentPropDefns.iterator();
        while (currentPropDefnsIter.hasNext()){
            PropertyDefinition pd = (PropertyDefinition)currentPropDefnsIter.next();
            defnHM.put(pd.getDisplayName(),pd.getName());
            descriptionHM.put(pd.getName(), pd.getShortDescription());
        }
        return currentPropDefns;
    }

    public HashMap getDotName(){
        return defnHM;
    }

    public HashMap getDescriptionHM(){
        return descriptionHM;
    }

    private void filterPropDefns(){
        if (propDefns != null){
            currentPropDefns.clear();
//            Iterator propDefnIterator = propDefns.iterator();
            if (isAllProperties())//||(!isBasicProperties()&& !isExpertProperties() &&  !isModifiableProperties() && !isReadOnlyProperties()))
                currentPropDefns.addAll(propDefns);
            else {
                ArrayList defAL = filterBEpropDefns(propDefns);
                if (!defAL.isEmpty()){
                    currentPropDefns = filterMRpropDefns(defAL);
                }

                    
            }

        }
        else {
            //ystem.out.println("Not find groupPropertyDefination,should call getPropertyDefinitions in ConsolePropertiedEditor to gain groupPropertyDefination");
        }

    }

    private ArrayList filterBEpropDefns(Collection pd){
        ArrayList propDefnsBE = new ArrayList();
        if (isBasicProperties()){
            Iterator iter = pd.iterator();
            while ( iter.hasNext()){
                PropertyDefinition propDefn = (PropertyDefinition)iter.next();
                if (!propDefn.isExpert()){
                    if(!propDefnsBE.contains(propDefn)){
                            propDefnsBE.add(propDefn);
                    }
                }
            }
        return propDefnsBE;
        }

        if (isExpertProperties()){
            Iterator iter = pd.iterator();
            while ( iter.hasNext()){
                PropertyDefinition propDefn = (PropertyDefinition)iter.next();
                if (propDefn.isExpert()){
                    if(!propDefnsBE.contains(propDefn)){
                            propDefnsBE.add(propDefn);
                    }
                }
            }
            return propDefnsBE;
        }

        if (isBothBEProperties()){
            Iterator iter = pd.iterator();
            while ( iter.hasNext()){
                PropertyDefinition propDefn = (PropertyDefinition)iter.next();
                if(!propDefnsBE.contains(propDefn)){
                    propDefnsBE.add(propDefn);
                }
            }
            return propDefnsBE;
        }
        return propDefnsBE;
    }

    private List filterMRpropDefns(List pdmr){
        if(!propFilter.isMRBEnable())
            return pdmr;

        List propDefnsMR = new ArrayList();
        //===
        if (isModifiableProperties()){
            Iterator mriter = pdmr.iterator();
            while (mriter.hasNext()){
                PropertyDefinition propDefn = (PropertyDefinition)mriter.next();
                if (propDefn.isModifiable())
                    if(!propDefnsMR.contains(propDefn)){
                        propDefnsMR.add(propDefn);
                    }
            }
            return propDefnsMR;
        }

        if (isReadOnlyProperties()){
            Iterator mriter = pdmr.iterator();
            while (mriter.hasNext()){
                PropertyDefinition propDefn = (PropertyDefinition)mriter.next();
                if (!propDefn.isModifiable())
                    if(!propDefnsMR.contains(propDefn)){
                        propDefnsMR.add(propDefn);

                    }
            }
            return propDefnsMR;
        }

        if (isBothMRProperties()){
            Iterator mriter = pdmr.iterator();
            while (mriter.hasNext()){
                PropertyDefinition propDefn = (PropertyDefinition)mriter.next();

                if(!propDefnsMR.contains(propDefn)){
                    propDefnsMR.add(propDefn);
                }

            }
            return propDefnsMR;
        }
        return propDefnsMR;
    }

    public String getGroupNameId(){
        return propFilter.getGroupName();
    }

    public boolean isAllProperties(){
        return propFilter.isAllProperties();

    }

    public boolean isBasicProperties(){
        return propFilter.isBasicProperties();
    }

    public boolean isExpertProperties(){
        return propFilter.isExpertProperties();
    }

    public boolean isBothBEProperties(){
        return propFilter.isBothBEProperties();
    }

    public boolean isModifiableProperties(){
        return propFilter.isModifiableProperties();
    }

    public boolean isReadOnlyProperties(){
        return propFilter.isReadOnlyProperties();
    }

    public boolean isBothMRProperties(){
        return propFilter.isBothMRProperties();
    }

    public boolean isReadOnly(){
        return true;
    //from tab name will know if it is readOnly;
    }
}
