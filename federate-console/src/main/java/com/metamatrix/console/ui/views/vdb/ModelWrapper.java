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

package com.metamatrix.console.ui.views.vdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.vdb.runtime.BasicModelInfo;

public class ModelWrapper {
	
    private BasicModelInfo theModelInfo = null;
    private Model theModel              = null;
    private boolean bContentIsModel         = false;
    private boolean bContentIsModelInfo     = false;
    private Collection /*<ServiceComponentDefn>*/ scdConnectorBindings = new ArrayList();

    public ModelWrapper(BasicModelInfo info) {
        super();
        this.theModelInfo = info;
        bContentIsModelInfo     = true;
    }

    public ModelWrapper( Model theModel) {
        super();
        this.theModel = theModel;
        bContentIsModel = true;
    }

    public String getName() {
        String sResult      = ""; //$NON-NLS-1$
        if( bContentIsModel ) {
            sResult = theModel.getName();
        } else if (bContentIsModelInfo) {
            sResult = theModelInfo.getName();
        }
        return sResult;
    }

    public String getVersion() {
        String sResult      = ""; //$NON-NLS-1$
        if( bContentIsModel )
        {
            ModelID mdlId = (ModelID)theModel.getID();
            sResult = mdlId.getVersion();
        }
        else
        if( bContentIsModelInfo )
        {
            sResult = theModelInfo.getVersion();
        }
        return sResult;
    }

    public List /*<String>*/ getConnectorBindingNames() {
        List result = new ArrayList(0);
        if (bContentIsModel) {
            result = theModel.getConnectorBindingNames();
        } else if (bContentIsModelInfo) {
            // Really, this should be empty because the ModelInfo does
            //  NOT have a connector binding; a CB originates in the GUI
            //  itself and the goes into the hashmap and finally into the
            //  setConnectorBindings method...
        }
        return result;
    }

    public boolean requiresConnectorBinding() {
        boolean bResult = false;
        if ( bContentIsModel ) {
            bResult = theModel.requireConnectorBinding();
        } else if ( bContentIsModelInfo ) {
            bResult = theModelInfo.requiresConnectorBinding();
        }
        return bResult;
    }

    public void setConnectorBindings(Collection /*<ServiceComponentDefn>*/ scdConnectorBindings) {
        this.scdConnectorBindings = scdConnectorBindings;
        if (bContentIsModelInfo) {
            int size = this.scdConnectorBindings.size();
            List bindingNames = new ArrayList(size);
            List bindingUUIDs = new ArrayList(size);
            Iterator it = this.scdConnectorBindings.iterator();
            while (it.hasNext()) {
                ServiceComponentDefn scd = (ServiceComponentDefn)it.next();
                bindingNames.add(scd.getName());
                bindingUUIDs.add(scd.getRoutingUUID());
            }
            theModelInfo.setConnectorBindingNames(bindingNames);
//            theModelInfo.setConnectorBindingUUIDS(bindingUUIDs);
        } else {
        }
    }

    public Collection /*<ServiceComponentDefn>*/ getConnectorBindings() {
        return scdConnectorBindings;
    }
    
    public boolean supportsMultiSourceBindings() {
        boolean result = false;
        if (bContentIsModel) {
            result = theModel.supportsMultiSourceBindings();
        } else if (bContentIsModelInfo) {
            result = theModelInfo.supportsMultiSourceBindings();
        }
        return result;
    }
    
    
    public boolean isMultiSourceBindingsEnabled() {
        boolean result = false;
        if (bContentIsModel) {
            result = theModel.isMultiSourceBindingEnabled();
        } else if (bContentIsModelInfo) {
            result = theModelInfo.isMultiSourceBindingEnabled();
        }
        return result;
    }    
}
