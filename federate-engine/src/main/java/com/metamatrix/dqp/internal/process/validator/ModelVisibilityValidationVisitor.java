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

package com.metamatrix.dqp.internal.process.validator;

import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.lang.*;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.validator.AbstractValidationVisitor;

/**
 */
public class ModelVisibilityValidationVisitor extends AbstractValidationVisitor {

    private VDBService vdbService;
    private String vdbName;
    private String vdbVersion;

    /**
     *
     */
    public ModelVisibilityValidationVisitor(VDBService vdbService, String vdbName, String vdbVersion ) {
        super();

        this.vdbService = vdbService;
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
    }

    // ############### Visitor methods for language objects ##################

    public void visit(Delete obj) {
        validateModelVisibility(obj);
    }

    public void visit(Insert obj) {
        validateModelVisibility(obj);
    }

    public void visit(Query obj) {
        validateModelVisibility(obj);
    }

    public void visit(Update obj) {
        validateModelVisibility(obj);
    }

    public void visit(StoredProcedure obj) {
        validateModelVisibility(obj);
    }

    // ######################### Validation methods #########################

    protected void validateModelVisibility(Command obj) {
        // first get list of groups from command
        Collection groups = GroupCollectorVisitor.getGroups(obj, true);

        try {
            // collect models used by this command
            Iterator groupIter = groups.iterator();
            while(groupIter.hasNext()) {
                GroupSymbol group = (GroupSymbol) groupIter.next();
                Object modelID = null;
                if(obj instanceof StoredProcedure){
                    modelID = ((StoredProcedure)obj).getModelID();
                }else{
                    modelID = getMetadata().getModelID(group.getMetadataID());
                }
                if(modelID instanceof TempMetadataID){
                	return;
                }
                String modelName = getMetadata().getFullName(modelID);
                int visibility = this.vdbService.getModelVisibility(this.vdbName, this.vdbVersion, modelName);
                if(visibility != ModelInfo.PUBLIC) {
                    handleValidationError(DQPPlugin.Util.getString("ERR.018.005.0088", getMetadata().getFullName(group.getMetadataID()))); //$NON-NLS-1$
                }
            }
        } catch(QueryMetadataException e) {
            handleException(e, obj);
        } catch(MetaMatrixComponentException e) {
            handleException(e, obj);
        }
    }

}
