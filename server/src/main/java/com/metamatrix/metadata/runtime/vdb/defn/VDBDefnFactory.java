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

package com.metamatrix.metadata.runtime.vdb.defn;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBStreamImpl;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.vdb.runtime.BasicModelInfo;
import com.metamatrix.vdb.runtime.BasicVDBDefn;


public class VDBDefnFactory {
    
    private static BasicVDBDefn createDEF(VirtualDatabase vdb, byte[] archive) throws Exception {
        
        BasicVDBDefn vdbDefn = new BasicVDBDefn(vdb.getName()); 
        vdbDefn.setUUID(vdb.getGUID());
        vdbDefn.setDescription( (vdb.getDescription()==null?"":vdb.getDescription()) ); //$NON-NLS-1$
        vdbDefn.setFileName(vdb.getFileName());
        
        vdbDefn.setVDBStream(new VDBStreamImpl(archive));
               
        vdbDefn.setCreatedBy(vdb.getVersionBy());
        vdbDefn.setDateCreated(vdb.getVersionDate());
        
        vdbDefn.setStatus(vdb.getStatus());
        vdbDefn.setVersion(vdb.getVirtualDatabaseID().getVersion());    
        
        return vdbDefn;
    }

    /**
     * This create is used when exporting a specific
     * vdb and version from the database repository. 
     * @param vdbName
     * @param vdbVersion
     * @return VDBDefn contains the vdb information to be exported
     * @throws Exception
     */

    public static VDBArchive createVDBArchive(String vdbName, String vdbVersion) throws Exception {
        if (vdbName == null) {
            Assertion.isNotNull(vdbName, RuntimeMetadataPlugin.Util.getString("VDBCreation.Invalid_VDB_name"));//$NON-NLS-1$
        }
        
        VirtualDatabaseID vdbID = getVirtualDatabaseID(vdbName, vdbVersion); 
        if (vdbID == null) {
            String msg = RuntimeMetadataPlugin.Util.getString("VDBDefnFactory.VDB_version_not_found", new Object[] { vdbName, vdbVersion });//$NON-NLS-1$
            throw new MetaMatrixException(msg);
        }

        VirtualDatabase vdb = getVirtualDatabase(vdbID);
        Collection models = getModels(vdbID);
        byte[] vdbContents = RuntimeMetadataCatalog.getInstance().getVDBArchive(vdbID); 
        
        BasicVDBDefn defn = createDEF(vdb, vdbContents);
        
        loadRuntimeModelFiles(models, defn);

        // load the VDB which was in the server; and replace the new DEF's connector bindings
        VDBArchive baseVDB = new VDBArchive(new ByteArrayInputStream(vdbContents));
        baseVDB.updateConfigurationDef(defn);        
        return baseVDB;
    }
        
    
    private static VirtualDatabaseID getVirtualDatabaseID(String vdbName, String vdbVersion) throws Exception {
        return RuntimeMetadataCatalog.getInstance().getVirtualDatabaseID(vdbName, vdbVersion);
    }
    private static VirtualDatabase getVirtualDatabase(VirtualDatabaseID vdbID) throws Exception {
       return RuntimeMetadataCatalog.getInstance().getVirtualDatabase(vdbID);
    }
    
    protected static Collection getModels(VirtualDatabaseID vdbID) throws Exception {
        return RuntimeMetadataCatalog.getInstance().getModels(vdbID);
        
    }

    private static void loadRuntimeModelFiles(Collection models, BasicVDBDefn defn) throws Exception {
        
        if( models != null) {
            
            for (Iterator it = models.iterator(); it.hasNext();) {
                Model m = (Model) it.next();
                BasicModelInfo modelDef = create(m);
                
                defn.addModelInfo(modelDef);
                
                List bindings = m.getConnectorBindingNames();
                for (Iterator bit=bindings.iterator(); bit.hasNext();) {
                    String name=(String) bit.next();
                    ConnectorBinding cb = getConnectorBinding(name);
                    
                    Assertion.isNotNull(cb, RuntimeMetadataPlugin.Util.getString("VDBDefnFactory.No_connector_binding_found", name)); //$NON-NLS-1$
      
                    ComponentType type = getComponentType(cb.getComponentTypeID());
                    
                    defn.addConnectorType(type);
                                        
                    defn.addConnectorBinding(modelDef.getName(), cb);
                }
                
            }
        }
    }
    
    private static BasicModelInfo create(Model model) {

		BasicModelInfo md = new BasicModelInfo(model.getName());

		md.setModelType(model.getModelType());
		md.setModelURI(model.getModelURI());
		md.setVersion(model.getVersion());
		md.setUuid(model.getGUID());
		md.setIsVisible(model.isVisible());
		md.enableMutliSourceBindings(model.isMultiSourceBindingEnabled());
		md.setVersionDate(model.getDateVersioned());
		md.setVersionedBy(model.getVersionedBy());
		md.setDescription(model.getDescription());

		return md;
	}

    private static ConnectorBinding getConnectorBinding(String routing) throws Exception {
        // the name passed in could either:
        //	1.  The name of the binding
        //  2.  The name of a system binding, for which no actual binding will be found

        ConnectorBinding cb = CurrentConfiguration.getInstance().getConfiguration().getConnectorBindingByRoutingID(routing);

        if (cb == null) {
        Collection b = CurrentConfiguration.getInstance().getConfiguration().getConnectorBindings();
        for (Iterator it = b.iterator(); it.hasNext();) {
              ConnectorBinding cb2 = (ConnectorBinding) it.next();
            
              if (cb2.getRoutingUUID().equals(routing)) {

                  return cb2;
            }
        }

        }
        return cb;
    }

    private static ComponentType getComponentType(ComponentTypeID typeID) throws Exception {
        return CurrentConfiguration.getInstance().getConfigurationModel().getComponentType(typeID.getFullName());

    }
}
