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

package com.metamatrix.metadata.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.basic.BasicTreeNode;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.id.ObjectIDFactory;
import com.metamatrix.metadata.runtime.api.Element;
import com.metamatrix.metadata.runtime.api.Group;
import com.metamatrix.metadata.runtime.api.GroupID;
import com.metamatrix.metadata.runtime.api.MetadataID;
import com.metamatrix.metadata.runtime.api.MetadataObject;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.Procedure;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseMetadata;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;
import com.metamatrix.platform.admin.api.PermissionDataNodeDefinition;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeDefinitionImpl;
import com.metamatrix.platform.admin.apiimpl.PermissionDataNodeImpl;
import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;

/**
 */
public class VDBTreeUtility {
    
    
    private VDBTreeUtility() {
    }
    
    /**
     * Builds a tree of <code>Models</code>, <code>Groups</code>, <code>Elements</code>
     * and <code>Proceedures</code> from given <code>VirtualDatabaseMetadata</code>.
     * <br>The given <code>root</code> will be filled with children</br>.
     * @param fakeRoot The reference to the node at which to root the tree.
     * @param idFactory The factory to use when creating IDs for the tree nodes.
     * @param vDBMetadata The metadata for the VDB from which to build the tree.
     */
    public static final void buildDataNodeTree(BasicTreeNode fakeRoot, ObjectIDFactory idFactory, VirtualDatabaseMetadata vDBMetadata) 
    	throws VirtualDatabaseException {

        // +++++++++++++++++++++++++++++++
        // Get Models from VDBMetadata
        // +++++++++++++++++++++++++++++++
        List models = new ArrayList(vDBMetadata.getDisplayableModels());

        // The ObjectID for a particular node
        ObjectID id;
        String resourceName;
        String name;

        // This variable will be used to track the union of a Model's decendants and to
        // set the allowed actions for the Model.
        AuthorizationActions modelAllowedActions = StandardAuthorizationActions.NONE;

        Comparator groupComparator = null;
        Comparator modelComparator = null;
        if (models.size() > 0) {
            // For sorting Groups
            groupComparator = new DataNodeCompare();
            // Sort Models
            modelComparator = new ModelNodeComparator();
            Collections.sort(models, modelComparator);
        }

        // ++++++++++++++++++++++++++++++++
        // Foreach Model
        // +++++++++++++++++++++++++++++++
        Iterator modelItr = models.iterator();
// DEBUG:
        while (modelItr.hasNext()) {
            Model model = (Model) modelItr.next();
            if (model == null) {
                String msg = RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBTREE_0001,new Object[]{vDBMetadata.getVirtualDatabaseID()});
                VirtualDatabaseException e = new VirtualDatabaseException(ErrorMessageKeys.VDBTREE_0001, msg);
                LogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, e, msg);
                continue;
            }

            if (!model.isVisible()) {
           
                // Don't include private Models
//                System.out.println("\t Model: " + model.getName() + " has PRIVATE VISIBILITY");

                continue;
            }

            resourceName = model.getFullName();

            // Create the allowable AuthorizationActions for this Model
            // Note this is just used as a default for node creation. The Model's
            // allowed actions will be set after all decendants are visited.
            AuthorizationActions allowedActions = StandardAuthorizationActions.NONE;

//            if (resourceName.equals(MetadataConstants.RUNTIME_MODEL.VIRTUAL_MODEL_NAME) ||
//                    resourceName.equals(MetadataConstants.RUNTIME_MODEL.XML_DOCUMENT_MODEL_NAME) ||
//                    resourceName.equals(MetadataConstants.RUNTIME_MODEL.ODBC_SYSTEM_MODEL_NAME)) {
//                // System Models can only be READ authorized
//                allowedActions = StandardAuthorizationActions.DATA_READ;
//                modelAllowedActions = allowedActions;
//            }

            // Add the Model...
            ModelID modelID = (ModelID) model.getID();
            name = model.getName();
            // Get an ID
            id = idFactory.create();
            // Create node defn
            PermissionDataNodeDefinition nodeDef = new PermissionDataNodeDefinitionImpl(resourceName,
                    name,
                    PermissionDataNodeDefinition.TYPE.MODEL);
            // Create Model node

            PermissionDataNodeImpl modelNode = new PermissionDataNodeImpl(fakeRoot,
                    allowedActions,
                    nodeDef,
                    model.isPhysical(),
                    id);
            
            PermissionDataNodeImpl parentNode = null;
            
            Map catMap = new HashMap();
            // +++++++++++++++++++++++++++++++
            // Get GroupIDs from Model
            // +++++++++++++++++++++++++++++++
            
            Collection groups = null;
            groups = vDBMetadata.getGroupsInModel(modelID);
//           System.out.println("*** Process Groups in the Model *** " + groups.size());

            // Sort groupIDs so that we can descend depth first down the tree
            List theGroups = new ArrayList(groups);
            Collections.sort(theGroups, groupComparator);

            // Record parent doc path because we'll encounter "group" children of these
            // documents that we don't want in the tree.
            String parentDocPath = null;

            List categories = null;
            // Foreach GroupID
            Iterator groupItr = theGroups.iterator();
            while (groupItr.hasNext()) {
                Group group = (Group) groupItr.next();

                if ( group == null ) {
                	LogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.VDBTREE_0002,new Object[]{vDBMetadata.getVirtualDatabaseID(),model.getFullName()}));
                    continue;
                 }
                
                // get the list of categories that may or may not
                // exist between the model name and the group name
                categories = getCategories(modelID, (GroupID) group.getID());
                
                // call to build the nodes (if not already built) from the
                // modelNode.  The returned parent node will be the
                // parent node for this group 
                parentNode = buildCategoryNodes(catMap, categories, modelID.getFullName(), modelNode, idFactory);
//                parentPath = group.getID().getParentFullName();
                
 //                System.out.println("\t Group In Tree: " + group.getFullName());
                
                // Get Group from VDBMetadata
                // Add the Group...
                resourceName = group.getFullName();
                // unless it's the child of a doc
                if (parentDocPath != null && resourceName.startsWith(parentDocPath)) {
                    continue;
                }
                name = group.getName();
                boolean groupSupportsUpdate = group.supportsUpdate();
                // Get an ID
                id = idFactory.create();
                // Create node defn
                int type = (group.isVirtualDocument() ? PermissionDataNodeDefinition.TYPE.DOCUMENT : PermissionDataNodeDefinition.TYPE.GROUP);
                nodeDef = new PermissionDataNodeDefinitionImpl(resourceName, name, type);
                
               
                // Create Actions
                allowedActions = (groupSupportsUpdate && !(type == PermissionDataNodeDefinition.TYPE.DOCUMENT) ?
                        StandardAuthorizationActions.ALL : StandardAuthorizationActions.DATA_READ);
                modelAllowedActions = StandardAuthorizationActions.getORedActions(modelAllowedActions, allowedActions);
                // Create Group (or Document) node
// DEBUG:
//                String groupType = (type == PermissionDataNodeDefinition.TYPE.DOCUMENT ? "Document" : "Group");
//                System.out.println("    Creating " + groupType + " node: <" + name + "> <" + resourceName + "> Under <" + modelNode.getResourceName() + ">");
                PermissionDataNodeImpl groupNode = new PermissionDataNodeImpl(parentNode,
                        allowedActions,
                        nodeDef,
                        group.isPhysical(),
                        id);

                // Don't descend into group's elements if group is a Document
                // Documents are only allowed READ
                if (type == PermissionDataNodeDefinition.TYPE.DOCUMENT) {
                    parentDocPath = resourceName;
                    continue;
                }
                parentDocPath = null;

                // +++++++++++++++++++++++++++++++
                // Get ElementIDs from Group
                // +++++++++++++++++++++++++++++++
                Collection elements = null;
                 elements = vDBMetadata.getElementsInGroup((GroupID)group.getID());
                 
//                System.out.println("*** Process Columns in Group " + group.getFullName() + " *** " + elements.size());
                 
                 if (elements == null) {
//                     System.out.println("NULL ELEMENTS FOR GROUP " + group.getName() );
                     continue;
                 }

                // Foreach ElementID
                Iterator elementItr = elements.iterator();
                while (elementItr.hasNext()) {
                    Element element = (Element) elementItr.next();
//                    System.out.println("\t Column: " + element.getFullName());

                    // Get Element from VDBMetadata
                    // Add the Element...
                    resourceName = element.getFullName();
                    name = element.getName();
                    // Get an ID
                    id = idFactory.create();
                    // Create node defn
                    nodeDef = new PermissionDataNodeDefinitionImpl(resourceName, name, PermissionDataNodeDefinition.TYPE.ELEMENT);
                    // Create Actions
                    if (groupSupportsUpdate && element.supportsUpdate()) {
                        allowedActions = StandardAuthorizationActions.getAuthorizationActions(
                                StandardAuthorizationActions.DATA_CREATE_VALUE |
                                StandardAuthorizationActions.DATA_READ_VALUE |
                                StandardAuthorizationActions.DATA_UPDATE_VALUE);
                    } else {
                        allowedActions = StandardAuthorizationActions.DATA_READ;
                    }
                    modelAllowedActions = StandardAuthorizationActions.getORedActions(modelAllowedActions, allowedActions);
                    // Create Element node
// DEBUG:
//                    System.out.println("      Creating element node: <" + name + "> <" + resourceName + "> Under <" + groupNode.getResourceName() + ">");
//                    PermissionDataNodeImpl elementNode =
                        new PermissionDataNodeImpl(groupNode,
                            allowedActions,
                            nodeDef,
                            element.isPhysical(),
                            id);
                } // End elementItr
            } // End groupItr
            
            // ++++++++++++++++++++++++++++++++
            // Get Procedures from VDBMetadata
            // +++++++++++++++++++++++++++++++
            Collection procedures = Collections.EMPTY_LIST;
                procedures = vDBMetadata.getProcedures(modelID);

//            System.out.println("*** Process Procedures in the Model *** " + procedures.size());


            // Foreach Procedure
            Iterator procedureItr = procedures.iterator();
            while (procedureItr.hasNext()) {
                Procedure procedure = (Procedure) procedureItr.next();

                // Add the Procedure...
                resourceName = procedure.getFullName();
                
                StringBuffer sb = new StringBuffer();
                List nameComponents = procedure.getID().getNameComponents();              
                for(int i=1; i< nameComponents.size(); i++) {
                    sb.append(nameComponents.get(i));
                    if (i < nameComponents.size()-1) {
                        sb.append(".");
                    }
                }
                
                //StringUtil.getTokens(resourceName, procedure.getID().)
                
                name = procedure.getName();
                // Get an ID
                id = idFactory.create();
                // Create node defn
                nodeDef = new PermissionDataNodeDefinitionImpl(resourceName, sb.toString(), PermissionDataNodeDefinition.TYPE.PROCEDURE);
                // Create Stored Procedure allowed actions
                allowedActions = StandardAuthorizationActions.DATA_READ;
                modelAllowedActions = StandardAuthorizationActions.getORedActions(modelAllowedActions, allowedActions);

                // Create Stored Procedure node
// DEBUG:
//                System.out.println("    Creating procedure node: <" + name + "> <" + resourceName + "> Under <" + modelNode.getResourceName() + ">");
//                PermissionDataNodeImpl procedureNode =
                    new PermissionDataNodeImpl(modelNode,
                        allowedActions,
                        nodeDef,
                        false,
                        id);
            }
            

            // Now that we know all decendant allowed actions, set allowed actions on the Model.
            modelNode.setAllowedActions(modelAllowedActions);
            modelAllowedActions = StandardAuthorizationActions.NONE;
        } // End Model Itr
    }
    
    private static List getCategories(ModelID modelID, GroupID groupID) {

      int mcnt = modelID.getNameComponents().size();
      int gcnt = groupID.getNameComponents().size();
                        
      // if the sum of the group name nodes is greater than
      // the sum of the model name nodes plus 1 then
      // there's an extra node between the model name
      // and the group name.
      // These extra nodes have to be accounted for
//      System.out.println("GetCategories for model " + modelID + " group: " + groupID);
      if (gcnt > (mcnt + 1)) {
          List n = groupID.getNameComponents();
          List categories = new ArrayList(n.size());
          for (int i=mcnt; i < (gcnt - 1); i++ ) {
//              System.out.println("CATEGORY " + n.get(i) );
             categories.add(n.get(i)); 
          }
          return categories;
      } 
        return Collections.EMPTY_LIST;

    } 
    
    private static PermissionDataNodeImpl buildCategoryNodes(Map catMap, List categories, String parentName, PermissionDataNodeImpl parentNode, ObjectIDFactory idFactory) {
        AuthorizationActions allowedActions = StandardAuthorizationActions.NONE;
        
          PermissionDataNodeDefinition catnodeDef = null;
          PermissionDataNodeImpl catNode = null;
                  
          String catName = null;      
           if (categories != null && categories.size() > 0) {
              StringBuffer catNameBuf = new StringBuffer(parentName);
                
              for (Iterator cit=categories.iterator(); cit.hasNext(); ) {
                  String category = (String) cit.next();
                    
                  catNameBuf.append("."); //$NON-NLS-1$
                  catNameBuf.append(category);
                  catName = catNameBuf.toString();
                  
                  if (catMap.containsKey(catName)) {
                      parentNode = (PermissionDataNodeImpl) catMap.get(catName);
                  } else {

                      catnodeDef = new PermissionDataNodeDefinitionImpl(catName,
                      category,
                              PermissionDataNodeDefinition.TYPE.CATEGORY);

//                        System.out.println("  Creating cat node: <" + catName + "> ");
        
                      catNode = new PermissionDataNodeImpl(parentNode,
                              allowedActions,
                              catnodeDef,
                              false,
                      idFactory.create());
                        
                      catMap.put(catName, catNode);
                        
                      parentNode = catNode;
                  }
                    
              }
          }
        return parentNode;
          
         
    }
    
    
}

class DataNodeCompare implements Comparator {

    public int compare(Object a, Object b) {
        String aName = null;
        String bName = null;
        if (a instanceof MetadataID) {
            aName = ((MetadataID)a).getFullName();
            bName = ((MetadataID)b).getFullName();
        } else if (a instanceof MetadataObject) {
            
            aName = ((MetadataObject)a).getFullName();
            bName = ((MetadataObject)b).getFullName();
        } else {
            return -1;
        }
        return aName.compareTo(bName);
    }
    
    public boolean equals(Object a, Object b) {
        if (a instanceof MetadataID) {
            return ((MetadataID)a).getFullName().equals(((MetadataID)b).getFullName());
            
        } else if (a instanceof MetadataObject) {
            return ((MetadataObject)a).getFullName().equals(((MetadataObject)b).getFullName());
            
        }
        
        return false;
    }
}
    
class ModelNodeComparator implements Comparator {

    public int compare( Object a, Object b ) {
        String aName = ((Model) a).getFullName();
        String bName = ((Model) b).getFullName();
        
        return aName.compareTo(bName);

    }

    public boolean equals( Object a, Object b ) {
        return ((Model) a).getFullName().equals(((Model) b).getFullName());
    }
}
    

