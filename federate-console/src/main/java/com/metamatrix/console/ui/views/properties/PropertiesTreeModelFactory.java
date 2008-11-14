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
import java.util.Iterator;
import java.util.List;

import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.console.ui.tree.SortableChildrenNode;
import com.metamatrix.console.util.HashedList;
import com.metamatrix.console.util.HashedListEntryWrapper;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeModel;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

public class PropertiesTreeModelFactory
{

    // Use HashedList to organize the data so it can be put
    //  into a recursive structure (tree model in this case)
    private HashedList hlPreTreeModel = null;
    private Collection colPropDefs   = null;

/* OBSOLETE
    // TreeModel class to use for all tree models.
    //  Default is DefaultTreeModel.
    private String sTreeModelClassName
        =  "javax.swing.tree.DefaultTreeModel";

    // TableModel class to use for all table models.
    //  Default is DefaultSortableTableModel.
    private String sTableModelClassName
        =  "com.metamatrix.console.ui.util.DefaultSortableTableModel";
*/

    public PropertiesTreeModelFactory( Collection /*<ProperyDefinition>*/ colPropDefs )
    {
        super();
        this.colPropDefs        = colPropDefs;
    }

    private Collection getPropertyDefinitions()
    {
        return colPropDefs;
    }




    // ========================
    //  INITIAL STUFF
    // ========================
    /**
     * Return the "pre tree model".  This contains all of the
     * data in the form of a HashedList.  From this a TreeModel can be produced.
     *
     */
    private HashedList getPreTreeModel()
    {
        if( hlPreTreeModel == null )
        {
            // Generate the pre tree model
            createPreTreeModel();
        }

        return hlPreTreeModel;
    }

    /**
     * Create the "pre tree model" from the data.
     *
     */
    private void createPreTreeModel()
    {
        if ( hlPreTreeModel == null )
            hlPreTreeModel = new HashedList();

        PropertyDefinition pdefTemp;
        Iterator itPropDefs
            = getPropertyDefinitions().iterator();

        while ( itPropDefs.hasNext() )
        {
            pdefTemp = (PropertyDefinition)itPropDefs.next();
            addPropDefToPreTreeModel( pdefTemp );
        }
    }


    /**
     * Add one ComponentSubResponse's data to the "pre tree model".
     */
    private void addPropDefToPreTreeModel( PropertyDefinition pdef )
    {
        // get the category name
        String sCategory    = parseCategory( pdef.getName() );

        HashedListEntryWrapper hlew = new HashedListEntryWrapper();
        hlew.setHLConcatenatedKey( sCategory );
        hlew.setHLLocalKey( pdef.getName() );
        hlew.setHLObject( pdef );

        hlPreTreeModel.put( hlew );
    }

    private String parseCategory( String sDottedName )
    {
        String sWorkString  = sDottedName;
        String sDot          = ".";
        String sResult      = "";
        String sPrefix1     = "com.metamatrix.";
        String sPrefix2     = "metamatrix.";


        // 1. strip prefix
        if ( sWorkString.startsWith( sPrefix1 ) )
            sWorkString = sWorkString.substring( sPrefix1.length() );
        else
        if ( sWorkString.startsWith( sPrefix2 ) )
            sWorkString = sWorkString.substring( sPrefix2.length() );

        // 2. parse out the first component that remains, before
        //    the first '.'
        int iDotPos     = sWorkString.indexOf( sDot );

        if ( iDotPos > -1 )
            sWorkString = sWorkString.substring( 0, iDotPos );
        else
        {
            // If no dots remain after stripping prefix, this goes into
            // the 'Other' category:
            sWorkString = "general";  // 'Other' if
        }
        sResult = sWorkString;

        return sResult;
    }

    // ========================
    //  MODEL GENERATION
    // ========================

    /**
     * Generate and return the tree model.
     *
     */
    public DefaultTreeModel getTreeModel()
    {
        String sRootTitle                   = "System Properties Root";
        String sSysPropsTitle               = "System Properties";
        DefaultTreeNode dtnRoot             = null;
        SortableChildrenNode dtnSysPropsNode     = null;
//        SortableChildrenNode dtn                 = null;
        SortableChildrenNode dtnNew              = null;


        // 1. create the tree model; its root is created automatically
        DefaultTreeModel dtmTreeModel
            = new DefaultTreeModel( sRootTitle );

        // 2. Get its root node
        dtnRoot = (DefaultTreeNode)dtmTreeModel.getRoot();

        // 3. Add the Sys Props node to the root
        dtnSysPropsNode
            = new SortableChildrenNode( sSysPropsTitle,
                                        getPropertyDefinitions() );
        dtnRoot.addChild( dtnSysPropsNode );

        // 3. Create the Category nodes and add them to the root

        HashedList hlPreTreeModel   = getPreTreeModel();
        String sKey                 = "";
        List lstListOfWrappedPropertyDefinitions;
//        List lstListOfPropertyDefinitions;
        HashedListEntryWrapper hlewTemp;


        Iterator itPreTree = hlPreTreeModel.getHashtable().keySet().iterator();

        while( itPreTree.hasNext() )
        {
            sKey = (String)itPreTree.next();
            lstListOfWrappedPropertyDefinitions
                = hlPreTreeModel.getList( sKey );

            // WHOA! don't you need to extract the PropertyDefinition
            //  objects from the HashedListEntryWrapper objects????

            if( lstListOfWrappedPropertyDefinitions.isEmpty() )
            {
                //ystem.out.println( "No defns exist for key: " + sKey );
            }
            else
            {
                ArrayList arylPropertyDefinitions = new ArrayList();
                Iterator itWrappedDefs
                    = lstListOfWrappedPropertyDefinitions.iterator();

                while( itWrappedDefs.hasNext() )
                {
                    hlewTemp
                        = (HashedListEntryWrapper)itWrappedDefs.next();

                    arylPropertyDefinitions
                        .add( hlewTemp.getHLObject() );
                }

            // Now you have everything...the key and the content...
            //  So create a node, adding it to the Sys Props node:
                dtnNew = new SortableChildrenNode( sKey,
                                                   arylPropertyDefinitions );
                dtnSysPropsNode.addChild( dtnNew );

            }
        }


        return dtmTreeModel;
    }

    // ========================
    //  SUPPORTING STUFF
    // ========================


    // =============================
    //  Inner Classes
    // =============================


}
