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

package com.metamatrix.console.util;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;


public class HashedList
{

    private boolean bAllowDupes = false;
    private Hashtable htHash    = null;

    public final static String ROOT = "ROOT";

    public HashedList()
    {
        super();
    }

    public Hashtable getHashtable()
    {
        if ( htHash == null )
        {
            htHash    = new Hashtable();
            htHash.put( ROOT, new Vector() );
        }

        return htHash;
    }


    public void put( HashedListEntry hleEntry )
    {


        // 1.   If there is no concat key, add this entry to ROOT.
        //
        if ( hleEntry.getHLConcatenatedKey() == null )
        {
            Vector v = (Vector)getHashtable().get( ROOT );
            v.add( hleEntry );
        }
        else
        // 2.   See if this concat key is already present
        //      If so, retrieve the List, and add this
        //      entry to it.
        //
        if ( getHashtable().containsKey( hleEntry.getHLConcatenatedKey() ) )
        {
            Vector v = (Vector)htHash.get( hleEntry.getHLConcatenatedKey() );
            // bAllowDupes
            if( v.contains( hleEntry ) )
            {
                if ( bAllowDupes )
                {
                    v.add( hleEntry );
                }
            }
            else
            {
                v.add( hleEntry );
            }
        }
        else
        // 3.   If not, construct a new Vector, holding this entry.
        //      put the (key, vector) into the hashtable
        {
            Vector v = new Vector();
            v.add( hleEntry );
            getHashtable().put( hleEntry.getHLConcatenatedKey(),
                                v );
        }
    }

    public List /* List of HashedListEntrys */
           getList( HashedListEntry hleEntry )
    {
        return getList( hleEntry.getHLConcatenatedKey() );
    }

    public List /* List of HashedListEntrys */
           getList( String sKey )
    {
        return (Vector)getHashtable().get( sKey );
    }

    public void remove( HashedListEntry hleEntry )
    {
        // get the list
        List lstValues = getList( hleEntry.getHLConcatenatedKey() );

        // remove the entry from the list
        lstValues.remove( hleEntry );

        if ( lstValues.size() == 0 )
        {
            // if the list is now empty, remove the whole
            //  hashtable entry
            removeList( hleEntry.getHLConcatenatedKey() );
        }
    }

    public void removeList( HashedListEntry hleEntry )
    {
        removeList( hleEntry.getHLConcatenatedKey() );
    }

    public void removeList( String sKey )
    {
        getHashtable().remove( sKey );
    }

    public void setAllowDupes( boolean b )
    {
        bAllowDupes = b;
    }

    public boolean getAllowDupes()
    {
        return bAllowDupes;
    }

}
