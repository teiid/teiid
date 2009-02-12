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

package com.metamatrix.metadata.runtime.util;

import com.metamatrix.metadata.runtime.api.*;

/**
 * The RuntimeIDParser provides static methods to obtain specific information from a fully qualfied name.  This would include asking for the path, group name, or data source name.
 */
public class RuntimeIDParser {
    private final static String PERIOD = ".";
    private final static String BLANK = "";

    /**
     * Returns the path for the given id. If no path then an empty <code>String</code> is returned.
     * @param MetadataID id to be parsed.
     * @return String path of the id.
     */
    public static String getPath(MetadataID id) {

	if (id instanceof VirtualDatabaseID ||
		id instanceof ModelID) {
		return BLANK;
	}

	int startIDX = 1;	
	int endIDX = -1;

	int size = id.getNameComponents().size();

	if (id instanceof GroupID ||
		id instanceof ProcedureID) {

	    // if size is less than 2 then the name only contains the DataSourceName and its name
        if (size <= 2) {
                	return BLANK;
        	}
        	
        	endIDX = size - 1;
        } else if (id instanceof ElementID || id instanceof KeyID) {
            // if size is less than 3 then the name only contains the DataSourceName, GroupName and its name
            if (size <= 3) {
                return BLANK;
            }
            endIDX = size - 2;
        }

        StringBuffer sb = new StringBuffer();
        for (int i = startIDX; i < endIDX; i++) {
                if (i > startIDX) {
                        sb.append(PERIOD);
                }
                sb.append( id.getNameComponent(i));
        }
       
        return sb.toString();
    }

    /**
     * Returns the group name for the given id.
     * @param MetadataID id to be parsed.
     * @return String group name of the id.
     */
    public static String getGroupName(MetadataID id) {
	    if (id instanceof ElementID || id instanceof KeyID) {
             int size = id.getNameComponents().size();
             return id.getNameComponent(size - 2);
        }
        return BLANK;
    }

    /**
     * Returns the fully qualified group name for the given id.
     * @param MetadataID id to be parsed.
     * @return String group name of the id.
     */
    public static String getGroupFullName(MetadataID id) {
	    if (id instanceof ElementID || id instanceof KeyID) {
             int size = id.getNameComponents().size();
             return id.getFullName().substring(0, id.getFullName().indexOf(id.getNameComponent(size - 1)) - 1);
        }
        return BLANK;
    }
}

