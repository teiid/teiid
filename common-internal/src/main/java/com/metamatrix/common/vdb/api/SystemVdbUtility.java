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

package com.metamatrix.common.vdb.api;

import com.metamatrix.core.util.ResourceNameUtil;

public class SystemVdbUtility {

	public static final String VDB_NAME = ResourceNameUtil.SYSTEM_NAME;;
	public static final String VIRTUAL_MODEL_NAME = ResourceNameUtil.SYSTEM_NAME; 
	public static final String PHYSICAL_MODEL_NAME = ResourceNameUtil.SYSTEMPHYSICAL_NAME;
	public static final String ADMIN_PHYSICAL_MODEL_NAME = ResourceNameUtil.SYSTEMADMINPHYSICAL_NAME;
	
    public final static String[] SYSTEM_MODEL_NAMES = {
		ResourceNameUtil.SYSTEM_NAME, 
		ResourceNameUtil.SYSTEMPHYSICAL_NAME,
		ResourceNameUtil.SYSTEMADMIN_NAME,
		ResourceNameUtil.SYSTEMADMINPHYSICAL_NAME,
		ResourceNameUtil.SYSTEMSCHEMA_NAME, 
		ResourceNameUtil.SYSTEMODBCMODEL,
		ResourceNameUtil.DATASERVICESYSTEMMODEL_NAME,
		ResourceNameUtil.WSDL1_1_NAME, ResourceNameUtil.WSDLSOAP_NAME,
		ResourceNameUtil.JDBCSYSTEM_NAME 
	};        


    /**
	 * Return true if the specified model name matches the name of any system
	 * model of TABLE_TYPES.SYSTEM_TYPE (match ignores case)
	 */
    public final static boolean isSystemModelWithSystemTableType(String modelName) {
        for(int i=0; i < SYSTEM_MODEL_NAMES.length; i++) {
            String matchName = SYSTEM_MODEL_NAMES[i];
            if(matchName.equalsIgnoreCase(modelName)) {
                return true;    
            }
        }
        return false;
    }
}
