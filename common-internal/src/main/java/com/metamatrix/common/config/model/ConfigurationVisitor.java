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

package com.metamatrix.common.config.model;

import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
/**
* This interface defines a visitor that can be passed to any Configuration
* object via its Accept() method.  This visitor will perform an action
* on each configuration object that it is Accept()ed by possibly accumulating
* results for further use.
*/
public abstract class ConfigurationVisitor {
    
    abstract void visitComponent(ComponentObject component);
    
    abstract void visitComponent(ComponentType compType);
    
  
//    abstract void visitConfiguration(Configuration configuration);
//    
//    abstract void visitConfigurationInfo(ConfigurationInfo info);
//    
//    abstract void visitVMComponentDefn(VMComponentDefn defn);
//    
//    abstract void visitDeployedComponent(DeployedComponent deployedComponent);
//    
//    abstract void visitServiceComponentDefn(ServiceComponentDefn defn);
//    
//    abstract void visitConnectorComponent(ConnectorBinding connector);
//    
//    abstract void visitResourceDescriptor(ResourceDescriptor descriptor);
//    
//    
//    abstract void visitProductServiceConfig(ProductServiceConfig config);  
//    
//    abstract void visitHost(Host host);   
//    
//    abstract void visitComponentType(ComponentType compType);    
//     
      
}
    
