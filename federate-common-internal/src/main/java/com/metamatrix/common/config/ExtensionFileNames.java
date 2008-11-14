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

package com.metamatrix.common.config;

public interface ExtensionFileNames {

    /**
    * These are the file names defined for extension files used by MetaMatrix.
    * This will be the name loaded into the extension table and should
    * be used for reference when loading an extension file.
    */

  public static final String FUNCTIONAL_DEFINITION = "FunctionDefinitions.xml"; //$NON-NLS-1$
  public static final String CONNECTOR = "MetaMatrix Standard Connectors"; //$NON-NLS-1$
  public static final String KEYWORDS= "modeler_keywords.txt"; //$NON-NLS-1$
  public static final String METAMODEL_EXTENSIONS = "MetamodelExtensions.xml"; //$NON-NLS-1$


  /**
  * These are the physical files that will be loaded and assigned the
  * the above file name at insert time.
  *
  */
  public class PhysicalFileName {
      public static final String FUNCTIONAL_DEFINITION = "FunctionDefinitions.xml"; //$NON-NLS-1$
      public static final String CONNECTOR = "connector.jar"; //$NON-NLS-1$
      public static final String KEYWORDS= "modeler_keywords.txt"; //$NON-NLS-1$
      public static final String METAMODEL_EXTENSIONS = "MetamodelExtensions.xml"; //$NON-NLS-1$

  }


} 
