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

package com.metamatrix.toolbox.ui.widget.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.metamatrix.core.util.Assertion;

public class FileResourceConstants {

    private static Set XMI_FILE_EXTENSIONS = new HashSet();
    public static final String EXTENSION_DELIMITER              = ".";
    public static final String XMI_FILE_EXTENSION               = ".xmi";
    public static final String XMI_MODEL_FILE_EXTENSION         = ".xml";
    public static final String XMI_METAMODEL_FILE_EXTENSION     = ".xml";
    public static final String XMI_METAMETAMODEL_FILE_EXTENSION = ".xml";
    public static final String XMI_PROJECT_FILE_EXTENSION       = ".prj";
    public static final String XMI_DTC_FILE_EXTENSION           = ".xml";
    public static final String ARCHIVE_EXTENSION                = ".zip";

    static {
        XMI_FILE_EXTENSIONS.add(XMI_FILE_EXTENSION);
        XMI_FILE_EXTENSIONS.add(XMI_MODEL_FILE_EXTENSION);
        XMI_FILE_EXTENSIONS.add(XMI_METAMODEL_FILE_EXTENSION);
        XMI_FILE_EXTENSIONS.add(XMI_METAMETAMODEL_FILE_EXTENSION);
        XMI_FILE_EXTENSIONS.add(XMI_PROJECT_FILE_EXTENSION);
        XMI_FILE_EXTENSIONS.add(XMI_DTC_FILE_EXTENSION);
        XMI_FILE_EXTENSIONS.add(ARCHIVE_EXTENSION);
    }

    /**
     * Return the list of supported XMI file extensions
     * @return the unmodifiable list of XMI file extensions; never null
     */
    public static Collection getXMIFileExtensions() {
        return Collections.unmodifiableSet(XMI_FILE_EXTENSIONS);
    }

    /**
     * Return true if the specified file name has one of the supported
     * XMI file extensions
     * @return true if the file name has one of the XMI file extensions,
     * otherwise false;
     */
    public static boolean hasXMIFileExtension(String filename) {
        Assertion.isNotNull(filename,"The filename may not be null");
        if ( filename.length() == 0 || XMI_FILE_EXTENSIONS.isEmpty() ) {
            return false;
        }
        Iterator itr = XMI_FILE_EXTENSIONS.iterator();
        while (itr.hasNext()) {
            String extension = (String) itr.next();
            if (filename.endsWith(extension)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if the specified extension is a member of the supported
     * list of XMI file extensions
     * @return true if the extension is valid, otherwise false;
     */
    public static boolean isValidXMIFileExtension(String extension) {
        String ext = extension;
        if ( !extension.startsWith(".")) {
            ext = "." + extension;
        }
        return XMI_FILE_EXTENSIONS.contains(ext);
    }

//    /**
//     * Return true if the specified extension is a member of the supported
//     * list of XMI file extensions
//     * @return true if the extension is valid, otherwise false;
//     */
//    public static boolean isExtensionValid(String extension) {
//        String ext = extension;
//        if ( !extension.startsWith(".")) {
//            ext = "." + extension;
//        }
//        return XMI_FILE_EXTENSIONS.contains(ext);
//    }

}

