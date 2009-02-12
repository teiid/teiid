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

package com.metamatrix.core.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Keeps track of all TempDirectories created so that they can be removed.
 * This class exists so that when test suites are run there is a simple way to cleanup.
 * Ideally the application and tests will clean up after themselves without the need for this class.
 */
public class TempDirectoryMonitor {
    private static List instances = new ArrayList();
    private static boolean on = false;
    
    protected void createdTempDirectory(TempDirectory tempDirectoryToCreate) {
        instances.add(tempDirectoryToCreate);
    }
    
    public static void turnOn() {
        if (on) {
        } else {
            on = true;
            TempDirectory.setMonitor(new TempDirectoryMonitor());
        }
    }
    
    public static void removeAll() {
        for (Iterator iterator = instances.iterator(); iterator.hasNext(); ) {
            TempDirectory instance = (TempDirectory) iterator.next();
            instance.remove();
        }
        instances = new ArrayList();
    }
    
    public static boolean hasTempDirectoryToRemove() {
        return instances.size() > 0;
    }
}
