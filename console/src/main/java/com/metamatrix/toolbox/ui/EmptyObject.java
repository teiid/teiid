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

//################################################################################################################################
package com.metamatrix.toolbox.ui;

// System imports
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
This object is intended for use in structures where primitives are not allowed (like some collections) and class
implementations of interfaces where returned values may not be null, but some representation of a null or empty value is still
required.
@since Golden Gate
@author John P. A. Verhaeg
@version Golden Gate
*/
public class EmptyObject {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    public static final EmptyObject INSTANCE = new EmptyObject();
    
    public static final Iterator ITERATOR = new Iterator() {
        public boolean hasNext() {
            return false;
        }
        public Object next() {
            throw new NoSuchElementException();
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
    };
    
    public static final ListIterator LIST_ITERATOR = new ListIterator() {
        public void add(final Object object) {
            throw new UnsupportedOperationException();
        }
        public boolean hasNext() {
            return false;
        }
        public boolean hasPrevious() {
            return false;
        }
        public Object next() {
            throw new NoSuchElementException();
        }
        public int nextIndex() {
            return 0;
        }
        public Object previous() {
            throw new NoSuchElementException();
        }
        public int previousIndex() {
            return -1;
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
        public void set(final Object object) {
            throw new UnsupportedOperationException();
        }
    };
}
