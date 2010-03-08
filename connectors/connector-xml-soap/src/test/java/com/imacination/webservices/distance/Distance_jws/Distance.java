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

/**
 * Distance.java
 *
 */

package com.imacination.webservices.distance.Distance_jws;

public class  Distance {
    public java.lang.String getState(java.lang.String zip) {
        return "IL"; //$NON-NLS-1$
        
    }
    public java.lang.String getLocation(java.lang.String zip) {
        return null;
    }
    public java.lang.String getCity(java.lang.String zip) {
        return null;
    }
    public double getDistance(java.lang.String fromZip, java.lang.String toZip) {
        return 0;
    }
    public double getLatitude(java.lang.String zip) {
        return 0;
    }
    public double getLongitude(java.lang.String zip)  {
        return 0;
    }
}
