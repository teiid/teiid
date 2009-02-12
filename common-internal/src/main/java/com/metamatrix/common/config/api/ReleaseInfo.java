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

package com.metamatrix.common.config.api;

import java.io.Serializable;

/**
 * This class is a data holder for release information about a product
 */
public class ReleaseInfo implements Serializable{

    private String name; // name of the product
    private String version; // version of the release
    private String date; // date of the build/release
    private String build; // build number

    /**
     * Constructor which sets the various details about release information.
     * @param name of the product.
     * @param version of the release.
     * @param date String build/release date.
     * @param build String build number.
     */
    public ReleaseInfo(String name, String version, String date, String build) {
        this.name = name;
        this.version = version;
        this.date = date;
        this.build = build;
    }

    /**
     * This method returns the name of the product.
     * @return product name.
     */
    public String getName() {
        return name;
    }

    /**
     * This method returns the version of the product.
     * @return release version.
     */
    public String getVersion() {
        return version;
    }

    /**
     * This method returns the build/release date of the product.
     * @return release date
     */
    public String getDate() {
        return date;
    }

    /**
     * This method returns the build number of the released product.
     * @return build number of the product.
     */
    public String getBuild() {
        return build;
    }

}
