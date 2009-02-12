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

package com.metamatrix.console.ui.views.summary;

public class SummaryHostInfo {


    public static final int RUNNING = 1;
    public static final int NOT_RUNNING = 2;

    private String hostName;
    private String hostPortNum;
    private int hostStatus;

    public SummaryHostInfo(String hostName, String hostPortNum, int status){
        this.hostName = hostName;
        this.hostPortNum = hostPortNum;
        this.hostStatus = status;
    }

    public String getHostName(){
        return hostName;
    }

    public String getHostPortNum(){
        return hostPortNum;
    }

    public int getHostStatus(){
        return hostStatus;
    }
} 
