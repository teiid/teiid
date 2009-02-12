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

package com.metamatrix.connector.xmlsource.soap.service;

import java.io.File;
import java.net.ServerSocket;

import org.apache.axis.client.AdminClient;
import org.apache.axis.transport.http.SimpleAxisServer;


/** 
 * A simple wrapper to start and stop a Axis based Web Service Server
 */
public class WebServiceServer {
    SimpleAxisServer server;
    int port;
    
    /**
     * start the web server at the given port 
     * @param port
     */
    public void startServer(int port) throws Exception{
        if (this.server == null) {
            this.port = port;
            this.server = new SimpleAxisServer();
            this.server.setServerSocket(new ServerSocket(port));
            this.server.start();
            //System.out.println("Web Service Server started at port:"+port); //$NON-NLS-1$
        }
        //System.out.println("Web Service Server already running"); //$NON-NLS-1$        
    }
        
    /**
     * stop the web server 
     *
     */
    public void stopServer() {
        if (this.server != null) {
            this.server.stop();
            this.server = null;
            File f = new File("server-config.wsdd"); //$NON-NLS-1$
            f.delete();
        }
    }
    
    /**
     * Is server running 
     * @return
     */
    public boolean isRunning() {
        return (this.server != null);
    }
    
    /**
     * Deploy the service using given WSDD File 
     */
    public void deployService(String wsddFile) throws Exception {
        //System.out.println("Deploying file="+wsddFile); //$NON-NLS-1$      
        String[] args = { "-h127.0.0.1","-p"+this.port, wsddFile }; //$NON-NLS-1$ //$NON-NLS-2$ 
        AdminClient client = new AdminClient();
        client.process(args);
        
        // list the services
        args = new String[] { "-h127.0.0.1","-p"+this.port, "list"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        client.process(args);        
    }
    
    /**
     * Undeploy the service using given WSDD File   
     * @param wsddFile
     */
    public void undeployService(String wsddFile) throws Exception {
        //System.out.println("Undeploying file="+wsddFile); //$NON-NLS-1$
        String[] args = { "-h127.0.0.1","-p"+this.port, wsddFile }; //$NON-NLS-1$ //$NON-NLS-2$ 
        AdminClient client = new AdminClient();
        client.process(args);
        
        // list the services
        args = new String[] { "-h127.0.0.1","-p"+this.port, "list"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        client.process(args);
    }
    
    public static void main(String[] args) throws Exception {
        WebServiceServer server = new WebServiceServer();
        
        server.startServer(7001);
        server.deployService("testdata/service/StockQuotes/invesbot-deploy.wsdd"); //$NON-NLS-1$
        System.out.println("Press Any Key to stop the server"); //$NON-NLS-1$
        System.in.read();
        server.undeployService("testdata/service/StockQuotes/undeploy.wsdd"); //$NON-NLS-1$
        server.stopServer();        
    }
    
}
