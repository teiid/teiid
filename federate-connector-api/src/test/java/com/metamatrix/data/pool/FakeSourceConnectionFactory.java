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

/*
 */
package com.metamatrix.data.pool;

import com.metamatrix.data.api.ConnectorEnvironment;

/**
 */
public abstract class FakeSourceConnectionFactory implements SourceConnectionFactory {
    static int connCnt;
    
    static boolean alive = true;
    static boolean failed = false;
    
    
    public SourceConnection createConnection(ConnectorIdentity id) {
        return new FakeSourceConnection(connCnt++);
    }

    class FakeSourceConnection implements SourceConnection{
        int id;
        
        FakeSourceConnection(int id){
            FakeSourceConnection.this.id = id;
        }
        
        public boolean isAlive() {
            return alive;
        }
        
        public boolean isFailed() {
            return failed;
        }

        public void closeSource() {                
        } 
        
        public boolean equals(Object other){
            return this.id == ((FakeSourceConnection)other).id;
        }
        
        public String toString(){
            return "Connection " + id; //$NON-NLS-1$
        }
    }

    /* 
     * @see com.metamatrix.data.pool.SourceConnectionFactory#setConnectorProperties(com.metamatrix.data.ConnectorEnvironment)
     */
    public void initialize(ConnectorEnvironment env) {
    }

}
