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
package org.teiid.stateservice.jaxb;

import javax.xml.bind.annotation.XmlRegistry;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the org.teiid.stateservice package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {


    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: org.teiid.stateservice
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link GetAllStateInfo }
     * 
     */
    public GetAllStateInfo createGetAllStateInfo() {
        return new GetAllStateInfo();
    }

    /**
     * Create an instance of {@link StateInfo }
     * 
     */
    public StateInfo createStateInfo() {
        return new StateInfo();
    }

    /**
     * Create an instance of {@link GetAllStateInfoResponse }
     * 
     */
    public GetAllStateInfoResponse createGetAllStateInfoResponse() {
        return new GetAllStateInfoResponse();
    }

    /**
     * Create an instance of {@link GetStateInfo }
     * 
     */
    public GetStateInfo createGetStateInfo() {
        return new GetStateInfo();
    }

    /**
     * Create an instance of {@link GetStateInfoFault }
     * 
     */
    public GetStateInfoFault createGetStateInfoFault() {
        return new GetStateInfoFault();
    }

    /**
     * Create an instance of {@link GetStateInfoResponse }
     * 
     */
    public GetStateInfoResponse createGetStateInfoResponse() {
        return new GetStateInfoResponse();
    }

}
