/**
 * SforceService.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public interface SforceService extends javax.xml.rpc.Service {

/**
 * Sforce SOAP API
 */
    public java.lang.String getSoapAddress();

    public com.sforce.soap.partner.Soap getSoap() throws javax.xml.rpc.ServiceException;

    public com.sforce.soap.partner.Soap getSoap(java.net.URL portAddress) throws javax.xml.rpc.ServiceException;
}
