/**
 * FaultCode.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner.fault;

public class FaultCode implements java.io.Serializable {
    private javax.xml.namespace.QName _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected FaultCode(javax.xml.namespace.QName value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final javax.xml.namespace.QName _value1 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}API_CURRENTLY_DISABLED");
    public static final javax.xml.namespace.QName _value2 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}API_DISABLED_FOR_ORG");
    public static final javax.xml.namespace.QName _value3 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}CANT_ADD_STANDADRD_PORTAL_USER_TO_TERRITORY");
    public static final javax.xml.namespace.QName _value4 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}CANT_ADD_STANDARD_PORTAL_USER_TO_TERRITORY");
    public static final javax.xml.namespace.QName _value5 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}CIRCULAR_OBJECT_GRAPH");
    public static final javax.xml.namespace.QName _value6 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}CLIENT_NOT_ACCESSIBLE_FOR_USER");
    public static final javax.xml.namespace.QName _value7 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}CLIENT_REQUIRE_UPDATE_FOR_USER");
    public static final javax.xml.namespace.QName _value8 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}CUSTOM_METADATA_LIMIT_EXCEEDED");
    public static final javax.xml.namespace.QName _value9 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}DUPLICATE_VALUE");
    public static final javax.xml.namespace.QName _value10 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EMAIL_BATCH_SIZE_LIMIT_EXCEEDED");
    public static final javax.xml.namespace.QName _value11 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EMAIL_TO_CASE_INVALID_ROUTING");
    public static final javax.xml.namespace.QName _value12 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EMAIL_TO_CASE_LIMIT_EXCEEDED");
    public static final javax.xml.namespace.QName _value13 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EMAIL_TO_CASE_NOT_ENABLED");
    public static final javax.xml.namespace.QName _value14 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EXCEEDED_ID_LIMIT");
    public static final javax.xml.namespace.QName _value15 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EXCEEDED_LEAD_CONVERT_LIMIT");
    public static final javax.xml.namespace.QName _value16 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EXCEEDED_MAX_SIZE_REQUEST");
    public static final javax.xml.namespace.QName _value17 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EXCEEDED_MAX_TYPES_LIMIT");
    public static final javax.xml.namespace.QName _value18 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}EXCEEDED_QUOTA");
    public static final javax.xml.namespace.QName _value19 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}FUNCTIONALITY_NOT_ENABLED");
    public static final javax.xml.namespace.QName _value20 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INACTIVE_OWNER_OR_USER");
    public static final javax.xml.namespace.QName _value21 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INACTIVE_PORTAL");
    public static final javax.xml.namespace.QName _value22 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INSUFFICIENT_ACCESS");
    public static final javax.xml.namespace.QName _value23 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_ASSIGNMENT_RULE");
    public static final javax.xml.namespace.QName _value24 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_BATCH_SIZE");
    public static final javax.xml.namespace.QName _value25 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_CLIENT");
    public static final javax.xml.namespace.QName _value26 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_CROSS_REFERENCE_KEY");
    public static final javax.xml.namespace.QName _value27 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_FIELD");
    public static final javax.xml.namespace.QName _value28 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_FILTER_LANGUAGE");
    public static final javax.xml.namespace.QName _value29 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_FILTER_VALUE");
    public static final javax.xml.namespace.QName _value30 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_ID_FIELD");
    public static final javax.xml.namespace.QName _value31 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_LOCALE_LANGUAGE");
    public static final javax.xml.namespace.QName _value32 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_LOCATOR");
    public static final javax.xml.namespace.QName _value33 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_LOGIN");
    public static final javax.xml.namespace.QName _value34 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_NEW_PASSWORD");
    public static final javax.xml.namespace.QName _value35 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_OPERATION");
    public static final javax.xml.namespace.QName _value36 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_OPERATION_WITH_EXPIRED_PASSWORD");
    public static final javax.xml.namespace.QName _value37 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_QUERY_FILTER_OPERATOR");
    public static final javax.xml.namespace.QName _value38 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_QUERY_LOCATOR");
    public static final javax.xml.namespace.QName _value39 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_QUERY_SCOPE");
    public static final javax.xml.namespace.QName _value40 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_REPLICATION_DATE");
    public static final javax.xml.namespace.QName _value41 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_SEARCH");
    public static final javax.xml.namespace.QName _value42 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_SEARCH_SCOPE");
    public static final javax.xml.namespace.QName _value43 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_SESSION_ID");
    public static final javax.xml.namespace.QName _value44 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_SOAP_HEADER");
    public static final javax.xml.namespace.QName _value45 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_SSO_GATEWAY_URL");
    public static final javax.xml.namespace.QName _value46 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_TYPE");
    public static final javax.xml.namespace.QName _value47 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}INVALID_TYPE_FOR_OPERATION");
    public static final javax.xml.namespace.QName _value48 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}LIMIT_EXCEEDED");
    public static final javax.xml.namespace.QName _value49 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}LOGIN_CHALLENGE_ISSUED");
    public static final javax.xml.namespace.QName _value50 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}LOGIN_CHALLENGE_PENDING");
    public static final javax.xml.namespace.QName _value51 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}LOGIN_DURING_RESTRICTED_DOMAIN");
    public static final javax.xml.namespace.QName _value52 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}LOGIN_DURING_RESTRICTED_TIME");
    public static final javax.xml.namespace.QName _value53 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}LOGIN_MUST_USE_SECURITY_TOKEN");
    public static final javax.xml.namespace.QName _value54 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}MALFORMED_ID");
    public static final javax.xml.namespace.QName _value55 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}MALFORMED_QUERY");
    public static final javax.xml.namespace.QName _value56 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}MALFORMED_SEARCH");
    public static final javax.xml.namespace.QName _value57 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}MISSING_ARGUMENT");
    public static final javax.xml.namespace.QName _value58 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}NOT_MODIFIED");
    public static final javax.xml.namespace.QName _value59 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}NO_SOFTPHONE_LAYOUT");
    public static final javax.xml.namespace.QName _value60 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}NUMBER_OUTSIDE_VALID_RANGE");
    public static final javax.xml.namespace.QName _value61 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}OPERATION_TOO_LARGE");
    public static final javax.xml.namespace.QName _value62 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}ORG_LOCKED");
    public static final javax.xml.namespace.QName _value63 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}ORG_NOT_OWNED_BY_INSTANCE");
    public static final javax.xml.namespace.QName _value64 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}PASSWORD_LOCKOUT");
    public static final javax.xml.namespace.QName _value65 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}PORTAL_NO_ACCESS");
    public static final javax.xml.namespace.QName _value66 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}QUERY_TIMEOUT");
    public static final javax.xml.namespace.QName _value67 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}QUERY_TOO_COMPLICATED");
    public static final javax.xml.namespace.QName _value68 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}REQUEST_LIMIT_EXCEEDED");
    public static final javax.xml.namespace.QName _value69 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}REQUEST_RUNNING_TOO_LONG");
    public static final javax.xml.namespace.QName _value70 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}SERVER_UNAVAILABLE");
    public static final javax.xml.namespace.QName _value71 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}SSO_SERVICE_DOWN");
    public static final javax.xml.namespace.QName _value72 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}TOO_MANY_APEX_REQUESTS");
    public static final javax.xml.namespace.QName _value73 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}TRIAL_EXPIRED");
    public static final javax.xml.namespace.QName _value74 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}UNKNOWN_EXCEPTION");
    public static final javax.xml.namespace.QName _value75 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}UNSUPPORTED_API_VERSION");
    public static final javax.xml.namespace.QName _value76 = javax.xml.namespace.QName.valueOf("{urn:fault.partner.soap.sforce.com}UNSUPPORTED_CLIENT");
    public static final FaultCode value1 = new FaultCode(_value1);
    public static final FaultCode value2 = new FaultCode(_value2);
    public static final FaultCode value3 = new FaultCode(_value3);
    public static final FaultCode value4 = new FaultCode(_value4);
    public static final FaultCode value5 = new FaultCode(_value5);
    public static final FaultCode value6 = new FaultCode(_value6);
    public static final FaultCode value7 = new FaultCode(_value7);
    public static final FaultCode value8 = new FaultCode(_value8);
    public static final FaultCode value9 = new FaultCode(_value9);
    public static final FaultCode value10 = new FaultCode(_value10);
    public static final FaultCode value11 = new FaultCode(_value11);
    public static final FaultCode value12 = new FaultCode(_value12);
    public static final FaultCode value13 = new FaultCode(_value13);
    public static final FaultCode value14 = new FaultCode(_value14);
    public static final FaultCode value15 = new FaultCode(_value15);
    public static final FaultCode value16 = new FaultCode(_value16);
    public static final FaultCode value17 = new FaultCode(_value17);
    public static final FaultCode value18 = new FaultCode(_value18);
    public static final FaultCode value19 = new FaultCode(_value19);
    public static final FaultCode value20 = new FaultCode(_value20);
    public static final FaultCode value21 = new FaultCode(_value21);
    public static final FaultCode value22 = new FaultCode(_value22);
    public static final FaultCode value23 = new FaultCode(_value23);
    public static final FaultCode value24 = new FaultCode(_value24);
    public static final FaultCode value25 = new FaultCode(_value25);
    public static final FaultCode value26 = new FaultCode(_value26);
    public static final FaultCode value27 = new FaultCode(_value27);
    public static final FaultCode value28 = new FaultCode(_value28);
    public static final FaultCode value29 = new FaultCode(_value29);
    public static final FaultCode value30 = new FaultCode(_value30);
    public static final FaultCode value31 = new FaultCode(_value31);
    public static final FaultCode value32 = new FaultCode(_value32);
    public static final FaultCode value33 = new FaultCode(_value33);
    public static final FaultCode value34 = new FaultCode(_value34);
    public static final FaultCode value35 = new FaultCode(_value35);
    public static final FaultCode value36 = new FaultCode(_value36);
    public static final FaultCode value37 = new FaultCode(_value37);
    public static final FaultCode value38 = new FaultCode(_value38);
    public static final FaultCode value39 = new FaultCode(_value39);
    public static final FaultCode value40 = new FaultCode(_value40);
    public static final FaultCode value41 = new FaultCode(_value41);
    public static final FaultCode value42 = new FaultCode(_value42);
    public static final FaultCode value43 = new FaultCode(_value43);
    public static final FaultCode value44 = new FaultCode(_value44);
    public static final FaultCode value45 = new FaultCode(_value45);
    public static final FaultCode value46 = new FaultCode(_value46);
    public static final FaultCode value47 = new FaultCode(_value47);
    public static final FaultCode value48 = new FaultCode(_value48);
    public static final FaultCode value49 = new FaultCode(_value49);
    public static final FaultCode value50 = new FaultCode(_value50);
    public static final FaultCode value51 = new FaultCode(_value51);
    public static final FaultCode value52 = new FaultCode(_value52);
    public static final FaultCode value53 = new FaultCode(_value53);
    public static final FaultCode value54 = new FaultCode(_value54);
    public static final FaultCode value55 = new FaultCode(_value55);
    public static final FaultCode value56 = new FaultCode(_value56);
    public static final FaultCode value57 = new FaultCode(_value57);
    public static final FaultCode value58 = new FaultCode(_value58);
    public static final FaultCode value59 = new FaultCode(_value59);
    public static final FaultCode value60 = new FaultCode(_value60);
    public static final FaultCode value61 = new FaultCode(_value61);
    public static final FaultCode value62 = new FaultCode(_value62);
    public static final FaultCode value63 = new FaultCode(_value63);
    public static final FaultCode value64 = new FaultCode(_value64);
    public static final FaultCode value65 = new FaultCode(_value65);
    public static final FaultCode value66 = new FaultCode(_value66);
    public static final FaultCode value67 = new FaultCode(_value67);
    public static final FaultCode value68 = new FaultCode(_value68);
    public static final FaultCode value69 = new FaultCode(_value69);
    public static final FaultCode value70 = new FaultCode(_value70);
    public static final FaultCode value71 = new FaultCode(_value71);
    public static final FaultCode value72 = new FaultCode(_value72);
    public static final FaultCode value73 = new FaultCode(_value73);
    public static final FaultCode value74 = new FaultCode(_value74);
    public static final FaultCode value75 = new FaultCode(_value75);
    public static final FaultCode value76 = new FaultCode(_value76);
    public javax.xml.namespace.QName getValue() { return _value_;}
    public static FaultCode fromValue(javax.xml.namespace.QName value)
          throws java.lang.IllegalArgumentException {
        FaultCode enumeration = (FaultCode)
            _table_.get(value);
        if (enumeration==null) throw new java.lang.IllegalArgumentException();
        return enumeration;
    }
    public static FaultCode fromString(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        try {
            return fromValue(javax.xml.namespace.QName.valueOf(value));
        } catch (Exception e) {
            throw new java.lang.IllegalArgumentException();
        }
    }
    public boolean equals(java.lang.Object obj) {return (obj == this);}
    public int hashCode() { return toString().hashCode();}
    public java.lang.String toString() { return _value_.toString();}
    public java.lang.Object readResolve() throws java.io.ObjectStreamException { return fromValue(_value_);}
    public static org.apache.axis.encoding.Serializer getSerializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new org.apache.axis.encoding.ser.EnumSerializer(
            _javaType, _xmlType);
    }
    public static org.apache.axis.encoding.Deserializer getDeserializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new org.apache.axis.encoding.ser.EnumDeserializer(
            _javaType, _xmlType);
    }
    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(FaultCode.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:fault.partner.soap.sforce.com", "FaultCode"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}
