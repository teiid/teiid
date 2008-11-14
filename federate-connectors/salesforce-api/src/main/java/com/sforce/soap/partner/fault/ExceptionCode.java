/**
 * ExceptionCode.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner.fault;

public class ExceptionCode implements java.io.Serializable {
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected ExceptionCode(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _API_CURRENTLY_DISABLED = "API_CURRENTLY_DISABLED";
    public static final java.lang.String _API_DISABLED_FOR_ORG = "API_DISABLED_FOR_ORG";
    public static final java.lang.String _CANT_ADD_STANDADRD_PORTAL_USER_TO_TERRITORY = "CANT_ADD_STANDADRD_PORTAL_USER_TO_TERRITORY";
    public static final java.lang.String _CANT_ADD_STANDARD_PORTAL_USER_TO_TERRITORY = "CANT_ADD_STANDARD_PORTAL_USER_TO_TERRITORY";
    public static final java.lang.String _CIRCULAR_OBJECT_GRAPH = "CIRCULAR_OBJECT_GRAPH";
    public static final java.lang.String _CLIENT_NOT_ACCESSIBLE_FOR_USER = "CLIENT_NOT_ACCESSIBLE_FOR_USER";
    public static final java.lang.String _CLIENT_REQUIRE_UPDATE_FOR_USER = "CLIENT_REQUIRE_UPDATE_FOR_USER";
    public static final java.lang.String _CUSTOM_METADATA_LIMIT_EXCEEDED = "CUSTOM_METADATA_LIMIT_EXCEEDED";
    public static final java.lang.String _DUPLICATE_VALUE = "DUPLICATE_VALUE";
    public static final java.lang.String _EMAIL_BATCH_SIZE_LIMIT_EXCEEDED = "EMAIL_BATCH_SIZE_LIMIT_EXCEEDED";
    public static final java.lang.String _EMAIL_TO_CASE_INVALID_ROUTING = "EMAIL_TO_CASE_INVALID_ROUTING";
    public static final java.lang.String _EMAIL_TO_CASE_LIMIT_EXCEEDED = "EMAIL_TO_CASE_LIMIT_EXCEEDED";
    public static final java.lang.String _EMAIL_TO_CASE_NOT_ENABLED = "EMAIL_TO_CASE_NOT_ENABLED";
    public static final java.lang.String _EXCEEDED_ID_LIMIT = "EXCEEDED_ID_LIMIT";
    public static final java.lang.String _EXCEEDED_LEAD_CONVERT_LIMIT = "EXCEEDED_LEAD_CONVERT_LIMIT";
    public static final java.lang.String _EXCEEDED_MAX_SIZE_REQUEST = "EXCEEDED_MAX_SIZE_REQUEST";
    public static final java.lang.String _EXCEEDED_MAX_TYPES_LIMIT = "EXCEEDED_MAX_TYPES_LIMIT";
    public static final java.lang.String _EXCEEDED_QUOTA = "EXCEEDED_QUOTA";
    public static final java.lang.String _FUNCTIONALITY_NOT_ENABLED = "FUNCTIONALITY_NOT_ENABLED";
    public static final java.lang.String _INACTIVE_OWNER_OR_USER = "INACTIVE_OWNER_OR_USER";
    public static final java.lang.String _INACTIVE_PORTAL = "INACTIVE_PORTAL";
    public static final java.lang.String _INSUFFICIENT_ACCESS = "INSUFFICIENT_ACCESS";
    public static final java.lang.String _INVALID_ASSIGNMENT_RULE = "INVALID_ASSIGNMENT_RULE";
    public static final java.lang.String _INVALID_BATCH_SIZE = "INVALID_BATCH_SIZE";
    public static final java.lang.String _INVALID_CLIENT = "INVALID_CLIENT";
    public static final java.lang.String _INVALID_CROSS_REFERENCE_KEY = "INVALID_CROSS_REFERENCE_KEY";
    public static final java.lang.String _INVALID_FIELD = "INVALID_FIELD";
    public static final java.lang.String _INVALID_FILTER_LANGUAGE = "INVALID_FILTER_LANGUAGE";
    public static final java.lang.String _INVALID_FILTER_VALUE = "INVALID_FILTER_VALUE";
    public static final java.lang.String _INVALID_ID_FIELD = "INVALID_ID_FIELD";
    public static final java.lang.String _INVALID_LOCALE_LANGUAGE = "INVALID_LOCALE_LANGUAGE";
    public static final java.lang.String _INVALID_LOCATOR = "INVALID_LOCATOR";
    public static final java.lang.String _INVALID_LOGIN = "INVALID_LOGIN";
    public static final java.lang.String _INVALID_NEW_PASSWORD = "INVALID_NEW_PASSWORD";
    public static final java.lang.String _INVALID_OPERATION = "INVALID_OPERATION";
    public static final java.lang.String _INVALID_OPERATION_WITH_EXPIRED_PASSWORD = "INVALID_OPERATION_WITH_EXPIRED_PASSWORD";
    public static final java.lang.String _INVALID_QUERY_FILTER_OPERATOR = "INVALID_QUERY_FILTER_OPERATOR";
    public static final java.lang.String _INVALID_QUERY_LOCATOR = "INVALID_QUERY_LOCATOR";
    public static final java.lang.String _INVALID_QUERY_SCOPE = "INVALID_QUERY_SCOPE";
    public static final java.lang.String _INVALID_REPLICATION_DATE = "INVALID_REPLICATION_DATE";
    public static final java.lang.String _INVALID_SEARCH = "INVALID_SEARCH";
    public static final java.lang.String _INVALID_SEARCH_SCOPE = "INVALID_SEARCH_SCOPE";
    public static final java.lang.String _INVALID_SESSION_ID = "INVALID_SESSION_ID";
    public static final java.lang.String _INVALID_SOAP_HEADER = "INVALID_SOAP_HEADER";
    public static final java.lang.String _INVALID_SSO_GATEWAY_URL = "INVALID_SSO_GATEWAY_URL";
    public static final java.lang.String _INVALID_TYPE = "INVALID_TYPE";
    public static final java.lang.String _INVALID_TYPE_FOR_OPERATION = "INVALID_TYPE_FOR_OPERATION";
    public static final java.lang.String _LIMIT_EXCEEDED = "LIMIT_EXCEEDED";
    public static final java.lang.String _LOGIN_CHALLENGE_ISSUED = "LOGIN_CHALLENGE_ISSUED";
    public static final java.lang.String _LOGIN_CHALLENGE_PENDING = "LOGIN_CHALLENGE_PENDING";
    public static final java.lang.String _LOGIN_DURING_RESTRICTED_DOMAIN = "LOGIN_DURING_RESTRICTED_DOMAIN";
    public static final java.lang.String _LOGIN_DURING_RESTRICTED_TIME = "LOGIN_DURING_RESTRICTED_TIME";
    public static final java.lang.String _LOGIN_MUST_USE_SECURITY_TOKEN = "LOGIN_MUST_USE_SECURITY_TOKEN";
    public static final java.lang.String _MALFORMED_ID = "MALFORMED_ID";
    public static final java.lang.String _MALFORMED_QUERY = "MALFORMED_QUERY";
    public static final java.lang.String _MALFORMED_SEARCH = "MALFORMED_SEARCH";
    public static final java.lang.String _MISSING_ARGUMENT = "MISSING_ARGUMENT";
    public static final java.lang.String _NOT_MODIFIED = "NOT_MODIFIED";
    public static final java.lang.String _NO_SOFTPHONE_LAYOUT = "NO_SOFTPHONE_LAYOUT";
    public static final java.lang.String _NUMBER_OUTSIDE_VALID_RANGE = "NUMBER_OUTSIDE_VALID_RANGE";
    public static final java.lang.String _OPERATION_TOO_LARGE = "OPERATION_TOO_LARGE";
    public static final java.lang.String _ORG_LOCKED = "ORG_LOCKED";
    public static final java.lang.String _ORG_NOT_OWNED_BY_INSTANCE = "ORG_NOT_OWNED_BY_INSTANCE";
    public static final java.lang.String _PASSWORD_LOCKOUT = "PASSWORD_LOCKOUT";
    public static final java.lang.String _PORTAL_NO_ACCESS = "PORTAL_NO_ACCESS";
    public static final java.lang.String _QUERY_TIMEOUT = "QUERY_TIMEOUT";
    public static final java.lang.String _QUERY_TOO_COMPLICATED = "QUERY_TOO_COMPLICATED";
    public static final java.lang.String _REQUEST_LIMIT_EXCEEDED = "REQUEST_LIMIT_EXCEEDED";
    public static final java.lang.String _REQUEST_RUNNING_TOO_LONG = "REQUEST_RUNNING_TOO_LONG";
    public static final java.lang.String _SERVER_UNAVAILABLE = "SERVER_UNAVAILABLE";
    public static final java.lang.String _SSO_SERVICE_DOWN = "SSO_SERVICE_DOWN";
    public static final java.lang.String _TOO_MANY_APEX_REQUESTS = "TOO_MANY_APEX_REQUESTS";
    public static final java.lang.String _TRIAL_EXPIRED = "TRIAL_EXPIRED";
    public static final java.lang.String _UNKNOWN_EXCEPTION = "UNKNOWN_EXCEPTION";
    public static final java.lang.String _UNSUPPORTED_API_VERSION = "UNSUPPORTED_API_VERSION";
    public static final java.lang.String _UNSUPPORTED_CLIENT = "UNSUPPORTED_CLIENT";
    public static final ExceptionCode API_CURRENTLY_DISABLED = new ExceptionCode(_API_CURRENTLY_DISABLED);
    public static final ExceptionCode API_DISABLED_FOR_ORG = new ExceptionCode(_API_DISABLED_FOR_ORG);
    public static final ExceptionCode CANT_ADD_STANDADRD_PORTAL_USER_TO_TERRITORY = new ExceptionCode(_CANT_ADD_STANDADRD_PORTAL_USER_TO_TERRITORY);
    public static final ExceptionCode CANT_ADD_STANDARD_PORTAL_USER_TO_TERRITORY = new ExceptionCode(_CANT_ADD_STANDARD_PORTAL_USER_TO_TERRITORY);
    public static final ExceptionCode CIRCULAR_OBJECT_GRAPH = new ExceptionCode(_CIRCULAR_OBJECT_GRAPH);
    public static final ExceptionCode CLIENT_NOT_ACCESSIBLE_FOR_USER = new ExceptionCode(_CLIENT_NOT_ACCESSIBLE_FOR_USER);
    public static final ExceptionCode CLIENT_REQUIRE_UPDATE_FOR_USER = new ExceptionCode(_CLIENT_REQUIRE_UPDATE_FOR_USER);
    public static final ExceptionCode CUSTOM_METADATA_LIMIT_EXCEEDED = new ExceptionCode(_CUSTOM_METADATA_LIMIT_EXCEEDED);
    public static final ExceptionCode DUPLICATE_VALUE = new ExceptionCode(_DUPLICATE_VALUE);
    public static final ExceptionCode EMAIL_BATCH_SIZE_LIMIT_EXCEEDED = new ExceptionCode(_EMAIL_BATCH_SIZE_LIMIT_EXCEEDED);
    public static final ExceptionCode EMAIL_TO_CASE_INVALID_ROUTING = new ExceptionCode(_EMAIL_TO_CASE_INVALID_ROUTING);
    public static final ExceptionCode EMAIL_TO_CASE_LIMIT_EXCEEDED = new ExceptionCode(_EMAIL_TO_CASE_LIMIT_EXCEEDED);
    public static final ExceptionCode EMAIL_TO_CASE_NOT_ENABLED = new ExceptionCode(_EMAIL_TO_CASE_NOT_ENABLED);
    public static final ExceptionCode EXCEEDED_ID_LIMIT = new ExceptionCode(_EXCEEDED_ID_LIMIT);
    public static final ExceptionCode EXCEEDED_LEAD_CONVERT_LIMIT = new ExceptionCode(_EXCEEDED_LEAD_CONVERT_LIMIT);
    public static final ExceptionCode EXCEEDED_MAX_SIZE_REQUEST = new ExceptionCode(_EXCEEDED_MAX_SIZE_REQUEST);
    public static final ExceptionCode EXCEEDED_MAX_TYPES_LIMIT = new ExceptionCode(_EXCEEDED_MAX_TYPES_LIMIT);
    public static final ExceptionCode EXCEEDED_QUOTA = new ExceptionCode(_EXCEEDED_QUOTA);
    public static final ExceptionCode FUNCTIONALITY_NOT_ENABLED = new ExceptionCode(_FUNCTIONALITY_NOT_ENABLED);
    public static final ExceptionCode INACTIVE_OWNER_OR_USER = new ExceptionCode(_INACTIVE_OWNER_OR_USER);
    public static final ExceptionCode INACTIVE_PORTAL = new ExceptionCode(_INACTIVE_PORTAL);
    public static final ExceptionCode INSUFFICIENT_ACCESS = new ExceptionCode(_INSUFFICIENT_ACCESS);
    public static final ExceptionCode INVALID_ASSIGNMENT_RULE = new ExceptionCode(_INVALID_ASSIGNMENT_RULE);
    public static final ExceptionCode INVALID_BATCH_SIZE = new ExceptionCode(_INVALID_BATCH_SIZE);
    public static final ExceptionCode INVALID_CLIENT = new ExceptionCode(_INVALID_CLIENT);
    public static final ExceptionCode INVALID_CROSS_REFERENCE_KEY = new ExceptionCode(_INVALID_CROSS_REFERENCE_KEY);
    public static final ExceptionCode INVALID_FIELD = new ExceptionCode(_INVALID_FIELD);
    public static final ExceptionCode INVALID_FILTER_LANGUAGE = new ExceptionCode(_INVALID_FILTER_LANGUAGE);
    public static final ExceptionCode INVALID_FILTER_VALUE = new ExceptionCode(_INVALID_FILTER_VALUE);
    public static final ExceptionCode INVALID_ID_FIELD = new ExceptionCode(_INVALID_ID_FIELD);
    public static final ExceptionCode INVALID_LOCALE_LANGUAGE = new ExceptionCode(_INVALID_LOCALE_LANGUAGE);
    public static final ExceptionCode INVALID_LOCATOR = new ExceptionCode(_INVALID_LOCATOR);
    public static final ExceptionCode INVALID_LOGIN = new ExceptionCode(_INVALID_LOGIN);
    public static final ExceptionCode INVALID_NEW_PASSWORD = new ExceptionCode(_INVALID_NEW_PASSWORD);
    public static final ExceptionCode INVALID_OPERATION = new ExceptionCode(_INVALID_OPERATION);
    public static final ExceptionCode INVALID_OPERATION_WITH_EXPIRED_PASSWORD = new ExceptionCode(_INVALID_OPERATION_WITH_EXPIRED_PASSWORD);
    public static final ExceptionCode INVALID_QUERY_FILTER_OPERATOR = new ExceptionCode(_INVALID_QUERY_FILTER_OPERATOR);
    public static final ExceptionCode INVALID_QUERY_LOCATOR = new ExceptionCode(_INVALID_QUERY_LOCATOR);
    public static final ExceptionCode INVALID_QUERY_SCOPE = new ExceptionCode(_INVALID_QUERY_SCOPE);
    public static final ExceptionCode INVALID_REPLICATION_DATE = new ExceptionCode(_INVALID_REPLICATION_DATE);
    public static final ExceptionCode INVALID_SEARCH = new ExceptionCode(_INVALID_SEARCH);
    public static final ExceptionCode INVALID_SEARCH_SCOPE = new ExceptionCode(_INVALID_SEARCH_SCOPE);
    public static final ExceptionCode INVALID_SESSION_ID = new ExceptionCode(_INVALID_SESSION_ID);
    public static final ExceptionCode INVALID_SOAP_HEADER = new ExceptionCode(_INVALID_SOAP_HEADER);
    public static final ExceptionCode INVALID_SSO_GATEWAY_URL = new ExceptionCode(_INVALID_SSO_GATEWAY_URL);
    public static final ExceptionCode INVALID_TYPE = new ExceptionCode(_INVALID_TYPE);
    public static final ExceptionCode INVALID_TYPE_FOR_OPERATION = new ExceptionCode(_INVALID_TYPE_FOR_OPERATION);
    public static final ExceptionCode LIMIT_EXCEEDED = new ExceptionCode(_LIMIT_EXCEEDED);
    public static final ExceptionCode LOGIN_CHALLENGE_ISSUED = new ExceptionCode(_LOGIN_CHALLENGE_ISSUED);
    public static final ExceptionCode LOGIN_CHALLENGE_PENDING = new ExceptionCode(_LOGIN_CHALLENGE_PENDING);
    public static final ExceptionCode LOGIN_DURING_RESTRICTED_DOMAIN = new ExceptionCode(_LOGIN_DURING_RESTRICTED_DOMAIN);
    public static final ExceptionCode LOGIN_DURING_RESTRICTED_TIME = new ExceptionCode(_LOGIN_DURING_RESTRICTED_TIME);
    public static final ExceptionCode LOGIN_MUST_USE_SECURITY_TOKEN = new ExceptionCode(_LOGIN_MUST_USE_SECURITY_TOKEN);
    public static final ExceptionCode MALFORMED_ID = new ExceptionCode(_MALFORMED_ID);
    public static final ExceptionCode MALFORMED_QUERY = new ExceptionCode(_MALFORMED_QUERY);
    public static final ExceptionCode MALFORMED_SEARCH = new ExceptionCode(_MALFORMED_SEARCH);
    public static final ExceptionCode MISSING_ARGUMENT = new ExceptionCode(_MISSING_ARGUMENT);
    public static final ExceptionCode NOT_MODIFIED = new ExceptionCode(_NOT_MODIFIED);
    public static final ExceptionCode NO_SOFTPHONE_LAYOUT = new ExceptionCode(_NO_SOFTPHONE_LAYOUT);
    public static final ExceptionCode NUMBER_OUTSIDE_VALID_RANGE = new ExceptionCode(_NUMBER_OUTSIDE_VALID_RANGE);
    public static final ExceptionCode OPERATION_TOO_LARGE = new ExceptionCode(_OPERATION_TOO_LARGE);
    public static final ExceptionCode ORG_LOCKED = new ExceptionCode(_ORG_LOCKED);
    public static final ExceptionCode ORG_NOT_OWNED_BY_INSTANCE = new ExceptionCode(_ORG_NOT_OWNED_BY_INSTANCE);
    public static final ExceptionCode PASSWORD_LOCKOUT = new ExceptionCode(_PASSWORD_LOCKOUT);
    public static final ExceptionCode PORTAL_NO_ACCESS = new ExceptionCode(_PORTAL_NO_ACCESS);
    public static final ExceptionCode QUERY_TIMEOUT = new ExceptionCode(_QUERY_TIMEOUT);
    public static final ExceptionCode QUERY_TOO_COMPLICATED = new ExceptionCode(_QUERY_TOO_COMPLICATED);
    public static final ExceptionCode REQUEST_LIMIT_EXCEEDED = new ExceptionCode(_REQUEST_LIMIT_EXCEEDED);
    public static final ExceptionCode REQUEST_RUNNING_TOO_LONG = new ExceptionCode(_REQUEST_RUNNING_TOO_LONG);
    public static final ExceptionCode SERVER_UNAVAILABLE = new ExceptionCode(_SERVER_UNAVAILABLE);
    public static final ExceptionCode SSO_SERVICE_DOWN = new ExceptionCode(_SSO_SERVICE_DOWN);
    public static final ExceptionCode TOO_MANY_APEX_REQUESTS = new ExceptionCode(_TOO_MANY_APEX_REQUESTS);
    public static final ExceptionCode TRIAL_EXPIRED = new ExceptionCode(_TRIAL_EXPIRED);
    public static final ExceptionCode UNKNOWN_EXCEPTION = new ExceptionCode(_UNKNOWN_EXCEPTION);
    public static final ExceptionCode UNSUPPORTED_API_VERSION = new ExceptionCode(_UNSUPPORTED_API_VERSION);
    public static final ExceptionCode UNSUPPORTED_CLIENT = new ExceptionCode(_UNSUPPORTED_CLIENT);
    public java.lang.String getValue() { return _value_;}
    public static ExceptionCode fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        ExceptionCode enumeration = (ExceptionCode)
            _table_.get(value);
        if (enumeration==null) throw new java.lang.IllegalArgumentException();
        return enumeration;
    }
    public static ExceptionCode fromString(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        return fromValue(value);
    }
    public boolean equals(java.lang.Object obj) {return (obj == this);}
    public int hashCode() { return toString().hashCode();}
    public java.lang.String toString() { return _value_;}
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
        new org.apache.axis.description.TypeDesc(ExceptionCode.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:fault.partner.soap.sforce.com", "ExceptionCode"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}
