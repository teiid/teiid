
package com.sforce.soap.partner.fault;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for ExceptionCode.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="ExceptionCode">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="API_CURRENTLY_DISABLED"/>
 *     &lt;enumeration value="API_DISABLED_FOR_ORG"/>
 *     &lt;enumeration value="CANT_ADD_STANDADRD_PORTAL_USER_TO_TERRITORY"/>
 *     &lt;enumeration value="CANT_ADD_STANDARD_PORTAL_USER_TO_TERRITORY"/>
 *     &lt;enumeration value="CIRCULAR_OBJECT_GRAPH"/>
 *     &lt;enumeration value="CLIENT_NOT_ACCESSIBLE_FOR_USER"/>
 *     &lt;enumeration value="CLIENT_REQUIRE_UPDATE_FOR_USER"/>
 *     &lt;enumeration value="CUSTOM_METADATA_LIMIT_EXCEEDED"/>
 *     &lt;enumeration value="DATACLOUD_API_CLIENT_EXCEPTION"/>
 *     &lt;enumeration value="DATACLOUD_API_SERVER_EXCEPTION"/>
 *     &lt;enumeration value="DATACLOUD_API_UNAVAILABLE"/>
 *     &lt;enumeration value="DUPLICATE_VALUE"/>
 *     &lt;enumeration value="EMAIL_BATCH_SIZE_LIMIT_EXCEEDED"/>
 *     &lt;enumeration value="EMAIL_TO_CASE_INVALID_ROUTING"/>
 *     &lt;enumeration value="EMAIL_TO_CASE_LIMIT_EXCEEDED"/>
 *     &lt;enumeration value="EMAIL_TO_CASE_NOT_ENABLED"/>
 *     &lt;enumeration value="EXCEEDED_ID_LIMIT"/>
 *     &lt;enumeration value="EXCEEDED_LEAD_CONVERT_LIMIT"/>
 *     &lt;enumeration value="EXCEEDED_MAX_SIZE_REQUEST"/>
 *     &lt;enumeration value="EXCEEDED_MAX_TYPES_LIMIT"/>
 *     &lt;enumeration value="EXCEEDED_QUOTA"/>
 *     &lt;enumeration value="FUNCTIONALITY_NOT_ENABLED"/>
 *     &lt;enumeration value="FUNCTIONALITY_TEMPORARILY_UNAVAILABLE"/>
 *     &lt;enumeration value="INACTIVE_OWNER_OR_USER"/>
 *     &lt;enumeration value="INACTIVE_PORTAL"/>
 *     &lt;enumeration value="INSUFFICIENT_ACCESS"/>
 *     &lt;enumeration value="INVALID_ASSIGNMENT_RULE"/>
 *     &lt;enumeration value="INVALID_BATCH_SIZE"/>
 *     &lt;enumeration value="INVALID_CLIENT"/>
 *     &lt;enumeration value="INVALID_CROSS_REFERENCE_KEY"/>
 *     &lt;enumeration value="INVALID_FIELD"/>
 *     &lt;enumeration value="INVALID_FILTER_LANGUAGE"/>
 *     &lt;enumeration value="INVALID_FILTER_VALUE"/>
 *     &lt;enumeration value="INVALID_ID_FIELD"/>
 *     &lt;enumeration value="INVALID_LOCALE_LANGUAGE"/>
 *     &lt;enumeration value="INVALID_LOCATOR"/>
 *     &lt;enumeration value="INVALID_LOGIN"/>
 *     &lt;enumeration value="INVALID_NEW_PASSWORD"/>
 *     &lt;enumeration value="INVALID_OPERATION"/>
 *     &lt;enumeration value="INVALID_OPERATION_WITH_EXPIRED_PASSWORD"/>
 *     &lt;enumeration value="INVALID_QUERY_FILTER_OPERATOR"/>
 *     &lt;enumeration value="INVALID_QUERY_LOCATOR"/>
 *     &lt;enumeration value="INVALID_QUERY_SCOPE"/>
 *     &lt;enumeration value="INVALID_REPLICATION_DATE"/>
 *     &lt;enumeration value="INVALID_SEARCH"/>
 *     &lt;enumeration value="INVALID_SEARCH_SCOPE"/>
 *     &lt;enumeration value="INVALID_SESSION_ID"/>
 *     &lt;enumeration value="INVALID_SOAP_HEADER"/>
 *     &lt;enumeration value="INVALID_SSO_GATEWAY_URL"/>
 *     &lt;enumeration value="INVALID_TYPE"/>
 *     &lt;enumeration value="INVALID_TYPE_FOR_OPERATION"/>
 *     &lt;enumeration value="JIGSAW_IMPORT_LIMIT_EXCEEDED"/>
 *     &lt;enumeration value="JIGSAW_REQUEST_NOT_SUPPORTED"/>
 *     &lt;enumeration value="JSON_PARSER_ERROR"/>
 *     &lt;enumeration value="LIMIT_EXCEEDED"/>
 *     &lt;enumeration value="LOGIN_CHALLENGE_ISSUED"/>
 *     &lt;enumeration value="LOGIN_CHALLENGE_PENDING"/>
 *     &lt;enumeration value="LOGIN_DURING_RESTRICTED_DOMAIN"/>
 *     &lt;enumeration value="LOGIN_DURING_RESTRICTED_TIME"/>
 *     &lt;enumeration value="LOGIN_MUST_USE_SECURITY_TOKEN"/>
 *     &lt;enumeration value="MALFORMED_ID"/>
 *     &lt;enumeration value="MALFORMED_QUERY"/>
 *     &lt;enumeration value="MALFORMED_SEARCH"/>
 *     &lt;enumeration value="MISSING_ARGUMENT"/>
 *     &lt;enumeration value="NOT_MODIFIED"/>
 *     &lt;enumeration value="NO_SOFTPHONE_LAYOUT"/>
 *     &lt;enumeration value="NUMBER_OUTSIDE_VALID_RANGE"/>
 *     &lt;enumeration value="OPERATION_TOO_LARGE"/>
 *     &lt;enumeration value="ORG_IN_MAINTENANCE"/>
 *     &lt;enumeration value="ORG_IS_DOT_ORG"/>
 *     &lt;enumeration value="ORG_LOCKED"/>
 *     &lt;enumeration value="ORG_NOT_OWNED_BY_INSTANCE"/>
 *     &lt;enumeration value="PASSWORD_LOCKOUT"/>
 *     &lt;enumeration value="PORTAL_NO_ACCESS"/>
 *     &lt;enumeration value="QUERY_TIMEOUT"/>
 *     &lt;enumeration value="QUERY_TOO_COMPLICATED"/>
 *     &lt;enumeration value="REQUEST_LIMIT_EXCEEDED"/>
 *     &lt;enumeration value="REQUEST_RUNNING_TOO_LONG"/>
 *     &lt;enumeration value="SERVER_UNAVAILABLE"/>
 *     &lt;enumeration value="SOCIALCRM_FEEDSERVICE_API_CLIENT_EXCEPTION"/>
 *     &lt;enumeration value="SOCIALCRM_FEEDSERVICE_API_SERVER_EXCEPTION"/>
 *     &lt;enumeration value="SOCIALCRM_FEEDSERVICE_API_UNAVAILABLE"/>
 *     &lt;enumeration value="SSO_SERVICE_DOWN"/>
 *     &lt;enumeration value="TOO_MANY_APEX_REQUESTS"/>
 *     &lt;enumeration value="TRIAL_EXPIRED"/>
 *     &lt;enumeration value="UNKNOWN_EXCEPTION"/>
 *     &lt;enumeration value="UNSUPPORTED_API_VERSION"/>
 *     &lt;enumeration value="UNSUPPORTED_CLIENT"/>
 *     &lt;enumeration value="UNSUPPORTED_MEDIA_TYPE"/>
 *     &lt;enumeration value="XML_PARSER_ERROR"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "ExceptionCode")
@XmlEnum
public enum ExceptionCode {

    API_CURRENTLY_DISABLED,
    API_DISABLED_FOR_ORG,
    CANT_ADD_STANDADRD_PORTAL_USER_TO_TERRITORY,
    CANT_ADD_STANDARD_PORTAL_USER_TO_TERRITORY,
    CIRCULAR_OBJECT_GRAPH,
    CLIENT_NOT_ACCESSIBLE_FOR_USER,
    CLIENT_REQUIRE_UPDATE_FOR_USER,
    CUSTOM_METADATA_LIMIT_EXCEEDED,
    DATACLOUD_API_CLIENT_EXCEPTION,
    DATACLOUD_API_SERVER_EXCEPTION,
    DATACLOUD_API_UNAVAILABLE,
    DUPLICATE_VALUE,
    EMAIL_BATCH_SIZE_LIMIT_EXCEEDED,
    EMAIL_TO_CASE_INVALID_ROUTING,
    EMAIL_TO_CASE_LIMIT_EXCEEDED,
    EMAIL_TO_CASE_NOT_ENABLED,
    EXCEEDED_ID_LIMIT,
    EXCEEDED_LEAD_CONVERT_LIMIT,
    EXCEEDED_MAX_SIZE_REQUEST,
    EXCEEDED_MAX_TYPES_LIMIT,
    EXCEEDED_QUOTA,
    FUNCTIONALITY_NOT_ENABLED,
    FUNCTIONALITY_TEMPORARILY_UNAVAILABLE,
    INACTIVE_OWNER_OR_USER,
    INACTIVE_PORTAL,
    INSUFFICIENT_ACCESS,
    INVALID_ASSIGNMENT_RULE,
    INVALID_BATCH_SIZE,
    INVALID_CLIENT,
    INVALID_CROSS_REFERENCE_KEY,
    INVALID_FIELD,
    INVALID_FILTER_LANGUAGE,
    INVALID_FILTER_VALUE,
    INVALID_ID_FIELD,
    INVALID_LOCALE_LANGUAGE,
    INVALID_LOCATOR,
    INVALID_LOGIN,
    INVALID_NEW_PASSWORD,
    INVALID_OPERATION,
    INVALID_OPERATION_WITH_EXPIRED_PASSWORD,
    INVALID_QUERY_FILTER_OPERATOR,
    INVALID_QUERY_LOCATOR,
    INVALID_QUERY_SCOPE,
    INVALID_REPLICATION_DATE,
    INVALID_SEARCH,
    INVALID_SEARCH_SCOPE,
    INVALID_SESSION_ID,
    INVALID_SOAP_HEADER,
    INVALID_SSO_GATEWAY_URL,
    INVALID_TYPE,
    INVALID_TYPE_FOR_OPERATION,
    JIGSAW_IMPORT_LIMIT_EXCEEDED,
    JIGSAW_REQUEST_NOT_SUPPORTED,
    JSON_PARSER_ERROR,
    LIMIT_EXCEEDED,
    LOGIN_CHALLENGE_ISSUED,
    LOGIN_CHALLENGE_PENDING,
    LOGIN_DURING_RESTRICTED_DOMAIN,
    LOGIN_DURING_RESTRICTED_TIME,
    LOGIN_MUST_USE_SECURITY_TOKEN,
    MALFORMED_ID,
    MALFORMED_QUERY,
    MALFORMED_SEARCH,
    MISSING_ARGUMENT,
    NOT_MODIFIED,
    NO_SOFTPHONE_LAYOUT,
    NUMBER_OUTSIDE_VALID_RANGE,
    OPERATION_TOO_LARGE,
    ORG_IN_MAINTENANCE,
    ORG_IS_DOT_ORG,
    ORG_LOCKED,
    ORG_NOT_OWNED_BY_INSTANCE,
    PASSWORD_LOCKOUT,
    PORTAL_NO_ACCESS,
    QUERY_TIMEOUT,
    QUERY_TOO_COMPLICATED,
    REQUEST_LIMIT_EXCEEDED,
    REQUEST_RUNNING_TOO_LONG,
    SERVER_UNAVAILABLE,
    SOCIALCRM_FEEDSERVICE_API_CLIENT_EXCEPTION,
    SOCIALCRM_FEEDSERVICE_API_SERVER_EXCEPTION,
    SOCIALCRM_FEEDSERVICE_API_UNAVAILABLE,
    SSO_SERVICE_DOWN,
    TOO_MANY_APEX_REQUESTS,
    TRIAL_EXPIRED,
    UNKNOWN_EXCEPTION,
    UNSUPPORTED_API_VERSION,
    UNSUPPORTED_CLIENT,
    UNSUPPORTED_MEDIA_TYPE,
    XML_PARSER_ERROR;

    public String value() {
        return name();
    }

    public static ExceptionCode fromValue(String v) {
        return valueOf(v);
    }

}
