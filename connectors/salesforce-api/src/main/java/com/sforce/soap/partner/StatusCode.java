/**
 * StatusCode.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class StatusCode implements java.io.Serializable {
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected StatusCode(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _ALREADY_IN_PROCESS = "ALREADY_IN_PROCESS";
    public static final java.lang.String _ASSIGNEE_TYPE_REQUIRED = "ASSIGNEE_TYPE_REQUIRED";
    public static final java.lang.String _BAD_CUSTOM_ENTITY_PARENT_DOMAIN = "BAD_CUSTOM_ENTITY_PARENT_DOMAIN";
    public static final java.lang.String _BCC_NOT_ALLOWED_IF_BCC_COMPLIANCE_ENABLED = "BCC_NOT_ALLOWED_IF_BCC_COMPLIANCE_ENABLED";
    public static final java.lang.String _CANNOT_CASCADE_PRODUCT_ACTIVE = "CANNOT_CASCADE_PRODUCT_ACTIVE";
    public static final java.lang.String _CANNOT_CHANGE_FIELD_TYPE_OF_APEX_REFERENCED_FIELD = "CANNOT_CHANGE_FIELD_TYPE_OF_APEX_REFERENCED_FIELD";
    public static final java.lang.String _CANNOT_CREATE_ANOTHER_MANAGED_PACKAGE = "CANNOT_CREATE_ANOTHER_MANAGED_PACKAGE";
    public static final java.lang.String _CANNOT_DEACTIVATE_DIVISION = "CANNOT_DEACTIVATE_DIVISION";
    public static final java.lang.String _CANNOT_DELETE_LAST_DATED_CONVERSION_RATE = "CANNOT_DELETE_LAST_DATED_CONVERSION_RATE";
    public static final java.lang.String _CANNOT_DELETE_MANAGED_OBJECT = "CANNOT_DELETE_MANAGED_OBJECT";
    public static final java.lang.String _CANNOT_DISABLE_LAST_ADMIN = "CANNOT_DISABLE_LAST_ADMIN";
    public static final java.lang.String _CANNOT_ENABLE_IP_RESTRICT_REQUESTS = "CANNOT_ENABLE_IP_RESTRICT_REQUESTS";
    public static final java.lang.String _CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY = "CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY";
    public static final java.lang.String _CANNOT_MODIFY_MANAGED_OBJECT = "CANNOT_MODIFY_MANAGED_OBJECT";
    public static final java.lang.String _CANNOT_RENAME_APEX_REFERENCED_FIELD = "CANNOT_RENAME_APEX_REFERENCED_FIELD";
    public static final java.lang.String _CANNOT_RENAME_APEX_REFERENCED_OBJECT = "CANNOT_RENAME_APEX_REFERENCED_OBJECT";
    public static final java.lang.String _CANNOT_REPARENT_RECORD = "CANNOT_REPARENT_RECORD";
    public static final java.lang.String _CANNOT_UPDATE_CONVERTED_LEAD = "CANNOT_UPDATE_CONVERTED_LEAD";
    public static final java.lang.String _CANT_DISABLE_CORP_CURRENCY = "CANT_DISABLE_CORP_CURRENCY";
    public static final java.lang.String _CANT_UNSET_CORP_CURRENCY = "CANT_UNSET_CORP_CURRENCY";
    public static final java.lang.String _CHILD_SHARE_FAILS_PARENT = "CHILD_SHARE_FAILS_PARENT";
    public static final java.lang.String _CIRCULAR_DEPENDENCY = "CIRCULAR_DEPENDENCY";
    public static final java.lang.String _CUSTOM_CLOB_FIELD_LIMIT_EXCEEDED = "CUSTOM_CLOB_FIELD_LIMIT_EXCEEDED";
    public static final java.lang.String _CUSTOM_ENTITY_OR_FIELD_LIMIT = "CUSTOM_ENTITY_OR_FIELD_LIMIT";
    public static final java.lang.String _CUSTOM_FIELD_INDEX_LIMIT_EXCEEDED = "CUSTOM_FIELD_INDEX_LIMIT_EXCEEDED";
    public static final java.lang.String _CUSTOM_INDEX_EXISTS = "CUSTOM_INDEX_EXISTS";
    public static final java.lang.String _CUSTOM_LINK_LIMIT_EXCEEDED = "CUSTOM_LINK_LIMIT_EXCEEDED";
    public static final java.lang.String _CUSTOM_TAB_LIMIT_EXCEEDED = "CUSTOM_TAB_LIMIT_EXCEEDED";
    public static final java.lang.String _DELETE_FAILED = "DELETE_FAILED";
    public static final java.lang.String _DELETE_REQUIRED_ON_CASCADE = "DELETE_REQUIRED_ON_CASCADE";
    public static final java.lang.String _DEPENDENCY_EXISTS = "DEPENDENCY_EXISTS";
    public static final java.lang.String _DUPLICATE_CASE_SOLUTION = "DUPLICATE_CASE_SOLUTION";
    public static final java.lang.String _DUPLICATE_COMM_NICKNAME = "DUPLICATE_COMM_NICKNAME";
    public static final java.lang.String _DUPLICATE_CUSTOM_ENTITY_DEFINITION = "DUPLICATE_CUSTOM_ENTITY_DEFINITION";
    public static final java.lang.String _DUPLICATE_CUSTOM_TAB_MOTIF = "DUPLICATE_CUSTOM_TAB_MOTIF";
    public static final java.lang.String _DUPLICATE_DEVELOPER_NAME = "DUPLICATE_DEVELOPER_NAME";
    public static final java.lang.String _DUPLICATE_EXTERNAL_ID = "DUPLICATE_EXTERNAL_ID";
    public static final java.lang.String _DUPLICATE_MASTER_LABEL = "DUPLICATE_MASTER_LABEL";
    public static final java.lang.String _DUPLICATE_USERNAME = "DUPLICATE_USERNAME";
    public static final java.lang.String _DUPLICATE_VALUE = "DUPLICATE_VALUE";
    public static final java.lang.String _EMAIL_NOT_PROCESSED_DUE_TO_PRIOR_ERROR = "EMAIL_NOT_PROCESSED_DUE_TO_PRIOR_ERROR";
    public static final java.lang.String _EMPTY_SCONTROL_FILE_NAME = "EMPTY_SCONTROL_FILE_NAME";
    public static final java.lang.String _ENTITY_FAILED_IFLASTMODIFIED_ON_UPDATE = "ENTITY_FAILED_IFLASTMODIFIED_ON_UPDATE";
    public static final java.lang.String _ENTITY_IS_ARCHIVED = "ENTITY_IS_ARCHIVED";
    public static final java.lang.String _ENTITY_IS_DELETED = "ENTITY_IS_DELETED";
    public static final java.lang.String _ENTITY_IS_LOCKED = "ENTITY_IS_LOCKED";
    public static final java.lang.String _ERROR_IN_MAILER = "ERROR_IN_MAILER";
    public static final java.lang.String _FAILED_ACTIVATION = "FAILED_ACTIVATION";
    public static final java.lang.String _FIELD_CUSTOM_VALIDATION_EXCEPTION = "FIELD_CUSTOM_VALIDATION_EXCEPTION";
    public static final java.lang.String _FIELD_INTEGRITY_EXCEPTION = "FIELD_INTEGRITY_EXCEPTION";
    public static final java.lang.String _HTML_FILE_UPLOAD_NOT_ALLOWED = "HTML_FILE_UPLOAD_NOT_ALLOWED";
    public static final java.lang.String _IMAGE_TOO_LARGE = "IMAGE_TOO_LARGE";
    public static final java.lang.String _INACTIVE_OWNER_OR_USER = "INACTIVE_OWNER_OR_USER";
    public static final java.lang.String _INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY = "INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY";
    public static final java.lang.String _INSUFFICIENT_ACCESS_OR_READONLY = "INSUFFICIENT_ACCESS_OR_READONLY";
    public static final java.lang.String _INVALID_ACCESS_LEVEL = "INVALID_ACCESS_LEVEL";
    public static final java.lang.String _INVALID_ARGUMENT_TYPE = "INVALID_ARGUMENT_TYPE";
    public static final java.lang.String _INVALID_ASSIGNEE_TYPE = "INVALID_ASSIGNEE_TYPE";
    public static final java.lang.String _INVALID_ASSIGNMENT_RULE = "INVALID_ASSIGNMENT_RULE";
    public static final java.lang.String _INVALID_BATCH_OPERATION = "INVALID_BATCH_OPERATION";
    public static final java.lang.String _INVALID_CREDIT_CARD_INFO = "INVALID_CREDIT_CARD_INFO";
    public static final java.lang.String _INVALID_CROSS_REFERENCE_KEY = "INVALID_CROSS_REFERENCE_KEY";
    public static final java.lang.String _INVALID_CROSS_REFERENCE_TYPE_FOR_FIELD = "INVALID_CROSS_REFERENCE_TYPE_FOR_FIELD";
    public static final java.lang.String _INVALID_CURRENCY_CONV_RATE = "INVALID_CURRENCY_CONV_RATE";
    public static final java.lang.String _INVALID_CURRENCY_CORP_RATE = "INVALID_CURRENCY_CORP_RATE";
    public static final java.lang.String _INVALID_CURRENCY_ISO = "INVALID_CURRENCY_ISO";
    public static final java.lang.String _INVALID_EMAIL_ADDRESS = "INVALID_EMAIL_ADDRESS";
    public static final java.lang.String _INVALID_EMPTY_KEY_OWNER = "INVALID_EMPTY_KEY_OWNER";
    public static final java.lang.String _INVALID_FIELD = "INVALID_FIELD";
    public static final java.lang.String _INVALID_FIELD_FOR_INSERT_UPDATE = "INVALID_FIELD_FOR_INSERT_UPDATE";
    public static final java.lang.String _INVALID_FIELD_WHEN_USING_TEMPLATE = "INVALID_FIELD_WHEN_USING_TEMPLATE";
    public static final java.lang.String _INVALID_FILTER_ACTION = "INVALID_FILTER_ACTION";
    public static final java.lang.String _INVALID_GOOGLE_DOCS_URL = "INVALID_GOOGLE_DOCS_URL";
    public static final java.lang.String _INVALID_ID_FIELD = "INVALID_ID_FIELD";
    public static final java.lang.String _INVALID_INET_ADDRESS = "INVALID_INET_ADDRESS";
    public static final java.lang.String _INVALID_LINEITEM_CLONE_STATE = "INVALID_LINEITEM_CLONE_STATE";
    public static final java.lang.String _INVALID_MASTER_OR_TRANSLATED_SOLUTION = "INVALID_MASTER_OR_TRANSLATED_SOLUTION";
    public static final java.lang.String _INVALID_OPERATION = "INVALID_OPERATION";
    public static final java.lang.String _INVALID_OPERATOR = "INVALID_OPERATOR";
    public static final java.lang.String _INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST = "INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST";
    public static final java.lang.String _INVALID_PARTNER_NETWORK_STATUS = "INVALID_PARTNER_NETWORK_STATUS";
    public static final java.lang.String _INVALID_PERSON_ACCOUNT_OPERATION = "INVALID_PERSON_ACCOUNT_OPERATION";
    public static final java.lang.String _INVALID_SAVE_AS_ACTIVITY_FLAG = "INVALID_SAVE_AS_ACTIVITY_FLAG";
    public static final java.lang.String _INVALID_SESSION_ID = "INVALID_SESSION_ID";
    public static final java.lang.String _INVALID_SETUP_OWNER = "INVALID_SETUP_OWNER";
    public static final java.lang.String _INVALID_STATUS = "INVALID_STATUS";
    public static final java.lang.String _INVALID_TYPE = "INVALID_TYPE";
    public static final java.lang.String _INVALID_TYPE_FOR_OPERATION = "INVALID_TYPE_FOR_OPERATION";
    public static final java.lang.String _INVALID_TYPE_ON_FIELD_IN_RECORD = "INVALID_TYPE_ON_FIELD_IN_RECORD";
    public static final java.lang.String _IP_RANGE_LIMIT_EXCEEDED = "IP_RANGE_LIMIT_EXCEEDED";
    public static final java.lang.String _LICENSE_LIMIT_EXCEEDED = "LICENSE_LIMIT_EXCEEDED";
    public static final java.lang.String _LIMIT_EXCEEDED = "LIMIT_EXCEEDED";
    public static final java.lang.String _MALFORMED_ID = "MALFORMED_ID";
    public static final java.lang.String _MANAGER_NOT_DEFINED = "MANAGER_NOT_DEFINED";
    public static final java.lang.String _MASSMAIL_RETRY_LIMIT_EXCEEDED = "MASSMAIL_RETRY_LIMIT_EXCEEDED";
    public static final java.lang.String _MASS_MAIL_LIMIT_EXCEEDED = "MASS_MAIL_LIMIT_EXCEEDED";
    public static final java.lang.String _MAXIMUM_CCEMAILS_EXCEEDED = "MAXIMUM_CCEMAILS_EXCEEDED";
    public static final java.lang.String _MAXIMUM_DASHBOARD_COMPONENTS_EXCEEDED = "MAXIMUM_DASHBOARD_COMPONENTS_EXCEEDED";
    public static final java.lang.String _MAXIMUM_HIERARCHY_LEVELS_REACHED = "MAXIMUM_HIERARCHY_LEVELS_REACHED";
    public static final java.lang.String _MAXIMUM_SIZE_OF_ATTACHMENT = "MAXIMUM_SIZE_OF_ATTACHMENT";
    public static final java.lang.String _MAXIMUM_SIZE_OF_DOCUMENT = "MAXIMUM_SIZE_OF_DOCUMENT";
    public static final java.lang.String _MAX_ACTIONS_PER_RULE_EXCEEDED = "MAX_ACTIONS_PER_RULE_EXCEEDED";
    public static final java.lang.String _MAX_ACTIVE_RULES_EXCEEDED = "MAX_ACTIVE_RULES_EXCEEDED";
    public static final java.lang.String _MAX_APPROVAL_STEPS_EXCEEDED = "MAX_APPROVAL_STEPS_EXCEEDED";
    public static final java.lang.String _MAX_FORMULAS_PER_RULE_EXCEEDED = "MAX_FORMULAS_PER_RULE_EXCEEDED";
    public static final java.lang.String _MAX_RULES_EXCEEDED = "MAX_RULES_EXCEEDED";
    public static final java.lang.String _MAX_RULE_ENTRIES_EXCEEDED = "MAX_RULE_ENTRIES_EXCEEDED";
    public static final java.lang.String _MAX_TASK_DESCRIPTION_EXCEEEDED = "MAX_TASK_DESCRIPTION_EXCEEEDED";
    public static final java.lang.String _MAX_TM_RULES_EXCEEDED = "MAX_TM_RULES_EXCEEDED";
    public static final java.lang.String _MAX_TM_RULE_ITEMS_EXCEEDED = "MAX_TM_RULE_ITEMS_EXCEEDED";
    public static final java.lang.String _MERGE_FAILED = "MERGE_FAILED";
    public static final java.lang.String _MISSING_ARGUMENT = "MISSING_ARGUMENT";
    public static final java.lang.String _MIXED_DML_OPERATION = "MIXED_DML_OPERATION";
    public static final java.lang.String _NONUNIQUE_SHIPPING_ADDRESS = "NONUNIQUE_SHIPPING_ADDRESS";
    public static final java.lang.String _NO_APPLICABLE_PROCESS = "NO_APPLICABLE_PROCESS";
    public static final java.lang.String _NO_ATTACHMENT_PERMISSION = "NO_ATTACHMENT_PERMISSION";
    public static final java.lang.String _NO_MASS_MAIL_PERMISSION = "NO_MASS_MAIL_PERMISSION";
    public static final java.lang.String _NUMBER_OUTSIDE_VALID_RANGE = "NUMBER_OUTSIDE_VALID_RANGE";
    public static final java.lang.String _NUM_HISTORY_FIELDS_BY_SOBJECT_EXCEEDED = "NUM_HISTORY_FIELDS_BY_SOBJECT_EXCEEDED";
    public static final java.lang.String _OPTED_OUT_OF_MASS_MAIL = "OPTED_OUT_OF_MASS_MAIL";
    public static final java.lang.String _PACKAGE_LICENSE_REQUIRED = "PACKAGE_LICENSE_REQUIRED";
    public static final java.lang.String _PORTAL_USER_ALREADY_EXISTS_FOR_CONTACT = "PORTAL_USER_ALREADY_EXISTS_FOR_CONTACT";
    public static final java.lang.String _PRIVATE_CONTACT_ON_ASSET = "PRIVATE_CONTACT_ON_ASSET";
    public static final java.lang.String _RECORD_IN_USE_BY_WORKFLOW = "RECORD_IN_USE_BY_WORKFLOW";
    public static final java.lang.String _REQUEST_RUNNING_TOO_LONG = "REQUEST_RUNNING_TOO_LONG";
    public static final java.lang.String _REQUIRED_FIELD_MISSING = "REQUIRED_FIELD_MISSING";
    public static final java.lang.String _SELF_REFERENCE_FROM_TRIGGER = "SELF_REFERENCE_FROM_TRIGGER";
    public static final java.lang.String _SHARE_NEEDED_FOR_CHILD_OWNER = "SHARE_NEEDED_FOR_CHILD_OWNER";
    public static final java.lang.String _STANDARD_PRICE_NOT_DEFINED = "STANDARD_PRICE_NOT_DEFINED";
    public static final java.lang.String _STORAGE_LIMIT_EXCEEDED = "STORAGE_LIMIT_EXCEEDED";
    public static final java.lang.String _STRING_TOO_LONG = "STRING_TOO_LONG";
    public static final java.lang.String _TABSET_LIMIT_EXCEEDED = "TABSET_LIMIT_EXCEEDED";
    public static final java.lang.String _TEMPLATE_NOT_ACTIVE = "TEMPLATE_NOT_ACTIVE";
    public static final java.lang.String _TERRITORY_REALIGN_IN_PROGRESS = "TERRITORY_REALIGN_IN_PROGRESS";
    public static final java.lang.String _TEXT_DATA_OUTSIDE_SUPPORTED_CHARSET = "TEXT_DATA_OUTSIDE_SUPPORTED_CHARSET";
    public static final java.lang.String _TOO_MANY_APEX_REQUESTS = "TOO_MANY_APEX_REQUESTS";
    public static final java.lang.String _TOO_MANY_ENUM_VALUE = "TOO_MANY_ENUM_VALUE";
    public static final java.lang.String _TRANSFER_REQUIRES_READ = "TRANSFER_REQUIRES_READ";
    public static final java.lang.String _UNABLE_TO_LOCK_ROW = "UNABLE_TO_LOCK_ROW";
    public static final java.lang.String _UNAVAILABLE_RECORDTYPE_EXCEPTION = "UNAVAILABLE_RECORDTYPE_EXCEPTION";
    public static final java.lang.String _UNDELETE_FAILED = "UNDELETE_FAILED";
    public static final java.lang.String _UNKNOWN_EXCEPTION = "UNKNOWN_EXCEPTION";
    public static final java.lang.String _UNSPECIFIED_EMAIL_ADDRESS = "UNSPECIFIED_EMAIL_ADDRESS";
    public static final java.lang.String _UNSUPPORTED_APEX_TRIGGER_OPERATON = "UNSUPPORTED_APEX_TRIGGER_OPERATON";
    public static final java.lang.String _WEBLINK_SIZE_LIMIT_EXCEEDED = "WEBLINK_SIZE_LIMIT_EXCEEDED";
    public static final java.lang.String _WRONG_CONTROLLER_TYPE = "WRONG_CONTROLLER_TYPE";
    public static final StatusCode ALREADY_IN_PROCESS = new StatusCode(_ALREADY_IN_PROCESS);
    public static final StatusCode ASSIGNEE_TYPE_REQUIRED = new StatusCode(_ASSIGNEE_TYPE_REQUIRED);
    public static final StatusCode BAD_CUSTOM_ENTITY_PARENT_DOMAIN = new StatusCode(_BAD_CUSTOM_ENTITY_PARENT_DOMAIN);
    public static final StatusCode BCC_NOT_ALLOWED_IF_BCC_COMPLIANCE_ENABLED = new StatusCode(_BCC_NOT_ALLOWED_IF_BCC_COMPLIANCE_ENABLED);
    public static final StatusCode CANNOT_CASCADE_PRODUCT_ACTIVE = new StatusCode(_CANNOT_CASCADE_PRODUCT_ACTIVE);
    public static final StatusCode CANNOT_CHANGE_FIELD_TYPE_OF_APEX_REFERENCED_FIELD = new StatusCode(_CANNOT_CHANGE_FIELD_TYPE_OF_APEX_REFERENCED_FIELD);
    public static final StatusCode CANNOT_CREATE_ANOTHER_MANAGED_PACKAGE = new StatusCode(_CANNOT_CREATE_ANOTHER_MANAGED_PACKAGE);
    public static final StatusCode CANNOT_DEACTIVATE_DIVISION = new StatusCode(_CANNOT_DEACTIVATE_DIVISION);
    public static final StatusCode CANNOT_DELETE_LAST_DATED_CONVERSION_RATE = new StatusCode(_CANNOT_DELETE_LAST_DATED_CONVERSION_RATE);
    public static final StatusCode CANNOT_DELETE_MANAGED_OBJECT = new StatusCode(_CANNOT_DELETE_MANAGED_OBJECT);
    public static final StatusCode CANNOT_DISABLE_LAST_ADMIN = new StatusCode(_CANNOT_DISABLE_LAST_ADMIN);
    public static final StatusCode CANNOT_ENABLE_IP_RESTRICT_REQUESTS = new StatusCode(_CANNOT_ENABLE_IP_RESTRICT_REQUESTS);
    public static final StatusCode CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY = new StatusCode(_CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY);
    public static final StatusCode CANNOT_MODIFY_MANAGED_OBJECT = new StatusCode(_CANNOT_MODIFY_MANAGED_OBJECT);
    public static final StatusCode CANNOT_RENAME_APEX_REFERENCED_FIELD = new StatusCode(_CANNOT_RENAME_APEX_REFERENCED_FIELD);
    public static final StatusCode CANNOT_RENAME_APEX_REFERENCED_OBJECT = new StatusCode(_CANNOT_RENAME_APEX_REFERENCED_OBJECT);
    public static final StatusCode CANNOT_REPARENT_RECORD = new StatusCode(_CANNOT_REPARENT_RECORD);
    public static final StatusCode CANNOT_UPDATE_CONVERTED_LEAD = new StatusCode(_CANNOT_UPDATE_CONVERTED_LEAD);
    public static final StatusCode CANT_DISABLE_CORP_CURRENCY = new StatusCode(_CANT_DISABLE_CORP_CURRENCY);
    public static final StatusCode CANT_UNSET_CORP_CURRENCY = new StatusCode(_CANT_UNSET_CORP_CURRENCY);
    public static final StatusCode CHILD_SHARE_FAILS_PARENT = new StatusCode(_CHILD_SHARE_FAILS_PARENT);
    public static final StatusCode CIRCULAR_DEPENDENCY = new StatusCode(_CIRCULAR_DEPENDENCY);
    public static final StatusCode CUSTOM_CLOB_FIELD_LIMIT_EXCEEDED = new StatusCode(_CUSTOM_CLOB_FIELD_LIMIT_EXCEEDED);
    public static final StatusCode CUSTOM_ENTITY_OR_FIELD_LIMIT = new StatusCode(_CUSTOM_ENTITY_OR_FIELD_LIMIT);
    public static final StatusCode CUSTOM_FIELD_INDEX_LIMIT_EXCEEDED = new StatusCode(_CUSTOM_FIELD_INDEX_LIMIT_EXCEEDED);
    public static final StatusCode CUSTOM_INDEX_EXISTS = new StatusCode(_CUSTOM_INDEX_EXISTS);
    public static final StatusCode CUSTOM_LINK_LIMIT_EXCEEDED = new StatusCode(_CUSTOM_LINK_LIMIT_EXCEEDED);
    public static final StatusCode CUSTOM_TAB_LIMIT_EXCEEDED = new StatusCode(_CUSTOM_TAB_LIMIT_EXCEEDED);
    public static final StatusCode DELETE_FAILED = new StatusCode(_DELETE_FAILED);
    public static final StatusCode DELETE_REQUIRED_ON_CASCADE = new StatusCode(_DELETE_REQUIRED_ON_CASCADE);
    public static final StatusCode DEPENDENCY_EXISTS = new StatusCode(_DEPENDENCY_EXISTS);
    public static final StatusCode DUPLICATE_CASE_SOLUTION = new StatusCode(_DUPLICATE_CASE_SOLUTION);
    public static final StatusCode DUPLICATE_COMM_NICKNAME = new StatusCode(_DUPLICATE_COMM_NICKNAME);
    public static final StatusCode DUPLICATE_CUSTOM_ENTITY_DEFINITION = new StatusCode(_DUPLICATE_CUSTOM_ENTITY_DEFINITION);
    public static final StatusCode DUPLICATE_CUSTOM_TAB_MOTIF = new StatusCode(_DUPLICATE_CUSTOM_TAB_MOTIF);
    public static final StatusCode DUPLICATE_DEVELOPER_NAME = new StatusCode(_DUPLICATE_DEVELOPER_NAME);
    public static final StatusCode DUPLICATE_EXTERNAL_ID = new StatusCode(_DUPLICATE_EXTERNAL_ID);
    public static final StatusCode DUPLICATE_MASTER_LABEL = new StatusCode(_DUPLICATE_MASTER_LABEL);
    public static final StatusCode DUPLICATE_USERNAME = new StatusCode(_DUPLICATE_USERNAME);
    public static final StatusCode DUPLICATE_VALUE = new StatusCode(_DUPLICATE_VALUE);
    public static final StatusCode EMAIL_NOT_PROCESSED_DUE_TO_PRIOR_ERROR = new StatusCode(_EMAIL_NOT_PROCESSED_DUE_TO_PRIOR_ERROR);
    public static final StatusCode EMPTY_SCONTROL_FILE_NAME = new StatusCode(_EMPTY_SCONTROL_FILE_NAME);
    public static final StatusCode ENTITY_FAILED_IFLASTMODIFIED_ON_UPDATE = new StatusCode(_ENTITY_FAILED_IFLASTMODIFIED_ON_UPDATE);
    public static final StatusCode ENTITY_IS_ARCHIVED = new StatusCode(_ENTITY_IS_ARCHIVED);
    public static final StatusCode ENTITY_IS_DELETED = new StatusCode(_ENTITY_IS_DELETED);
    public static final StatusCode ENTITY_IS_LOCKED = new StatusCode(_ENTITY_IS_LOCKED);
    public static final StatusCode ERROR_IN_MAILER = new StatusCode(_ERROR_IN_MAILER);
    public static final StatusCode FAILED_ACTIVATION = new StatusCode(_FAILED_ACTIVATION);
    public static final StatusCode FIELD_CUSTOM_VALIDATION_EXCEPTION = new StatusCode(_FIELD_CUSTOM_VALIDATION_EXCEPTION);
    public static final StatusCode FIELD_INTEGRITY_EXCEPTION = new StatusCode(_FIELD_INTEGRITY_EXCEPTION);
    public static final StatusCode HTML_FILE_UPLOAD_NOT_ALLOWED = new StatusCode(_HTML_FILE_UPLOAD_NOT_ALLOWED);
    public static final StatusCode IMAGE_TOO_LARGE = new StatusCode(_IMAGE_TOO_LARGE);
    public static final StatusCode INACTIVE_OWNER_OR_USER = new StatusCode(_INACTIVE_OWNER_OR_USER);
    public static final StatusCode INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY = new StatusCode(_INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY);
    public static final StatusCode INSUFFICIENT_ACCESS_OR_READONLY = new StatusCode(_INSUFFICIENT_ACCESS_OR_READONLY);
    public static final StatusCode INVALID_ACCESS_LEVEL = new StatusCode(_INVALID_ACCESS_LEVEL);
    public static final StatusCode INVALID_ARGUMENT_TYPE = new StatusCode(_INVALID_ARGUMENT_TYPE);
    public static final StatusCode INVALID_ASSIGNEE_TYPE = new StatusCode(_INVALID_ASSIGNEE_TYPE);
    public static final StatusCode INVALID_ASSIGNMENT_RULE = new StatusCode(_INVALID_ASSIGNMENT_RULE);
    public static final StatusCode INVALID_BATCH_OPERATION = new StatusCode(_INVALID_BATCH_OPERATION);
    public static final StatusCode INVALID_CREDIT_CARD_INFO = new StatusCode(_INVALID_CREDIT_CARD_INFO);
    public static final StatusCode INVALID_CROSS_REFERENCE_KEY = new StatusCode(_INVALID_CROSS_REFERENCE_KEY);
    public static final StatusCode INVALID_CROSS_REFERENCE_TYPE_FOR_FIELD = new StatusCode(_INVALID_CROSS_REFERENCE_TYPE_FOR_FIELD);
    public static final StatusCode INVALID_CURRENCY_CONV_RATE = new StatusCode(_INVALID_CURRENCY_CONV_RATE);
    public static final StatusCode INVALID_CURRENCY_CORP_RATE = new StatusCode(_INVALID_CURRENCY_CORP_RATE);
    public static final StatusCode INVALID_CURRENCY_ISO = new StatusCode(_INVALID_CURRENCY_ISO);
    public static final StatusCode INVALID_EMAIL_ADDRESS = new StatusCode(_INVALID_EMAIL_ADDRESS);
    public static final StatusCode INVALID_EMPTY_KEY_OWNER = new StatusCode(_INVALID_EMPTY_KEY_OWNER);
    public static final StatusCode INVALID_FIELD = new StatusCode(_INVALID_FIELD);
    public static final StatusCode INVALID_FIELD_FOR_INSERT_UPDATE = new StatusCode(_INVALID_FIELD_FOR_INSERT_UPDATE);
    public static final StatusCode INVALID_FIELD_WHEN_USING_TEMPLATE = new StatusCode(_INVALID_FIELD_WHEN_USING_TEMPLATE);
    public static final StatusCode INVALID_FILTER_ACTION = new StatusCode(_INVALID_FILTER_ACTION);
    public static final StatusCode INVALID_GOOGLE_DOCS_URL = new StatusCode(_INVALID_GOOGLE_DOCS_URL);
    public static final StatusCode INVALID_ID_FIELD = new StatusCode(_INVALID_ID_FIELD);
    public static final StatusCode INVALID_INET_ADDRESS = new StatusCode(_INVALID_INET_ADDRESS);
    public static final StatusCode INVALID_LINEITEM_CLONE_STATE = new StatusCode(_INVALID_LINEITEM_CLONE_STATE);
    public static final StatusCode INVALID_MASTER_OR_TRANSLATED_SOLUTION = new StatusCode(_INVALID_MASTER_OR_TRANSLATED_SOLUTION);
    public static final StatusCode INVALID_OPERATION = new StatusCode(_INVALID_OPERATION);
    public static final StatusCode INVALID_OPERATOR = new StatusCode(_INVALID_OPERATOR);
    public static final StatusCode INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST = new StatusCode(_INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST);
    public static final StatusCode INVALID_PARTNER_NETWORK_STATUS = new StatusCode(_INVALID_PARTNER_NETWORK_STATUS);
    public static final StatusCode INVALID_PERSON_ACCOUNT_OPERATION = new StatusCode(_INVALID_PERSON_ACCOUNT_OPERATION);
    public static final StatusCode INVALID_SAVE_AS_ACTIVITY_FLAG = new StatusCode(_INVALID_SAVE_AS_ACTIVITY_FLAG);
    public static final StatusCode INVALID_SESSION_ID = new StatusCode(_INVALID_SESSION_ID);
    public static final StatusCode INVALID_SETUP_OWNER = new StatusCode(_INVALID_SETUP_OWNER);
    public static final StatusCode INVALID_STATUS = new StatusCode(_INVALID_STATUS);
    public static final StatusCode INVALID_TYPE = new StatusCode(_INVALID_TYPE);
    public static final StatusCode INVALID_TYPE_FOR_OPERATION = new StatusCode(_INVALID_TYPE_FOR_OPERATION);
    public static final StatusCode INVALID_TYPE_ON_FIELD_IN_RECORD = new StatusCode(_INVALID_TYPE_ON_FIELD_IN_RECORD);
    public static final StatusCode IP_RANGE_LIMIT_EXCEEDED = new StatusCode(_IP_RANGE_LIMIT_EXCEEDED);
    public static final StatusCode LICENSE_LIMIT_EXCEEDED = new StatusCode(_LICENSE_LIMIT_EXCEEDED);
    public static final StatusCode LIMIT_EXCEEDED = new StatusCode(_LIMIT_EXCEEDED);
    public static final StatusCode MALFORMED_ID = new StatusCode(_MALFORMED_ID);
    public static final StatusCode MANAGER_NOT_DEFINED = new StatusCode(_MANAGER_NOT_DEFINED);
    public static final StatusCode MASSMAIL_RETRY_LIMIT_EXCEEDED = new StatusCode(_MASSMAIL_RETRY_LIMIT_EXCEEDED);
    public static final StatusCode MASS_MAIL_LIMIT_EXCEEDED = new StatusCode(_MASS_MAIL_LIMIT_EXCEEDED);
    public static final StatusCode MAXIMUM_CCEMAILS_EXCEEDED = new StatusCode(_MAXIMUM_CCEMAILS_EXCEEDED);
    public static final StatusCode MAXIMUM_DASHBOARD_COMPONENTS_EXCEEDED = new StatusCode(_MAXIMUM_DASHBOARD_COMPONENTS_EXCEEDED);
    public static final StatusCode MAXIMUM_HIERARCHY_LEVELS_REACHED = new StatusCode(_MAXIMUM_HIERARCHY_LEVELS_REACHED);
    public static final StatusCode MAXIMUM_SIZE_OF_ATTACHMENT = new StatusCode(_MAXIMUM_SIZE_OF_ATTACHMENT);
    public static final StatusCode MAXIMUM_SIZE_OF_DOCUMENT = new StatusCode(_MAXIMUM_SIZE_OF_DOCUMENT);
    public static final StatusCode MAX_ACTIONS_PER_RULE_EXCEEDED = new StatusCode(_MAX_ACTIONS_PER_RULE_EXCEEDED);
    public static final StatusCode MAX_ACTIVE_RULES_EXCEEDED = new StatusCode(_MAX_ACTIVE_RULES_EXCEEDED);
    public static final StatusCode MAX_APPROVAL_STEPS_EXCEEDED = new StatusCode(_MAX_APPROVAL_STEPS_EXCEEDED);
    public static final StatusCode MAX_FORMULAS_PER_RULE_EXCEEDED = new StatusCode(_MAX_FORMULAS_PER_RULE_EXCEEDED);
    public static final StatusCode MAX_RULES_EXCEEDED = new StatusCode(_MAX_RULES_EXCEEDED);
    public static final StatusCode MAX_RULE_ENTRIES_EXCEEDED = new StatusCode(_MAX_RULE_ENTRIES_EXCEEDED);
    public static final StatusCode MAX_TASK_DESCRIPTION_EXCEEEDED = new StatusCode(_MAX_TASK_DESCRIPTION_EXCEEEDED);
    public static final StatusCode MAX_TM_RULES_EXCEEDED = new StatusCode(_MAX_TM_RULES_EXCEEDED);
    public static final StatusCode MAX_TM_RULE_ITEMS_EXCEEDED = new StatusCode(_MAX_TM_RULE_ITEMS_EXCEEDED);
    public static final StatusCode MERGE_FAILED = new StatusCode(_MERGE_FAILED);
    public static final StatusCode MISSING_ARGUMENT = new StatusCode(_MISSING_ARGUMENT);
    public static final StatusCode MIXED_DML_OPERATION = new StatusCode(_MIXED_DML_OPERATION);
    public static final StatusCode NONUNIQUE_SHIPPING_ADDRESS = new StatusCode(_NONUNIQUE_SHIPPING_ADDRESS);
    public static final StatusCode NO_APPLICABLE_PROCESS = new StatusCode(_NO_APPLICABLE_PROCESS);
    public static final StatusCode NO_ATTACHMENT_PERMISSION = new StatusCode(_NO_ATTACHMENT_PERMISSION);
    public static final StatusCode NO_MASS_MAIL_PERMISSION = new StatusCode(_NO_MASS_MAIL_PERMISSION);
    public static final StatusCode NUMBER_OUTSIDE_VALID_RANGE = new StatusCode(_NUMBER_OUTSIDE_VALID_RANGE);
    public static final StatusCode NUM_HISTORY_FIELDS_BY_SOBJECT_EXCEEDED = new StatusCode(_NUM_HISTORY_FIELDS_BY_SOBJECT_EXCEEDED);
    public static final StatusCode OPTED_OUT_OF_MASS_MAIL = new StatusCode(_OPTED_OUT_OF_MASS_MAIL);
    public static final StatusCode PACKAGE_LICENSE_REQUIRED = new StatusCode(_PACKAGE_LICENSE_REQUIRED);
    public static final StatusCode PORTAL_USER_ALREADY_EXISTS_FOR_CONTACT = new StatusCode(_PORTAL_USER_ALREADY_EXISTS_FOR_CONTACT);
    public static final StatusCode PRIVATE_CONTACT_ON_ASSET = new StatusCode(_PRIVATE_CONTACT_ON_ASSET);
    public static final StatusCode RECORD_IN_USE_BY_WORKFLOW = new StatusCode(_RECORD_IN_USE_BY_WORKFLOW);
    public static final StatusCode REQUEST_RUNNING_TOO_LONG = new StatusCode(_REQUEST_RUNNING_TOO_LONG);
    public static final StatusCode REQUIRED_FIELD_MISSING = new StatusCode(_REQUIRED_FIELD_MISSING);
    public static final StatusCode SELF_REFERENCE_FROM_TRIGGER = new StatusCode(_SELF_REFERENCE_FROM_TRIGGER);
    public static final StatusCode SHARE_NEEDED_FOR_CHILD_OWNER = new StatusCode(_SHARE_NEEDED_FOR_CHILD_OWNER);
    public static final StatusCode STANDARD_PRICE_NOT_DEFINED = new StatusCode(_STANDARD_PRICE_NOT_DEFINED);
    public static final StatusCode STORAGE_LIMIT_EXCEEDED = new StatusCode(_STORAGE_LIMIT_EXCEEDED);
    public static final StatusCode STRING_TOO_LONG = new StatusCode(_STRING_TOO_LONG);
    public static final StatusCode TABSET_LIMIT_EXCEEDED = new StatusCode(_TABSET_LIMIT_EXCEEDED);
    public static final StatusCode TEMPLATE_NOT_ACTIVE = new StatusCode(_TEMPLATE_NOT_ACTIVE);
    public static final StatusCode TERRITORY_REALIGN_IN_PROGRESS = new StatusCode(_TERRITORY_REALIGN_IN_PROGRESS);
    public static final StatusCode TEXT_DATA_OUTSIDE_SUPPORTED_CHARSET = new StatusCode(_TEXT_DATA_OUTSIDE_SUPPORTED_CHARSET);
    public static final StatusCode TOO_MANY_APEX_REQUESTS = new StatusCode(_TOO_MANY_APEX_REQUESTS);
    public static final StatusCode TOO_MANY_ENUM_VALUE = new StatusCode(_TOO_MANY_ENUM_VALUE);
    public static final StatusCode TRANSFER_REQUIRES_READ = new StatusCode(_TRANSFER_REQUIRES_READ);
    public static final StatusCode UNABLE_TO_LOCK_ROW = new StatusCode(_UNABLE_TO_LOCK_ROW);
    public static final StatusCode UNAVAILABLE_RECORDTYPE_EXCEPTION = new StatusCode(_UNAVAILABLE_RECORDTYPE_EXCEPTION);
    public static final StatusCode UNDELETE_FAILED = new StatusCode(_UNDELETE_FAILED);
    public static final StatusCode UNKNOWN_EXCEPTION = new StatusCode(_UNKNOWN_EXCEPTION);
    public static final StatusCode UNSPECIFIED_EMAIL_ADDRESS = new StatusCode(_UNSPECIFIED_EMAIL_ADDRESS);
    public static final StatusCode UNSUPPORTED_APEX_TRIGGER_OPERATON = new StatusCode(_UNSUPPORTED_APEX_TRIGGER_OPERATON);
    public static final StatusCode WEBLINK_SIZE_LIMIT_EXCEEDED = new StatusCode(_WEBLINK_SIZE_LIMIT_EXCEEDED);
    public static final StatusCode WRONG_CONTROLLER_TYPE = new StatusCode(_WRONG_CONTROLLER_TYPE);
    public java.lang.String getValue() { return _value_;}
    public static StatusCode fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        StatusCode enumeration = (StatusCode)
            _table_.get(value);
        if (enumeration==null) throw new java.lang.IllegalArgumentException();
        return enumeration;
    }
    public static StatusCode fromString(java.lang.String value)
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
        new org.apache.axis.description.TypeDesc(StatusCode.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "StatusCode"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}
