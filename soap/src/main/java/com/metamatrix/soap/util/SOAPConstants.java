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

package com.metamatrix.soap.util;

import com.metamatrix.common.api.MMURL;

/**
 * This file will contain all of the constants used by the MetaMatrix SOAP service
 */
public final class SOAPConstants {

    /**
     * Private constructor
     */
    private SOAPConstants() {
    }

    /**
     * Name of property used to set the application name in a MetaMatrix connection.
     */
    public static final String APP_NAME = MMURL.CONNECTION.APP_NAME; 

    /**
     * Non-secure MetaMatrix Protocol.
     */
    public static final String NON_SECURE_PROTOCOL = MMURL.CONNECTION.NON_SECURE_PROTOCOL; 

    /**
     * Non-secure MetaMatrix Protocol.
     */
    public static final String SECURE_PROTOCOL = MMURL.CONNECTION.SECURE_PROTOCOL; 

    /**
     * Indicates that the client did not have enough information to process the authentication or that the authentication message
     * was incorrectly formed.
     */
    public static final String CLIENT_AUTHENTICATION_FAULT = "Client.Authentication"; //$NON-NLS-1$

    /**
     * Indicates that there was an authentication failure when attempting to authenticate
     */
    public static final String SERVER_AUTHENTICATION_FAULT = "Server.Authentication"; //$NON-NLS-1$

    /**
     * Indicates that there was a failure when attempting to establish a connection on the server side to the MetaMatrixServer
     */
    public static final String SERVER_CONNECTION_FAULT = "Server.Connection"; //$NON-NLS-1$

    /**
     * Indicates that there was a failure when attempting to establish a connection to the SOAP Connection architecture
     */
    public static final String CLIENT_CONNECTION_FAULT = "Client.Connection"; //$NON-NLS-1$

    /**
     * Indicates that the client did not have enough information in the request to submit a query to the MetaMatrix server
     */
    public static final String CLIENT_QUERY_EXECUTION_FAULT = "Client.QueryExecution"; //$NON-NLS-1$

    /**
     * Indicates that there was a failure when attempting to execute the query against the MetaMatrix server
     */
    public static final String SERVER_QUERY_EXECUTION_FAULT = "Server.QueryExecution"; //$NON-NLS-1$

    /**
     * Indicates that there was a translation error when attempting to translate the results into a SOAP specific return message
     * on the server
     */
    public static final String SERVER_TRANSLATION_FAULT = "Server.Translation"; //$NON-NLS-1$

    /**
     * Indicates that the server is not licensed for the MetaMatrix SOAP API
     */
    public static final String SERVER_LICENSE_EXCEPTION = "Server.LicenseException"; //$NON-NLS-1$

    /**
     * Indicates that the client did not have enough information to submit an insert, update, or delete statement to the
     * MetaMatrix server
     */
    public static final String CLIENT_UPDATE_EXECUTION_FAULT = "Client.UpdateExecution"; //$NON-NLS-1$

    /**
     * Indicates that there was a failure executing the insert,update, or delete against the MetaMatrix server
     */
    public static final String SERVER_UPDATE_EXECUTION_FAULT = "Server.UpdateExecution"; //$NON-NLS-1$

    /**
     * Indicates that the client did not have enough information to process a Stored procedure execution request against the
     * MetaMatrix server.
     */
    public static final String CLIENT_STOREDPROCEDURE_EXECUTION_FAULT = "Client.StoredProcedureExecution"; //$NON-NLS-1$

    /**
     * Indicates that an error occurred in the MetaMatrix server while executing the Stored procedure execution
     */
    public static final String SERVER_STOREDPROCEDURE_EXECUTION_FAULT = "Server.StoredProcedureExecution"; //$NON-NLS-1$

    /**
     * A timeout has occured on the Client side
     */
    public static final String CLIENT_TIMEOUT = "Client.Timeout"; //$NON-NLS-1$

    /**
     * A timeout has occured on the Server side
     */
    public static final String SERVER_TIMEOUT = "Server.Timeout"; //$NON-NLS-1$

    /**
     * A general server fault
     */
    public static final String SERVER_FAULT = "Server"; //$NON-NLS-1$

    /**
     * A general client fault
     */
    public static final String CLIENT_FAULT = "Client"; //$NON-NLS-1$

    /**
     * This class contains the literal XML tag names and attributes for the session id
     */
    public static final class SESSION_ID {

        public static final class TAGS {

            public static final String SESSION_ID_TAG = "session"; //$NON-NLS-1$
        }
    }

    /**
     * This class contains the literal XML tag names and attributes tags for the authentication XML used in the MetaMatrix SOAP
     * service.
     */
    public static final class AUTHENTICATION {

        public static final class TAGS {

            public static final String AUTHENTICATION_TAG = "authentication"; //$NON-NLS-1$

            public static final class USERNAME {

                public static final String USERNAME_TAG = "username"; //$NON-NLS-1$

                public static final class ATTRIBUTES {
                }
            }

            public static final class PASSWORD {

                public static final String PASSWORD_TAG = "password"; //$NON-NLS-1$

                public static final class ATTRIBUTES {

                    public static final String IS_ENCRYPTED_TAG = "isencrypted"; //$NON-NLS-1$
                }
            }

            public static final class VDB {

                public static final String VDB_TAG = "vdb"; //$NON-NLS-1$

                public static final class ATTRIBUTES {

                    public static final String VERSION_TAG = "version"; //$NON-NLS-1$
                }
            }

            public static final class SERVERURL {

                public static final String SERVER_URL_TAG = "serverurl"; //$NON-NLS-1$

                public static final class ATTRIBUTES {

                    public static final String APPSERVER_TAG = "appserver"; //$NON-NLS-1$
                }
            }

            public static final class TRUSTED {

                public static final String TRUSTED_TAG = "trusted"; //$NON-NLS-1$

                public static final class ATTRIBUTES {
                }
            }
        }
    }

    public static final class STORED_PROCEDURE_PARAMETERS {

        public static final String PARAMETERS_TAG = "parameters"; //$NON-NLS-1$

        public static final class Param {

            public static final String PARAM_TAG = "param"; //$NON-NLS-1$

            public static final class Attributes {

                public static final String INDEX_TAG = "index"; //$NON-NLS-1$
            }
        }
    }

    public static final class METADATA_TYPES {

        public static final class CROSS_REFERENCES {

            public static final String NAME = "crossreferences"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String PRIMARY_GROUP_NAME = "cr_primarygroupname"; //$NON-NLS-1$
                public static final String FOREIGN_GROUP_NAME = "cr_foreigngroupname"; //$NON-NLS-1$
            }
        }

        public static final class DATATYPES {

            public static final String NAME = "datatypes"; //$NON-NLS-1$

            public static final class Parameters {
            }
        }

        public static final class ELEMENTS {

            public static final String NAME = "elements"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String GROUP_PATTERN = "e_grouppattern"; //$NON-NLS-1$
                public static final String ELEMENT_PATTERN = "e_elementpattern"; //$NON-NLS-1$
            }
        }

        public static final class EXPORTED_KEYS {

            public static final String NAME = "exportedkeys"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String PRIMARY_GROUP_NAME = "ek_primarygroupname"; //$NON-NLS-1$
            }
        }

        public static final class FOREIGN_KEYS {

            public static final String NAME = "foreignkeys"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String GROUP_NAME = "fk_groupname"; //$NON-NLS-1$
            }
        }

        public static final class GROUPS {

            public static final String NAME = "groups"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String GROUP_PATTERN = "g_grouppattern"; //$NON-NLS-1$
            }
        }

        public static final class IMPORTED_KEYS {

            public static final String NAME = "importedkeys"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String FOREIGN_GROUP_NAME = "ik_foreigngroupname"; //$NON-NLS-1$
            }
        }

        public static final class MODELS {

            public static final String NAME = "models"; //$NON-NLS-1$

            public static final class Parameters {
            }
        }

        public static final class PRIMARY_KEYS {

            public static final String NAME = "primarykeys"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String GROUP_NAME = "pk_groupname"; //$NON-NLS-1$
            }
        }

        public static final class PROCEDURES {

            public static final String NAME = "procedures"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String PROCEDURE_PATTERN = "p_procedurepattern"; //$NON-NLS-1$
            }
        }

        public static final class PROCEDURE_PARAMETERS {

            public static final String NAME = "procedureparameters"; //$NON-NLS-1$

            public static final class Parameters {

                public static final String PROCEDURE_NAME_PATTERN = "pp_procedurenamepattern"; //$NON-NLS-1$
                public static final String PARAMETER_NAME_PATTERN = "pp_parameternamepattern"; //$NON-NLS-1$
            }
        }

        public static final class USER_DEFINED_DATATYPES {

            public static final String NAME = "userdefineddatatypes"; //$NON-NLS-1$

            public static final class Parameters {
            }
        }

        public static final class VIRTUALDATABASES {

            public static final String NAME = "virtualdatabases"; //$NON-NLS-1$

            public static final class Parameters {
            }
        }
    }

}
