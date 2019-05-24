/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.ldap;

import javax.naming.directory.*;

/**
 * Utility class to maintain list of constants for the LDAPConnector.
 * Please modify constants here; changes should be reflected throughout
 * the connector code.
 */
public class LDAPConnectorConstants {

    public static final String ldapDefaultSortName = "guid"; //$NON-NLS-1$
    public static final int ldapDefaultSearchScope = SearchControls.ONELEVEL_SCOPE;
    public static final boolean ldapDefaultIsAscending = true;

    public static final String ldapTimestampFormat = "yyyyMMddhhmmss\'Z\'"; //$NON-NLS-1$
}
