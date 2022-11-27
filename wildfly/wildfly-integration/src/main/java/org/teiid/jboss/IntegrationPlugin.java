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
package org.teiid.jboss;

import java.util.Locale;
import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class IntegrationPlugin {
    private static final String PLUGIN_ID = "org.teiid.jboss" ; //$NON-NLS-1$
    static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,BUNDLE_NAME,ResourceBundle.getBundle(BUNDLE_NAME));

    public static ResourceBundle getResourceBundle(Locale locale) {
        if (locale == null) {
            locale = Locale.getDefault();
        }
        return ResourceBundle.getBundle(IntegrationPlugin.BUNDLE_NAME, locale);
    }

    public static enum Event implements BundleUtil.Event {
        TEIID50001,
        TEIID50002,
        TEIID50003,
        TEIID50005,
        TEIID50006,
        TEIID50007, // failed to load module
        TEIID50008,
        TEIID50009,
        TEIID50010,
        TEIID50011,
        TEIID50012, // socket enabled
        TEIID50013, // Wrong socket protocol
        TEIID50016, // invalid vdb file
        TEIID50017, // vdb.xml parse exception
        TEIID50018, // failed VDB dependency processing
        TEIID50019, // redeploying VDB
        TEIID50021, // vdb defined translator not found
        TEIID50024, // failed metadata load
        TEIID50025, // VDB deployed
        TEIID50026, // VDB undeployed
        TEIID50035, // translator not found
        TEIID50037, // odbc enabled
        TEIID50038, // embedded enabled
        TEIID50039, // socket_disabled
        TEIID50040, // odbc_disabled
        TEIID50041, // embedded disabled
        TEIID50043,
        TEIID50044, // vdb save failed
        TEIID50047,
        TEIID50048,
        TEIID50049,
        TEIID50054,
        TEIID50055,
        TEIID50056,
        TEIID50057,
        TEIID50069,
        TEIID50070,
        TEIID50071,
        TEIID50072,
        TEIID50074,
        TEIID50075,
        TEIID50077,
        TEIID50088,
        TEIID50089,
        TEIID50091, // rest different # of param count
        TEIID50092, // rest procedure execution
        TEIID50093,
        TEIID50094,
        TEIID50095,
        TEIID50096,
        TEIID50097,
        TEIID50098,
        TEIID50099,
        TEIID50100,
        TEIID50101,
        TEIID50102,
        TEIID50103,
        TEIID50105,
        TEIID50106,
        TEIID50107,
        TEIID50108,
        TEIID50109,
        TEIID50110,
        TEIID50111,
        TEIID50112,
        TEIID50114,
        TEIID50115,
        TEIID50116
    }
}
