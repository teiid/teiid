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
package org.teiid.olingo;

import java.util.Locale;
import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class ODataPlugin {
    private static final String PLUGIN_ID = "org.teiid.olingo" ; //$NON-NLS-1$
    static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,BUNDLE_NAME,ResourceBundle.getBundle(BUNDLE_NAME));

    public static ResourceBundle getResourceBundle(Locale locale) {
        if (locale == null) {
            locale = Locale.getDefault();
        }
        return ResourceBundle.getBundle(ODataPlugin.BUNDLE_NAME, locale);
    }

    public static enum Event implements BundleUtil.Event {
        TEIID16001,
        TEIID16003,
        TEIID16004,
        TEIID16005,
        TEIID16006,
        TEIID16007,
        TEIID16008,
        TEIID16009,
        TEIID16010,
        TEIID16011,
        TEIID16012,
        TEIID16013,
        TEIID16014,
        TEIID16015,
        TEIID16016,
        TEIID16017,
        TEIID16018,
        TEIID16019,
        TEIID16020,
        TEIID16021,
        TEIID16022,
        TEIID16023,
        TEIID16024,
        TEIID16025,
        TEIID16026,
        TEIID16027,
        TEIID16028,
        TEIID16029,
        TEIID16030,
        TEIID16031,
        TEIID16032,
        TEIID16033,
        TEIID16035,
        TEIID16036,
        TEIID16037,
        TEIID16038,
        TEIID16039,
        TEIID16040,
        TEIID16043,
        TEIID16044,
        TEIID16045,
        TEIID16046,
        TEIID16047,
        TEIID16048,
        TEIID16049,
        TEIID16050,
        TEIID16051,
        TEIID16052,
        TEIID16053,
        TEIID16054,
        TEIID16055,
        TEIID16056,
        TEIID16057,
        TEIID16058,
        TEIID16059,
        TEIID16060,
        TEIID16062,
        TEIID16063,
    }
}
