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
package org.teiid.translator.mongodb;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class MongoDBPlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.mongodb" ; //$NON-NLS-1$

    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,BUNDLE_NAME,ResourceBundle.getBundle(BUNDLE_NAME));

    public static enum Event implements BundleUtil.Event{
        TEIID18001,
        TEIID18002,
        TEIID18003,
        TEIID18004,
        TEIID18005,
        TEIID18006,
        TEIID18007,
        TEIID18008,
        TEIID18009,
        TEIID18010,
        TEIID18011,
        TEIID18012,
        TEIID18013,
        TEIID18014,
        TEIID18015,
        TEIID18016,
        TEIID18017,
        TEIID18018,
        TEIID18019,
        TEIID18020,
        TEIID18021,
        TEIID18022,
        TEIID18023,
        TEIID18024,
        TEIID18025,
        TEIID18026,
        TEIID18027,
        TEIID18028,
        TEIID18029,
        TEIID18030,
        TEIID18031,
        TEIID18032,
        TEIID18033,
        TEIID18034,
        TEIID18035,
        TEIID18036,
        TEIID18037
    }
}
