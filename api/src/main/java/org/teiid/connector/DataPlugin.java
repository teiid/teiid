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

package org.teiid.connector;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class DataPlugin {

    public static final String PLUGIN_ID = DataPlugin.class.getPackage().getName();

    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
                                                         PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$

    public static enum Event implements BundleUtil.Event {
        TEIID60000,
        TEIID60001,
        TEIID60004,
        TEIID60005,
        TEIID60008,
        TEIID60009,
        TEIID60010,
        TEIID60011,
        TEIID60012,
        TEIID60013,
        TEIID60014,
        TEIID60015,
        TEIID60016,
        TEIID60017,
        TEIID60018,
        TEIID60019,

        TEIID60021,
        TEIID60022,
        TEIID60023,
        TEIID60024,

        TEIID60026,
        TEIID60027,
        TEIID60028,
        TEIID60029,
        TEIID60030,
        TEIID60032,
        TEIID60033,
        TEIID60034,
        TEIID60035,
        TEIID60036,
        TEIID60037,
        TEIID60038,
        TEIID60039,
        TEIID60040,
        TEIID60041,
        TEIID60042
    }
}
