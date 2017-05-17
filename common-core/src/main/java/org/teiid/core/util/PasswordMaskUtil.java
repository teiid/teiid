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

package org.teiid.core.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * <p>This is a simple utility class to help keep password values from being
 * printed out, primarily from java.util.Properties object, into
 * MetaMatrix log files.  This is <i>not</i> a robust security solution
 * for protecting passwords, and is only good insofar as we get all
 * other code to use this when necessary.  When passwords are properly
 * encrypted in the database, this class will be no longer necessary.</p>
 *
 * <p>This class can be used in one of two ways.  First, a Properties object
 * can be wrapped in an instance of this class - the properties object,
 * wrapped in this instance, can then be safely sent to LogManager to be
 * logged, or can otherwise be printed out.  This instance will basically
 * duplicate the toString() method of a normal Properties object, but will
 * obfuscate any property values whose names end in any of the well-known
 * {@link #PASSWORD_PROP_SUFFIXES password property suffixes}.</p>
 *
 * <p>The second way is to just use this class's static
 * {@link #doesNameEndWithPasswordSuffix doesNameEndWithPasswordSuffix}
 * utility method by hand, passing in a String property name to
 * see if it ends with any of the well-known
 * {@link #PASSWORD_PROP_SUFFIXES password property suffixes}.</p>
 */
public final class PasswordMaskUtil {

    /**
     * The known String suffixes that MetaMatrix property names end in.
     * Each of these is checked.
     */
    public static final String[] PASSWORD_PROP_SUFFIXES = new String[]{"password", "Password"}; //$NON-NLS-1$ //$NON-NLS-2$

    /**
     * The "mask" String that is printed out of the
     * {@link #toString} method, instead of the actual password value.
     */
    public static final String MASK_STRING = "*****"; //$NON-NLS-1$

    private Properties properties;

    /**
     * A Properties object can be wrapped by this class before, say,
     * being sent to LogManager.  If this {@link #toString} method
     * is called, password properties will be printed out as
     * a {@link #MASK_STRING masked string}.
     * @param propertiesWithPassword Properties object that has
     * password property values inside it
     */
    public PasswordMaskUtil(Properties propertiesWithPassword){
        this.properties = propertiesWithPassword;
    }

    /**
     * This toString() method is basically the same as the
     * java.util.Properties class's toString() method, except that
     * if any of the properties in this class end with any of the
     * well-known {@link #PASSWORD_PROP_SUFFIXES password suffixes},
     * then the value will
     * be printed out as {@link #MASK_STRING}
     */
    public String toString(){
        int max = properties.size() - 1;
        StringBuffer buf = new StringBuffer();
        Iterator it = properties.entrySet().iterator();

        buf.append("{"); //$NON-NLS-1$
        String key = null;
        for (int i = 0; i <= max; i++) {
            Map.Entry e = (Map.Entry) (it.next());
            key = (String)e.getKey();
            if (doesNameEndWithPasswordSuffix(key)){
                buf.append(key + "=" + MASK_STRING); //$NON-NLS-1$
            } else {
                buf.append(key + "=" + e.getValue()); //$NON-NLS-1$
            }
            if (i < max)
            buf.append(", "); //$NON-NLS-1$
        }
        buf.append("}"); //$NON-NLS-1$
        return buf.toString();
    }

    /**
     * Indicates if the String propName ends in any of the well-known
     * {@link #PASSWORD_PROP_SUFFIXES password property suffixes}.</p>
     */
    public static boolean doesNameEndWithPasswordSuffix(String propName){
        for (int i=0; i<PASSWORD_PROP_SUFFIXES.length; i++){
            if (propName.endsWith(PASSWORD_PROP_SUFFIXES[i])){
                return true;
            }
        }
        return false;
    }

}
