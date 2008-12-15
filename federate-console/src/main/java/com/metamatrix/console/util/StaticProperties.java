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

package com.metamatrix.console.util;

import java.awt.Cursor;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.core.util.BuildVersion;
import com.metamatrix.toolbox.preference.UserPreferences;

/**
 *
 */
public class StaticProperties {

    private static final String DEFAULT_FILENAME = "preferences.prop"; //$NON-NLS-1$
    private static final String BLANK = ""; //$NON-NLS-1$
    public static final String DEFAULT_USERNAME = "default.username"; //$NON-NLS-1$
    private static final String ERR_LOG_FILE_PROP_NAME = "metamatrix.stderr.file"; //$NON-NLS-1$
    private static final String OUT_LOG_FILE_PROP_NAME = "metamatrix.stdout.file"; //$NON-NLS-1$

    public static final String TRUE = "true"; //$NON-NLS-1$
    public static final String FALSE = "false"; //$NON-NLS-1$

    // these references are passed to the logon dialog for displayed
    private static java.util.List /*<String>*/ urls = null;

    // this references is maintained as soon as the url is selected on
    // the logon dialog.
    private static String currentSystemURL;

    /**Directory to which the console logs*/
    private static File logDirectory = null;

    
    
    // variables used to construct the property file
    // Original 'refresh' vars, now used as IDs of AutoRefreshable panels:
    public static final String DATA_SESSION = "data.session"; //$NON-NLS-1$
    public static final String DATA_SUMMARY = "data.summary"; //$NON-NLS-1$
    public static final String DATA_QUERY = "data.query"; //$NON-NLS-1$
    public static final String DATA_TRANSACTION = "data.transaction"; //$NON-NLS-1$
    public static final String DATA_SYSLOG = "data.syslog"; //$NON-NLS-1$
    public static final String DATA_RESOURCE_POOLS = "data.resource_pools"; //$NON-NLS-1$

    

    // New 'refresh' vars:
	public static final String REFRESH_ENABLED_SESSION = "refresh_enabled.session"; //$NON-NLS-1$
	public static final String REFRESH_RATE_SESSION = "refresh_rate.session"; //$NON-NLS-1$
	public static final String REFRESH_ENABLED_QUERY = "refresh_enabled.query"; //$NON-NLS-1$
	public static final String REFRESH_RATE_QUERY = "refresh_rate.query"; //$NON-NLS-1$
	public static final String REFRESH_ENABLED_SUMMARY = "refresh_enabled.summary"; //$NON-NLS-1$
	public static final String REFRESH_RATE_SUMMARY = "refresh_rate.summary"; //$NON-NLS-1$
	public static final String REFRESH_ENABLED_TRANSACTION = "refresh_enabled.transaction"; //$NON-NLS-1$
	public static final String REFRESH_RATE_TRANSACTION = "refresh_rate.transaction"; //$NON-NLS-1$
	public static final String REFRESH_ENABLED_SYSLOG = "refresh_enabled.syslog"; //$NON-NLS-1$
	public static final String REFRESH_RATE_SYSLOG = "refresh_rate.syslog"; //$NON-NLS-1$
	public static final String REFRESH_ENABLED_RESOURCE_POOLS = "refresh_enabled.resource_pools"; //$NON-NLS-1$
	public static final String REFRESH_RATE_RESOURCE_POOLS = "refresh_rate.resource_pools"; //$NON-NLS-1$

    public static final String CONN_URL = "connection.url.name."; //$NON-NLS-1$
    public static final String CONN_DEFAULT = "connection_default"; //$NON-NLS-1$
    public static final String CONN_USE_LAST_URL = "connection.use_last_url_as_default"; //$NON-NLS-1$
    public static final String DATEFORMAT = "dateformat"; //$NON-NLS-1$
    public static final String TIMEFORMAT = "timeformat"; //$NON-NLS-1$
    
    public static final String UDDI_REGISTRY_NAME = "uddi.registry.name."; //$NON-NLS-1$
    public static final int UDDI_REGISTRY_NAME_STR_LEN = UDDI_REGISTRY_NAME.length();
    public static final String UDDI_REGISTRY_USER = "uddi.registry.user."; //$NON-NLS-1$
    public static final int UDDI_REGISTRY_USER_STR_LEN = UDDI_REGISTRY_USER.length();
    public static final String UDDI_REGISTRY_HOST = "uddi.registry.host."; //$NON-NLS-1$
    public static final int UDDI_REGISTRY_HOST_STR_LEN = UDDI_REGISTRY_HOST.length();
    public static final String UDDI_REGISTRY_PORT = "uddi.registry.port."; //$NON-NLS-1$
    public static final int UDDI_REGISTRY_PORT_STR_LEN = UDDI_REGISTRY_PORT.length();

    // supported date formats
    public static final String MONDDCCYY = "MMM dd, yyyy"; //$NON-NLS-1$
    public static final String DDMONCCYY = "dd MMM yyyy"; //$NON-NLS-1$
    public static final String MMDDYY = "MM/dd/yy"; //$NON-NLS-1$
    public static final String DDMMYY = "dd/MM/yy"; //$NON-NLS-1$
    public static final String MM_DD_YY = "MM.dd.yy"; //$NON-NLS-1$
    public static final String DD_MM_YY = "dd.MM.yy"; //$NON-NLS-1$

    // supported time formats
    public static final String HHMMSS = "hh:mm:ss a"; //$NON-NLS-1$
    public static final String MIL_HHMMSS = "kk:mm:ss"; //$NON-NLS-1$
    public static final String GENERIC_LOG = "CONSOLE"; //$NON-NLS-1$

	public static final Cursor CURSOR_WAIT = Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR);
	public static final Cursor CURSOR_DEFAULT = Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR);

    public static final int MIN_REFRESH_RATE = 15;
    public static final int MAX_REFRESH_RATE = 3600;

    public static boolean summaryEnabled = false;
    public static int summaryRefreshRate = 0;
    public static boolean sessionEnabled = false;
    public static int sessionRefreshRate = 0;
    protected static boolean queryEnabled = false;
	protected static int queryRefreshRate = 0;
	protected static boolean transactionEnabled = false;
	protected static int transactionRefreshRate = 0;
	protected static boolean syslogEnabled = false;
    public static int syslogRefreshRate = 0;
    public static boolean resourcePoolsEnabled = false;
    public static int resourcePoolsRefreshRate = 0;
    private static String newDefaultURL = null;
    private static boolean useLastURLAsDefault = true;
    private static int numUDDIRegistries = -1;

    static BuildVersion build = new BuildVersion();
    public static String getVersions() {
        return build.getReleaseNumber();
    }

    public static String getBuild() {
        return build.getBuildNumber();
    }

    public StaticProperties() {
        super();
    }

    public static String getDefaultURL() {
        return getProperty(CONN_DEFAULT);
    }
    
    public static void setURLs(java.util.List /*<String>*/ theURLs,
            String defaultURL, boolean useLastLogin) {
        StaticProperties.setURLs(theURLs);
        newDefaultURL = defaultURL;
        useLastURLAsDefault = useLastLogin;
    }

    public static void setURLs(java.util.List /*<String>*/ theURLs) {
       
        //unset the old URL properties
        Properties properties = getProperties();
        java.util.List connectionKeys = new ArrayList();
        Iterator iter = properties.keySet().iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            if (key.startsWith(StaticProperties.CONN_URL)) {
                connectionKeys.add(key);
            }            
        }
        iter = connectionKeys.iterator();
        while (iter.hasNext()) {
            String key = (String)iter.next();
            UserPreferences.getInstance().removeValue(key);
        }

        
        //set the new URL properties        
        urls = theURLs;        
        StringBuffer sb;
        Iterator it = urls.iterator();
        for (int i = 1; it.hasNext(); i++) {
            String url = (String)it.next();
            sb = new StringBuffer(StaticProperties.CONN_URL);
            sb.append(i);
            setProperty(sb.toString(), url);
        }        
        sortURLs();
    }

    public static void setCurrentURL(String systemURL){
        currentSystemURL = systemURL;

        if (getUseLastURLAsDefault()) {
            setDefaultURL(systemURL);
        }
    }

    public static String getCurrentURL(){
        return currentSystemURL ;
    }

    public static java.util.List /*<String>*/ getURLsCopy() {
        if (urls == null) {
            urls = new ArrayList(0);
        }
        ArrayList copy = new ArrayList(urls);
        return copy;
    }

    public static SavedUDDIRegistryInfo[] getUDDIRegistryInfo() {
        Map /*<Integer (index stored with property) to SavedUDDIRegistryInfo>*/ infoMap = new HashMap();
        Properties properties = getProperties();
        Iterator it = properties.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry me = (Map.Entry)it.next();
            String key = (String)me.getKey();
            if (key.startsWith(UDDI_REGISTRY_NAME)) {
                String remainder = key.substring(UDDI_REGISTRY_NAME_STR_LEN);
                Integer index = null;
                try {
                    index = new Integer(remainder);
                } catch (Exception ex) {
                }
                if (index != null) {
                    String name = (String)me.getValue();
                    SavedUDDIRegistryInfo info = (SavedUDDIRegistryInfo)infoMap.get(index);
                    if (info == null) {
                        info = new SavedUDDIRegistryInfo(name, null, null, null);
                        infoMap.put(index, info);
                    } else {
                        info.setName(name);
                    }
                }
            } else if (key.startsWith(UDDI_REGISTRY_USER)) {
                String remainder = key.substring(UDDI_REGISTRY_USER_STR_LEN);
                Integer index = null;
                try {
                    index = new Integer(remainder);
                } catch (Exception ex) {
                }
                if (index != null) {
                    String user = (String)me.getValue();
                    SavedUDDIRegistryInfo info = (SavedUDDIRegistryInfo)infoMap.get(index);
                    if (info == null) {
                        info = new SavedUDDIRegistryInfo(null, user, null, null);
                        infoMap.put(index, info);
                    } else {
                        info.setUserName(user);
                    }
                } 
            } else if (key.startsWith(UDDI_REGISTRY_HOST)) {
                String remainder = key.substring(UDDI_REGISTRY_HOST_STR_LEN);
                Integer index = null;
                try {
                    index = new Integer(remainder);
                } catch (Exception ex) {
                }
                if (index != null) {
                    String host = (String)me.getValue();
                    SavedUDDIRegistryInfo info = (SavedUDDIRegistryInfo)infoMap.get(index);
                    if (info == null) {
                        info = new SavedUDDIRegistryInfo(null, null, host, null);
                        infoMap.put(index, info);
                    } else {
                        info.setHost(host);
                    }
                }
            } else if (key.startsWith(UDDI_REGISTRY_PORT)) {
                String remainder = key.substring(UDDI_REGISTRY_PORT_STR_LEN);
                Integer index = null;
                try {
                    index = new Integer(remainder);
                } catch (Exception ex) {
                }
                if (index != null) {
                    String port = (String)me.getValue();
                    SavedUDDIRegistryInfo info = (SavedUDDIRegistryInfo)infoMap.get(index);
                    if (info == null) {
                        info = new SavedUDDIRegistryInfo(null, null, null, port);
                        infoMap.put(index, info);
                    } else {
                        info.setPort(port);
                    }
                }
            }
        }
        Map revisedInfoMap = new HashMap();
        it = infoMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry me = (Map.Entry)it.next();
            SavedUDDIRegistryInfo info = (SavedUDDIRegistryInfo)me.getValue();
            if ((info.getUserName() != null) && (info.getHost() != null) && (info.getPort() != null)) {
                revisedInfoMap.put(me.getKey(), info);
            }
        }
        SavedUDDIRegistryInfo[] result = new SavedUDDIRegistryInfo[revisedInfoMap.size()];
        int[] indices = new int[result.length];
        it = revisedInfoMap.entrySet().iterator();
        for (int i = 0; it.hasNext(); i++) {
            Map.Entry me = (Map.Entry)it.next();
            Integer intKey = (Integer)me.getKey();
            indices[i] = intKey.intValue();
            result[i] = (SavedUDDIRegistryInfo)me.getValue();
        }
        //Do bubble sort to put items in ascending order by index value
        boolean done = false;
        while (!done) {
            done = true;
            for (int i = 0; i < indices.length - 1; i++) {
                if (indices[i] > indices[i + 1]) {
                    done = false;
                    int tempInt = indices[i];
                    indices[i] = indices[i + 1];
                    indices[i + 1] = tempInt;
                    SavedUDDIRegistryInfo tempInfo = result[i];
                    result[i] = result[i + 1];
                    result[i + 1] = tempInfo;
                }
            }
        }
        if (StaticProperties.numUDDIRegistries < 0) {
            StaticProperties.numUDDIRegistries = result.length;
        }
        return result;
    }
    
    public static void setUDDIRegistryInfo(SavedUDDIRegistryInfo[] info) {
        //First clean out the old
        Properties properties = getProperties();
        //To avoid ConcurrentModificationException, copy the keys to a separate list and
        //iterate through that list.
        java.util.List tempList = new ArrayList(properties.size());
        Iterator it = properties.keySet().iterator();
        while (it.hasNext()) {
            String key = (String)it.next();
            tempList.add(key);
        }
        it = tempList.iterator();
        while (it.hasNext()) {
            String key = (String)it.next();
            if (key.startsWith(UDDI_REGISTRY_NAME) || key.startsWith(UDDI_REGISTRY_USER) 
                    || key.startsWith(UDDI_REGISTRY_HOST) || key.startsWith(UDDI_REGISTRY_PORT)) {
                UserPreferences.getInstance().removeValue(key);
            }
        }
        //Then add the new
        for (int i = 0; i < info.length; i++) {
            int index = i + 1;
            String key = UDDI_REGISTRY_NAME + index;
            setProperty(key, info[i].getName());
            key = UDDI_REGISTRY_USER + index;
            setProperty(key, info[i].getUserName());
            key = UDDI_REGISTRY_HOST + index;
            setProperty(key, info[i].getHost());
            key = UDDI_REGISTRY_PORT + index;
            setProperty(key, info[i].getPort());
        }
    }
    
    public static Properties getProperties() {
        return UserPreferences.getInstance().getProperties();
    }

    public static void loadBootStrap() throws ExternalException {
        if (System.getProperty(OUT_LOG_FILE_PROP_NAME) != null) {
            File f = new File(System.getProperty(OUT_LOG_FILE_PROP_NAME));
            try {
                f.createNewFile();
                PrintStream stream = new PrintStream(new FileOutputStream(f));
                System.setOut(stream);
            } catch (IOException e) {
                String msg = "Invalid value for property " //$NON-NLS-1$
                           + OUT_LOG_FILE_PROP_NAME
                           + " = " + System.getProperty(OUT_LOG_FILE_PROP_NAME); //$NON-NLS-1$

                LogManager.logError(LogContexts.CONFIG, e, msg);
            }
        }

        if (System.getProperty(ERR_LOG_FILE_PROP_NAME) != null) {
            File f = new File(System.getProperty(ERR_LOG_FILE_PROP_NAME));
            try {
                f.createNewFile();
                PrintStream stream = new PrintStream(new FileOutputStream(f));
                System.setErr(stream);
            } catch (IOException e) {
                String msg = "Invalid value for property " //$NON-NLS-1$
                             + ERR_LOG_FILE_PROP_NAME
                             + " = " //$NON-NLS-1$
                             + System.getProperty(ERR_LOG_FILE_PROP_NAME);

                LogManager.logError(LogContexts.CONFIG, e, msg);
            }
        }

        if (System.getProperty("prefFile") != null) { //$NON-NLS-1$
            loadBootStrap(System.getProperty("prefFile")); //$NON-NLS-1$
        } else {
            loadBootStrap(DEFAULT_FILENAME);
        }
    }

    /**
     * Load user preferences file.
     */
    private static final String SIXTY = "60"; //$NON-NLS-1$
    public static void loadBootStrap(String fileName) throws ExternalException {
        
        // trigger the loading of preferences.
        Properties properties = getProperties();
//        file = new File(fileName);

        // create the properties object
 //       properties = new Properties();

//        // if the file exists, load its contents in the the properties object
//        if (file.exists()) {
//             try {
//                FileInputStream fis = new FileInputStream(file.getPath());
//                properties.load(fis);
//            } catch (Exception e) {
//                throw new ExternalException(e);
//            }
//            // this process is only done at load time.  The references are maintained
//            // via the url array - "urls" between the PreferencesDialog and LogonPanel
//            setupURLS();
//        }
        setupURLS(properties);
        // set defaults for any props that are not present in the file
        if (!properties.contains(DATEFORMAT)) {
            setProperty(DATEFORMAT, MONDDCCYY);
        }
        if (!properties.contains(TIMEFORMAT)) {
            setProperty(TIMEFORMAT, HHMMSS);
        }

        if (properties.getProperty(REFRESH_RATE_SESSION) == null) {
            setProperty(REFRESH_RATE_SESSION, SIXTY);
        }

        if (properties.getProperty(REFRESH_RATE_QUERY) == null) {
            setProperty(REFRESH_RATE_QUERY, SIXTY);            
        }
        if (properties.getProperty(REFRESH_RATE_SUMMARY) == null) {
            setProperty(REFRESH_RATE_SUMMARY, SIXTY);
        }
        if (properties.getProperty(REFRESH_RATE_TRANSACTION) == null) {
            setProperty(REFRESH_RATE_TRANSACTION, SIXTY);
        }
        if (properties.getProperty(REFRESH_RATE_SYSLOG) == null) {
            setProperty(REFRESH_RATE_SYSLOG, SIXTY);
        }
        if (properties.getProperty(REFRESH_RATE_RESOURCE_POOLS) == null) {
            setProperty(REFRESH_RATE_RESOURCE_POOLS, SIXTY);
        }

        if (properties.getProperty(REFRESH_ENABLED_SESSION) == null) {
            setProperty(REFRESH_ENABLED_SESSION, SIXTY);
        }

        if (properties.getProperty(REFRESH_ENABLED_QUERY) == null) {
            setProperty(REFRESH_ENABLED_QUERY, SIXTY);
        }
        if (properties.getProperty(REFRESH_ENABLED_SUMMARY) == null) {
            setProperty(REFRESH_ENABLED_SUMMARY, SIXTY);
        }
        if (properties.getProperty(REFRESH_ENABLED_TRANSACTION) == null) {
            setProperty(REFRESH_ENABLED_TRANSACTION, SIXTY);
        }
        if (properties.getProperty(REFRESH_ENABLED_SYSLOG) == null) {
            setProperty(REFRESH_ENABLED_SYSLOG, SIXTY);
        }
        if (properties.getProperty(REFRESH_ENABLED_RESOURCE_POOLS) == null) {
            setProperty(REFRESH_ENABLED_RESOURCE_POOLS, SIXTY);
        }
        
//        Properties props = UserPreferences.getInstance().getProperties();
//        properties = PropertiesUtils.clone(props, false);
        
    }

    private static void setupURLS(Properties properties) {
        // this process is only done at load time.  The references are maintained
        // via the url array - "urls" between the PreferencesDialog and LogonPanel

        urls = new Vector(9);
        String key;
        String url;

        Iterator it = properties.keySet().iterator();
        while (it.hasNext()) {
            key = (String) it.next();
            if (key.startsWith(CONN_URL))  {
                url = properties.getProperty(key);
                urls.add(url);
            }
        }
        sortURLs();
        newDefaultURL = properties.getProperty(CONN_DEFAULT);
        String boolString = properties.getProperty(CONN_USE_LAST_URL);
        if (boolString != null) {
            setUseLastURLAsDefault(Boolean.valueOf(boolString).booleanValue());
        } else if ((newDefaultURL == null) || (newDefaultURL.length() == 0)) {
            //In the case where both the CONN_DEFAULT and CONN_USE_LAST_URL
            //properties are absent, which will be the initial condition, we
            //we set the useLastURLAsDefault flag to true.  In other words,
            //always using the last login URL as the default will be the
            //starting condition on a new installation.
            setUseLastURLAsDefault(true);
        }
    }

    public static void setUseLastURLAsDefault(boolean flag) {
        useLastURLAsDefault = flag;
    }

    public static boolean getUseLastURLAsDefault() {
        return useLastURLAsDefault;
    }

    private static void sortURLs() {
        Collection /*<String>*/ sortedURLs =
                StaticQuickSorter.quickStringCollectionSort(urls);
        urls = new ArrayList(sortedURLs);
    }

    /**
     *
     */
    public static void loadProperties() {
        applyProperties(getProperties());
    }

    public static void applyRefreshProperties(Properties props) {
        sessionEnabled      = getEnabledState(REFRESH_ENABLED_SESSION, props);
        sessionRefreshRate  = getIntegerValue(REFRESH_RATE_SESSION, props);

        summaryEnabled      = getEnabledState(REFRESH_ENABLED_SUMMARY, props);
        summaryRefreshRate  = getIntegerValue(REFRESH_RATE_SUMMARY, props);

        queryEnabled        = getEnabledState(REFRESH_ENABLED_QUERY, props);
        queryRefreshRate    = getIntegerValue(REFRESH_RATE_QUERY, props);

        transactionEnabled      = getEnabledState(REFRESH_ENABLED_TRANSACTION, props);
        transactionRefreshRate  = getIntegerValue(REFRESH_RATE_TRANSACTION, props);

        syslogEnabled       = getEnabledState(REFRESH_ENABLED_SYSLOG, props);
        syslogRefreshRate   = getIntegerValue(REFRESH_RATE_SYSLOG, props);
        
        resourcePoolsEnabled = getEnabledState(REFRESH_ENABLED_RESOURCE_POOLS,
        		props);
        resourcePoolsRefreshRate = getIntegerValue(REFRESH_RATE_RESOURCE_POOLS,
        		props);
    }

    private static boolean getEnabledState(String sPropertyKey, Properties props) {
        boolean bEnabledState           = false;
        String sValue   = props.getProperty(sPropertyKey);
        if (sValue != null && (!sValue.trim().equals(""))) //$NON-NLS-1$
        {
            if (sValue.equals(TRUE))
                bEnabledState = true;
            else
            if (sValue.equals(FALSE))
                bEnabledState = false;
        }

        return bEnabledState;
    }

    private static int getIntegerValue(String sPropertyKey, Properties props) {
        int iValue             = 0;
        String sValue   = props.getProperty(sPropertyKey);
        if (sValue != null && (!sValue.trim().equals(""))) { //$NON-NLS-1$
            iValue = new Integer(sValue).intValue();
        }
        return iValue;
    }

    public static void setDefaultURL(String url) {
        newDefaultURL = url;
        setProperty(CONN_DEFAULT,url);
    }

    public static boolean isDefaultURL(String url) {
        return url.equalsIgnoreCase(newDefaultURL);
    }

    /**
     *
     */
    public static void applyProperties(Properties prop) {
        applyRefreshProperties(prop);
        
        boolean unmodifiable = (prop instanceof UnmodifiableProperties);
        StringBuffer sbDT= new StringBuffer();
        if (prop.getProperty(DATEFORMAT) != null &&
            !prop.getProperty(DATEFORMAT).equals(BLANK)) {
            sbDT.append(prop.getProperty(DATEFORMAT));
        } else {            
            if (! unmodifiable) {
                prop.setProperty(DATEFORMAT, MONDDCCYY);
            }
            sbDT.append(MONDDCCYY);
        }

        sbDT.append(" "); //$NON-NLS-1$
        if (prop.getProperty(TIMEFORMAT)!= null &&
            !prop.getProperty(TIMEFORMAT).equals(BLANK)) {
             sbDT.append(prop.getProperty(TIMEFORMAT));
        } else {
            if (! unmodifiable) {
                prop.setProperty(DATEFORMAT, MONDDCCYY);
            }
            sbDT.append(HHMMSS);
        }

        StaticUtilities.setDateFormat(sbDT.toString());
    }

    public static void setProperty(String key, String value) {
        UserPreferences.getInstance().setValue(key, value);
    }

    public static String getProperty(String key) {
        return UserPreferences.getInstance().getProperties().getProperty(key);
    }


    public static void setUserName(String sUserName) {
        setProperty(DEFAULT_USERNAME, sUserName);
    }

    /**
     * Save the properties to the UserPreferences.
     */
    public static void saveProperties() throws ExternalException {
        boolean useLastURL = getUseLastURLAsDefault();
        if (useLastURL) {
            setProperty(CONN_DEFAULT, getCurrentURL());
        } else {
            setProperty(CONN_DEFAULT, newDefaultURL);
        }
        
        setProperty(CONN_USE_LAST_URL, new Boolean(useLastURL).toString());

        try {
            UserPreferences.getInstance().saveChanges();

            loadProperties();

        } catch (Exception e) {
            throw new ExternalException("Error Saving Preference File", e); //$NON-NLS-1$
        }
    }

    public static boolean getSummaryRefreshEnabled() {
        return summaryEnabled;
    }

    public static int getSummaryRefreshRate() {
        return summaryRefreshRate;
    }

    public static boolean getSessionRefreshEnabled() {
        return sessionEnabled;
    }

    public static int getSessionRefreshRate() {
        return sessionRefreshRate;
    }

    public static boolean getQueryRefreshEnabled() {
        return queryEnabled;
    }

    public static int getQueryRefreshRate() {
        return queryRefreshRate;
    }

    public static boolean getTransactionRefreshEnabled() {
        return transactionEnabled;
    }

    public static int getTransactionRefreshRate() {
        return transactionRefreshRate;
    }

    public static boolean getSysLogRefreshEnabled() {
        return syslogEnabled;
    }

    public static int getSysLogRefreshRate() {
        return syslogRefreshRate;
    }
    
    public static boolean getResourcePoolsRefreshEnabled() {
        return resourcePoolsEnabled;
    }
    
    public static int getResourcePoolsRefreshRate() {
        return resourcePoolsRefreshRate;
    }
    
    
    public static void setLogDirectory(File file) {
        logDirectory = file;
    }
    public static File getLogDirectory() {
        return logDirectory;
    }
}