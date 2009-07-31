/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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
package org.teiid.rhq.plugin;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.event.EventSeverity;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.event.EventContext;
import org.rhq.core.pluginapi.event.EventPoller;
import org.rhq.core.pluginapi.event.log.LogFileEventPoller;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Process;
import org.teiid.rhq.plugin.log.JBEDSPErrorLogEntryProcessor;


/**
 * 
 * MetaMatrix server component class. This class represents the node for the
 * MMProcess.
 * 
 */
public class ProcessComponent extends Facet {
	private final Log LOG = LogFactory.getLog(ProcessComponent.class);
    
    
    public static final String PLUGIN_CONFIG_PROP_ERROR_LOG_EVENTS_ENABLED = "enabled"; //$NON-NLS-1$
    public static final String PLUGIN_CONFIG_PROP_ERROR_LOG_MINIMUM_SEVERITY = "minimumSeverity"; //$NON-NLS-1$
    public static final String PLUGIN_CONFIG_PROP_ERROR_LOG_INCLUDES_PATTERN = "errorLogIncludesPattern"; //$NON-NLS-1$
    public static final String PLUGIN_CONFIG_PROP_ERROR_LOG_FILE_PATH = "errorLogFilePath"; //$NON-NLS-1$
	public static final String INSTALL_DIR = "install.dir"; //$NON-NLS-1$
   
    private static final String ERROR_LOG_ENTRY_EVENT_TYPE = "errorLogEntry"; //$NON-NLS-1$
   
    
    
    private EventContext eventContext;   
    private File errorLogFile;

    
    /** 
     * @see org.teiid.rhq.plugin.Facet#start(org.rhq.core.pluginapi.inventory.ResourceContext)
     */
    @Override
    public void start(ResourceContext context) {
        super.start(context);
        
        this.eventContext = resourceContext.getEventContext();
       
 //       startEventPollers();
    }
    
    /** 
     * @see org.teiid.rhq.plugin.Facet#stop()
     */
    @Override
    public void stop() {
        stopEventPollers();
        super.stop();
            
    }   
    
	public AvailabilityType getAvailability() {

		return AvailabilityType.UP;
		}
    
    /** 
     * @see org.teiid.rhq.plugin.Facet#getComponentType()
     * @since 1.0
     */
    @Override
    String getComponentType() {
        return Process.TYPE;
    }
    
	protected void setOperationArguments(String name, Configuration configuration,
			Map argumentMap) {

		if (name.equals(ConnectionConstants.ComponentType.Operation.GET_PROPERTIES)){
			String key = ConnectionConstants.IDENTIFIER;
			argumentMap.put(key, getComponentIdentifier());
		}
 		
	}   
    

	@Override
	public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> requests) throws Exception {
		 Connection conn = null;
		    Map valueMap = new HashMap();
		    
		    try{ 
		        conn = getConnection();
                if (!conn.isValid()) {
                    return;
                }                
		        for (MeasurementScheduleRequest request : requests) {
		            String name = request.getName();
		            LOG.info("Measurement name = " + name);  //$NON-NLS-1$
		            
		            Object metricReturnObject = conn.getMetric(getComponentType(), this.getComponentIdentifier(), name, valueMap);

		            try {
		                if (request.getName().equals(ComponentType.Metric.HIGH_WATER_MARK)) {
		                    report.addData(new MeasurementDataNumeric(request,
		                            (Double)metricReturnObject));
		                }
		            } catch (Exception e) {
		                LOG.error("Failed to obtain measurement [" + name  //$NON-NLS-1$
		                        + "]. Cause: " + e); //$NON-NLS-1$
		                throw(e);
		            }
		        }
		    }finally{
		        conn.close();
		    }
		
	}
	
	
	/**
	 * The plugin container will call this method and it needs to obtain the
	 * current configuration of the managed resource. Your plugin will obtain
	 * the managed resource's configuration in your own custom way and populate
	 * the returned Configuration object with the managed resource's
	 * configuration property values.
	 * 
	 * @see ConfigurationFacet#loadResourceConfiguration()
	 * 
	 */
	@Override
	public Configuration loadResourceConfiguration() {
		// here we simulate the loading of the managed resource's configuration
		Configuration config = this.getResourceConfiguration() ;
		if (config == null) {
			// for this example, we will create a simple dummy configuration to
			// start with.
			// note that it is empty, so we're assuming there are no required
			// configs in the plugin descriptor.
			config = new Configuration();
		}

   		Properties props;
		try {
			props = getConnection().getProperties(this.getComponentType(), this.getComponentIdentifier());
		} catch (ConnectionException e) {
			LOG.error("Failed to obtain process properties for [" + this.getComponentIdentifier() //$NON-NLS-1$
					+ "]. Cause: " + e); //$NON-NLS-1$
			 throw new InvalidPluginConfigurationException(e); 
		}
        
   		if (props != null && props.size() > 0) {
   			Iterator it=props.keySet().iterator();
   			while(it.hasNext())  {
   				String k = (String)it.next();
   				
   				config.put(new PropertySimple(k, props.get(k)));

   			}
   			
   		}

				
		this.setResourceConfiguration(config);
		return this.getResourceConfiguration();
	}	

    
	protected static String deriveFileName(final String identifier) {
		
		 String startFileName = identifier.substring(0, identifier.indexOf("|")); //$NON-NLS-1$
         String endFileName = identifier.substring(identifier.indexOf("|")+1, identifier.length()); //$NON-NLS-1$
         
         startFileName = replaceAll(startFileName, ".", "_"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
         String logfilename = startFileName.toLowerCase() + "_" + endFileName + ".log"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
         
         return logfilename;
		
	}
    
    private void startEventPollers() {
		Configuration pluginConfig = resourceContext.getPluginConfiguration();
		Boolean enabled = Boolean.valueOf(pluginConfig.getSimpleValue(
				PLUGIN_CONFIG_PROP_ERROR_LOG_EVENTS_ENABLED, null)); //$NON-NLS-1$
		if (enabled) {

			String installdir = pluginConfig.getSimpleValue(
					INSTALL_DIR, null); //$NON-NLS-1$
			if (installdir == null) {
				throw new InvalidPluginConfigurationException(
						"Installation directory could not be determined in order for the process to monitor the log files"); //$NON-NLS-1$ //$NON-NLS-2$
				
			}

			String logFileName = deriveFileName(this.getComponentIdentifier());

			String relativelogname = pluginConfig.getSimpleValue(
					PLUGIN_CONFIG_PROP_ERROR_LOG_FILE_PATH,
					"/log/" + logFileName); //$NON-NLS-1$

			errorLogFile = new File(installdir + "/" + relativelogname); //$NON-NLS-1$

			LOG.info("Start event polling on logfile: " + errorLogFile.getAbsolutePath()); //$NON-NLS-1$

			JBEDSPErrorLogEntryProcessor processor = new JBEDSPErrorLogEntryProcessor(
					ERROR_LOG_ENTRY_EVENT_TYPE, errorLogFile);
			String includesPatternString = pluginConfig.getSimpleValue(
					PLUGIN_CONFIG_PROP_ERROR_LOG_INCLUDES_PATTERN, null);
			if (includesPatternString != null) {
				try {
					Pattern includesPattern = Pattern
							.compile(includesPatternString);
					processor.setIncludesPattern(includesPattern);
				} catch (PatternSyntaxException e) {
					throw new InvalidPluginConfigurationException(
							"Includes pattern [" + includesPatternString + "] is not a valid regular expression."); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
			String minimumSeverityString = pluginConfig.getSimpleValue(
					PLUGIN_CONFIG_PROP_ERROR_LOG_MINIMUM_SEVERITY, null);
			if (minimumSeverityString != null) {
				EventSeverity minimumSeverity = EventSeverity
						.valueOf(minimumSeverityString.toUpperCase());
				processor.setMinimumSeverity(minimumSeverity);
			}
			EventPoller poller = new LogFileEventPoller(this.eventContext,
					ERROR_LOG_ENTRY_EVENT_TYPE, errorLogFile, processor);
			this.eventContext.registerEventPoller(poller, 30, errorLogFile
					.getPath());
		}
	}

    private void stopEventPollers() {
// Configuration pluginConfig = this.resourceContext.getPluginConfiguration();
// File errorLogFile =
// resolvePathRelativeToServerRoot(pluginConfig.getSimpleValue(PLUGIN_CONFIG_PROP_ERROR_LOG_FILE_PATH,
// DEFAULT_ERROR_LOG_PATH));
        this.eventContext.unregisterEventPoller(ERROR_LOG_ENTRY_EVENT_TYPE, errorLogFile.getPath());
    }
    
    
    /*
     * Replace all occurrences of the search string with the replace string
     * in the source string. If any of the strings is null or the search string
     * is zero length, the source string is returned.
     * @param source the source string whose contents will be altered
     * @param search the string to search for in source
     * @param replace the string to substitute for search if present
     * @return source string with *all* occurrences of the search string
     * replaced with the replace string
     */
    private static String replaceAll(String source, String search, String replace) {
        if (source != null && search != null && search.length() > 0 && replace != null) {
            int start = source.indexOf(search);
            if (start > -1) {
                StringBuffer newString = new StringBuffer(source);
                replaceAll(newString, search, replace);
                return newString.toString();
            }
        }
        return source;    
    }
    
    private static void replaceAll(StringBuffer source, String search, String replace) {
        if (source != null && search != null && search.length() > 0 && replace != null) {
            int start = source.toString().indexOf(search);
            while (start > -1) {
                int end = start + search.length();
                source.replace(start, end, replace);
                start = source.toString().indexOf(search, start + replace.length());
            }
        }
    }
    

}