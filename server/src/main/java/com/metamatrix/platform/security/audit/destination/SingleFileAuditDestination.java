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

package com.metamatrix.platform.security.audit.destination;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.service.AuditMessage;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.util.LogSecurityConstants;
import com.metamatrix.platform.util.ErrorMessageKeys;

/**
 * This is a auditing destination that writes to a single file.
 */
public class SingleFileAuditDestination extends AbstractAuditDestination {

    /**
     * The name of the property that contains the name of the AuditMessageFormat
     * class that is used to format messages sent to the file destination.
     * This is an optional property; if not specified then the
     * {@link com.metamatrix.platform.security.audit.format.DelimitedAuditMessageFormat DelimitedAuditMessageFormat}
     * is used.
     */
    public static final String MESSAGE_FORMAT_PROPERTY_NAME = AuditDestination.PROPERTY_PREFIX + "fileFormat"; //$NON-NLS-1$

    /**
     * The name of the property that contains the name of the file to which
     * log messages are to be recorded.  This is an optional property that
     * defaults to "metamatrix.log".
     * <p>
     * To have multiple VMs on the same machine output to different files (without
     * having to know explicitly what each output filename ahead of time), the
     * <code>%VM_NAME%</code> token can be included in the value for this property.
     * In this case, the <code>%VM_NAME%</code> token gets replaced with the
     * name of the VM (which is a System environment property) or a unique ID if
     * the name is not specified.
     * <i>Note: if</i> <code>%VM_NAME%</code> <i>is specified in the file name but
     * no name is supplied to the VM, a pseudo-random number is used in place of the
     * name.</i>
     * @see com.metamatrix.common.util.NetUtils
     */
    public static final String FILE_NAME_PROPERTY_NAME = AuditDestination.PROPERTY_PREFIX + "file"; //$NON-NLS-1$

    /**
     * The name of the property that specifies whether the file should be appened
     * or overwritten.  This is an optional property that defaults to "false".
     */
    public static final String APPEND_PROPERTY_NAME = AuditDestination.PROPERTY_PREFIX + "fileAppend"; //$NON-NLS-1$

    /**
     * The token that is used in the filename and that is replaced with the
     * name of the property that specifies whether the file should be appened
     * or overwritten.  This is an optional property that defaults to "false".
     */
    public static final String VM_NAME_TOKEN = "%VM_NAME%"; //$NON-NLS-1$

    // Default values for properties
    protected static final String DEFAULT_FILE_NAME = "metamatrix.log"; //$NON-NLS-1$
    protected static final String DEFAULT_APPEND = "false"; //$NON-NLS-1$

	private String fileName;
	private boolean append;
	private FileWriter fileWriter;

	public SingleFileAuditDestination() {
        super();
	}

	/**
	 * Return description
	 * @return Description
	 */
	public String getDescription() {
		return "File \"" + fileName + "\" (append = " + append + ")"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	/**
	 * Initialize this destination with the specified properties.
     * @param props the properties that this destination should use to initialize
     * itself.
     * @throws AuditDestinationInitFailedException if there was an error during initialization.
	 */
	public void initialize(Properties props) throws AuditDestinationInitFailedException {
        super.initialize(props);
        super.setFormat( props.getProperty(MESSAGE_FORMAT_PROPERTY_NAME) );

        // Get properties
        fileName = props.getProperty(FILE_NAME_PROPERTY_NAME, DEFAULT_FILE_NAME);
        append = Boolean.valueOf(props.getProperty(APPEND_PROPERTY_NAME, DEFAULT_APPEND)).booleanValue();

        // Replace the token (if any) with the VM name (or ID) ...
        int index = fileName.indexOf(VM_NAME_TOKEN);
        if ( index != -1 ) {
            StringBuffer tempFileName = new StringBuffer(fileName);
            String processName = CurrentConfiguration.getInstance().getConfigurationName()+"_"+CurrentConfiguration.getInstance().getProcessName(); //$NON-NLS-1$
            tempFileName.replace(index,index+VM_NAME_TOKEN.length(),processName);
            fileName = tempFileName.toString();
        }

        // Create file writer
        try {
            fileWriter = new FileWriter(fileName, append);
        } catch(IOException e) {
            throw new AuditDestinationInitFailedException(e, ErrorMessageKeys.SEC_AUDIT_0023, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0023, fileName, new Boolean(append)));
    	}
	}

	/**
	 * Get names of all properties used for this destination.
	 */
	public List getPropertyNames() {
		List pnames = new ArrayList();
		pnames.add(MESSAGE_FORMAT_PROPERTY_NAME);
		pnames.add(FILE_NAME_PROPERTY_NAME);
		pnames.add(APPEND_PROPERTY_NAME);
		return pnames;
	}

	/**
	 * Print to the file writer
	 * @param message Message to print
	 */
	public void record(AuditMessage message) {
	    try {
    	    fileWriter.write(this.getFormat().formatMessage(message));
    	    fileWriter.write(StringUtil.getLineSeparator());
    	    fileWriter.flush();
    	} catch(IOException e) {
            LogManager.logError(LogSecurityConstants.CTX_AUDIT, e, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0024, e.getMessage()));
    	}
	}

	/**
	 * Shutdown - close file.
	 */
	public void shutdown() {
        try {
            fileWriter.close();
        } catch(Exception e) {
            // ignore
        }
	}

}
