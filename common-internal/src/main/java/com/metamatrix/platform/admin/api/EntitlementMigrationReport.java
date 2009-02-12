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

package com.metamatrix.platform.admin.api;

import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.platform.admin.api.EntitlementMigrationReport;

/**  
 * Defines an object for holding results of an entitlement migration. A migration
 * report is generated when an attempt is made to migrate all entitlements from
 * a source VDB to a target VDB.
 * <p>
 * All values in each line (the inner list) are <code>String</code>s.<br></br>
 * The headere for each entry in the report are described as follows:
 * <ol>
 *   <li> Migrated (Yes, No) </li>
 *   <li> Resource (data node path) </li>
 *   <li> Source PolicyID (The source <code>AuthorizationPolicyID</code>) </li>
 *   <li> Target PolicyID (The target <code>AuthorizationPolicyID</code> that was created for migration) </li>
 *   <li> Actions (CRUD labels) </li>
 *   <li> Reason Migration Failed (Only filled in if this data node failed to migrate) </li>
 * </ol>
 * </p>
 */
public class EntitlementMigrationReport implements Serializable {
    
    /** Index in inner list of where header's value resides. */
    public static final int MIGRATED_INDEX          = 0;
    /** Index in inner list of where header's value resides. */
    public static final int RESOURCE_INDEX          = 1;
    /** Index in inner list of where header's value resides. */
    public static final int SOURCE_POLICYID_INDEX   = 2;
    /** Index in inner list of where header's value resides. */
    public static final int TARGET_POLICYID_INDEX   = 3;
    /** Index in inner list of where header's value resides. */
    public static final int ACTIONS_INDEX           = 4;
    /** Index in inner list of where header's value resides. */
    public static final int REASON_INDEX            = 5;

    /** The number of values that can be expected in each line entry. */
    public static final int NUMBER_OF_VALUES        = 6;

    private static final List headerValues;
    private String sourceVDB;
    private String targetVDB;
    private List entries;

    // Init header list
    static {
        headerValues = new ArrayList();
        headerValues.add("Migrated"); //$NON-NLS-1$
        headerValues.add("Resource"); //$NON-NLS-1$
        headerValues.add("Source PolicyID"); //$NON-NLS-1$
        headerValues.add("Target PolicyID"); //$NON-NLS-1$
        headerValues.add("Actions"); //$NON-NLS-1$
        headerValues.add("Reason Migration Failed"); //$NON-NLS-1$
    }

    /**
     * Construct with a source and a target VDB.
     * @param sourceVDB The name and version of the source VDB.
     * @param targetVDB The name and version of the target VDB.
     */
    public EntitlementMigrationReport(String sourceVDB, String targetVDB) {
        this.sourceVDB = sourceVDB;
        this.targetVDB = targetVDB;
        entries = new ArrayList();
    }
    
    public void addResourceEntry(Object migrated, Object resource, Object source_policy, Object target_policy, Object actions, Object reason) {
        this.entries.add(Arrays.asList(new Object[] {migrated, resource, source_policy, target_policy, actions, reason}));
    }

    /**
     * Get the source VDB ID from which the entitlement migration was attmpted.
     * @return The source Virtual Database name and version.
     */
    public String getSourceVDBID() {
        return sourceVDB;
    }

    /**
     * Set the source VDB ID from which the entitlement migration was attmpted.
     * @param The source Virtual Database name and version.
     */
    public void setSourceVDBID(String sourceVDB) {
        this.sourceVDB = sourceVDB;
    }

    /**
     * Get the target VDB ID to which the entitlement migration was attmpted.
     * @return The target Virtual Database name and version.
     */
    public String getTargetVDBID() {
        return targetVDB;
    }

    /**
     * Set the target VDB ID to which the entitlement migration was attmpted.
     * @param The target Virtual Database name and version.
     */
    public void setTargetVDBID(String targetVDB) {
        this.targetVDB = targetVDB;
    }

    /**
     * Get all the entries in the report. This is a <code>List</code> of
     * <code>List</code>s.  The entries of the inner <code>List</code> are defined
     * in this class' header.
     * @return The report.
     */
    public List getEntries() {
        return entries;
    }

    /**
     * Get the column headers in the report. This is a <code>List</code> of
     * <code>String</code>s.  The values of this list reside at the index locations
     * above.
     * @return The header list.
     */
    public List getHeaderList() {
        return headerValues;
    }
    
    private static final String TAB_CHAR     = "\t"; //$NON-NLS-1$
   
    public void writeReport(String fileName) throws Exception {
        // write the file
        FileWriter writer = new FileWriter(fileName);
        writer.write(toString());
        writer.flush();
        writer.close();
    }

    public String toString() {
        String LINE_SEP = System.getProperty("line.separator"); //$NON-NLS-1$
        StringBuffer txt = new StringBuffer();
        
        // append column headers
        String sHeaders
            = "Migrated"  //$NON-NLS-1$             
            + TAB_CHAR
            + "Resource"  //$NON-NLS-1$            
            + TAB_CHAR
            + "Source Policy ID"  //$NON-NLS-1$ 
            + TAB_CHAR
            + "Target Policy ID"  //$NON-NLS-1$
            + TAB_CHAR
            + "Actions"  //$NON-NLS-1$
            + TAB_CHAR            
            + "Reason"; //$NON-NLS-1$

        txt.append(sHeaders);
        txt.append(LINE_SEP);

        // append data
        Iterator it
            = getEntries().iterator();

        while (it.hasNext()) {

            StringBuffer sbRow = new StringBuffer();
            List lstEntry = (List)it.next();

            sbRow.append( lstEntry.get(EntitlementMigrationReport.MIGRATED_INDEX) );
            sbRow.append( TAB_CHAR );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.RESOURCE_INDEX) );
            sbRow.append( TAB_CHAR );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.SOURCE_POLICYID_INDEX) );
            sbRow.append( TAB_CHAR );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.TARGET_POLICYID_INDEX) );
            sbRow.append( TAB_CHAR );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.ACTIONS_INDEX) );
            sbRow.append( TAB_CHAR );
            sbRow.append( lstEntry.get(EntitlementMigrationReport.REASON_INDEX) );


            txt.append(sbRow);
            txt.append(LINE_SEP);
        }
        return txt.toString();
    }

}
