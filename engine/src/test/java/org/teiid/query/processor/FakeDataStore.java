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

package org.teiid.query.processor;

import java.util.Arrays;
import java.util.List;

import org.teiid.core.TeiidException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.StoredProcedureInfo;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

/** 
 * This is sample data go along with FakeMetaDataFactory and FakeDataManager
 */
@SuppressWarnings("nls")
public class FakeDataStore {
    
    public static void sampleData1(FakeDataManager dataMgr, QueryMetadataInterface metadata) throws TeiidException {
		addTable("pm1.g1", dataMgr, metadata);    
		addTable("pm1.g2", dataMgr, metadata);
		addTable("pm1.g3", dataMgr, metadata);    
		addTable("pm2.g1", dataMgr, metadata);
		addTable("pm2.g2", dataMgr, metadata);
		addTable("pm2.g3", dataMgr, metadata);
		//addTable("tm1.g1", dataMgr, metadata);

        //stored procedure pm1.sp1
        StoredProcedureInfo procInfo = metadata.getStoredProcedureInfoForProcedure("pm1.sp1"); //$NON-NLS-1$
        dataMgr.registerProcTuples(
            procInfo.getProcedureCallableName().toUpperCase(),
            new List[] { 
                Arrays.asList(new Object[] { "a",   new Integer(0) }), //$NON-NLS-1$
                Arrays.asList(new Object[] { null,  new Integer(1)}),
                Arrays.asList(new Object[] { "a",   new Integer(3) }), //$NON-NLS-1$
                Arrays.asList(new Object[] { "c",   new Integer(1)}), //$NON-NLS-1$
                Arrays.asList(new Object[] { "b",   new Integer(2)}), //$NON-NLS-1$
                Arrays.asList(new Object[] { "a",   new Integer(0) }) //$NON-NLS-1$
                } );    
    }

	public static void addTable(String name, FakeDataManager dataMgr,
			QueryMetadataInterface metadata) throws TeiidException {
		List[] tuples =  new List[] { 
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
                Arrays.asList(new Object[] { null,  new Integer(1),     Boolean.FALSE,  new Double(1.0) }),
                Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0) }), //$NON-NLS-1$
                Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
                Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
                };
		
		dataMgr.registerTuples(metadata, name, tuples);
	}

    public static void sampleData2(FakeDataManager dataMgr) throws TeiidException {
		TransformationMetadata metadata = RealMetadataFactory.example1Cached();

		dataMgr.registerTuples(metadata, "pm1.g1", new List[] {
				Arrays.asList(new Object[] {
						"a", new Integer(0), Boolean.FALSE, new Double(2.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"b", new Integer(1), Boolean.TRUE, null }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"c", new Integer(2), Boolean.FALSE, new Double(0.0) }), //$NON-NLS-1$
		});

		// Group pm1.g2
		dataMgr.registerTuples(metadata, "pm1.g2", new List[] {
				Arrays.asList(new Object[] {
						"a", new Integer(1), Boolean.TRUE, new Double(2.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"b", new Integer(0), Boolean.FALSE, new Double(0.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"b", new Integer(5), Boolean.TRUE, new Double(2.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"b", new Integer(2), Boolean.FALSE, null }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"d", new Integer(2), Boolean.FALSE, new Double(1.0) }), //$NON-NLS-1$
		});

		// Group pm2.g1
		dataMgr.registerTuples(metadata, "pm2.g1", new List[] {
				Arrays.asList(new Object[] {
						"b", new Integer(0), Boolean.FALSE, new Double(2.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"d", new Integer(3), Boolean.TRUE, new Double(7.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"e", new Integer(1), Boolean.TRUE, null }), //$NON-NLS-1$
		});

		// Group pm2.g2
		dataMgr.registerTuples(metadata, "pm2.g2", new List[] {
				Arrays.asList(new Object[] {
						"a", new Integer(1), Boolean.TRUE, new Double(2.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"b", new Integer(0), Boolean.FALSE, new Double(0.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"b", new Integer(5), Boolean.TRUE, new Double(2.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"b", new Integer(2), Boolean.FALSE, null }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"d", new Integer(2), Boolean.FALSE, new Double(1.0) }), //$NON-NLS-1$
		});

		// Group pm1.table1
		dataMgr.registerTuples(metadata, "pm1.table1", new List[] {
				Arrays.asList(new Object[] {
						"a", new Integer(0), Boolean.FALSE, new Double(2.0) }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"b", new Integer(1), Boolean.TRUE, null }), //$NON-NLS-1$
				Arrays.asList(new Object[] {
						"c", new Integer(2), Boolean.FALSE, new Double(0.0) }), //$NON-NLS-1$
		});
	}                  

}
