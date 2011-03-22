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

package org.teiid.query.unittest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.metadata.Table;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingSequenceNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.symbol.ElementSymbol;

@SuppressWarnings("nls")
public class FakeMetadataFactory {

	public static SystemFunctionManager SFM = new SystemFunctionManager();
    private static FakeMetadataFacade CACHED_EXAMPLE1 = example1();
    private static FakeMetadataFacade CACHED_AGGREGATES = exampleAggregates();
        
	private FakeMetadataFactory() { }
	
    public static FakeMetadataFacade example1Cached() {
        return CACHED_EXAMPLE1;
    }

    public static TransformationMetadata exampleBQTCached() {
        return RealMetadataFactory.exampleBQTCached();
    }
    
    public static void setCardinality(String group, int cardinality, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
    	if (metadata instanceof TransformationMetadata) {
    		Table t = (Table)metadata.getGroupID(group);
    		t.setCardinality(cardinality);
    	} else if (metadata instanceof FakeMetadataFacade) {
    		FakeMetadataObject fmo = (FakeMetadataObject)metadata.getGroupID(group);
    		fmo.putProperty(FakeMetadataObject.Props.CARDINALITY, cardinality);
    	} else {
    		throw new RuntimeException("unknown metadata"); //$NON-NLS-1$
    	}
    	
    }
    
    public static FakeMetadataFacade exampleAggregatesCached() {
        return CACHED_AGGREGATES;
    }       
    
    public static DQPWorkContext buildWorkContext(TransformationMetadata metadata) {
    	return buildWorkContext(metadata, metadata.getVdbMetaData());
    }
    
	public static DQPWorkContext buildWorkContext(QueryMetadataInterface metadata, VDBMetaData vdb) {
		DQPWorkContext workContext = new DQPWorkContext();
		SessionMetadata session = new SessionMetadata();
		workContext.setSession(session);
		session.setVDBName(vdb.getName()); 
		session.setVDBVersion(vdb.getVersion()); 
		session.setSessionId(String.valueOf(1));
		session.setUserName("foo"); //$NON-NLS-1$
		session.setVdb(vdb);
        workContext.getVDB().addAttchment(QueryMetadataInterface.class, metadata);
        DQPWorkContext.setWorkContext(workContext);
		return workContext;
	}
    
    public static FakeMetadataFacade exampleBitwise() { 
        FakeMetadataObject phys = createPhysicalModel("phys"); //$NON-NLS-1$
        FakeMetadataObject t = createPhysicalGroup("phys.t", phys); //$NON-NLS-1$
        List tElem = createElements(t, 
                                    new String[] { "ID", "Name", "source_bits" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
                                    new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });

        FakeMetadataObject virt = createVirtualModel("virt"); //$NON-NLS-1$
        FakeMetadataObject rs = createResultSet("rs", virt, new String[] { "ID", "Name", "source_bits" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        FakeMetadataObject paramRS = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs);  //$NON-NLS-1$
        QueryNode qn = new QueryNode("agg", //$NON-NLS-1$
                                          "CREATE VIRTUAL PROCEDURE " //$NON-NLS-1$
                                          + "BEGIN " //$NON-NLS-1$
                                          + "        DECLARE integer VARIABLES.BITS;" //$NON-NLS-1$
                                          + "        create local temporary table #temp (id integer, name string, bits integer);" //$NON-NLS-1$
                                          + "        LOOP ON (SELECT DISTINCT phys.t.ID, phys.t.Name FROM phys.t) AS idCursor" //$NON-NLS-1$
                                          + "        BEGIN" //$NON-NLS-1$
                                          + "                VARIABLES.BITS = 0;" //$NON-NLS-1$
                                          + "                LOOP ON (SELECT phys.t.source_bits FROM phys.t WHERE phys.t.ID = idCursor.id) AS bitsCursor" //$NON-NLS-1$
                                          + "                BEGIN" //$NON-NLS-1$
                                          + "                        VARIABLES.BITS = bitor(VARIABLES.BITS, bitsCursor.source_bits);" //$NON-NLS-1$
                                          + "                END" //$NON-NLS-1$
                                          + "                SELECT idCursor.id, idCursor.name, VARIABLES.BITS INTO #temp;" //$NON-NLS-1$
                                          + "        END" //$NON-NLS-1$
                                          + "        SELECT ID, Name, #temp.BITS AS source_bits FROM #temp;" //$NON-NLS-1$                                          
                                          + "END"); //$NON-NLS-1$ 
        FakeMetadataObject proc = createVirtualProcedure("virt.agg", virt, Arrays.asList(new FakeMetadataObject[] { paramRS }), qn); //$NON-NLS-1$

        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(phys);
        store.addObject(t);
        store.addObjects(tElem);
        store.addObject(virt);
        store.addObject(proc);
        
        return new FakeMetadataFacade(store);
        
    }
    
    public static VDBMetaData example1VDB() {
    	VDBMetaData vdb = new VDBMetaData();
    	vdb.setName("example1");
    	vdb.setVersion(1);
    	vdb.addModel(createModel("pm1", true));
    	vdb.addModel(createModel("pm2", true));
    	vdb.addModel(createModel("pm3", true));
    	vdb.addModel(createModel("pm4", true));
    	vdb.addModel(createModel("pm5", true));
    	vdb.addModel(createModel("pm6", true));
    	vdb.addModel(createModel("vm1", false));
    	vdb.addModel(createModel("vm2", false));
    	vdb.addModel(createModel("tm1", false));
    	
    	return vdb;
    }
    
    public static ModelMetaData createModel(String name, boolean source) {
    	ModelMetaData model = new ModelMetaData();
    	model.setName(name);
    	if (source) {
    		model.setModelType(Model.Type.PHYSICAL);
    	}
    	else {
    		model.setModelType(Model.Type.VIRTUAL);
    	}
    	model.setVisible(true);
    	model.setSupportsMultiSourceBindings(false);
    	model.addSourceMapping(name, name, null);
    	
    	return model;
    }
    
	public static FakeMetadataFacade example1() { 
		// Create models
		FakeMetadataObject pm1 = createPhysicalModel("pm1"); //$NON-NLS-1$
		FakeMetadataObject pm2 = createPhysicalModel("pm2"); //$NON-NLS-1$
        FakeMetadataObject pm3 = createPhysicalModel("pm3"); //allows push of SELECT DISTINCT //$NON-NLS-1$
        FakeMetadataObject pm4 = createPhysicalModel("pm4"); //all groups w/ access pattern(s) //$NON-NLS-1$
		FakeMetadataObject pm5 = createPhysicalModel("pm5"); //all groups w/ access pattern(s); model supports join //$NON-NLS-1$
        FakeMetadataObject pm6 = createPhysicalModel("pm6"); //model does not support where all //$NON-NLS-1$
		FakeMetadataObject vm1 = createVirtualModel("vm1");	 //$NON-NLS-1$
		FakeMetadataObject vm2 = createVirtualModel("vm2");	 //$NON-NLS-1$
        FakeMetadataObject tm1 = createVirtualModel("tm1"); //$NON-NLS-1$

		// Create physical groups
		FakeMetadataObject pm1g1 = createPhysicalGroup("pm1.g1", pm1); //$NON-NLS-1$
		FakeMetadataObject pm1g2 = createPhysicalGroup("pm1.g2", pm1); //$NON-NLS-1$
		FakeMetadataObject pm1g3 = createPhysicalGroup("pm1.g3", pm1); //$NON-NLS-1$
        FakeMetadataObject pm1g4 = createPhysicalGroup("pm1.g4", pm1); //$NON-NLS-1$
        FakeMetadataObject pm1g5 = createPhysicalGroup("pm1.g5", pm1); //$NON-NLS-1$
        FakeMetadataObject pm1g6 = createPhysicalGroup("pm1.g6", pm1); //$NON-NLS-1$
        FakeMetadataObject pm1table = createPhysicalGroup("pm1.table1", pm1); //$NON-NLS-1$
		FakeMetadataObject pm2g1 = createPhysicalGroup("pm2.g1", pm2); //$NON-NLS-1$
		FakeMetadataObject pm2g2 = createPhysicalGroup("pm2.g2", pm2); //$NON-NLS-1$
		FakeMetadataObject pm2g3 = createPhysicalGroup("pm2.g3", pm2); //$NON-NLS-1$
        FakeMetadataObject pm3g1 = createPhysicalGroup("pm3.g1", pm3); //$NON-NLS-1$
        FakeMetadataObject pm3g2 = createPhysicalGroup("pm3.g2", pm3); //$NON-NLS-1$
        FakeMetadataObject pm4g1 = createPhysicalGroup("pm4.g1", pm4); //$NON-NLS-1$
        FakeMetadataObject pm4g2 = createPhysicalGroup("pm4.g2", pm4); //$NON-NLS-1$
		FakeMetadataObject pm5g1 = createPhysicalGroup("pm5.g1", pm5); //$NON-NLS-1$
		FakeMetadataObject pm5g2 = createPhysicalGroup("pm5.g2", pm5); //$NON-NLS-1$
		FakeMetadataObject pm5g3 = createPhysicalGroup("pm5.g3", pm5); //$NON-NLS-1$
        FakeMetadataObject pm6g1 = createPhysicalGroup("pm6.g1", pm6); //$NON-NLS-1$
        
				
		// Create physical elements
		List pm1g1e = createElements(pm1g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm1g2e = createElements(pm1g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm1g3e = createElements(pm1g3, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List pm1g4e = createElements(pm1g4,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        ((FakeMetadataObject)pm1g4e.get(1)).putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        ((FakeMetadataObject)pm1g4e.get(3)).putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        List pm1g5e = createElements(pm1g5,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        ((FakeMetadataObject)pm1g5e.get(0)).putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        List pm1g6e = createElements(pm1g6,
            new String[] { "in", "in3" }, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List pm1tablee = createElements(pm1table, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g1e = createElements(pm2g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g2e = createElements(pm2g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g3e = createElements(pm2g3, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm3g1e = createElements(pm3g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, DataTypeManager.DefaultDataTypes.TIMESTAMP });
		List pm3g2e = createElements(pm3g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, DataTypeManager.DefaultDataTypes.TIMESTAMP });
        List pm4g1e = createElements(pm4g1, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List pm4g2e = createElements(pm4g2, 
            new String[] { "e1", "e2", "e3", "e4", "e5", "e6" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
		List pm5g1e = createElements(pm5g1,
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm5g2e = createElements(pm5g2,
			new String[] { "e1", "e2", "e3", "e4", "e5", "e6" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
		List pm5g3e = createElements(pm5g3,
	        new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
	        new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.SHORT });
        List pm6g1e = createElements(pm6g1,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        

        // Create access patterns - pm4
        List elements = new ArrayList(1);
        elements.add(pm4g1e.iterator().next());       
        FakeMetadataObject pm4g1ap1 = createAccessPattern("pm4.g1.ap1", pm4g1, elements); //e1 //$NON-NLS-1$
        elements = new ArrayList(2);
        Iterator iter = pm4g2e.iterator();
        elements.add(iter.next());       
        elements.add(iter.next());       
        FakeMetadataObject pm4g2ap1 = createAccessPattern("pm4.g2.ap1", pm4g2, elements); //e1,e2 //$NON-NLS-1$
		elements = new ArrayList(1);
		elements.add(pm4g2e.get(4)); //"e5"
		FakeMetadataObject pm4g2ap2 =createAccessPattern("pm4.g2.ap2", pm4g2, elements); //e5 //$NON-NLS-1$
		// Create access patterns - pm5
		elements = new ArrayList(1);
		elements.add(pm5g1e.iterator().next());
		FakeMetadataObject pm5g1ap1 = createAccessPattern("pm5.g1.ap1", pm5g1, elements); //e1 //$NON-NLS-1$
		elements = new ArrayList(2);
		iter = pm5g2e.iterator();
		elements.add(iter.next());
		elements.add(iter.next());
		FakeMetadataObject pm5g2ap1 = createAccessPattern("pm5.g2.ap1", pm5g2, elements); //e1,e2 //$NON-NLS-1$
		elements = new ArrayList(1);
		elements.add(pm5g2e.get(4)); //"e5"
		FakeMetadataObject pm5g2ap2 = createAccessPattern("pm5.g2.ap2", pm5g2, elements); //e5 //$NON-NLS-1$

        // Create temp groups
        FakeMetadataObject tm1g1 = createTempGroup("tm1.g1", tm1, null); //$NON-NLS-1$

        // Create temp elements - the element "node1" is purposely named to be ambiguous with a document node named "node1"
        List tm1g1e = createElements(tm1g1, 
            new String[] { "e1", "e2", "e3", "e4", "node1"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING });
            
        // Create temp groups
        FakeMetadataObject tm1g2 = createTempGroup("tm1.g1", tm1, null); //$NON-NLS-1$

        // Create temp elements
        List tm1g2e = createElements(tm1g2, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });            

		// Create virtual groups
		QueryNode vm1g1n1 = new QueryNode("vm1.g1", "SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g1 = createUpdatableVirtualGroup("vm1.g1", vm1, vm1g1n1); //$NON-NLS-1$

		QueryNode vm2g1n1 = new QueryNode("vm2.g1", "SELECT pm1.g1.* FROM pm1.g1, pm1.g2 where pm1.g1.e2 = pm1.g2.e2"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm2g1 = FakeMetadataFactory.createUpdatableVirtualGroup("vm2.g1", vm2, vm2g1n1); //$NON-NLS-1$		
		
        QueryNode vm1g1n1_defect10711 = new QueryNode("vm1.g1a", "SELECT * FROM vm1.g1 as X"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g1_defect10711 = createVirtualGroup("vm1.g1a", vm1, vm1g1n1_defect10711); //$NON-NLS-1$

        QueryNode vm1g1n1_defect12081 = new QueryNode("vm1.g1b", "SELECT e1, upper(e1) as e1Upper FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g1_defect12081 = createVirtualGroup("vm1.g1b", vm1, vm1g1n1_defect12081); //$NON-NLS-1$

        QueryNode vm1g1n1c = new QueryNode("vm1.g1c", "SELECT PARSETIMESTAMP(pm1.g1.e1, 'MMM dd yyyy hh:mm:ss') as e5, e2, e3, e4 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g1c = createVirtualGroup("vm1.g1c", vm1, vm1g1n1c); //$NON-NLS-1$
        
        QueryNode vm1g2an1 = new QueryNode("vm1.g2a", "SELECT * FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g2a = createVirtualGroup("vm1.g2a", vm1, vm1g2an1); //$NON-NLS-1$

		QueryNode vm1g2n1 = new QueryNode("vm1.g2", "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1=pm1.g2.e1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g2 = createVirtualGroup("vm1.g2", vm1, vm1g2n1); //$NON-NLS-1$

        QueryNode vm1g4n1 = new QueryNode("vm1.g4", "SELECT e1 FROM pm1.g1 UNION ALL SELECT convert(e2, string) as x FROM pm1.g2 ORDER BY e1");         //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g4 = createVirtualGroup("vm1.g4", vm1, vm1g4n1); //$NON-NLS-1$
	
        QueryNode vm1g5n1 = new QueryNode("vm1.g5", "SELECT concat(e1, 'val'), e2 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g5 = createVirtualGroup("vm1.g5", vm1, vm1g5n1); //$NON-NLS-1$

        QueryNode vm1g6n1 = new QueryNode("vm1.g6", "SELECT concat(e1, 'val') AS e, e2 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g6 = createVirtualGroup("vm1.g6", vm1, vm1g6n1); //$NON-NLS-1$

        QueryNode vm1g7n1 = new QueryNode("vm1.g7", "SELECT concat(e1, e2) AS e, e2 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g7 = createVirtualGroup("vm1.g7", vm1, vm1g7n1); //$NON-NLS-1$

        QueryNode vm1g8n1 = new QueryNode("vm1.g8", "SELECT concat(e1, 'val') AS e, e2 FROM pm1.g1 ORDER BY e"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g8 = createVirtualGroup("vm1.g8", vm1, vm1g8n1); //$NON-NLS-1$

        QueryNode vm1g9n1 = new QueryNode("vm1.g9", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1, pm4.g1 WHERE pm1.g1.e1 = pm4.g1.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g9 = createVirtualGroup("vm1.g9", vm1, vm1g9n1); //$NON-NLS-1$

        QueryNode vm1g10n1 = new QueryNode("vm1.g10", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1, pm4.g2 WHERE pm1.g1.e1 = pm4.g2.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g10 = createVirtualGroup("vm1.g10", vm1, vm1g10n1); //$NON-NLS-1$

        QueryNode vm1g11n1 = new QueryNode("vm1.g11", "SELECT * FROM pm4.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g11 = createVirtualGroup("vm1.g11", vm1, vm1g11n1); //$NON-NLS-1$

        QueryNode vm1g12n1 = new QueryNode("vm1.g12", "SELECT DISTINCT * FROM pm3.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g12 = createVirtualGroup("vm1.g12", vm1, vm1g12n1); //$NON-NLS-1$

        QueryNode vm1g13n1 = new QueryNode("vm1.g13", "SELECT DISTINCT * FROM pm3.g1 ORDER BY e1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g13 = createVirtualGroup("vm1.g13", vm1, vm1g13n1); //$NON-NLS-1$

        QueryNode vm1g14n1 = new QueryNode("vm1.g14", "SELECT * FROM pm3.g1 ORDER BY e1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g14 = createVirtualGroup("vm1.g14", vm1, vm1g14n1); //$NON-NLS-1$
   
        QueryNode vm1g15n1 = new QueryNode("vm1.g15", "SELECT e1, concat(e1, convert(e2, string)) AS x FROM pm3.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g15 = createVirtualGroup("vm1.g15", vm1, vm1g15n1); //$NON-NLS-1$

        QueryNode vm1g16n1 = new QueryNode("vm1.g16", "SELECT concat(e1, 'val') AS e, e2 FROM pm3.g1 ORDER BY e"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g16 = createVirtualGroup("vm1.g16", vm1, vm1g16n1); //$NON-NLS-1$

        QueryNode vm1g17n1 = new QueryNode("vm1.g17", "SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1 UNION ALL SELECT pm3.g2.e1, pm3.g2.e2 FROM pm3.g2 ORDER BY e2");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g17 = createVirtualGroup("vm1.g17", vm1, vm1g17n1); //$NON-NLS-1$

        QueryNode vm1g18n1 = new QueryNode("vm1.g18", "SELECT (e4 * 100.0) as x FROM pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g18 = createVirtualGroup("vm1.g18", vm1, vm1g18n1); //$NON-NLS-1$

        // Transformations with subqueries and correlated subqueries
        QueryNode vm1g19n1 = new QueryNode("vm1.g19", "Select * from vm1.g4 where not (e1 in (select e1 FROM vm1.g1 WHERE vm1.g4.e1 = e1))");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g19 = createVirtualGroup("vm1.g19", vm1, vm1g19n1); //$NON-NLS-1$

        QueryNode vm1g20n1 = new QueryNode("vm1.g20", "Select * from vm1.g1 where exists (select e1 FROM vm1.g2 WHERE vm1.g1.e1 = e1)");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g20 = createVirtualGroup("vm1.g20", vm1, vm1g20n1); //$NON-NLS-1$

        QueryNode vm1g21n1 = new QueryNode("vm1.g21", "Select * from pm1.g1 where exists (select e1 FROM pm2.g1 WHERE pm1.g1.e1 = e1)");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g21 = createVirtualGroup("vm1.g21", vm1, vm1g21n1); //$NON-NLS-1$

        QueryNode vm1g22n1 = new QueryNode("vm1.g22", "Select e1, e2, e3, e4, (select e4 FROM vm1.g21 WHERE vm1.g20.e4 = e4 and e4 = 7.0) as E5 from vm1.g20");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g22 = createVirtualGroup("vm1.g22", vm1, vm1g22n1); //$NON-NLS-1$

        QueryNode vm1g23n1 = new QueryNode("vm1.g23", "Select e1, e2, e3, e4, (select e4 FROM vm1.g21 WHERE vm1.g20.e4 = 7.0 and e4 = 7.0) as E5 from vm1.g20");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g23 = createVirtualGroup("vm1.g23", vm1, vm1g23n1); //$NON-NLS-1$

        QueryNode vm1g24n1 = new QueryNode("vm1.g24", "Select * from vm1.g20 where exists (select * FROM vm1.g21 WHERE vm1.g20.e4 = E4)");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g24 = createVirtualGroup("vm1.g24", vm1, vm1g24n1); //$NON-NLS-1$

        QueryNode vm1g25n1 = new QueryNode("vm1.g25", "Select e1, e2, e3, e4, (select e4 FROM pm1.g2 WHERE e1 = 'b') as E5 from pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g25 = createVirtualGroup("vm1.g25", vm1, vm1g25n1); //$NON-NLS-1$

        QueryNode vm1g26n1 = new QueryNode("vm1.g26", "Select e1, e2, e3, e4, (select e4 FROM pm1.g2 WHERE e4 = pm1.g1.e4 and e1 = 'b') as E5 from pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g26 = createVirtualGroup("vm1.g26", vm1, vm1g26n1); //$NON-NLS-1$

        //defect 10976
//        QueryNode vm1g27n1 = new QueryNode("vm1.g27", "SELECT DISTINCT x as a, lower(e1) as x FROM vm1.g28");         //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vm1g27n1 = new QueryNode("vm1.g27", "SELECT upper(e1) as x, e1 FROM pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g27 = createVirtualGroup("vm1.g27", vm1, vm1g27n1); //$NON-NLS-1$

        QueryNode vm1g28n1 = new QueryNode("vm1.g28", "SELECT DISTINCT x as a, lower(e1) as x FROM vm1.g27");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g28 = createVirtualGroup("vm1.g28", vm1, vm1g28n1); //$NON-NLS-1$

        QueryNode vm1g29n1 = new QueryNode("vm1.g29", "SELECT DISTINCT x, lower(e1) FROM vm1.g27");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g29 = createVirtualGroup("vm1.g29", vm1, vm1g29n1); //$NON-NLS-1$

        QueryNode vm1g30n1 = new QueryNode("vm1.g30", "SELECT DISTINCT e1 as x, e1 as y FROM pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g30 = createVirtualGroup("vm1.g30", vm1, vm1g30n1); //$NON-NLS-1$

        QueryNode vm1g31n1 = new QueryNode("vm1.g31", "SELECT e1 as x, e1 as y FROM pm1.g1 ORDER BY x");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g31 = createVirtualGroup("vm1.g31", vm1, vm1g31n1); //$NON-NLS-1$

        QueryNode vm1g32n1 = new QueryNode("vm1.g32", "SELECT DISTINCT e1 as x, e1 as y FROM pm1.g1 ORDER BY x");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g32 = createVirtualGroup("vm1.g32", vm1, vm1g32n1); //$NON-NLS-1$

        QueryNode vm1g33n1 = new QueryNode("vm1.g33", "SELECT e2 FROM pm1.g1 WHERE 2 = e2");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g33 = createVirtualGroup("vm1.g33", vm1, vm1g33n1); //$NON-NLS-1$

        QueryNode vm1g34n1 = new QueryNode("vm1.g34", "SELECT e1 as e1_, e2 as e2_ FROM pm1.g1 UNION ALL SELECT e1 as e1_, e2 as e2_ FROM pm2.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g34 = createVirtualGroup("vm1.g34", vm1, vm1g34n1); //$NON-NLS-1$

        QueryNode vm1g36n1 = new QueryNode("vm1.g36", "SELECT pm1.g1.e1 as ve1, pm1.g2.e1 as ve2 FROM pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g36 = createVirtualGroup("vm1.g36", vm1, vm1g36n1); //$NON-NLS-1$

        QueryNode vm1g37n1 = new QueryNode("vm1.g37", "SELECT * from pm4.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g37 = createVirtualGroup("vm1.g37", vm1, vm1g37n1); //$NON-NLS-1$
        vm1g37.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE);

        QueryNode vm1g38n1 = new QueryNode("vm1.g38", "SELECT a.e1, b.e2 from pm1.g1 as a, pm6.g1 as b where a.e1=b.e1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g38 = createVirtualGroup("vm1.g38", vm1, vm1g38n1); //$NON-NLS-1$
        
		// Create virtual groups
		QueryNode vm1g39n1 = new QueryNode("vm1.g39", "SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g39 = createUpdatableVirtualGroup("vm1.g39", vm1, vm1g39n1, "CREATE PROCEDURE BEGIN LOOP ON (SELECT pm1.g1.e2 FROM pm1.g1 where pm1.g1.e2=3) AS mycursor begin update pm1.g1 set pm1.g1.e1 = input.e1 where pm1.g1.e1 = input.e1; ROWS_UPDATED = ROWS_UPDATED + ROWCOUNT;\nupdate pm1.g1 set pm1.g1.e2 = input.e2 where pm1.g1.e2 = input.e2; END END"); //$NON-NLS-1$ //$NON-NLS-2$
        
		// Create virtual elements
		List vm1g39e = createElements(vm1g39, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List vm1g1e = createElements(vm1g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List vm2g1e = FakeMetadataFactory.createElements(vm2g1, 
    		new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g1e_defect10711 = createElements(vm1g1_defect10711, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g1e_defect12081 = createElements(vm1g1_defect12081, 
            new String[] { "e1", "e1Upper" }, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g1ce = createElements(vm1g1c, 
            new String[] { "e5", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g2ae = createElements(vm1g2a, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List vm1g2e = createElements(vm1g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List vm1g4e = createElements(vm1g4,
			new String[] { "e1" }, //$NON-NLS-1$
			new String[] { DataTypeManager.DefaultDataTypes.STRING });
        List vm1g5e = createElements(vm1g5,
            new String[] { "expr", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g6e = createElements(vm1g6,
            new String[] { "e", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g7e = createElements(vm1g7,
            new String[] { "e", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g8e = createElements(vm1g8,
            new String[] { "e", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g9e = createElements(vm1g9,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g10e = createElements(vm1g10,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g11e = createElements(vm1g11,
            new String[] { "e1", "e2", "e3", "e4", "e5", "e6"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g12e = createElements(vm1g12,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g13e = createElements(vm1g13,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g14e = createElements(vm1g14,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g15e = createElements(vm1g15,
            new String[] { "e1", "x" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g16e = createElements(vm1g16,
            new String[] { "e", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g17e = createElements(vm1g17,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g18e = createElements(vm1g18,
            new String[] { "x" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g19e = createElements(vm1g19,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        List vm1g20e = createElements(vm1g20,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g21e = createElements(vm1g21,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g22e = createElements(vm1g22,
            new String[] { "e1", "e2", "e3", "e4", "e5" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g23e = createElements(vm1g23,
            new String[] { "e1", "e2", "e3", "e4", "e5" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g24e = createElements(vm1g24,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g25e = createElements(vm1g25,
            new String[] { "e1", "e2", "e3", "e4", "e5" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g26e = createElements(vm1g26,
            new String[] { "e1", "e2", "e3", "e4", "e5" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g27e = createElements(vm1g27,
            new String[] { "x", "e1"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g28e = createElements(vm1g28,
            new String[] { "a", "x"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g29e = createElements(vm1g29,
            new String[] { "x", "expr"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g30e = createElements(vm1g30,
            new String[] { "x", "y"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g31e = createElements(vm1g31,
            new String[] { "x", "y"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g32e = createElements(vm1g32,
            new String[] { "x", "y"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g33e = createElements(vm1g33,
            new String[] { "e2"}, //$NON-NLS-1$  
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g34e = createElements(vm1g34,
            new String[] { "e1_", "e2_"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List vm1g36e = createElements(vm1g36,
             new String[] { "ve1", "ve2" }, //$NON-NLS-1$ //$NON-NLS-2$
             new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List vm1g37e = createElements(vm1g37,
              new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
              new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List vm1g38e = createElements(vm1g38,
              new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
              new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });

        //Create access patterns on vm1.g37
        elements = new ArrayList(1);
        elements.add(vm1g37e.iterator().next());       
        FakeMetadataObject vm1g37ap1 = createAccessPattern("vm1.g37.ap1", vm1g37, elements); //e1 //$NON-NLS-1$
        
        //XML STUFF =============================================
        FakeMetadataObject doc1 = FakeMetadataFactory.createVirtualGroup("xmltest.doc1", vm1, exampleDoc1()); //$NON-NLS-1$
        FakeMetadataObject doc2 = FakeMetadataFactory.createVirtualGroup("xmltest.doc2", vm1, exampleDoc2()); //$NON-NLS-1$
        FakeMetadataObject doc3 = FakeMetadataFactory.createVirtualGroup("xmltest.doc3", vm1, exampleDoc3()); //$NON-NLS-1$
        FakeMetadataObject doc4 = FakeMetadataFactory.createVirtualGroup("xmltest.doc4", vm1, exampleDoc4());         //$NON-NLS-1$
        FakeMetadataObject doc5 = FakeMetadataFactory.createVirtualGroup("xmltest.doc5", vm1, exampleDoc5()); //$NON-NLS-1$
        FakeMetadataObject doc6 = FakeMetadataFactory.createVirtualGroup("xmltest.doc6", vm1, exampleDoc6()); //$NON-NLS-1$

        // Defect 11479 - test ambiguous doc short names
        FakeMetadataObject docAmbiguous1 = FakeMetadataFactory.createVirtualGroup("xmltest2.docA", vm1, exampleDoc1()); //$NON-NLS-1$
        FakeMetadataObject docAmbiguous2 = FakeMetadataFactory.createVirtualGroup("xmltest3.docA", vm1, exampleDoc2()); //$NON-NLS-1$

		List docE1 = createElements(doc1, new String[] { "root", "root.node1", "root.node1.node2", "root.node1.node2.node3" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
		List docE2 = createElements(doc2, new String[] { "root", "root.node1", "root.node1.node3" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
		List docE3 = createElements(doc3, new String[] { "root", "root.node1.node2", "root.node2" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
		List docE4 = createElements(doc4, new String[] { "root", "root.@node6", "root.node1", "root.node1.@node2", "root.node3", "root.node3.@node4", "root.node3.node4", "root.node3.root.node6" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
		Collection tempGroups = new HashSet();
		tempGroups.add(tm1g1);
		doc4.putProperty("TEMP_GROUPS", tempGroups); //$NON-NLS-1$
        List docE5 = createElements(doc5, new String[] { "root", "root.node1", "root.node1.node2" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
            
        // Create mapping classes for xmltest.doc5
        QueryNode mc1n1 = new QueryNode("xmltest.mc1", "SELECT e1 FROM pm1.g1 UNION ALL SELECT e1 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1mc1 = createVirtualGroup("xmltest.mc1", vm1, mc1n1); //$NON-NLS-1$
        List vm1mc1e = createElements(vm1mc1,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });

        List docE6 = createElements(doc6, new String[] { "root", "root.node", "root.thenode" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
            
        //XML STUFF =============================================

        // Procedures and stored queries
        FakeMetadataObject rs1 = createResultSet("pm1.rs1", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs1p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        QueryNode sq1n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq1 = createVirtualProcedure("pm1.sq1", pm1, Arrays.asList(new FakeMetadataObject[] { rs1p1 }), sq1n1); //$NON-NLS-1$
        
        FakeMetadataObject rs2 = createResultSet("pm1.rs2", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq2", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq2.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq2 = createVirtualProcedure("pm1.sq2", pm1, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        FakeMetadataObject rs5 = createResultSet("pm1.r5", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs5p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs5);  //$NON-NLS-1$
        FakeMetadataObject rs5p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs5p3 = createParameter("in2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq3n1 = new QueryNode("pm1.sq3", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq3.in UNION ALL SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sq3.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq3 = createVirtualProcedure("pm1.sq3", pm1, Arrays.asList(new FakeMetadataObject[] { rs5p1, rs5p2, rs5p3 }), sq3n1);  //$NON-NLS-1$

        //For defect 8211 - this stored query has two input params, no return param, and
        //the input params are PURPOSELY numbered with indices "1" and "3" - see defect 8211
        FakeMetadataObject rs5a = createResultSet("pm1.r5a", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs5p1a = createParameter("in", 1, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs5p2a = createParameter("in2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq3n1a = new QueryNode("pm1.sq3a", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq3a.in UNION ALL SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sq3a.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq3a = createVirtualProcedure("pm1.sq3a", pm1, Arrays.asList(new FakeMetadataObject[] { rs5p1a, rs5p2a }), sq3n1a);  //$NON-NLS-1$

        //Case 3281 - create procedures with optional parameter(s)
        
        //make "in2" parameter optional, make "in3" required but with a default value
        FakeMetadataObject rs5b = createResultSet("pm1.r5b", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs5p1b = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs5b);  //$NON-NLS-1$
        FakeMetadataObject rs5p2b = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs5p3b = createParameter("in2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        FakeMetadataObject rs5p4b = createParameter("in3", 4, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        rs5p3b.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rs5p4b.setDefaultValue("YYZ"); //$NON-NLS-1$
        QueryNode sq3n1b = new QueryNode("pm1.sq3b", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq3b.in UNION ALL SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sq3b.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq3b = createVirtualProcedure("pm1.sq3b", pm1, Arrays.asList(new FakeMetadataObject[] { rs5p1b, rs5p2b, rs5p3b, rs5p4b }), sq3n1b);  //$NON-NLS-1$
        
        //Make parameters of all different types, all with appropriate default values
        //Make some parameters required, some optional
        //Also, fully-qualify the param names
        FakeMetadataObject rsDefaults = createResultSet("pm1.rDefaults", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rsDefaultsParameterReturn = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rsDefaults);  //$NON-NLS-1$
        FakeMetadataObject rsDefaultsParameterString = createParameter("inString", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        //rsDefaultsParameterString.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsDefaultsParameterString.setDefaultValue(new String("x")); //$NON-NLS-1$
        FakeMetadataObject rsParameterBigDecimal = createParameter("inBigDecimal", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, null);  //$NON-NLS-1$
        rsParameterBigDecimal.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterBigDecimal.setDefaultValue(new String("13.0")); //$NON-NLS-1$
        FakeMetadataObject rsParameterBigInteger = createParameter("inBigInteger", 4, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BIG_INTEGER, null);  //$NON-NLS-1$
        rsParameterBigInteger.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterBigInteger.setDefaultValue(new String("13")); //$NON-NLS-1$
        FakeMetadataObject rsParameterBoolean = createParameter("inBoolean", 5, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BOOLEAN, null);  //$NON-NLS-1$
        rsParameterBoolean.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterBoolean.setDefaultValue(new String("True")); //$NON-NLS-1$
        FakeMetadataObject rsParameterByte = createParameter("inByte", 6, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BYTE, null);  //$NON-NLS-1$
        rsParameterByte.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterByte.setDefaultValue(new String("1")); //$NON-NLS-1$
        FakeMetadataObject rsParameterChar = createParameter("inChar", 7, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.CHAR, null);  //$NON-NLS-1$
        rsParameterChar.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterChar.setDefaultValue(new String("q")); //$NON-NLS-1$
        FakeMetadataObject rsParameterDate = createParameter("inDate", 8, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.DATE, null);  //$NON-NLS-1$
        rsParameterDate.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterDate.setDefaultValue(new String("2003-03-20")); //$NON-NLS-1$
        FakeMetadataObject rsParameterDouble = createParameter("inDouble", 9, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.DOUBLE, null);  //$NON-NLS-1$
        rsParameterDouble.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterDouble.setDefaultValue(new String("13.0")); //$NON-NLS-1$
        FakeMetadataObject rsParameterFloat = createParameter("inFloat", 10, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.FLOAT, null);  //$NON-NLS-1$
        rsParameterFloat.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterFloat.setDefaultValue(new String("13")); //$NON-NLS-1$
        FakeMetadataObject rsParameterInteger = createParameter("inInteger", 11, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        rsParameterInteger.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterInteger.setDefaultValue(new String("13")); //$NON-NLS-1$
        FakeMetadataObject rsParameterLong = createParameter("inLong", 12, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.LONG, null);  //$NON-NLS-1$
        rsParameterLong.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterLong.setDefaultValue(new String("13")); //$NON-NLS-1$
        FakeMetadataObject rsParameterShort = createParameter("inShort", 13, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.SHORT, null);  //$NON-NLS-1$
        rsParameterShort.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterShort.setDefaultValue(new String("13")); //$NON-NLS-1$
        FakeMetadataObject rsParameterTimestamp = createParameter("inTimestamp", 14, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.TIMESTAMP, null);  //$NON-NLS-1$
        rsParameterTimestamp.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterTimestamp.setDefaultValue(new String("2003-03-20 21:26:00.000000")); //$NON-NLS-1$
        FakeMetadataObject rsParameterTime = createParameter("inTime", 15, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.TIME, null);  //$NON-NLS-1$
        rsParameterTime.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        rsParameterTime.setDefaultValue(new String("21:26:00")); //$NON-NLS-1$
        QueryNode sqDefaultsNode = new QueryNode("pm1.sqDefaults", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sqDefaults.inString UNION ALL SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sqDefaults.inInteger; END"); //$NON-NLS-1$ //$NON-NLS-2$

        FakeMetadataObject sqDefaults = createVirtualProcedure("pm1.sqDefaults", pm1, //$NON-NLS-1$
                                                          Arrays.asList(new FakeMetadataObject[] { 
                                                              rsDefaultsParameterReturn, 
                                                              rsDefaultsParameterString, 
                                                              rsParameterBigDecimal, 
                                                              rsParameterBigInteger, 
                                                              rsParameterBoolean, 
                                                              rsParameterByte, 
                                                              rsParameterChar, 
                                                              rsParameterDate, 
                                                              rsParameterDouble, 
                                                              rsParameterFloat, 
                                                              rsParameterInteger, 
                                                              rsParameterLong, 
                                                              rsParameterShort, 
                                                              rsParameterTimestamp, 
                                                              rsParameterTime, 
                                                          }), sqDefaultsNode);  
        
        
        FakeMetadataObject rsBadDefault = createResultSet("pm1.rBadDefault", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject paramBadDefaultRet = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rsBadDefault);  //$NON-NLS-1$
        FakeMetadataObject paramBadDefaultIn = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        paramBadDefaultIn.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        paramBadDefaultIn.setDefaultValue("Clearly Not An Integer"); //$NON-NLS-1$
        QueryNode sqnBadDefault = new QueryNode("pm1.sqBadDefault", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sqBadDefault.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sqBadDefault = createVirtualProcedure("pm1.sqBadDefault", pm1, Arrays.asList(new FakeMetadataObject[] { paramBadDefaultRet, paramBadDefaultIn }), sqnBadDefault);  //$NON-NLS-1$
        
        //end case 3281
        
		FakeMetadataObject rs3 = createResultSet("pm1.rs3", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs3p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs3);  //$NON-NLS-1$
        FakeMetadataObject sp1 = createStoredProcedure("pm1.sp1", pm1, Arrays.asList(new FakeMetadataObject[] { rs3p1 }));  //$NON-NLS-1$ //$NON-NLS-2$

		FakeMetadataObject rs4 = createResultSet("pm1.rs4", pm1, new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs4p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs4);  //$NON-NLS-1$
        QueryNode sqsp1n1 = new QueryNode("pm1.sqsp1", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sp1()) as x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sqsp1 = createVirtualProcedure("pm1.sqsp1", pm1, Arrays.asList(new FakeMetadataObject[] { rs4p1 }), sqsp1n1);  //$NON-NLS-1$

		FakeMetadataObject rs6 = createResultSet("pm1.rs6", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs6p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs6);  //$NON-NLS-1$
        QueryNode sq4n1 = new QueryNode("pm1.sq4", "CREATE VIRTUAL PROCEDURE BEGIN EXEC pm1.sq1(); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq4 = createVirtualProcedure("pm1.sq4", pm1, Arrays.asList(new FakeMetadataObject[] { rs6p1}), sq4n1);  //$NON-NLS-1$

		FakeMetadataObject rs7 = createResultSet("pm1.rs7", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs7p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs7);  //$NON-NLS-1$
        FakeMetadataObject rs7p2 = createParameter("in1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq5n1 = new QueryNode("pm1.sq5", "CREATE VIRTUAL PROCEDURE BEGIN EXEC pm1.sq2(pm1.sq5.in1); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq5 = createVirtualProcedure("pm1.sq5", pm1, Arrays.asList(new FakeMetadataObject[] { rs7p1, rs7p2 }), sq5n1);  //$NON-NLS-1$

		FakeMetadataObject rs8 = createResultSet("pm1.rs8", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs8p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs8);  //$NON-NLS-1$
        QueryNode sq6n1 = new QueryNode("pm1.sq6", "CREATE VIRTUAL PROCEDURE BEGIN EXEC pm1.sq2(\'1\'); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq6 = createVirtualProcedure("pm1.sq6", pm1, Arrays.asList(new FakeMetadataObject[] { rs8p1 }), sq6n1);  //$NON-NLS-1$

		FakeMetadataObject rs9 = createResultSet("pm1.rs9", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs9p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs9);  //$NON-NLS-1$
        QueryNode sq7n1 = new QueryNode("pm1.sq7", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sq1()) as x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq7 = createVirtualProcedure("pm1.sq7", pm1, Arrays.asList(new FakeMetadataObject[] { rs9p1 }), sq7n1);  //$NON-NLS-1$

		FakeMetadataObject rs10 = createResultSet("pm1.rs10", pm1, new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs10p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs10);  //$NON-NLS-1$
        FakeMetadataObject rs10p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq8n1 = new QueryNode("pm1.sq8", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sq1()) as x WHERE x.e1=pm1.sq8.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq8 = createVirtualProcedure("pm1.sq8", pm1, Arrays.asList(new FakeMetadataObject[] { rs10p1, rs10p2 }), sq8n1);  //$NON-NLS-1$

		FakeMetadataObject rs11 = createResultSet("pm1.rs11", pm1, new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs11p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs11);  //$NON-NLS-1$
        FakeMetadataObject rs11p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq9n1 = new QueryNode("pm1.sq9", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sq2(pm1.sq9.in)) as x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq9 = createVirtualProcedure("pm1.sq9", pm1, Arrays.asList(new FakeMetadataObject[] { rs11p1, rs11p2 }), sq9n1);  //$NON-NLS-1$

		FakeMetadataObject rs12 = createResultSet("pm1.rs12", pm1, new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs12p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs12);  //$NON-NLS-1$
        FakeMetadataObject rs12p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs12p3 = createParameter("in2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq10n1 = new QueryNode("pm1.sq10", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sq2(pm1.sq10.in)) as x where e2=pm1.sq10.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq10 = createVirtualProcedure("pm1.sq10", pm1, Arrays.asList(new FakeMetadataObject[] { rs12p1, rs12p2,  rs12p3}), sq10n1);  //$NON-NLS-1$

		FakeMetadataObject rs13 = createResultSet("pm1.rs13", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs13p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs13);  //$NON-NLS-1$
        FakeMetadataObject rs13p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        FakeMetadataObject sp2 = createStoredProcedure("pm1.sp2", pm1, Arrays.asList(new FakeMetadataObject[] { rs13p1, rs13p2 }));  //$NON-NLS-1$ //$NON-NLS-2$

		FakeMetadataObject rs14 = createResultSet("pm1.rs14", pm1, new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs14p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs14);  //$NON-NLS-1$
        FakeMetadataObject rs14p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs14p3 = createParameter("in2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq11n1 = new QueryNode("pm1.sq11", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sp2(?)) as x where e2=pm1.sq11.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq11 = createVirtualProcedure("pm1.sq11", pm1, Arrays.asList(new FakeMetadataObject[] { rs14p1, rs14p2,  rs14p3}), sq11n1);  //$NON-NLS-1$

		FakeMetadataObject rs15 = createResultSet("pm1.rs15", pm1, new String[] { "count" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs15p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs15);  //$NON-NLS-1$
        FakeMetadataObject rs15p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs15p3 = createParameter("in2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq12n1 = new QueryNode("pm1.sq12", "CREATE VIRTUAL PROCEDURE BEGIN INSERT INTO pm1.g1 ( e1, e2 ) VALUES( pm1.sq12.in, pm1.sq12.in2 ); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq12 = createVirtualProcedure("pm1.sq12", pm1, Arrays.asList(new FakeMetadataObject[] { rs15p1, rs15p2, rs15p3 }), sq12n1);  //$NON-NLS-1$

		FakeMetadataObject rs16 = createResultSet("pm1.rs16", pm1, new String[] { "count" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs16p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs16);  //$NON-NLS-1$
        FakeMetadataObject rs16p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq13n1 = new QueryNode("pm1.sq13", "CREATE VIRTUAL PROCEDURE BEGIN INSERT INTO pm1.g1 ( e1, e2 ) VALUES( pm1.sq13.in, 2 ); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq13 = createVirtualProcedure("pm1.sq13", pm1, Arrays.asList(new FakeMetadataObject[] { rs16p1, rs16p2 }), sq13n1);  //$NON-NLS-1$

		FakeMetadataObject rs17 = createResultSet("pm1.rs17", pm1, new String[] { "count" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs17p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs17);  //$NON-NLS-1$
        FakeMetadataObject rs17p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs17p3 = createParameter("in2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq14n1 = new QueryNode("pm1.sq14", "CREATE VIRTUAL PROCEDURE BEGIN UPDATE pm1.g1 SET e1 = pm1.sq14.in WHERE e2 = pm1.sq14.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq14 = createVirtualProcedure("pm1.sq14", pm1, Arrays.asList(new FakeMetadataObject[] { rs17p1, rs17p2, rs17p3 }), sq14n1);  //$NON-NLS-1$

		FakeMetadataObject rs18 = createResultSet("pm1.rs17", pm1, new String[] { "count" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs18p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs18);  //$NON-NLS-1$
        FakeMetadataObject rs18p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs18p3 = createParameter("in2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null);  //$NON-NLS-1$
        QueryNode sq15n1 = new QueryNode("pm1.sq15", "CREATE VIRTUAL PROCEDURE BEGIN DELETE FROM pm1.g1 WHERE e1 = pm1.sq15.in AND e2 = pm1.sq15.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq15 = createVirtualProcedure("pm1.sq15", pm1, Arrays.asList(new FakeMetadataObject[] { rs18p1, rs18p2, rs18p3 }), sq15n1);  //$NON-NLS-1$

		QueryNode sq16n1 = new QueryNode("pm1.sq16", "CREATE VIRTUAL PROCEDURE BEGIN INSERT INTO pm1.g1 ( e1, e2 ) VALUES( 1, 2 ); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq16 = createVirtualProcedure("pm1.sq16", pm1, new ArrayList(), sq16n1);  //$NON-NLS-1$

        FakeMetadataObject rs19 = createResultSet("pm1.rs19", pm1, new String[] { "xml" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq17p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs19);  //$NON-NLS-1$
        QueryNode sq17n1 = new QueryNode("pm1.sq17", "CREATE VIRTUAL PROCEDURE BEGIN SELECT * FROM xmltest.doc1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq17 = createVirtualProcedure("pm1.sq17", pm1, Arrays.asList(new FakeMetadataObject[] { sq17p1 }), sq17n1);  //$NON-NLS-1$

		FakeMetadataObject sp3 = createStoredProcedure("pm1.sp3", pm1, new ArrayList());  //$NON-NLS-1$ //$NON-NLS-2$

        FakeMetadataObject rs20 = createResultSet("pm1.rs20", pm1, new String[] { "xml" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq18p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs20); //$NON-NLS-1$
        QueryNode sq18n1 = new QueryNode("pm1.sq18", "CREATE VIRTUAL PROCEDURE BEGIN SELECT * FROM xmltest.doc1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq18 = createVirtualProcedure("pm1.sq18", pm1, Arrays.asList(new FakeMetadataObject[] { sq18p1 }), sq18n1); //$NON-NLS-1$

        FakeMetadataObject rs21 = createResultSet("pm1.rs21", pm1, new String[] { "xml" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq19p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs21); //$NON-NLS-1$
        FakeMetadataObject sq19p2 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null); //$NON-NLS-1$
        QueryNode sq19n1 = new QueryNode("pm1.sq19", "CREATE VIRTUAL PROCEDURE BEGIN SELECT * FROM xmltest.doc4 WHERE root.node1 = param1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject sq19 = createVirtualProcedure("pm1.sq19", pm1, Arrays.asList(new FakeMetadataObject[] { sq19p1, sq19p2 }), sq19n1); //$NON-NLS-1$

        FakeMetadataObject rs22p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BIG_INTEGER, null);  //$NON-NLS-1$
        FakeMetadataObject sp4 = createStoredProcedure("pm1.sp4", pm1, Arrays.asList(new FakeMetadataObject[] { rs13p1, rs22p2 }));  //$NON-NLS-1$ //$NON-NLS-2$

        // no params or result set at all
        FakeMetadataObject sp5 = createStoredProcedure("pm1.sp5", pm1, Arrays.asList(new FakeMetadataObject[] {}));  //$NON-NLS-1$ //$NON-NLS-2$

        //virtual stored procedures
        FakeMetadataObject vsprs1 = createResultSet("pm1.vsprs1", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vspp1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsprs1); //$NON-NLS-1$
//        QueryNode vspqn1 = new QueryNode("vsp1", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; LOOP ON (SELECT e1 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e1; IF(x > 5) BEGIN CONTINUE; END END SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn1 = new QueryNode("vsp1", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e2; IF(x = 15) BEGIN BREAK; END END SELECT e1 FROM pm1.g1 where pm1.g1.e2 = x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp1 = createVirtualProcedure("pm1.vsp1", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn1); //$NON-NLS-1$

        QueryNode vspqn2 = new QueryNode("vsp2", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e2; END SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp2 = createVirtualProcedure("pm1.vsp2", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn2); //$NON-NLS-1$

        QueryNode vspqn3 = new QueryNode("vsp3", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp3 = createVirtualProcedure("pm1.vsp3", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn3); //$NON-NLS-1$

        QueryNode vspqn4 = new QueryNode("vsp4", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN IF(mycursor.e2 > 10) BEGIN BREAK; END x=mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp4 = createVirtualProcedure("pm1.vsp4", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn4); //$NON-NLS-1$
        
        QueryNode vspqn5 = new QueryNode("vsp5", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN IF(mycursor.e2 > 10) BEGIN CONTINUE; END x=mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp5 = createVirtualProcedure("pm1.vsp5", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn5); //$NON-NLS-1$

        QueryNode vspqn6 = new QueryNode("vsp6", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE (x < 15) BEGIN x=x+1; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp6 = createVirtualProcedure("pm1.vsp6", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn6); //$NON-NLS-1$

        FakeMetadataObject vspp2 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn7 = new QueryNode("vsp7", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE (x < 12) BEGIN x=x+pm1.vsp7.param1; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp7 = createVirtualProcedure("pm1.vsp7", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1, vspp2 }), vspqn7); //$NON-NLS-1$

        FakeMetadataObject vspp8 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn8 = new QueryNode("vsp8", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE (x < 12) BEGIN x=x+pm1.vsp8.param1; END SELECT e1 FROM pm1.g1 WHERE e2 >= param1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp8 = createVirtualProcedure("pm1.vsp8", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1, vspp8 }), vspqn8); //$NON-NLS-1$

        FakeMetadataObject vspp9 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn9 = new QueryNode("vsp9", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE (x < param1) BEGIN x=x+pm1.vsp9.param1; END SELECT e1 FROM pm1.g1 WHERE e2 >= param1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp9 = createVirtualProcedure("pm1.vsp9", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1, vspp9 }), vspqn9); //$NON-NLS-1$

        FakeMetadataObject vspp3 = createParameter("param1", 1, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn10 = new QueryNode("vsp10", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1 WHERE e2=param1) AS mycursor BEGIN x=mycursor.e2; END END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp10 = createVirtualProcedure("pm1.vsp10", pm1, Arrays.asList(new FakeMetadataObject[] { vspp3 }), vspqn10); //$NON-NLS-1$

        //invalid
        QueryNode vspqn11 = new QueryNode("vsp11", "CREATE VIRTUAL PROCEDURE BEGIN LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN LOOP ON (SELECT e1 FROM pm1.g1) AS mycursor BEGIN END END SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp11 = createVirtualProcedure("pm1.vsp11", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn11); //$NON-NLS-1$

        //invalid
        QueryNode vspqn12 = new QueryNode("vsp12", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN END x=mycursor.e2; SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp12 = createVirtualProcedure("pm1.vsp12", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn12); //$NON-NLS-1$

        FakeMetadataObject vsprs2 = createResultSet("pm1.vsprs2", pm1, new String[] { "e1", "const" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject vspp4 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsprs2); //$NON-NLS-1$
        QueryNode vspqn13 = new QueryNode("vsp13", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; LOOP ON (SELECT e1 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e1; END SELECT x, 5; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp13 = createVirtualProcedure("pm1.vsp13", pm1, Arrays.asList(new FakeMetadataObject[] { vspp4 }), vspqn13); //$NON-NLS-1$

        QueryNode vspqn14 = new QueryNode("vsp14", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO #temptable FROM pm1.g1; SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp14 = createVirtualProcedure("pm1.vsp14", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn14); //$NON-NLS-1$

        QueryNode vspqn15 = new QueryNode("vsp15", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable, pm1.g2 WHERE #temptable.e2 = pm1.g2.e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp15 = createVirtualProcedure("pm1.vsp15", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn15); //$NON-NLS-1$
        
        QueryNode vspqn16 = new QueryNode("vsp16", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT a.e1 FROM (SELECT pm1.g2.e1 FROM #temptable, pm1.g2 WHERE #temptable.e2 = pm1.g2.e2) AS a; END"); //$NON-NLS-1$ //$NON-NLS-2$
        //QueryNode vspqn16 = new QueryNode("vsp16", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT e1 FROM #temptable where e1 in (SELECT pm1.g2.e1 FROM  #temptable, pm1.g2 WHERE #temptable.e2 = pm1.g2.e2); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp16 = createVirtualProcedure("pm1.vsp16", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn16); //$NON-NLS-1$

        QueryNode vspqn17 = new QueryNode("vsp17", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; SELECT e1, e2 INTO #temptable FROM pm1.g1; LOOP ON (SELECT e1, e2 FROM #temptable) AS mycursor BEGIN x=mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp17 = createVirtualProcedure("pm1.vsp17", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn17); //$NON-NLS-1$

        //invalid
         QueryNode vspqn18 = new QueryNode("vsp18", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO temptable FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
         FakeMetadataObject vsp18 = createVirtualProcedure("pm1.vsp18", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn18); //$NON-NLS-1$

        QueryNode vspqn19 = new QueryNode("vsp19", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO #temptable FROM pm1.g1; SELECT e1 INTO #temptable FROM pm1.g1; SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp19 = createVirtualProcedure("pm1.vsp19", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn19); //$NON-NLS-1$

        QueryNode vspqn20 = new QueryNode("vsp20", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO #temptable FROM pm1.g1; INSERT INTO #temptable(e1) VALUES( 'Fourth'); SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp20 = createVirtualProcedure("pm1.vsp20", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn20); //$NON-NLS-1$

        FakeMetadataObject vspp21 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn21 = new QueryNode("vsp21", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; INSERT INTO #temptable(#temptable.e1, e2) VALUES( 'Fourth', param1); SELECT e1, e2 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp21 = createVirtualProcedure("pm1.vsp21", pm1, Arrays.asList(new FakeMetadataObject[] { vspp4, vspp21 }), vspqn21); //$NON-NLS-1$

        FakeMetadataObject vspp22 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn22 = new QueryNode("vsp22", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1 where e2 > param1; SELECT e1, e2 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp22 = createVirtualProcedure("pm1.vsp22", pm1, Arrays.asList(new FakeMetadataObject[] { vspp4, vspp22 }), vspqn22); //$NON-NLS-1$

        FakeMetadataObject vspp23 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn23 = new QueryNode("vsp23", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; SELECT e1, e2 INTO #temptable FROM pm1.g1 where e2 > param1; x = SELECT e1 FROM #temptable WHERE e2=15; SELECT x, 15; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp23 = createVirtualProcedure("pm1.vsp23", pm1, Arrays.asList(new FakeMetadataObject[] { vspp4, vspp23 }), vspqn23); //$NON-NLS-1$
 
        QueryNode vspqn24 = new QueryNode("vsp24", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable WHERE #temptable.e2=15; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp24 = createVirtualProcedure("pm1.vsp24", pm1, Arrays.asList(new FakeMetadataObject[] { vspp4 }), vspqn24); //$NON-NLS-1$
 
        QueryNode vspqn25 = new QueryNode("vsp25", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO #temptable FROM pm1.g1 WHERE e1 ='no match'; SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp25 = createVirtualProcedure("pm1.vsp25", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn25); //$NON-NLS-1$

        QueryNode vspqn27 = new QueryNode("vsp27", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 from (exec pm1.vsp25())as c; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp27 = createVirtualProcedure("pm1.vsp27", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn27); //$NON-NLS-1$

        QueryNode vspqn28 = new QueryNode("vsp28", "CREATE VIRTUAL PROCEDURE BEGIN SELECT 0 AS e1 ORDER BY e1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp28 = createVirtualProcedure("pm1.vsp28", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn28); //$NON-NLS-1$

        QueryNode vspqn29 = new QueryNode("vsp29", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM pm1.g1 ORDER BY e1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp29 = createVirtualProcedure("pm1.vsp29", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn29); //$NON-NLS-1$

        FakeMetadataObject vsprs30 = createResultSet("pm1.vsprs30", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp30p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, null, vsprs30); //$NON-NLS-1$
        QueryNode vspqn30 = new QueryNode("vsp30", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp30 = createVirtualProcedure("pm1.vsp30", pm1, Arrays.asList(new FakeMetadataObject[] {vsp30p1}), vspqn30); //$NON-NLS-1$        

        FakeMetadataObject vsprs31 = createResultSet("pm1.vsprs31", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp31p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, null, vsprs31); //$NON-NLS-1$
        FakeMetadataObject vsp31p2 = createParameter("p1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn31 = new QueryNode("vsp31", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM pm1.g1 WHERE e2 = pm1.vsp31.p1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp31 = createVirtualProcedure("pm1.vsp31", pm1, Arrays.asList(new FakeMetadataObject[] {vsp31p1, vsp31p2}), vspqn31); //$NON-NLS-1$        

        QueryNode vspqn38 = new QueryNode("vsp38", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer VARIABLES.y; VARIABLES.y=5; EXEC pm1.vsp7(VARIABLES.y); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp38 = createVirtualProcedure("pm1.vsp38", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn38); //$NON-NLS-1$
  
        QueryNode vspqn39 = new QueryNode("vsp39", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer VARIABLES.x; VARIABLES.x=5; EXEC pm1.vsp7(VARIABLES.x); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp39 = createVirtualProcedure("pm1.vsp39", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn39); //$NON-NLS-1$

        QueryNode vspqn40 = new QueryNode("vsp40", "CREATE VIRTUAL PROCEDURE BEGIN LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN EXEC pm1.vsp41(); END END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp40 = createVirtualProcedure("pm1.vsp40", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn40); //$NON-NLS-1$

        QueryNode vspqn41 = new QueryNode("vsp41", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM pm1.g1 where e2=15; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp41 = createVirtualProcedure("pm1.vsp41", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn41); //$NON-NLS-1$

        vm1g1.putProperty(FakeMetadataObject.Props.INSERT_PROCEDURE, "CREATE PROCEDURE BEGIN ROWS_UPDATED = INSERT INTO pm1.g1(e1, e2, e3, e4) values(INPUT.e1, INPUT.e2, INPUT.e3, INPUT.e4); END"); //$NON-NLS-1$
        vm1g1.putProperty(FakeMetadataObject.Props.UPDATE_PROCEDURE, "CREATE PROCEDURE BEGIN ROWS_UPDATED = UPDATE pm1.g1 SET e1 = INPUT.e1, e2 = INPUT.e2, e3 = INPUT.e3, e4=INPUT.e4 WHERE TRANSLATE CRITERIA; END"); //$NON-NLS-1$       
        vm1g1.putProperty(FakeMetadataObject.Props.DELETE_PROCEDURE, "CREATE PROCEDURE BEGIN ROWS_UPDATED = DELETE FROM pm1.g1 WHERE TRANSLATE CRITERIA; END"); //$NON-NLS-1$       

        vm1g37.putProperty(FakeMetadataObject.Props.INSERT_PROCEDURE, "CREATE PROCEDURE BEGIN ROWS_UPDATED = INSERT INTO pm4.g1(e1, e2, e3, e4) values(INPUT.e1, INPUT.e2, INPUT.e3, INPUT.e4); END"); //$NON-NLS-1$
        vm1g37.putProperty(FakeMetadataObject.Props.DELETE_PROCEDURE, "CREATE PROCEDURE BEGIN ROWS_UPDATED = DELETE FROM pm4.g1 where translate criteria; END"); //$NON-NLS-1$
        QueryNode vspqn37 = new QueryNode("vsp37", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; VARIABLES.x=5; INSERT INTO vm1.g1(e2) values(VARIABLES.x); SELECT ROWCOUNT; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp37 = createVirtualProcedure("pm1.vsp37", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn37); //$NON-NLS-1$

        QueryNode vspqn33 = new QueryNode("vsp33", new StringBuffer("CREATE VIRTUAL PROCEDURE")  //$NON-NLS-1$//$NON-NLS-2$
                                                            .append(" BEGIN") //$NON-NLS-1$
                                                            .append(" SELECT 3 AS temp1 INTO #myTempTable;") //$NON-NLS-1$
                                                            .append(" SELECT 2 AS temp1 INTO #myTempTable;") //$NON-NLS-1$
                                                            .append(" SELECT 1 AS temp1 INTO #myTempTable;") //$NON-NLS-1$
                                                            .append(" SELECT temp1 AS e1 FROM #myTempTable ORDER BY e1;") //$NON-NLS-1$
                                                            .append(" END").toString() //$NON-NLS-1$
                                         );
        FakeMetadataObject vsp33 = createVirtualProcedure("pm1.vsp33", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn33); //$NON-NLS-1$

        QueryNode vspqn35 = new QueryNode("vsp35", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer VARIABLES.ID; VARIABLES.ID = pm1.vsp35.p1; SELECT e1 FROM pm1.g1 WHERE e2 = VARIABLES.ID; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp35 = createVirtualProcedure("pm1.vsp35", pm1, Arrays.asList(new FakeMetadataObject[] {vsp31p1, vsp31p2}), vspqn35); //$NON-NLS-1$        

        QueryNode vspqn34 = new QueryNode("vsp34", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, 0 AS const FROM pm1.g1 ORDER BY const; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp34 = createVirtualProcedure("pm1.vsp34", pm1, Arrays.asList(new FakeMetadataObject[] { vspp4 }), vspqn34); //$NON-NLS-1$

        QueryNode vspqn45 = new QueryNode("vsp45", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable where #temptable.e1 in (SELECT pm1.g2.e1 FROM pm1.g2 ); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp45 = createVirtualProcedure("pm1.vsp45", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn45); //$NON-NLS-1$
        
        // Virtual group w/ procedure in transformation, optional params, named parameter syntax
        QueryNode vspqn47 = new QueryNode("vsp47", "CREATE VIRTUAL PROCEDURE BEGIN IF (pm1.vsp47.param1 IS NOT NULL) BEGIN SELECT 'FOO' as e1, pm1.vsp47.param1 as e2; END ELSE BEGIN SELECT pm1.vsp47.param2 as e1, 2112 as e2; END END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsprs47 = createResultSet("pm1.vsprs47", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject vspp47_1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsprs47); //$NON-NLS-1$
        FakeMetadataObject vspp47_2 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        vspp47_2.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        FakeMetadataObject vspp47_3 = createParameter("param2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null); //$NON-NLS-1$
        vspp47_3.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        FakeMetadataObject vsp47 = createVirtualProcedure("pm1.vsp47", pm1, Arrays.asList(new FakeMetadataObject[] { vspp47_1, vspp47_2, vspp47_3 }), vspqn47); //$NON-NLS-1$
        QueryNode vgvpn7 = new QueryNode("vm1.vgvp7", "SELECT P.e2 as ve3, P.e1 as ve4 FROM (EXEC pm1.vsp47(param1=vm1.vgvp7.ve1, param2=vm1.vgvp7.ve2)) as P"); //$NON-NLS-1$ //$NON-NLS-2$
//        QueryNode vgvpn7 = new QueryNode("vm1.vgvp7", "SELECT P.e2 as ve1, P.e1 as ve2 FROM (EXEC pm1.vsp47(vm1.vgvp7.ve1, vm1.vgvp7.ve2)) as P"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vgvp7 = createVirtualGroup("vm1.vgvp7", vm1, vgvpn7); //$NON-NLS-1$
        FakeMetadataObject vgvp7e1 = FakeMetadataFactory.createElement("vm1.vgvp7.ve1", vgvp7, DataTypeManager.DefaultDataTypes.INTEGER, 0); //$NON-NLS-1$
        vgvp7e1.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp7e2 = FakeMetadataFactory.createElement("vm1.vgvp7.ve2", vgvp7, DataTypeManager.DefaultDataTypes.STRING, 1); //$NON-NLS-1$
        vgvp7e2.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp7e3 = FakeMetadataFactory.createElement("vm1.vgvp7.ve3", vgvp7, DataTypeManager.DefaultDataTypes.STRING, 2); //$NON-NLS-1$
        FakeMetadataObject vgvp7e4 = FakeMetadataFactory.createElement("vm1.vgvp7.ve4", vgvp7, DataTypeManager.DefaultDataTypes.STRING, 3); //$NON-NLS-1$
        
        
        //invalid
        QueryNode vspqn32 = new QueryNode("vsp32", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS #mycursor BEGIN IF(#mycursor.e2 > 10) BEGIN CONTINUE; END x=#mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp32 = createVirtualProcedure("pm1.vsp32", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn32); //$NON-NLS-1$

        //virtual group with procedure in transformation
        QueryNode vspqn26 = new QueryNode("vsp26", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e2 >= pm1.vsp26.param1 and e1 = pm1.vsp26.param2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vspp26_1 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        FakeMetadataObject vspp26_2 = createParameter("param2", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null); //$NON-NLS-1$
        FakeMetadataObject vsprs3 = createResultSet("pm1.vsprs3", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject vspp6 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsprs3); //$NON-NLS-1$
        FakeMetadataObject vsp26 = createVirtualProcedure("pm1.vsp26", pm1, Arrays.asList(new FakeMetadataObject[] { vspp6, vspp26_1, vspp26_2 }), vspqn26); //$NON-NLS-1$
		QueryNode vgvpn1 = new QueryNode("vm1.vgvp1", "SELECT P.e1 as ve3 FROM (EXEC pm1.vsp26(vm1.vgvp1.ve1, vm1.vgvp1.ve2)) as P"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vgvp1 = createVirtualGroup("vm1.vgvp1", vm1, vgvpn1); //$NON-NLS-1$
        FakeMetadataObject vgvp1e1 = FakeMetadataFactory.createElement("vm1.vgvp1.ve1", vgvp1, DataTypeManager.DefaultDataTypes.INTEGER, 0); //$NON-NLS-1$
        vgvp1e1.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp1e2 = FakeMetadataFactory.createElement("vm1.vgvp1.ve2", vgvp1, DataTypeManager.DefaultDataTypes.STRING, 1); //$NON-NLS-1$
        vgvp1e2.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp1e3 = FakeMetadataFactory.createElement("vm1.vgvp1.ve3", vgvp1, DataTypeManager.DefaultDataTypes.STRING, 2); //$NON-NLS-1$
      
		QueryNode vgvpn2 = new QueryNode("vm1.vgvp2", "SELECT P.e1 as ve3 FROM (EXEC pm1.vsp26(vm1.vgvp2.ve1, vm1.vgvp2.ve2)) as P where P.e1='a'"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vgvp2 = createVirtualGroup("vm1.vgvp2", vm1, vgvpn2); //$NON-NLS-1$
        FakeMetadataObject vgvp2e1 = FakeMetadataFactory.createElement("vm1.vgvp2.ve1", vgvp2, DataTypeManager.DefaultDataTypes.INTEGER, 0); //$NON-NLS-1$
        vgvp2e1.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp2e2 = FakeMetadataFactory.createElement("vm1.vgvp2.ve2", vgvp2, DataTypeManager.DefaultDataTypes.STRING, 1); //$NON-NLS-1$
        vgvp2e2.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp2e3 = FakeMetadataFactory.createElement("vm1.vgvp2.ve3", vgvp2, DataTypeManager.DefaultDataTypes.STRING, 2); //$NON-NLS-1$
   
		QueryNode vgvpn3 = new QueryNode("vm1.vgvp3", "SELECT P.e1 as ve3 FROM (EXEC pm1.vsp26(vm1.vgvp3.ve1, vm1.vgvp3.ve2)) as P, pm1.g2 where P.e1=g2.e1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vgvp3 = createVirtualGroup("vm1.vgvp3", vm1, vgvpn3); //$NON-NLS-1$
        FakeMetadataObject vgvp3e1 = FakeMetadataFactory.createElement("vm1.vgvp3.ve1", vgvp3, DataTypeManager.DefaultDataTypes.INTEGER, 0); //$NON-NLS-1$
        vgvp3e1.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp3e2 = FakeMetadataFactory.createElement("vm1.vgvp3.ve2", vgvp3, DataTypeManager.DefaultDataTypes.STRING, 1); //$NON-NLS-1$
        vgvp3e2.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp3e3 = FakeMetadataFactory.createElement("vm1.vgvp3.ve3", vgvp3, DataTypeManager.DefaultDataTypes.STRING, 2); //$NON-NLS-1$

		QueryNode vgvpn4 = new QueryNode("vm1.vgvp4", "SELECT P.e1 as ve3 FROM (EXEC pm1.vsp26(vm1.vgvp4.ve1, vm1.vgvp4.ve2)) as P, vm1.g1 where P.e1=g1.e1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vgvp4 = createVirtualGroup("vm1.vgvp4", vm1, vgvpn4); //$NON-NLS-1$
        FakeMetadataObject vgvp4e1 = FakeMetadataFactory.createElement("vm1.vgvp4.ve1", vgvp4, DataTypeManager.DefaultDataTypes.INTEGER, 0); //$NON-NLS-1$
        vgvp4e1.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp4e2 = FakeMetadataFactory.createElement("vm1.vgvp4.ve2", vgvp4, DataTypeManager.DefaultDataTypes.STRING, 1); //$NON-NLS-1$
        vgvp4e2.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp4e3 = FakeMetadataFactory.createElement("vm1.vgvp4.ve3", vgvp4, DataTypeManager.DefaultDataTypes.STRING, 2); //$NON-NLS-1$
        
		QueryNode vgvpn5 = new QueryNode("vm1.vgvp5", "SELECT * FROM vm1.vgvp4 where vm1.vgvp4.ve1=vm1.vgvp5.ve1 and  vm1.vgvp4.ve2=vm1.vgvp5.ve2"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vgvp5 = createVirtualGroup("vm1.vgvp5", vm1, vgvpn5); //$NON-NLS-1$
        FakeMetadataObject vgvp5e1 = FakeMetadataFactory.createElement("vm1.vgvp5.ve1", vgvp5, DataTypeManager.DefaultDataTypes.INTEGER, 0); //$NON-NLS-1$
        vgvp5e1.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp5e2 = FakeMetadataFactory.createElement("vm1.vgvp5.ve2", vgvp5, DataTypeManager.DefaultDataTypes.STRING, 1); //$NON-NLS-1$
        vgvp5e2.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp5e3 = FakeMetadataFactory.createElement("vm1.vgvp5.ve3", vgvp5, DataTypeManager.DefaultDataTypes.STRING, 2); //$NON-NLS-1$

		QueryNode vgvpn6 = new QueryNode("vm1.vgvp6", "SELECT P.e1 as ve3, P.e2 as ve4 FROM (EXEC pm1.vsp26(vm1.vgvp6.ve1, vm1.vgvp6.ve2)) as P"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vgvp6 = createVirtualGroup("vm1.vgvp6", vm1, vgvpn6); //$NON-NLS-1$
        FakeMetadataObject vgvp6e1 = FakeMetadataFactory.createElement("vm1.vgvp6.ve1", vgvp6, DataTypeManager.DefaultDataTypes.INTEGER, 0); //$NON-NLS-1$
        vgvp6e1.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp6e2 = FakeMetadataFactory.createElement("vm1.vgvp6.ve2", vgvp6, DataTypeManager.DefaultDataTypes.STRING, 1); //$NON-NLS-1$
        vgvp6e2.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vgvp6e3 = FakeMetadataFactory.createElement("vm1.vgvp6.ve3", vgvp6, DataTypeManager.DefaultDataTypes.STRING, 2); //$NON-NLS-1$
        FakeMetadataObject vgvp6e4 = FakeMetadataFactory.createElement("vm1.vgvp6.ve4", vgvp6, DataTypeManager.DefaultDataTypes.INTEGER, 3); //$NON-NLS-1$

        //virtual group with two elements. One selectable, one not.
        QueryNode vm1g35n1 = new QueryNode("vm1.g35", "SELECT e1, e2 FROM pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g35 = createVirtualGroup("vm1.g35", vm1, vm1g35n1); //$NON-NLS-1$
        FakeMetadataObject vm1g35e1 = FakeMetadataFactory.createElement("vm1.g35.e1", vm1g35, DataTypeManager.DefaultDataTypes.STRING, 1); //$NON-NLS-1$
        vm1g35e1.putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        FakeMetadataObject vm1g35e2 = FakeMetadataFactory.createElement("vm1.g35.e2", vm1g35, DataTypeManager.DefaultDataTypes.STRING, 2); //$NON-NLS-1$
		
        FakeMetadataObject vsprs36 = createResultSet("pm1.vsprs36", pm1, new String[] { "x" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp36p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, null, vsprs36); //$NON-NLS-1$
        FakeMetadataObject vsp36p2 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn36 = new QueryNode("vsp36", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x = pm1.vsp36.param1 * 2; SELECT x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp36 = createVirtualProcedure("pm1.vsp36", pm1, Arrays.asList(new FakeMetadataObject[] { vsp36p1, vsp36p2 }), vspqn36); //$NON-NLS-1$

        FakeMetadataObject vsprs42 = createResultSet("pm1.vsprs42", pm1, new String[] { "x" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp42p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, null, vsprs42); //$NON-NLS-1$
        FakeMetadataObject vsp42p2 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn42 = new QueryNode("vsp42", "CREATE VIRTUAL PROCEDURE BEGIN IF (pm1.vsp42.param1 > 0) SELECT 1 AS x; ELSE SELECT 0 AS x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp42 = createVirtualProcedure("pm1.vsp42", pm1, Arrays.asList(new FakeMetadataObject[] { vsp42p1, vsp42p2 }), vspqn42); //$NON-NLS-1$

        FakeMetadataObject vspp44 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn44 = new QueryNode("vsp44", "CREATE VIRTUAL PROCEDURE BEGIN SELECT pm1.vsp44.param1 INTO #temptable; SELECT e1 from pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$    
        FakeMetadataObject vsp44 = createVirtualProcedure("pm1.vsp44", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1, vspp44 }), vspqn44); //$NON-NLS-1$

        FakeMetadataObject vspp43 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn43 = new QueryNode("vsp43", "CREATE VIRTUAL PROCEDURE BEGIN exec pm1.vsp44(pm1.vsp43.param1); END"); //$NON-NLS-1$ //$NON-NLS-2$    
        FakeMetadataObject vsp43 = createVirtualProcedure("pm1.vsp43", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1, vspp43 }), vspqn43); //$NON-NLS-1$
        
        QueryNode vspqn46 = new QueryNode("vsp46", "CREATE VIRTUAL PROCEDURE BEGIN create local temporary table #temptable (e1 string, e2 string); LOOP ON (SELECT e1 FROM pm1.g1) AS mycursor BEGIN select mycursor.e1, a.e1 as e2 into #temptable from (SELECT pm1.g1.e1 FROM pm1.g1 where pm1.g1.e1 = mycursor.e1) a; END SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp46 = createVirtualProcedure("pm1.vsp46", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn46); //$NON-NLS-1$
        
        FakeMetadataObject vsp48rs = createResultSet("pm1vsp48.rs", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject vsp48p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsp48rs);  //$NON-NLS-1$
        FakeMetadataObject vsp48p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode vspqn48 = new QueryNode("vsp48", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; SELECT e1 FROM (EXEC pm1.sq2(pm1.vsp48.in)) as e; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp48 = createVirtualProcedure("pm1.vsp48", pm1, Arrays.asList(new FakeMetadataObject[] { vsp48p1, vsp48p2 }), vspqn48); //$NON-NLS-1$
        
        FakeMetadataObject vsp49rs = createResultSet("pm1vsp49.rs", pm1, new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        FakeMetadataObject vsp49p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsp49rs);  //$NON-NLS-1$
        QueryNode vspqn49 = new QueryNode("vsp49", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'b'; EXEC pm1.sq2(x); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp49 = createVirtualProcedure("pm1.vsp49", pm1, Arrays.asList(new FakeMetadataObject[] { vsp49p1 }), vspqn49); //$NON-NLS-1$

        FakeMetadataObject vsp50rs = createResultSet("pm1vsp50.rs", pm1, new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject vsp50p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsp50rs);  //$NON-NLS-1$
        QueryNode vspqn50 = new QueryNode("vsp50", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'b'; SELECT e1 FROM (EXEC pm1.sq2(x)) as e; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp50 = createVirtualProcedure("pm1.vsp50", pm1, Arrays.asList(new FakeMetadataObject[] { vsp50p1 }), vspqn50); //$NON-NLS-1$

        FakeMetadataObject vsp51rs = createResultSet("pm1vsp51.rs", pm1, new String[] { "result" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject vsp51p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsp51rs);  //$NON-NLS-1$
        QueryNode vspqn51 = new QueryNode("vsp51", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'b'; LOOP ON (SELECT e1 FROM (EXEC pm1.sq2(x)) as e) AS c BEGIN x = x || 'b'; END SELECT x AS result; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp51 = createVirtualProcedure("pm1.vsp51", pm1, Arrays.asList(new FakeMetadataObject[] { vsp51p1 }), vspqn51); //$NON-NLS-1$

        FakeMetadataObject vsp52rs = createResultSet("pm1vsp52.rs", pm1, new String[] { "result" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject vsp52p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsp52rs);  //$NON-NLS-1$
        QueryNode vspqn52 = new QueryNode("vsp52", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'c'; x = SELECT e1 FROM (EXEC pm1.sq2(x)) as e; SELECT x AS result; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp52 = createVirtualProcedure("pm1.vsp52", pm1, Arrays.asList(new FakeMetadataObject[] { vsp52p1 }), vspqn52); //$NON-NLS-1$

        FakeMetadataObject vsp53rs = createResultSet("pm1vsp53.rs", pm1, new String[] { "result" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject vsp53p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsp53rs);  //$NON-NLS-1$
        FakeMetadataObject vsp53p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode vspqn53 = new QueryNode("vsp53", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'b'; LOOP ON (SELECT e1 FROM (EXEC pm1.sq2(pm1.vsp53.in)) as e) AS c BEGIN x = x || 'b'; END SELECT x AS result; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp53 = createVirtualProcedure("pm1.vsp53", pm1, Arrays.asList(new FakeMetadataObject[] { vsp53p1, vsp53p2 }), vspqn53); //$NON-NLS-1$

        FakeMetadataObject vsp54rs = createResultSet("pm1vsp54.rs", pm1, new String[] { "result" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ 
        FakeMetadataObject vsp54p1 = createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, vsp54rs);  //$NON-NLS-1$
        FakeMetadataObject vsp54p2 = createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode vspqn54 = new QueryNode("vsp54", "CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'c'; x = SELECT e1 FROM (EXEC pm1.sq2(pm1.vsp54.in)) as e; SELECT x AS result; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp54 = createVirtualProcedure("pm1.vsp54", pm1, Arrays.asList(new FakeMetadataObject[] { vsp54p1, vsp54p2 }), vspqn54); //$NON-NLS-1$
        
        FakeMetadataObject vspp55 = createParameter("param1", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn55 = new QueryNode("vsp55", "CREATE VIRTUAL PROCEDURE BEGIN select e1, param1 as a from vm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp55 = createVirtualProcedure("pm1.vsp55", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1, vspp55 }), vspqn55); //$NON-NLS-1$

        QueryNode vspqn56 = new QueryNode("vsp56", "CREATE VIRTUAL PROCEDURE BEGIN SELECT * INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp56 = createVirtualProcedure("pm1.vsp56", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn56); //$NON-NLS-1$

        QueryNode vspqn57 = new QueryNode("vsp57", "CREATE VIRTUAL PROCEDURE BEGIN SELECT * INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable order by #temptable.e1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp57 = createVirtualProcedure("pm1.vsp57", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn57); //$NON-NLS-1$

        FakeMetadataObject vspp58 = createParameter("inp", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER, null); //$NON-NLS-1$
        QueryNode vspqn58 = new QueryNode("vsp58", "CREATE VIRTUAL PROCEDURE BEGIN SELECT vsp58.inp; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp58 = createVirtualProcedure("pm1.vsp58", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1, vspp58 }), vspqn58); //$NON-NLS-1$
        
        QueryNode vspqn59 = new QueryNode("vsp59", "CREATE VIRTUAL PROCEDURE BEGIN SELECT * INTO #temp FROM pm5.g3;INSERT INTO #temp (e1, e2) VALUES('integer',1); END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp59 = createVirtualProcedure("pm5.vsp59", pm6, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn59); //$NON-NLS-1$
        
        QueryNode vspqn60 = new QueryNode("vsp60", "CREATE VIRTUAL PROCEDURE BEGIN create local temporary table temp_table (column1 string);insert into temp_table (column1) values ('First');insert into temp_table (column1) values ('Second');insert into temp_table (column1) values ('Third');select * from temp_table; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp60 = createVirtualProcedure("pm1.vsp60", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn60); //$NON-NLS-1$

        QueryNode vspqn61 = new QueryNode("vsp61", "CREATE VIRTUAL PROCEDURE BEGIN create local temporary table temp_table (column1 string);insert into temp_table (column1) values ('First');drop table temp_table;create local temporary table temp_table (column1 string);insert into temp_table (column1) values ('First');insert into temp_table (column1) values ('Second');insert into temp_table (column1) values ('Third');select * from temp_table; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp61 = createVirtualProcedure("pm1.vsp61", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn61); //$NON-NLS-1$

        QueryNode vspqn62 = new QueryNode("vsp62", "CREATE VIRTUAL PROCEDURE BEGIN create local temporary table temp_table (column1 string); select e1 as column1 into temp_table from pm1.g1;select * from temp_table; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp62 = createVirtualProcedure("pm1.vsp62", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn62); //$NON-NLS-1$

        QueryNode vspqn63 = new QueryNode("vsp63", "CREATE VIRTUAL PROCEDURE BEGIN declare string o; if(1>0) begin declare string a; a='b'; o=a; end if(1>0) begin declare string a; a='c'; o=a; end  select o; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vsp63 = createVirtualProcedure("pm1.vsp63", pm1, Arrays.asList(new FakeMetadataObject[] { vspp1 }), vspqn63); //$NON-NLS-1$

        // Add all objects to the store
		FakeMetadataStore store = new FakeMetadataStore();
		store.addObject(vsp63);
		store.addObject(pm1);
		store.addObject(pm1g1);		
		store.addObjects(pm1g1e);
		store.addObject(pm1g2);		
		store.addObjects(pm1g2e);
		store.addObject(pm1g3);	
		store.addObjects(pm1g3e);
        store.addObject(pm1g4);
        store.addObjects(pm1g4e);
        store.addObject(pm1g5);
        store.addObjects(pm1g5e);
        store.addObject(pm1g6);
        store.addObjects(pm1g6e);
        store.addObject(pm1table);
        store.addObjects(pm1tablee);
        
		store.addObject(pm2);
		store.addObject(pm2g1);		
		store.addObjects(pm2g1e);
		store.addObject(pm2g2);		
		store.addObjects(pm2g2e);
		store.addObject(pm2g3);		
		store.addObjects(pm2g3e);

		store.addObject(pm3);
        store.addObject(pm3g1);
        store.addObjects(pm3g1e);
        store.addObject(pm3g2);
        store.addObjects(pm3g2e);

        store.addObject(pm4);
        store.addObject(pm4g1);
        store.addObjects(pm4g1e);
        store.addObject(pm4g2);
        store.addObjects(pm4g2e);
        store.addObject(pm4g1ap1);
        store.addObject(pm4g2ap1);
		store.addObject(pm4g2ap2);

		store.addObject(pm5);
		store.addObject(pm5g1);
		store.addObjects(pm5g1e);
		store.addObject(pm5g2);
		store.addObjects(pm5g2e);
		store.addObject(pm5g3);
		store.addObjects(pm5g3e);
		store.addObject(pm5g1ap1);
		store.addObject(pm5g2ap1);
		store.addObject(pm5g2ap2);

        store.addObject(pm6);
        store.addObject(pm6g1);
        store.addObjects(pm6g1e);

        store.addObject(tm1);
        store.addObject(tm1g1);
        store.addObjects(tm1g1e);
        store.addObject(tm1g2);
        store.addObjects(tm1g2e);        

		store.addObject(vm1);
		store.addObject(vm1g1);
		store.addObjects(vm1g1e);
        store.addObject(vm1g1_defect10711);
        store.addObjects(vm1g1e_defect10711);
        store.addObject(vm1g1_defect12081);
        store.addObjects(vm1g1e_defect12081);
        store.addObject(vm1g1c);
        store.addObjects(vm1g1ce);
        store.addObject(vm1g2a);
        store.addObjects(vm1g2ae);
		store.addObject(vm1g2);
		store.addObjects(vm1g2e);
		store.addObject(vm1g4);
		store.addObjects(vm1g4e);
        store.addObject(vm1g5);
        store.addObjects(vm1g5e);
        store.addObject(vm1g6);
        store.addObjects(vm1g6e);
        store.addObject(vm1g7);
        store.addObjects(vm1g7e);
        store.addObject(vm1g8);
        store.addObjects(vm1g8e);
        store.addObject(vm1g9);
        store.addObjects(vm1g9e);
        store.addObject(vm1g10);
        store.addObjects(vm1g10e);
        store.addObject(vm1g11);
        store.addObjects(vm1g11e);
        store.addObject(vm1g12);
        store.addObjects(vm1g12e);
        store.addObject(vm1g13);
        store.addObjects(vm1g13e);
        store.addObject(vm1g14);
        store.addObjects(vm1g14e);
        store.addObject(vm1g15);
        store.addObjects(vm1g15e);
        store.addObject(vm1g16);
        store.addObjects(vm1g16e);
        store.addObject(vm1g17);
        store.addObjects(vm1g17e);
        store.addObject(vm1g18);
        store.addObjects(vm1g18e);
        store.addObject(vm1g19);
        store.addObjects(vm1g19e);
        store.addObject(vm1g20);
        store.addObjects(vm1g20e);
        store.addObject(vm1g21);
        store.addObjects(vm1g21e);
        store.addObject(vm1g22);
        store.addObjects(vm1g22e);
        store.addObject(vm1g23);
        store.addObjects(vm1g23e);
        store.addObject(vm1g24);
        store.addObjects(vm1g24e);
        store.addObject(vm1g25);
        store.addObjects(vm1g25e);
        store.addObject(vm1g26);
        store.addObjects(vm1g26e);
        store.addObject(vm1g27);
        store.addObjects(vm1g27e);
        store.addObject(vm1g28);
        store.addObjects(vm1g28e);
        store.addObject(vm1g29);
        store.addObjects(vm1g29e);
        store.addObject(vm1g30);
        store.addObjects(vm1g30e);
        store.addObject(vm1g31);
        store.addObjects(vm1g31e);
        store.addObject(vm1g32);
        store.addObjects(vm1g32e);
        store.addObject(vm1g33);
        store.addObjects(vm1g33e);
        store.addObject(vm1g34);
        store.addObjects(vm1g34e);
        store.addObject(vm1g36);
        store.addObjects(vm1g36e);
        store.addObject(vm1g37);
        store.addObjects(vm1g37e);
        store.addObject(vm1g37ap1);
        store.addObject(vm1g38);
        store.addObjects(vm1g38e);
                
        store.addObject(vm2);
		store.addObject(vm2g1);
		store.addObjects(vm2g1e);

        store.addObject(doc1);
        store.addObject(doc2);
        store.addObject(doc3);
        store.addObject(doc4);        
        store.addObject(doc5);
        store.addObject(doc6);
        store.addObject(docAmbiguous1);
        store.addObject(docAmbiguous2);
		store.addObjects(docE1);
		store.addObjects(docE2);
		store.addObjects(docE3);
		store.addObjects(docE4);
        store.addObjects(docE5);
        store.addObject(vm1mc1);
        store.addObjects(vm1mc1e);
        store.addObjects(docE6);
        
        
        store.addObject(rs1);
        store.addObject(sq1);
        store.addObject(rs2);
        store.addObject(sq2);
        store.addObject(rs3);
        store.addObject(sp1);
        store.addObject(rs4);
        store.addObject(sqsp1);
        store.addObject(rs5);
		store.addObject(rs5a);
        store.addObject(sq3);
		store.addObject(sq3a);
        store.addObject(sq3b);
        store.addObject(sqDefaults);
        store.addObject(sqBadDefault);
        store.addObject(rs6);
        store.addObject(sq4);
        store.addObject(rs7);
        store.addObject(sq5);
        store.addObject(rs8);
        store.addObject(sq6);
        store.addObject(rs9);
        store.addObject(sq7);
        store.addObject(rs10);
        store.addObject(sq8);
        store.addObject(rs11);
        store.addObject(sq9);
        store.addObject(rs12);
        store.addObject(sq10);
        store.addObject(rs13);
        store.addObject(sp2);
        store.addObject(rs14);
        store.addObject(sq11);
        store.addObject(rs15);
        store.addObject(sq12);
        store.addObject(rs16);
        store.addObject(sq13);
        store.addObject(rs17);
        store.addObject(sq14);
        store.addObject(rs18);
        store.addObject(sq15);
        store.addObject(sq16);
        store.addObject(rs19);
        store.addObject(sq17);
        store.addObject(sp3);
        store.addObject(rs20);
        store.addObject(sq18);
        store.addObject(rs21);
        store.addObject(sq19);
        store.addObject(vsp1);        
        store.addObject(vsp2);
        store.addObject(vsp3);
        store.addObject(vsp4);
        store.addObject(vsp5);
        store.addObject(vsp6);
        store.addObject(vsp7);
        store.addObject(vsp8);
        store.addObject(vsp9);
        store.addObject(vsp10);
        store.addObject(vsp11);
        store.addObject(vsp12);
        store.addObject(vsp13);
        store.addObject(vsp14);
        store.addObject(vsp15);
        store.addObject(vsp16);
        store.addObject(vsp17);
        store.addObject(vsp18);
        store.addObject(vsp19);
        store.addObject(vsp20);
        store.addObject(vsp21);
        store.addObject(vsp22);
        store.addObject(vsp23);
        store.addObject(vsp24);
        store.addObject(vsp25);
        store.addObject(vsp27);
        store.addObject(vsp28);
        store.addObject(vsp29);
        store.addObject(vsp33);
        store.addObject(vsp34);
        store.addObject(sp4);
        store.addObject(sp5);
		store.addObject(vsp26);
        store.addObject(vsprs30);
        store.addObject(vsp30);
        store.addObject(vsprs31);
        store.addObject(vsp31);
        store.addObject(vsp35);
		store.addObject(vgvp1);
		store.addObject(vgvp1e1);
		store.addObject(vgvp1e2);
		store.addObject(vgvp1e3);
		store.addObject(vgvp2);
		store.addObject(vgvp2e1);
		store.addObject(vgvp2e2);
		store.addObject(vgvp2e3);
		store.addObject(vgvp3);
		store.addObject(vgvp3e1);
		store.addObject(vgvp3e2);
		store.addObject(vgvp3e3);
		store.addObject(vgvp4);
		store.addObject(vgvp4e1);
		store.addObject(vgvp4e2);
		store.addObject(vgvp4e3);
		store.addObject(vgvp5);
		store.addObject(vgvp5e1);
		store.addObject(vgvp5e2);
		store.addObject(vgvp5e3);
		store.addObject(vgvp6);
		store.addObject(vgvp6e1);
		store.addObject(vgvp6e2);
		store.addObject(vgvp6e3);
		store.addObject(vgvp6e4);
        store.addObject(vgvp7);
        store.addObject(vgvp7e1);
        store.addObject(vgvp7e2);
        store.addObject(vgvp7e3);
        store.addObject(vgvp7e4);
		store.addObject(vm1g35);
		store.addObject(vm1g35e1);
		store.addObject(vm1g35e2);
        store.addObject(vsp36);
        store.addObject(vsprs36);
        store.addObject(vsp32);
        store.addObject(vsp37);
        store.addObject(vsp38);
        store.addObject(vsp39);
        store.addObject(vsp40);
        store.addObject(vsp41);
        store.addObject(vsp42);
        store.addObject(vsp43);
        store.addObject(vsp44);
        store.addObject(vsp45);
        store.addObject(vsp46);
        store.addObject(vsp47);
        store.addObject(vsp48rs);
        store.addObject(vsp48);
        store.addObject(vsp49rs);
        store.addObject(vsp49);
        store.addObject(vsp50rs);
        store.addObject(vsp50);
        store.addObject(vsp51rs);
        store.addObject(vsp51);
        store.addObject(vsp52rs);
        store.addObject(vsp52);
        store.addObject(vsp53rs);
        store.addObject(vsp53);
        store.addObject(vsp54rs);
        store.addObject(vsp54);
        store.addObject(vsp55);
        store.addObject(vsp56);
        store.addObject(vsp57);
        store.addObject(vsp58);
        store.addObject(vsp59);
        store.addObject(vsp60);
        store.addObject(vsp61);
        store.addObject(vsp62);
		store.addObject(vm1g39);
		store.addObjects(vm1g39e);
                
		// Create the facade from the store
		return new FakeMetadataFacade(store);
	}

    private static MappingDocument exampleDoc1() {
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        MappingElement node1 = root.addChildElement(new MappingElement("node1")); //$NON-NLS-1$
        MappingElement node2 = node1.addChildElement(new MappingElement("node2")); //$NON-NLS-1$
        node2.addChildElement(new MappingElement("node3")); //$NON-NLS-1$        
        return doc;
    }

    private static MappingDocument exampleDoc2() {
        
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        MappingElement node1 = root.addChildElement(new MappingElement("node1")); //$NON-NLS-1$
        
        MappingSequenceNode node2 = node1.addSequenceNode(new MappingSequenceNode());    
        node2.addChildElement(new MappingElement("node3")); //$NON-NLS-1$
        root.addChildElement(new MappingElement("node2")); //$NON-NLS-1$
        return doc;
    }

    // has ambiguous short and long names
    private static MappingDocument exampleDoc3() {
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        MappingSequenceNode node1 = root.addSequenceNode(new MappingSequenceNode());    
        node1.addChildElement(new MappingElement("node2")); //$NON-NLS-1$
        root.addChildElement(new MappingElement("node2")); //$NON-NLS-1$
        return doc;
    }
    
   
    // has attributes and elements
    private static MappingDocument exampleDoc4() {
        
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        root.addAttribute(new MappingAttribute("node6")); //$NON-NLS-1$
        root.addStagingTable("tm1.g1"); //$NON-NLS-1$
        
        MappingElement node1 =root.addChildElement(new MappingElement("node1")); //$NON-NLS-1$
        node1.addAttribute(new MappingAttribute("node2")); //$NON-NLS-1$
        
        MappingElement node3 =root.addChildElement(new MappingElement("node3")); //$NON-NLS-1$
        node3.addAttribute(new MappingAttribute("node4")); //$NON-NLS-1$
        
        MappingElement node5 = node3.addChildElement(new MappingElement("node4")); //$NON-NLS-1$
        MappingElement duplicateRoot = node5.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        duplicateRoot.addChildElement(new MappingElement("node6")); //$NON-NLS-1$        
        return doc;
    }    

    // has a union in the mapping class
    private static MappingDocument exampleDoc5() {
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        MappingElement node1 = root.addChildElement(new MappingElement("node1")); //$NON-NLS-1$
        node1.addChildElement(new MappingElement("node2","xmltest.mc1.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        node1.setSource("xmltest.mc1"); //$NON-NLS-1$
        node1.setMaxOccurrs(-1);
        return doc;
    }	
    
    // has two elements with common suffix, but not ambiguous
    private static MappingDocument exampleDoc6() {
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        root.addChildElement(new MappingElement("node")); //$NON-NLS-1$
        root.addChildElement(new MappingElement("thenode")); //$NON-NLS-1$
        return doc;
    }    
    
    public static FakeMetadataFacade example3() {
		// Create models
		FakeMetadataObject pm1 = createPhysicalModel("pm1"); //$NON-NLS-1$
		FakeMetadataObject pm2 = createPhysicalModel("pm2"); //$NON-NLS-1$
        FakeMetadataObject pm3 = createPhysicalModel("pm3"); //$NON-NLS-1$

		// Create physical groups
		FakeMetadataObject pm1g1 = createPhysicalGroup("pm1.cat1.cat2.cat3.g1", pm1); //$NON-NLS-1$
		FakeMetadataObject pm1g2 = createPhysicalGroup("pm1.cat1.g2", pm1); //$NON-NLS-1$
		FakeMetadataObject pm1g3 = createPhysicalGroup("pm1.cat2.g3", pm1); //$NON-NLS-1$
		FakeMetadataObject pm2g1 = createPhysicalGroup("pm2.cat1.g1", pm2); //$NON-NLS-1$
		FakeMetadataObject pm2g2 = createPhysicalGroup("pm2.cat2.g2", pm2); //$NON-NLS-1$
		FakeMetadataObject pm2g3 = createPhysicalGroup("pm2.g3", pm2); //$NON-NLS-1$
        FakeMetadataObject pm2g4 = createPhysicalGroup("pm2.g4", pm3);		 //$NON-NLS-1$
		FakeMetadataObject pm2g5 = createPhysicalGroup("pm2.cat3.g1", pm2); //$NON-NLS-1$
						
		// Create physical elements
		List pm1g1e = createElements(pm1g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm1g2e = createElements(pm1g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm1g3e = createElements(pm1g3, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g1e = createElements(pm2g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g2e = createElements(pm2g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g3e = createElements(pm2g3, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g4e = createElements(pm2g4, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g5e = createElements(pm2g5, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });			

		
		// Add all objects to the store
		FakeMetadataStore store = new FakeMetadataStore();
		store.addObject(pm1);
		store.addObject(pm1g1);		
		store.addObjects(pm1g1e);
		store.addObject(pm1g2);		
		store.addObjects(pm1g2e);
		store.addObject(pm1g3);	
		store.addObjects(pm1g3e);
        	
		store.addObject(pm2);
		store.addObject(pm2g1);		
		store.addObjects(pm2g1e);
		store.addObject(pm2g2);		
		store.addObjects(pm2g2e);
		store.addObject(pm2g3);
		store.addObject(pm2g4);
		store.addObject(pm2g5);		
		store.addObjects(pm2g3e);
		store.addObjects(pm2g4e);
		store.addObjects(pm2g5e);		
        
		store.addObject(pm3);
						
		// Create the facade from the store
		return new FakeMetadataFacade(store);
	}

	/**
	 * This example is for testing static costing using cardinality information from
	 * metadata, as well as key information and maybe access patterns
	 * @return FakeMetadataFacade
	 */
	public static FakeMetadataFacade example4() {
		// Create models - physical ones will support joins
		FakeMetadataObject pm1 = createPhysicalModel("pm1"); //$NON-NLS-1$
		FakeMetadataObject pm2 = createPhysicalModel("pm2"); //$NON-NLS-1$
		FakeMetadataObject pm3 = createPhysicalModel("pm3"); //$NON-NLS-1$
        FakeMetadataObject pm4 = createPhysicalModel("pm4"); //$NON-NLS-1$
		FakeMetadataObject vm1 = createVirtualModel("vm1");	 //$NON-NLS-1$

		// Create physical groups
		FakeMetadataObject pm1g1 = createPhysicalGroup("pm1.g1", pm1); //$NON-NLS-1$
		FakeMetadataObject pm1g2 = createPhysicalGroup("pm1.g2", pm1); //$NON-NLS-1$
		FakeMetadataObject pm1g3 = createPhysicalGroup("pm1.g3", pm1); //$NON-NLS-1$
		FakeMetadataObject pm2g1 = createPhysicalGroup("pm2.g1", pm2); //$NON-NLS-1$
		FakeMetadataObject pm2g2 = createPhysicalGroup("pm2.g2", pm2); //$NON-NLS-1$
		FakeMetadataObject pm2g3 = createPhysicalGroup("pm2.g3", pm2); //$NON-NLS-1$
		FakeMetadataObject pm3g1 = createPhysicalGroup("pm3.g1", pm3); //$NON-NLS-1$
		FakeMetadataObject pm3g2 = createPhysicalGroup("pm3.g2", pm3); //$NON-NLS-1$
		FakeMetadataObject pm3g3 = createPhysicalGroup("pm3.g3", pm3); //$NON-NLS-1$
        FakeMetadataObject pm4g1 = createPhysicalGroup("pm4.g1", pm4); //$NON-NLS-1$
        FakeMetadataObject pm4g2 = createPhysicalGroup("pm4.g2", pm4); //$NON-NLS-1$
		// Add group cardinality metadata
		pm1g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10));
		pm1g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10));
		pm1g3.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10));
		pm2g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(1000));
		pm2g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(1000));
		pm3g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(100000));
		pm3g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(100000));
        pm3g3.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(100000));
        // leave pm4.g1 as unknown
				
		// Create physical elements
		List pm1g1e = createElements(pm1g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm1g2e = createElements(pm1g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm1g3e = createElements(pm1g3, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g1e = createElements(pm2g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g2e = createElements(pm2g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g3e = createElements(pm2g3, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm3g1e = createElements(pm3g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm3g2e = createElements(pm3g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm3g3e = createElements(pm3g3, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List pm4g1e = createElements(pm4g1, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List pm4g2e = createElements(pm4g2, 
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

		// Add key metadata
		FakeMetadataObject pm1g1key1 = createKey("pm1.g1.key1", pm1g1, pm1g1e.subList(0, 1)); //e1 //$NON-NLS-1$
		FakeMetadataObject pm3g1key1 = createKey("pm3.g1.key1", pm3g1, pm3g1e.subList(0, 1)); //e1 //$NON-NLS-1$
		FakeMetadataObject pm3g3key1 = createKey("pm3.g3.key1", pm3g3, pm3g3e.subList(0, 1)); //e1 //$NON-NLS-1$
        FakeMetadataObject pm4g1key1 = createKey("pm4.g1.key1", pm4g1, pm4g1e.subList(0, 2)); //e1, e2 //$NON-NLS-1$
        FakeMetadataObject pm4g2key1 = createForeignKey("pm4.g2.fk", pm4g2, pm4g2e.subList(0, 2), pm4g1key1); //$NON-NLS-1$
		// Add access pattern metadata
		// Create access patterns - pm1
		List elements = new ArrayList(1);
		elements.add(pm1g1e.iterator().next());       
		FakeMetadataObject pm1g1ap1 = createAccessPattern("pm1.g1.ap1", pm1g1, elements); //e1 //$NON-NLS-1$
		elements = new ArrayList(2);
		Iterator iter = pm1g3e.iterator();
		elements.add(iter.next());       
		elements.add(iter.next());       
		FakeMetadataObject pm1g3ap1 = createAccessPattern("pm1.g3.ap1", pm1g3, elements); //e1,e2 //$NON-NLS-1$
		// Create access patterns - pm2
		elements = new ArrayList(1);
		elements.add(pm2g1e.iterator().next());
		FakeMetadataObject pm2g1ap1 = createAccessPattern("pm2.g1.ap1", pm2g1, elements); //e1 //$NON-NLS-1$


		// Create virtual groups
		QueryNode vm1g1n1 = new QueryNode("vm1.g1", "SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g1 = createUpdatableVirtualGroup("vm1.g1", vm1, vm1g1n1); //$NON-NLS-1$

		QueryNode vm1g2n1 = new QueryNode("vm1.g2", "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g2 = createUpdatableVirtualGroup("vm1.g2", vm1, vm1g2n1); //$NON-NLS-1$

		QueryNode vm1g3n1 = new QueryNode("vm1.g3", "SELECT pm1.g3.e1 AS x, pm1.g3.e2 AS y from pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g3 = createUpdatableVirtualGroup("vm1.g3", vm1, vm1g3n1); //$NON-NLS-1$
	
        QueryNode vm1g4n1 = new QueryNode("vm1.g4", "SELECT distinct pm1.g2.e1 as ve1, pm1.g1.e1 as ve2 FROM pm1.g2 LEFT OUTER JOIN /* optional */ pm1.g1 on pm1.g1.e1 = pm1.g2.e1");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g4 = createVirtualGroup("vm1.g4", vm1, vm1g4n1); //$NON-NLS-1$
        List vm1g4e = createElements(vm1g4,
                  new String[] { "ve1", "ve2" }, //$NON-NLS-1$ //$NON-NLS-2$
                  new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

		// Create virtual elements
		List vm1g1e = createElements(vm1g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List vm1g2e = createElements(vm1g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List vm1g3e = createElements(vm1g3,
			new String[] { "e1", "e2","x", "y" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
			
		// Add all objects to the store
		FakeMetadataStore store = new FakeMetadataStore();
		store.addObject(pm1);
		store.addObject(pm1g1);		
		store.addObjects(pm1g1e);
		store.addObject(pm1g2);		
		store.addObjects(pm1g2e);
		store.addObject(pm1g3);	
		store.addObjects(pm1g3e);
		store.addObject(pm1g1key1);
		store.addObject(pm1g1ap1);
		store.addObject(pm1g3ap1);
        	
		store.addObject(pm2);
		store.addObject(pm2g1);		
		store.addObjects(pm2g1e);
		store.addObject(pm2g2);		
		store.addObjects(pm2g2e);
		store.addObject(pm2g3);		
		store.addObjects(pm2g3e);
		store.addObject(pm2g1ap1);
        
		store.addObject(pm3);
		store.addObject(pm3g1);
		store.addObjects(pm3g1e);
		store.addObject(pm3g2);
		store.addObjects(pm3g2e);
		store.addObject(pm3g3);
		store.addObjects(pm3g3e);
		store.addObject(pm3g1key1);
		store.addObject(pm3g3key1);

        store.addObject(pm4);
        store.addObject(pm4g1);
        store.addObjects(pm4g1e);
        store.addObject(pm4g1key1);
        store.addObject(pm4g2);
        store.addObjects(pm4g2e);
        store.addObject(pm4g2key1);
        
        store.addObject(vm1);
		store.addObject(vm1g1);
		store.addObjects(vm1g1e);
		store.addObject(vm1g2);
		store.addObjects(vm1g2e);
		store.addObject(vm1g3);
		store.addObjects(vm1g3e);
        store.addObject(vm1g4);
        store.addObjects(vm1g4e);
						
		// Create the facade from the store
		return new FakeMetadataFacade(store);
	}
	
	public static FakeMetadataFacade exampleUpdateProc(String procedureType, String procedure) {
		// Create models
		FakeMetadataObject pm1 = createPhysicalModel("pm1"); //$NON-NLS-1$
		FakeMetadataObject pm2 = createPhysicalModel("pm2"); //$NON-NLS-1$
		FakeMetadataObject vm1 = createVirtualModel("vm1"); //$NON-NLS-1$

		// Create physical groups
		FakeMetadataObject pm1g1 = createPhysicalGroup("pm1.g1", pm1); //$NON-NLS-1$
		FakeMetadataObject pm1g2 = createPhysicalGroup("pm1.g2", pm1); //$NON-NLS-1$
        FakeMetadataObject pm2g1 = createPhysicalGroup("pm2.g1", pm2); //$NON-NLS-1$
        FakeMetadataObject pm2g2 = createPhysicalGroup("pm2.g2", pm2); //$NON-NLS-1$

		// Create physical group elements
		List pm1g1e = createElements(pm1g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm1g2e = createElements(pm1g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g1e = createElements(pm2g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
		List pm2g2e = createElements(pm2g2, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

		// Create virtual groups
		QueryNode vm1g1n1 = new QueryNode("vm1.g1", "SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g1 = createUpdatableVirtualGroup("vm1.g1", vm1, vm1g1n1); //$NON-NLS-1$

		QueryNode vm1g2n1 = new QueryNode("vm1.g2", "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g2 = createUpdatableVirtualGroup("vm1.g2", vm1, vm1g2n1); //$NON-NLS-1$

		QueryNode vm1g3n1 = new QueryNode("vm1.g3", "SELECT CONCAT(e1, 'm') as x, (e2 +1) as y, 1 as e3, e4*50 as e4 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g3 = createUpdatableVirtualGroup("vm1.g3", vm1, vm1g3n1); //$NON-NLS-1$

		QueryNode vm1g4n1 = new QueryNode("vm1.g4", "SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
		FakeMetadataObject vm1g4 = createUpdatableVirtualGroup("vm1.g4", vm1, vm1g4n1); //$NON-NLS-1$

		// Create virtual elements
		List vm1g1e = createElementsWithDefaults(vm1g1, 
			new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE },
			new Object[] { "xyz", Integer.getInteger("123"), Boolean.valueOf("true"), Double.valueOf("123.456")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		List vm1g2e = createElementsWithDefaults(vm1g2, 
			new String[] { "e1", "e2", "e3" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN },
			new Object[] { "abc", Integer.getInteger("456"), Boolean.valueOf("false")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		List vm1g3e = createElementsWithDefaults(vm1g3,
			new String[] { "x", "y", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER , DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.DOUBLE },
			new Object[] { "mno", Integer.getInteger("789"), Boolean.valueOf("true"), Double.valueOf("789.012")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		List vm1g4e = createElementsWithDefaults(vm1g4, 
				new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
				new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE },
				new Object[] { "xyz", Integer.getInteger("123"), Boolean.valueOf("true"), Double.valueOf("123.456")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

		vm1g1.putProperty(procedureType, procedure);
		vm1g2.putProperty(procedureType, procedure);
		vm1g3.putProperty(procedureType, procedure);				
		vm1g4.putProperty(procedureType, procedure);

		// Add all objects to the store
		FakeMetadataStore store = new FakeMetadataStore();
		store.addObject(pm1);
		store.addObject(pm1g1);		
		store.addObjects(pm1g1e);
		store.addObject(pm1g2);		
		store.addObjects(pm1g2e);
        	
		store.addObject(pm2);
		store.addObject(pm2g1);		
		store.addObjects(pm2g1e);
		store.addObject(pm2g2);		
		store.addObjects(pm2g2e);

		store.addObject(vm1);
		store.addObject(vm1g1);
		store.addObjects(vm1g1e);
		store.addObject(vm1g2);
		store.addObjects(vm1g2e);
		store.addObject(vm1g3);
		store.addObjects(vm1g3e);
		store.addObject(vm1g4);
		store.addObjects(vm1g4e);

		// Create the facade from the store
		return new FakeMetadataFacade(store);
	}

    public static FakeMetadataFacade exampleUpdateProc(String procedureType, String procedure1, String procedure2) {
        // Create models
        FakeMetadataObject pm1 = createPhysicalModel("pm1"); //$NON-NLS-1$
        FakeMetadataObject pm2 = createPhysicalModel("pm2"); //$NON-NLS-1$
        FakeMetadataObject vm1 = createVirtualModel("vm1"); //$NON-NLS-1$

        // Create physical groups
        FakeMetadataObject pm1g1 = createPhysicalGroup("pm1.g1", pm1); //$NON-NLS-1$
        FakeMetadataObject pm1g2 = createPhysicalGroup("pm1.g2", pm1); //$NON-NLS-1$
        FakeMetadataObject pm2g1 = createPhysicalGroup("pm2.g1", pm2); //$NON-NLS-1$
        FakeMetadataObject pm2g2 = createPhysicalGroup("pm2.g2", pm2); //$NON-NLS-1$

        // Create physical group elements
        List pm1g1e = createElements(pm1g1, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List pm1g2e = createElements(pm1g2, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List pm2g1e = createElements(pm2g1, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List pm2g2e = createElements(pm2g2, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("vm1.g1", "SELECT * FROM vm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g1 = createUpdatableVirtualGroup("vm1.g1", vm1, vm1g1n1); //$NON-NLS-1$

        QueryNode vm1g2n1 = new QueryNode("vm1.g2", "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g2 = createUpdatableVirtualGroup("vm1.g2", vm1, vm1g2n1); //$NON-NLS-1$

        // Create virtual elements
        List vm1g1e = createElementsWithDefaults(vm1g1, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE },
            new Object[] { "xyz", Integer.getInteger("123"), Boolean.valueOf("true"), Double.valueOf("123.456")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        List vm1g2e = createElementsWithDefaults(vm1g2, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE },
            new Object[] { "abc", Integer.getInteger("456"), Boolean.valueOf("false"), null}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        vm1g1.putProperty(procedureType, procedure1);
        vm1g2.putProperty(procedureType, procedure2);

        // Add all objects to the store
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(pm1);
        store.addObject(pm1g1);     
        store.addObjects(pm1g1e);
        store.addObject(pm1g2);     
        store.addObjects(pm1g2e);
            
        store.addObject(pm2);
        store.addObject(pm2g1);     
        store.addObjects(pm2g1e);
        store.addObject(pm2g2);     
        store.addObjects(pm2g2e);

        store.addObject(vm1);
        store.addObject(vm1g1);
        store.addObjects(vm1g1e);
        store.addObject(vm1g2);
        store.addObjects(vm1g2e);

        // Create the facade from the store
        return new FakeMetadataFacade(store);
    }
    
    public static VDBMetaData exampleBQTVDB() {
    	VDBMetaData vdb = new VDBMetaData();
    	vdb.setName("example1");
    	vdb.setVersion(1);
    	vdb.addModel(createModel("BQT1", true));
    	vdb.addModel(createModel("BQT2", true));
    	vdb.addModel(createModel("BQT3", true));
    	vdb.addModel(createModel("LOB", true));
    	vdb.addModel(createModel("VQT", false));
    	vdb.addModel(createModel("pm1", true));
    	vdb.addModel(createModel("pm2", true));
    	vdb.addModel(createModel("pm3", true));
    	vdb.addModel(createModel("pm4", true));
    	
    	return vdb;
    }
    
    public static TransformationMetadata exampleBQT() { 
    	return RealMetadataFactory.exampleBQT();
    }

    public static FakeMetadataFacade exampleYahoo() { 
        // Create models
        FakeMetadataObject yahoo = createPhysicalModel("Yahoo"); //$NON-NLS-1$
        
        // Create physical groups
        FakeMetadataObject quotes = createPhysicalGroup("Yahoo.QuoteServer", yahoo); //$NON-NLS-1$
                
        // Create physical elements
        String[] elemNames = new String[] {
            "TickerSymbol", "LastTrade",  //$NON-NLS-1$ //$NON-NLS-2$
            "LastTradeDate", "LastTradeTime", //$NON-NLS-1$ //$NON-NLS-2$
            "PercentageChange", "TickerSymbol2",  //$NON-NLS-1$ //$NON-NLS-2$
            "DaysHigh", "DaysLow",  //$NON-NLS-1$ //$NON-NLS-2$
            "TotalVolume"             //$NON-NLS-1$
        };
        String[] elemTypes = new String[] {  
            DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DOUBLE,
            DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME,
            DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING,
            DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE,
            DataTypeManager.DefaultDataTypes.BIG_INTEGER            
        };
        
        List cols = createElements(quotes, elemNames, elemTypes);
        
        // Set name in source on each column
        String[] nameInSource = new String[] {
           "Symbol", "Last", "Date", "Time", "Change", "Symbol2", "High", "Low", "Volume"             //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
        };
        for(int i=0; i<9; i++) {
            FakeMetadataObject obj = (FakeMetadataObject) cols.get(i);
            obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, nameInSource[i]);
        }
        
        // Set column-specific properties
        ((FakeMetadataObject) cols.get(0)).putProperty(FakeMetadataObject.Props.SELECT, Boolean.FALSE);
        for(int i=1; i<9; i++) {
            ((FakeMetadataObject) cols.get(0)).putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
            ((FakeMetadataObject) cols.get(0)).putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, Boolean.FALSE);
        }
        
        // Add all objects to the store
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(yahoo);
        store.addObject(quotes);     
        store.addObjects(cols);
        
        // Create the facade from the store
        return new FakeMetadataFacade(store);
    }
    
    public static VDBMetaData examplePrivatePhysicalModelVDB() {
    	VDBMetaData vdb = new VDBMetaData();
    	vdb.setName("example1");
    	vdb.setVersion(1);
    	ModelMetaData m = createModel("pm1", true);
    	m.setVisible(false);
    	vdb.addModel(m);
    	vdb.addModel(createModel("vm1", false));
    	
    	return vdb;
    }    
    
    public static FakeMetadataFacade examplePrivatePhysicalModel() { 
        // Create models
        FakeMetadataObject pm1 = createPhysicalModel("pm1"); //$NON-NLS-1$
        FakeMetadataObject vm1 = createVirtualModel("vm1");  //$NON-NLS-1$

        // Create physical groups
        FakeMetadataObject pm1g1 = createPhysicalGroup("pm1.g1", pm1); //$NON-NLS-1$
            
        QueryNode vm1g1n1 = new QueryNode("vm1.g1", "SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vm1g1 = createVirtualGroup("vm1.g1", vm1, vm1g1n1); //$NON-NLS-1$
        
        FakeMetadataObject pm1g2 = createPhysicalGroup("pm1.g2", pm1); //$NON-NLS-1$
        
        // Create physical elements
        List pm1g1e = createElements(pm1g1, 
            new String[] { "e1"}, //$NON-NLS-1$ 
            new String[] { DataTypeManager.DefaultDataTypes.SHORT});
 
        // Create physical elements
        List pm1g2e = createElements(pm1g2, 
            new String[] { "e1"}, //$NON-NLS-1$ 
            new String[] { DataTypeManager.DefaultDataTypes.BIG_DECIMAL});
        
        FakeMetadataObject e1 = (FakeMetadataObject)pm1g2e.get(0);
        e1.putProperty(FakeMetadataObject.Props.PRECISION, "19"); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.LENGTH, "21"); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SCALE, "4"); //$NON-NLS-1$
        
        List vm1g1e = createElements(vm1g1, 
                                    new String[] { "e1" }, //$NON-NLS-1$
                                    new String[] { DataTypeManager.DefaultDataTypes.STRING });
        // Add all objects to the store
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(pm1);
        store.addObject(pm1g1);     
        store.addObjects(pm1g1e);
        
        store.addObject(vm1);
        store.addObject(vm1g1);
        store.addObjects(vm1g1e);
        
        store.addObject(pm1g2);     
        store.addObjects(pm1g2e);

        // Create the facade from the store
        return new FakeMetadataFacade(store);
    }

    public static FakeMetadataFacade exampleBusObj() { 
        // Create db2 tables
        FakeMetadataObject db2Model = createPhysicalModel("db2model"); //$NON-NLS-1$
        
        FakeMetadataObject db2Table = createPhysicalGroup("db2model.DB2_TABLE", db2Model); //$NON-NLS-1$
        List db2Elements = createElements(db2Table, 
            new String[] { "PRODUCT", "REGION", "SALES"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DOUBLE});

        FakeMetadataObject salesTable = createPhysicalGroup("db2model.SALES", db2Model); //$NON-NLS-1$
        salesTable.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(1000000));
        List salesElem = createElements(salesTable, 
            new String[] { "CITY", "MONTH", "SALES"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DOUBLE});

        FakeMetadataObject geographyTable2 = createPhysicalGroup("db2model.GEOGRAPHY2", db2Model); //$NON-NLS-1$
        geographyTable2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(1000));
        List geographyElem2 = createElements(geographyTable2, 
            new String[] { "CITY", "REGION"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        List geoPkElem2 = new ArrayList();
        geoPkElem2.add(geographyElem2.get(0));
        FakeMetadataObject geoPk2 = createKey("db2model.GEOGRAPHY2.GEOGRAPHY_PK", geographyTable2, geoPkElem2); //$NON-NLS-1$

        FakeMetadataObject db2Table2 = createPhysicalGroup("db2model.DB2TABLE", db2Model); //$NON-NLS-1$
        List db2Elements2 = createElements(db2Table2, 
            new String[] { "c0", "c1", "c2"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER});

        // Create oracle tables 
        FakeMetadataObject oraModel = createPhysicalModel("oraclemodel"); //$NON-NLS-1$

        FakeMetadataObject oraTable = createPhysicalGroup("oraclemodel.Oracle_table", oraModel); //$NON-NLS-1$
        List oracleElements = createElements(oraTable, 
            new String[] { "COSTS", "REGION", "YEAR"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        FakeMetadataObject geographyTable = createPhysicalGroup("oraclemodel.GEOGRAPHY", oraModel); //$NON-NLS-1$
        geographyTable.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(1000));
        List geographyElem = createElements(geographyTable, 
            new String[] { "CITY", "REGION"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        List geoPkElem = new ArrayList();
        geoPkElem.add(geographyElem.get(0));
        FakeMetadataObject geoPk = createKey("oraclemodel.GEOGRAPHY.GEOGRAPHY_PK", geographyTable, geoPkElem); //$NON-NLS-1$

        FakeMetadataObject oraTable2 = createPhysicalGroup("oraclemodel.OraTable", oraModel); //$NON-NLS-1$
        List oracleElements2 = createElements(oraTable2, 
            new String[] { "b0", "b1", "b2"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        // Create sql server tables 
        FakeMetadataObject msModel = createPhysicalModel("msmodel"); //$NON-NLS-1$

        FakeMetadataObject timeTable = createPhysicalGroup("msmodel.TIME", msModel); //$NON-NLS-1$
        timeTable.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(120));
        List timeElem = createElements(timeTable, 
            new String[] { "MONTH", "YEAR"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        List timePkElem = new ArrayList();
        timePkElem.add(timeElem.get(0));
        FakeMetadataObject timePk = createKey("msmodel.TIME.TIME_PK", timeTable, timePkElem); //$NON-NLS-1$

        FakeMetadataObject virtModel = createVirtualModel("logical"); //$NON-NLS-1$
        QueryNode n1 = new QueryNode("vm1.g1", "select sum(c0) as c0, c1, c2 from db2Table group by c1, c2"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject logicalTable1 = createVirtualGroup("logical.logicalTable1", virtModel, n1); //$NON-NLS-1$
        List logicalElem1 = createElements(logicalTable1, 
            new String[] { "c0", "c1", "c2"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER});

        QueryNode n2 = new QueryNode("vm1.g1", "select sum(c0) as c0, c1, c2 from db2Table group by c1, c2"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject logicalTable2 = createVirtualGroup("logical.logicalTable2", virtModel, n2); //$NON-NLS-1$
        List logicalElem2 = createElements(logicalTable2, 
            new String[] { "b0", "b1", "b2"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER});

        // Add all objects to the store
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(db2Model);
        store.addObject(db2Table);     
        store.addObjects(db2Elements);
        store.addObject(salesTable);     
        store.addObjects(salesElem);
        store.addObject(geographyTable2);     
        store.addObjects(geographyElem2);
        store.addObject(geoPk2);     
        store.addObject(db2Table2);     
        store.addObjects(db2Elements2);
        
        store.addObject(oraModel);
        store.addObject(oraTable);
        store.addObjects(oracleElements);
        store.addObject(geographyTable);     
        store.addObjects(geographyElem);
        store.addObject(geoPk);     
        store.addObject(oraTable2);
        store.addObjects(oracleElements2);

        store.addObject(msModel);
        store.addObject(timeTable);
        store.addObjects(timeElem);
        store.addObject(timePk);

        store.addObject(virtModel);
        store.addObject(logicalTable1);
        store.addObjects(logicalElem1);
        store.addObject(logicalTable2);
        store.addObjects(logicalElem2);

        // Create the facade from the store
        return new FakeMetadataFacade(store);
    }

    public static FakeMetadataFacade exampleAggregates() { 
        FakeMetadataStore store = new FakeMetadataStore();
        addAggregateTablesToModel("m1", store); //$NON-NLS-1$
        addAggregateTablesToModel("m2", store); //$NON-NLS-1$

        // Create the facade from the store
        return new FakeMetadataFacade(store);
    }
    
    public static void addAggregateTablesToModel(String modelName, FakeMetadataStore store) {
        // Create db2 tables
        FakeMetadataObject model = createPhysicalModel(modelName); 
        
        FakeMetadataObject orders = createPhysicalGroup(modelName + ".order", model); //$NON-NLS-1$
        orders.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(1000000));
        List orderElements = createElements(orders, 
            new String[] { "O_OrderID", "O_ProductID", "O_DealerID", "O_Amount", "O_Date"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ 
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, DataTypeManager.DefaultDataTypes.DATE });

        FakeMetadataObject products = createPhysicalGroup(modelName + ".product", model); //$NON-NLS-1$
        products.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(1000));
        List productsElements = createElements(products, 
            new String[] { "P_ProductID", "P_Overhead", "P_DivID"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, DataTypeManager.DefaultDataTypes.INTEGER});

        FakeMetadataObject divisions = createPhysicalGroup(modelName + ".division", model); //$NON-NLS-1$
        divisions.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(100));
        List divisionsElements = createElements(divisions, 
            new String[] { "V_DIVID", "V_SectorID"}, //$NON-NLS-1$ //$NON-NLS-2$ 
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER});

        FakeMetadataObject dealers = createPhysicalGroup(modelName + ".dealer", model); //$NON-NLS-1$
        dealers.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(1000));
        List dealersElements = createElements(dealers, 
            new String[] { "D_DealerID", "D_State", "D_Address"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        // Add all objects to the store
        store.addObject(model);
        store.addObject(orders);     
        store.addObjects(orderElements);
        store.addObject(products);     
        store.addObjects(productsElements);
        store.addObject(divisions);     
        store.addObjects(divisionsElements);
        store.addObject(dealers);     
        store.addObjects(dealersElements);
    }
    
    
    public static VDBMetaData exampleMultiBindingVDB() {
    	VDBMetaData vdb = new VDBMetaData();
    	vdb.setName("exampleMultiBinding");
    	vdb.setVersion(1);
    	
    	ModelMetaData model = new ModelMetaData();
    	model.setName("MultiModel");
   		model.setModelType(Model.Type.PHYSICAL);
    	model.setVisible(true);
    	
    	model.setSupportsMultiSourceBindings(true);
    	vdb.addModel(model);
    	vdb.addModel(createModel("Virt", false));
    	
    	return vdb;
    }
    
    /** 
     * Metadata for Multi-Binding models
     * @return example
     * @since 4.2
     */
    public static FakeMetadataFacade exampleMultiBinding() {
        FakeMetadataObject virtModel = createVirtualModel("Virt"); //$NON-NLS-1$
        FakeMetadataObject physModel = createPhysicalModel("MultiModel"); //$NON-NLS-1$
        
        FakeMetadataObject physGroup = createPhysicalGroup("MultiModel.Phys", physModel); //$NON-NLS-1$
        List physElements = createElements(physGroup,
                                      new String[] { "a", "b" }, //$NON-NLS-1$ //$NON-NLS-2$ 
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        
        QueryNode virtTrans = new QueryNode("Virt.view", "SELECT * FROM MultiModel.Phys");         //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject virtGroup = createVirtualGroup("Virt.view", virtModel, virtTrans); //$NON-NLS-1$
        List virtElements = createElements(virtGroup,
                                           new String[] { "a", "b" }, //$NON-NLS-1$ //$NON-NLS-2$ 
                                           new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        
        FakeMetadataObject rs2 = FakeMetadataFactory.createResultSet("Virt.rs1", virtModel, new String[] { "a", "b" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs2p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs2);  //$NON-NLS-1$
        FakeMetadataObject rs2p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("pm1.sq1", "CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'SELECT a, b FROM MultiModel.Phys where SOURCE_NAME = Virt.sq1.in'; END"); //$NON-NLS-1$ 
        FakeMetadataObject sq1 = FakeMetadataFactory.createVirtualProcedure("Virt.sq1", virtModel, Arrays.asList(new FakeMetadataObject[] { rs2p1, rs2p2 }), sq2n1);  //$NON-NLS-1$

        FakeMetadataObject rs3 = FakeMetadataFactory.createResultSet("MultiModel.rs1", virtModel, new String[] { "a", "b" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeMetadataObject rs3p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs3);  //$NON-NLS-1$
        FakeMetadataObject rs3p2 = FakeMetadataFactory.createParameter("in", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        FakeMetadataObject rs3p3 = FakeMetadataFactory.createParameter("source_name", 3, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        rs3p3.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        FakeMetadataObject sq2 = FakeMetadataFactory.createStoredProcedure("MultiModel.proc", physModel, Arrays.asList(new FakeMetadataObject[] { rs3p1, rs3p2, rs3p3 }));
        rs3p2.putProperty(FakeMetadataObject.Props.GROUP, sq2);
        rs3p3.putProperty(FakeMetadataObject.Props.GROUP, sq2);
        
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(virtModel);
        store.addObject(physModel);
        store.addObject(physGroup);
        store.addObjects(physElements);
        store.addObject(virtGroup);
        store.addObjects(virtElements);
        
        store.addObject(rs2);
        store.addObject(sq1);
        store.addObject(rs3);
        store.addObject(sq2);
        
        return new FakeMetadataFacade(store);
    }

    /**
     * set up metadata for virtual doc model of this basic structure:
     * <pre>
     * 
     * items
     *   --suppliers (many-to-many relationship between items and suppliers)
     *       --orders
     *       --employees (an employees works for a supplier and "specializes" in an item)
     * 
     * </pre> 
     * @return
     */
    public static FakeMetadataFacade exampleCase3225() {
        FakeMetadataStore store = new FakeMetadataStore();
        FakeMetadataFacade facade = new FakeMetadataFacade(store);
        
        // Create models
        FakeMetadataObject stock = FakeMetadataFactory.createPhysicalModel("stock"); //$NON-NLS-1$
        FakeMetadataObject xmltest = FakeMetadataFactory.createVirtualModel("xmltest");     //$NON-NLS-1$

        // Create physical groups
        FakeMetadataObject items = FakeMetadataFactory.createPhysicalGroup("stock.items", stock); //$NON-NLS-1$
        FakeMetadataObject item_supplier = FakeMetadataFactory.createPhysicalGroup("stock.item_supplier", stock); //$NON-NLS-1$
        FakeMetadataObject suppliers = FakeMetadataFactory.createPhysicalGroup("stock.suppliers", stock); //$NON-NLS-1$
        FakeMetadataObject orders = FakeMetadataFactory.createPhysicalGroup("stock.orders", stock); //$NON-NLS-1$
        FakeMetadataObject employees = FakeMetadataFactory.createPhysicalGroup("stock.employees", stock); //$NON-NLS-1$
             
        // Create physical elements
        List itemElements = FakeMetadataFactory.createElements(items, 
            new String[] { "itemNum", "itemName", "itemQuantity", "itemStatus" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING });

        //many-to-many join table
        List itemSupplierElements = FakeMetadataFactory.createElements(item_supplier, 
            new String[] { "itemNum", "supplierNum" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

        List supplierElements = FakeMetadataFactory.createElements(suppliers, 
            new String[] { "supplierNum", "supplierName", "supplierZipCode" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

        List stockOrders = FakeMetadataFactory.createElements(orders, 
            new String[] { "orderNum", "itemFK", "supplierFK", "orderDate", "orderQty", "orderStatus" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING});

        List supplierEmployees = FakeMetadataFactory.createElements(employees, 
            new String[] { "employeeNum", "supplierNumFK", "specializesInItemNum", "supervisorNum", "firstName", "lastName" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        // Create mapping classes - items doc
        QueryNode rsQuery = new QueryNode("xmltest.items", "SELECT itemNum, itemName, itemQuantity, itemStatus FROM stock.items"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rsItems = FakeMetadataFactory.createVirtualGroup("xmltest.items", xmltest, rsQuery); //$NON-NLS-1$

        QueryNode rsQuery2 = new QueryNode("xmltest.suppliers", "SELECT stock.suppliers.supplierNum, supplierName, supplierZipCode, stock.item_supplier.itemNum FROM stock.suppliers, stock.item_supplier WHERE stock.suppliers.supplierNum = stock.item_supplier.supplierNum AND stock.item_supplier.itemNum = ?"); //$NON-NLS-1$ //$NON-NLS-2$
        rsQuery2.addBinding("xmltest.items.itemNum"); //$NON-NLS-1$
        FakeMetadataObject rsSuppliers = FakeMetadataFactory.createVirtualGroup("xmltest.suppliers", xmltest, rsQuery2); //$NON-NLS-1$

        QueryNode rsQuery3 = new QueryNode("xmltest.orders", "SELECT orderNum, orderDate, orderQty, orderStatus, itemFK, supplierFK FROM stock.orders WHERE itemFK = ? AND supplierFK = ?"); //$NON-NLS-1$ //$NON-NLS-2$
        rsQuery3.addBinding("xmltest.suppliers.itemNum"); //$NON-NLS-1$
        rsQuery3.addBinding("xmltest.suppliers.supplierNum"); //$NON-NLS-1$
        FakeMetadataObject rsOrders = FakeMetadataFactory.createVirtualGroup("xmltest.orders", xmltest, rsQuery3); //$NON-NLS-1$

        QueryNode rsQuery4 = new QueryNode("xmltest.employees", "SELECT employeeNum, firstName, lastName, supervisorNum, specializesInItemNum, supplierNumFK FROM stock.employees WHERE specializesInItemNum = ? AND supplierNumFK = ?"); //$NON-NLS-1$ //$NON-NLS-2$
        rsQuery4.addBinding("xmltest.suppliers.itemNum"); //$NON-NLS-1$
        rsQuery4.addBinding("xmltest.suppliers.supplierNum"); //$NON-NLS-1$
        FakeMetadataObject rsEmployees = FakeMetadataFactory.createVirtualGroup("xmltest.employees", xmltest, rsQuery4); //$NON-NLS-1$

        // Create mapping classes elements - items doc
        List rsItemsElements = FakeMetadataFactory.createElements(rsItems, 
            new String[] { "itemNum", "itemName", "itemQuantity", "itemStatus" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING });        

        List rsSuppliersElements = FakeMetadataFactory.createElements(rsSuppliers, 
            new String[] { "supplierNum", "supplierName", "supplierZipCode", "itemNum" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

        List rsOrdersElements = FakeMetadataFactory.createElements(rsOrders, 
            new String[] { "orderNum", "orderDate", "orderQty", "orderStatus", "itemFK", "supplierFK" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING});

        List rsEmployeesElements = FakeMetadataFactory.createElements(rsEmployees, 
            new String[] { "employeeNum", "firstName", "lastName", "supervisorNum", "specializesInItemNum", "supplierNumFK" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING});

        // MAPPING DOC ======================================================================
        MappingDocument doc = new MappingDocument(true);
        MappingElement root = doc.addChildElement(new MappingElement("Catalogs")); //$NON-NLS-1$
        
        MappingElement cats = root.addChildElement(new MappingElement("Catalog")); //$NON-NLS-1$
        MappingElement itemsA = cats.addChildElement(new MappingElement("Items")); //$NON-NLS-1$

        MappingElement item = itemsA.addChildElement(new MappingElement("Item")); //$NON-NLS-1$
        item.setSource("xmltest.items");//$NON-NLS-1$
        item.setMaxOccurrs(-1);
        item.addAttribute(new MappingAttribute("ItemID", "xmltest.items.itemNum")); //$NON-NLS-1$ //$NON-NLS-2$
        item.addChildElement(new MappingElement("Name", "xmltest.items.itemName")); //$NON-NLS-1$ //$NON-NLS-2$
        item.addChildElement(new MappingElement("Quantity", "xmltest.items.itemQuantity")); //$NON-NLS-1$ //$NON-NLS-2$
        
        //NESTED STUFF======================================================================
        MappingElement nestedWrapper = item.addChildElement(new MappingElement("Suppliers")); //$NON-NLS-1$
        MappingElement supplier = nestedWrapper.addChildElement(new MappingElement("Supplier")); //$NON-NLS-1$
        supplier.setSource("xmltest.suppliers");//$NON-NLS-1$
        supplier.setMaxOccurrs(-1);
        supplier.addAttribute(new MappingAttribute("SupplierID", "xmltest.suppliers.supplierNum")); //$NON-NLS-1$ //$NON-NLS-2$
        supplier.addChildElement(new MappingElement("Name","xmltest.suppliers.supplierName")); //$NON-NLS-1$ //$NON-NLS-2$
        supplier.addChildElement(new MappingElement("Zip", "xmltest.suppliers.supplierZipCode")); //$NON-NLS-1$ //$NON-NLS-2$
        
        MappingElement ordersWrapper = supplier.addChildElement(new MappingElement("Orders")); //$NON-NLS-1$
        MappingElement order = ordersWrapper.addChildElement(new MappingElement("Order")); //$NON-NLS-1$
        order.setSource("xmltest.orders"); //$NON-NLS-1$
        order.setMaxOccurrs(-1);
        order.addAttribute(new MappingAttribute("OrderID", "xmltest.orders.orderNum")); //$NON-NLS-1$ //$NON-NLS-2$
        order.addChildElement(new MappingElement("OrderDate", "xmltest.orders.orderDate")); //$NON-NLS-1$ //$NON-NLS-2$
        order.addChildElement(new MappingElement("OrderQuantity", "xmltest.orders.orderQty")); //$NON-NLS-1$ //$NON-NLS-2$

        order.addChildElement(new MappingElement("OrderStatus", "xmltest.orders.orderStatus")) //$NON-NLS-1$ //$NON-NLS-2$
            .setMinOccurrs(0);                
        //NESTED STUFF======================================================================
        
        MappingElement employeesWrapper = supplier.addChildElement(new MappingElement("Employees")); //$NON-NLS-1$
        MappingElement employee = employeesWrapper.addChildElement(new MappingElement("Employee")); //$NON-NLS-1$
        employee.setSource("xmltest.employees"); //$NON-NLS-1$
        employee.setMaxOccurrs(-1);
        employee.addAttribute(new MappingAttribute("EmployeeID", "xmltest.employees.employeeNum")); //$NON-NLS-1$ //$NON-NLS-2$
        employee.addChildElement(new MappingElement("FirstName", "xmltest.employees.firstName")); //$NON-NLS-1$ //$NON-NLS-2$
        employee.addChildElement(new MappingElement("LastName", "xmltest.employees.lastName")); //$NON-NLS-1$ //$NON-NLS-2$
        employee.addAttribute(new MappingAttribute("SupervisorID", "xmltest.employees.supervisorNum")); //$NON-NLS-1$ //$NON-NLS-2$
        
        // END MAPPING DOC ======================================================================
        
        // Create virtual docs and doc elements
        FakeMetadataObject itemsDoc = FakeMetadataFactory.createVirtualGroup("xmltest.itemsDoc", xmltest, doc); //$NON-NLS-1$
        List docE1 = FakeMetadataFactory.createElements(itemsDoc, 
            new String[] { "Catalogs",  //$NON-NLS-1$
                           "Catalogs.Catalog",  //$NON-NLS-1$
                           "Catalogs.Catalog.items",  //$NON-NLS-1$
                           "Catalogs.Catalog.items.item",  //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.@ItemID",  //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Name",  //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Quantity", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.@SupplierID", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Name", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Zip", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Orders", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Orders.Order", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Orders.Order.@OrderID", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Orders.Order.OrderDate", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Orders.Order.OrderQuantity", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Orders.Order.OrderStatus", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Employees", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Employees.Employee", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Employees.Employee.@EmployeeID", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Employees.Employee.FirstName", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Employees.Employee.LastName", //$NON-NLS-1$
                           "Catalogs.Catalog.items.item.Suppliers.Supplier.Employees.Employee.@SupervisorID", //$NON-NLS-1$
        
            }, 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.INTEGER, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           });
            
        // Create mapping classes - baseball players employees doc
        QueryNode playersNode = new QueryNode("xmltest.players", "SELECT stock.employees.employeeNum, firstName, lastName, supervisorNum FROM stock.employees WHERE specializesInItemNum is not null"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rsPlayers = FakeMetadataFactory.createVirtualGroup("xmltest.players", xmltest, playersNode); //$NON-NLS-1$

        QueryNode managersNode = new QueryNode("xmltest.managers", "SELECT stock.employees.employeeNum, firstName, lastName, supervisorNum FROM stock.employees WHERE stock.employees.employeeNum = ?"); //$NON-NLS-1$ //$NON-NLS-2$
        managersNode.addBinding("xmltest.players.supervisorNum"); //$NON-NLS-1$
        FakeMetadataObject rsManagers = FakeMetadataFactory.createVirtualGroup("xmltest.managers", xmltest, managersNode); //$NON-NLS-1$

            // TODO what if elements in criteria weren't fully qualified? see defect 19541
        QueryNode ownersNode = new QueryNode("xmltest.owners", "SELECT stock.employees.employeeNum, firstName, lastName, supervisorNum FROM stock.employees WHERE stock.employees.employeeNum = ?"); //$NON-NLS-1$ //$NON-NLS-2$ 
        ownersNode.addBinding("xmltest.managers.supervisorNum"); //$NON-NLS-1$
        FakeMetadataObject rsOwners = FakeMetadataFactory.createVirtualGroup("xmltest.owners", xmltest, ownersNode); //$NON-NLS-1$

        // Create mapping classes elements - items doc
        List rsPlayersElements = FakeMetadataFactory.createElements(rsPlayers, 
            new String[] { "employeeNum", "firstName", "lastName", "supervisorNum" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        List rsManagersElements = FakeMetadataFactory.createElements(rsManagers, 
             new String[] { "employeeNum", "firstName", "lastName", "supervisorNum" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
             new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        List rsOwnersElements = FakeMetadataFactory.createElements(rsOwners, 
           new String[] { "employeeNum", "firstName", "lastName", "supervisorNum" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
           new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});


        
        // MAPPING DOC - baseball players ======================================================================
        MappingDocument doc2 = new MappingDocument(true);
        MappingElement root2 = doc2.addChildElement(new MappingElement("BaseballPlayers")); //$NON-NLS-1$
        
        MappingElement player = root2.addChildElement(new MappingElement("Player")); //$NON-NLS-1$
        player.setSource("xmltest.players"); //$NON-NLS-1$
        player.setMaxOccurrs(-1);
        player.addAttribute(new MappingAttribute("PlayerID", "xmltest.players.employeeNum")); //$NON-NLS-1$ //$NON-NLS-2$
        player.addChildElement(new MappingElement("FirstName", "xmltest.players.firstName")); //$NON-NLS-1$ //$NON-NLS-2$
        player.addChildElement(new MappingElement("LastName", "xmltest.players.lastName")); //$NON-NLS-1$ //$NON-NLS-2$

        MappingElement manager = player.addChildElement(new MappingElement("Manager")); //$NON-NLS-1$
        manager.setSource("xmltest.managers");//$NON-NLS-1$
        manager.setMaxOccurrs(-1);
        manager.addAttribute(new MappingAttribute("ManagerID", "xmltest.managers.employeeNum")); //$NON-NLS-1$ //$NON-NLS-2$
        manager.addChildElement(new MappingElement("FirstName", "xmltest.managers.firstName")); //$NON-NLS-1$ //$NON-NLS-2$
        manager.addChildElement(new MappingElement("LastName", "xmltest.managers.lastName")); //$NON-NLS-1$ //$NON-NLS-2$
                
        MappingElement owner = manager.addChildElement(new MappingElement("Owner")); //$NON-NLS-1$
        owner.setSource("xmltest.owners"); //$NON-NLS-1$
        owner.setMaxOccurrs(-1);
        owner.addAttribute(new MappingAttribute("OwnerID", "xmltest.owners.employeeNum")); //$NON-NLS-1$ //$NON-NLS-2$
        owner.addChildElement(new MappingElement("FirstName", "xmltest.owners.firstName")); //$NON-NLS-1$ //$NON-NLS-2$
        owner.addChildElement(new MappingElement("LastName", "xmltest.owners.lastName")); //$NON-NLS-1$ //$NON-NLS-2$       
        // END MAPPING DOC ======================================================================
        
        // Create virtual docs and doc elements
        FakeMetadataObject playersDoc = FakeMetadataFactory.createVirtualGroup("xmltest.playersDoc", xmltest, doc2); //$NON-NLS-1$
        List playersDocElements = FakeMetadataFactory.createElements(playersDoc, 
            new String[] { "BaseballPlayers",  //$NON-NLS-1$
                           "BaseballPlayers.Player",  //$NON-NLS-1$
                           "BaseballPlayers.Player.@PlayerID",  //$NON-NLS-1$
                           "BaseballPlayers.Player.FirstName",  //$NON-NLS-1$
                           "BaseballPlayers.Player.LastName",  //$NON-NLS-1$
                           "BaseballPlayers.Player.Manager",  //$NON-NLS-1$
                           "BaseballPlayers.Player.Manager.@ManagerID",  //$NON-NLS-1$
                           "BaseballPlayers.Player.Manager.FirstName",  //$NON-NLS-1$
                           "BaseballPlayers.Player.Manager.LastName",  //$NON-NLS-1$
                           "BaseballPlayers.Player.Manager.Owner",  //$NON-NLS-1$
                           "BaseballPlayers.Player.Manager.Owner.@OwnerID",  //$NON-NLS-1$
                           "BaseballPlayers.Player.Manager.Owner.FirstName",  //$NON-NLS-1$
                           "BaseballPlayers.Player.Manager.Owner.LastName",  //$NON-NLS-1$
        
            }, 
            new String[] { DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
                           DataTypeManager.DefaultDataTypes.STRING, 
            });        
        
        
        
        // Add all objects to the store
        store.addObject(stock);
        store.addObject(items);
        store.addObject(item_supplier);
   
        store.addObject(suppliers);
        store.addObject(orders);
        store.addObject(employees);
        store.addObjects(itemElements);
        store.addObjects(itemSupplierElements);
        store.addObjects(supplierElements);
        store.addObjects(stockOrders);
        store.addObjects(supplierEmployees);
     
        store.addObject(xmltest);
        store.addObject(rsItems);
        store.addObject(rsSuppliers);
        store.addObject(rsOrders);
        store.addObject(rsEmployees);
        store.addObject(rsPlayers);
        store.addObject(rsManagers);
        store.addObject(rsOwners);
  
        store.addObjects(rsItemsElements);
        store.addObjects(rsSuppliersElements);
        store.addObjects(rsOrdersElements);
        store.addObjects(rsEmployeesElements);
        store.addObjects(rsPlayersElements);
        store.addObjects(rsManagersElements);
        store.addObjects(rsOwnersElements);

        store.addObject(itemsDoc);
        store.addObject(playersDoc);
        
        store.addObjects(docE1);
        store.addObjects(playersDocElements);
        return facade;
    }    
    
    /**
     * Create a physical model with default settings.
     * @param name Name of model
     * @return FakeMetadataObject Metadata object for model
     */
	public static FakeMetadataObject createPhysicalModel(String name) {
		FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.MODEL);
		obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, Boolean.FALSE);		
		obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE);
		return obj;	
	}
	
    /**
     * Create a virtual model with default settings.
     * @param name Name of virtual model
     * @return FakeMetadataObject Metadata object for model
     */
	public static FakeMetadataObject createVirtualModel(String name) {
		FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.MODEL);
		obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, Boolean.TRUE);
		return obj;	
	}
	
    /**
     * Create a physical group with default settings.
     * @param name Name of physical group, must match model name
     * @param model Associated model
     * @return FakeMetadataObject Metadata object for group
     */
	public static FakeMetadataObject createPhysicalGroup(String name, FakeMetadataObject model) {
		FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.GROUP);
		obj.putProperty(FakeMetadataObject.Props.MODEL, model);
		obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, model.getProperty(FakeMetadataObject.Props.IS_VIRTUAL));
		obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE);	
        obj.putProperty(FakeMetadataObject.Props.TEMP, Boolean.FALSE);
        obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, (name.lastIndexOf(".") == -1)? name : name.substring(name.lastIndexOf(".") + 1) );	 //$NON-NLS-1$ //$NON-NLS-2$
		return obj;	
	}

    public static FakeMetadataObject createPhysicalGroup(String name, FakeMetadataObject model, boolean flag) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.GROUP);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
        obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, model.getProperty(FakeMetadataObject.Props.IS_VIRTUAL));
        obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE); 
        obj.putProperty(FakeMetadataObject.Props.TEMP, Boolean.FALSE);
        // actually, the name may be fully qualified, which means, the model + categories + name
        obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, name ); 
        return obj; 
    }
    	
    /**
     * Create a virtual group with default settings.
     * @param name Name of virtual group, must match model name
     * @param model Associated model
     * @param plan Appropriate query plan definition object for the virtual group
     * @return FakeMetadataObject Metadata object for group
     */
	public static FakeMetadataObject createVirtualGroup(String name, FakeMetadataObject model, Object plan) {
		FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.GROUP);
		obj.putProperty(FakeMetadataObject.Props.MODEL, model);
		obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, model.getProperty(FakeMetadataObject.Props.IS_VIRTUAL));
		obj.putProperty(FakeMetadataObject.Props.PLAN, plan);
		obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.FALSE);		
        obj.putProperty(FakeMetadataObject.Props.TEMP, Boolean.FALSE);  
		return obj;
	}

    /**
     * Create a temp group with default settings.
     * @param name Name of virtual group, must match model name
     * @param model Associated model
     * @param plan Appropriate query plan definition object for the temp group
     * @return FakeMetadataObject Metadata object for group
     */
    public static FakeMetadataObject createTempGroup(String name, FakeMetadataObject model, Object plan) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.GROUP);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
        obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, Boolean.FALSE);
        obj.putProperty(FakeMetadataObject.Props.PLAN, plan);
        obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.FALSE); 
        obj.putProperty(FakeMetadataObject.Props.TEMP, Boolean.TRUE);  
        return obj; 
    }
	
    /**
     * Create a virtual group that allows updates with default settings.
     * @param name Name of virtual group, must match model name
     * @param model Associated model
     * @param plan Appropriate query plan definition object for the virtual group
     * @return FakeMetadataObject Metadata object for group
     */
	public static FakeMetadataObject createUpdatableVirtualGroup(String name, FakeMetadataObject model, QueryNode plan) {
		return createUpdatableVirtualGroup(name, model, plan, null);
	}
    
    public static FakeMetadataObject createUpdatableVirtualGroup(String name, FakeMetadataObject model, QueryNode plan, String updatePlan) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.GROUP);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
        obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, model.getProperty(FakeMetadataObject.Props.IS_VIRTUAL));
        obj.putProperty(FakeMetadataObject.Props.PLAN, plan);
        obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE);
        obj.putProperty(FakeMetadataObject.Props.UPDATE_PROCEDURE, updatePlan);
        obj.putProperty(FakeMetadataObject.Props.INSERT_PROCEDURE, "");
        obj.putProperty(FakeMetadataObject.Props.DELETE_PROCEDURE, "");
        return obj;
    }
		
    /**
     * Create element with default settings.
     * @param name Name of virtual group, must match model name
     * @param group Associated group
     * @param type Type of the element (see DataTypeManager)
     * @param index Column index in group
     * @return FakeMetadataObject Metadata object for element
     */
    public static FakeMetadataObject createElement(String name, FakeMetadataObject group, String type, int index) {
    	return createElement(name, group, type, index, true);
    }

    public static FakeMetadataObject createElement(String name, FakeMetadataObject group, String type, int index, boolean flag) { 
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.ELEMENT);
        obj.putProperty(FakeMetadataObject.Props.MODEL, group.getProperty(FakeMetadataObject.Props.MODEL));
        obj.putProperty(FakeMetadataObject.Props.GROUP, group);
        obj.putProperty(FakeMetadataObject.Props.TYPE, type);
        
        obj.putProperty(FakeMetadataObject.Props.SELECT, Boolean.TRUE); 
        if(type.equals(DataTypeManager.DefaultDataTypes.STRING)) {  
            obj.putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, Boolean.TRUE);        
        } else {
            obj.putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, Boolean.FALSE);
        }   
        obj.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.TRUE);     
        obj.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        obj.putProperty(FakeMetadataObject.Props.AUTO_INCREMENT, Boolean.FALSE);
        obj.putProperty(FakeMetadataObject.Props.DEFAULT_VALUE, null);
        obj.putProperty(FakeMetadataObject.Props.INDEX, new Integer(index));
        obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE);
        obj.putProperty(FakeMetadataObject.Props.LENGTH, "100"); //$NON-NLS-1$
        
        int indexOfDot = name.lastIndexOf("."); //$NON-NLS-1$
        if (flag) {
            name = name.substring(indexOfDot+1);
        } else {
            name = String.valueOf(index);
        }
        
        obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, name);
        return obj; 
    }
    
    /**
     * Create a set of elements in batch 
     * @param group The group metadata object
     * @param names Array of element names 
     * @param types Array of element types
     * @return List Ordered list of elements in the group
     */
	public static List<FakeMetadataObject> createElements(FakeMetadataObject group, String[] names, String[] types) { 
		String groupRoot = group.getName() + "."; //$NON-NLS-1$
		List<FakeMetadataObject> elements = new ArrayList<FakeMetadataObject>();
		
		for(int i=0; i<names.length; i++) { 
            FakeMetadataObject element = createElement(groupRoot + names[i], group, types[i], i);
			elements.add(element);		
		}
		
		return elements;
	}

    public static List createElements(FakeMetadataObject group, String[] names, String[] types, boolean flag) { 
        String groupRoot = group.getName() + "."; //$NON-NLS-1$
        List elements = new ArrayList();
        
        for(int i=0; i<names.length; i++) { 
            FakeMetadataObject element = createElement(groupRoot + names[i], group, types[i], i, flag);
            elements.add(element);      
        }
        
        return elements;
    }
    	
    /**
     * Create a set of elements in batch 
     * @param group The group metadata object
     * @param names Array of element names 
     * @param types Array of element types
     * @return List Ordered list of elements in the group
     */
	public static List createElementsWithDefaults(FakeMetadataObject group, String[] names, String[] types, Object[] defaults) {
		String groupRoot = group.getName() + "."; //$NON-NLS-1$
		List elements = new ArrayList();
		
		for(int i=0; i<names.length; i++) { 
            FakeMetadataObject element = createElement(groupRoot + names[i], group, types[i], i);
            element.setDefaultValue(defaults[i]);
			elements.add(element);		
		}
		
		return elements;
	}	

    /**
     * Create index.  The name will be used as the Object metadataID.
     * @param name String name of index
     * @param group the group for the index
     * @param elements the elements of the index (will be used as if they were
     * metadata IDs)
     * @return key metadata object
     */
    public static FakeMetadataObject createIndex(String name, FakeMetadataObject group, List elements) { 
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.KEY);
        obj.putProperty(FakeMetadataObject.Props.KEY_TYPE, FakeMetadataObject.TYPE_INDEX);
        obj.putProperty(FakeMetadataObject.Props.KEY_ELEMENTS, elements);
        Collection keys = (Collection)group.getProperty(FakeMetadataObject.Props.KEYS);
        if (keys == null){
            keys = new ArrayList();
            group.putProperty(FakeMetadataObject.Props.KEYS, keys);
        }
        keys.add(obj);
        return obj; 
    }

	/**
	 * Create primary key.  The name will be used as the Object metadataID.
	 * @param name String name of key
	 * @param group the group for the key
	 * @param elements the elements of the key (will be used as if they were
	 * metadata IDs)
	 * @return key metadata object
	 */
	public static FakeMetadataObject createKey(String name, FakeMetadataObject group, List elements) { 
		FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.KEY);
        obj.putProperty(FakeMetadataObject.Props.KEY_TYPE, FakeMetadataObject.TYPE_PRIMARY_KEY);
		obj.putProperty(FakeMetadataObject.Props.KEY_ELEMENTS, elements);
		Collection keys = (Collection)group.getProperty(FakeMetadataObject.Props.KEYS);
		if (keys == null){
			keys = new ArrayList();
			group.putProperty(FakeMetadataObject.Props.KEYS, keys);
		}
		keys.add(obj);
		return obj; 
	}

    /**
     * Create foreign key.  The name will be used as the Object metadataID.
     * @param name String name of key
     * @param group the group for the key
     * @param elements the elements of the key (will be used as if they were
     * @param primaryKey referenced by this foreign key
     * metadata IDs)
     * @return key metadata object
     */
    public static FakeMetadataObject createForeignKey(String name, FakeMetadataObject group, List elements, FakeMetadataObject primaryKey) { 
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.KEY);
        obj.putProperty(FakeMetadataObject.Props.KEY_TYPE, FakeMetadataObject.TYPE_FOREIGN_KEY);
        obj.putProperty(FakeMetadataObject.Props.KEY_ELEMENTS, elements);
        obj.putProperty(FakeMetadataObject.Props.REFERENCED_KEY, primaryKey);
        Collection keys = (Collection)group.getProperty(FakeMetadataObject.Props.KEYS);
        if (keys == null){
            keys = new ArrayList();
            group.putProperty(FakeMetadataObject.Props.KEYS, keys);
        }
        keys.add(obj);
        return obj; 
    }

    /**
     * Create access pattern (currently an access pattern is implemented as a type of key).  The name will
     * be used as the Object metadataID.
     * @param name String name of key
     * @param group the group for the access pattern
     * @param elements the elements of the access pattern (will be used as if they were
     * metadata IDs)
     * @return Access pattern metadata object
     */
    public static FakeMetadataObject createAccessPattern(String name, FakeMetadataObject group, List elements) { 
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.KEY);
        obj.putProperty(FakeMetadataObject.Props.KEY_TYPE, FakeMetadataObject.TYPE_ACCESS_PATTERN);
        obj.putProperty(FakeMetadataObject.Props.KEY_ELEMENTS, elements);
        Collection keys = (Collection)group.getProperty(FakeMetadataObject.Props.KEYS);
        if (keys == null){
            keys = new ArrayList();
            group.putProperty(FakeMetadataObject.Props.KEYS, keys);
        }
        keys.add(obj);
        return obj; 
    }
    
    /**
     * Create stored procedure parameter.
     * @param name Name of parameter
     * @param direction Direction of parameter
     * @param type Type of parameter
     * @param resultSet Result set metadata object or null if type is not result set
     * @return Metadata object for parameter
     */
    public static FakeMetadataObject createParameter(String name, int index, int direction, String type, Object resultSet) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.PARAMETER);
        obj.putProperty(FakeMetadataObject.Props.INDEX, new Integer(index));
        obj.putProperty(FakeMetadataObject.Props.DIRECTION, new Integer(direction));
        if(resultSet != null) {
            obj.putProperty(FakeMetadataObject.Props.RESULT_SET, resultSet);
            obj.putProperty(FakeMetadataObject.Props.TYPE, DataTypeManager.DefaultDataTypes.INTEGER);
        } else {
            obj.putProperty(FakeMetadataObject.Props.TYPE, type);            
        }
        obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, name);
        return obj;
    }

    /**
     * Create stored procedure.
     * @param name Name of procedure, must match model name
     * @param model Metadata object for the model
     * @param params List of FakeMetadataObject that are the parameters for the procedure
     * @return Metadata object for stored procedure
     */
    public static FakeMetadataObject createStoredProcedure(String name, FakeMetadataObject model, List params) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.PROCEDURE);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
        obj.putProperty(FakeMetadataObject.Props.PARAMS, params);
        
        return obj;    
    }
    
    /**
     * Create virtual sotred procedure.
     * @param name Name of stored query, must match model name
     * @param model Metadata object for the model
     * @param params List of FakeMetadataObject that are the parameters for the procedure
     * @param queryPlan Object representing query plan
     * @return Metadata object for stored procedure
     */
    public static FakeMetadataObject createVirtualProcedure(String name, FakeMetadataObject model, List params, Object queryPlan) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.PROCEDURE);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
        obj.putProperty(FakeMetadataObject.Props.PARAMS, params);
        obj.putProperty(FakeMetadataObject.Props.PLAN, queryPlan);
        return obj;
    }

    /**
     * Create a result set.
     * @param name Name of result set, must match model name
     * @param model Associated model
     * @param colNames Result set column names
     * @param colTypes Result set column types
     * @return FakeMetadataObject Metadata object for result set
     */
    public static FakeMetadataObject createResultSet(String name, FakeMetadataObject model, String[] colNames, String[] colTypes) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.RESULT_SET);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
                
        List columns = new ArrayList();
        for(int i=0; i<colNames.length; i++) {
            ElementSymbol col = new ElementSymbol(colNames[i]);
            col.setType(DataTypeManager.getDataTypeClass(colTypes[i]));
            FakeMetadataObject colId = new FakeMetadataObject(colNames[i], FakeMetadataObject.ELEMENT);
            colId.putProperty(FakeMetadataObject.Props.TYPE, colTypes[i]);
            colId.putProperty(FakeMetadataObject.Props.LENGTH, "10"); //$NON-NLS-1$
            colId.putProperty(FakeMetadataObject.Props.SELECT, Boolean.TRUE);
            col.setMetadataID(colId);
            columns.add(col);
        }
        
        obj.putProperty(FakeMetadataObject.Props.COLUMNS, columns);
        return obj;
    }

}
