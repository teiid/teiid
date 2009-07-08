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

package com.metamatrix.metadata.runtime.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.vdb.runtime.BasicModelInfo;

/**
 */
public class TestModelClasses  extends TestCase {

        
        public TestModelClasses(String name) {
            super(name);
        }
        
        public void testModel1() throws Exception {
            BasicModelID id = new BasicModelID("model", "1");
            BasicVirtualDatabaseID vdbid = new BasicVirtualDatabaseID("vdb", "1");
            
            BasicModel m  = new BasicModel(id, vdbid);
            
            m = buildModel(m, 0, 0, Model.PUBLIC);
            validateModel(m, 0, 0, Model.PUBLIC);
            
        }
        
        
        public void testModel2() throws Exception {
            BasicModelID id = new BasicModelID("model", "2");
            BasicVirtualDatabaseID vdbid = new BasicVirtualDatabaseID("vdb", "2");
            
            BasicModel m  = new BasicModel(id, vdbid);
            
            m = buildModel(m, 2, 1, Model.PUBLIC);
            validateModel(m, 2, 1, Model.PUBLIC);
        } 
        
        
        public void testModel3() throws Exception {
            BasicModelID id = new BasicModelID("model", "2");
            BasicVirtualDatabaseID vdbid = new BasicVirtualDatabaseID("vdb", "2");
            
            BasicModelInfo bmi = new BasicModelInfo("basicmodelinfoX");
            bmi = buildModelInfo(bmi, 2, 3);
            validateModelInfo(bmi, 2, 3);
            
            BasicModel m  = new BasicModel(id, vdbid, bmi);
            
            validateModel(m, 2, 0, bmi.getModelType());
        }         
        
        public void testModelInfo1() throws Exception {
            
            BasicModelInfo bmi = new BasicModelInfo("basicmodelinfo1");
            bmi = buildModelInfo(bmi, 0, 0);
            validateModelInfo(bmi, 0, 0);

        }

        public void testModelInfo2() throws Exception {
            
            BasicModelInfo bmi = new BasicModelInfo("basicmodelinfo1");
            bmi = buildModelInfo(bmi, 2, 3);
            validateModelInfo(bmi, 2, 3);

        }
        
//        public void testModelInfo3() throws Exception {
//            BasicModelID id = new BasicModelID("model", "3");
//            BasicVirtualDatabaseID vdbid = new BasicVirtualDatabaseID("vdb", "3");
//            
//            BasicModel m  = new BasicModel(id, vdbid);                       
//            m = buildModel(m, 2, 1, Model.PUBLIC);
//            
//            BasicModelInfo bmi = new BasicModelInfo(m);
//            validateModelInfo(bmi, 2, 0);
//
//        }        
        
            
        private BasicModel buildModel(BasicModel m, int bindings, int props, int modelType) throws Exception {
            
            m.setDescription("model description");
            m.setGUID("guid");
            m.setIsVisible(true);
            m.setModelType(modelType);
            m.setModelURI("uri");
            m.setProperties(new Properties());
            m.enableMutliSourceBindings(false);
            m.setVersionDate(new Date());
            m.setVersionedBy("versionedby");
            m.setVisibility(Model.PUBLIC);
            
            for (int x=0; x < bindings; x++) {
                m.addConnectorBindingName("connectorbinding" + x);
            }
            
            for (int x=0; x < props; x++) {
                m.addProperty("name" + x, "property" + x);
            }
            
            return m;

        }
        
        private void validateModel(BasicModel m, int bindings, int props, int modelType) throws Exception {

            if (m.getConnectorBindingNames().size() != bindings) {
                fail("Model " + m.getID() + " should have had " + bindings + " connector bindings");
            }
            if (props > 0 && m.getProperties().size() != props ) {
                fail("Model " + m.getID() + " should have had " + props + " properties");
            } else if (props == 0 && 
                            (m.getProperties() != null && 
                                            m.getProperties().size() != props)) {
                fail("Model " + m.getID() + " should have had " + props + " properties");
                
                
            }
            
            //m.getAlias();
            assertNotNull(m.getDateVersioned());
            
            assertNotNull(m.getDescription());
            assertNotNull(m.getFullName());
            assertNotNull(m.getGUID());
            assertNotNull(m.getID());
            assertEquals(modelType, m.getModelType());
            assertNotNull(m.getModelTypeName());
            assertNotNull(m.getModelURI());
            assertNotNull(m.getName());
            assertNotNull(m.getNameInSource());
            assertNotNull(m.getVersion());
            assertNotNull(m.getVersionedBy());
            assertNotNull(m.getVirtualDatabaseID());
            assertEquals(Model.PUBLIC,  m.getVisibility());

        }  
        
     private BasicModelInfo buildModelInfo(BasicModelInfo bmi, int bindings, int files) {
         
         bmi.setIsVisible(true);
         if (files > 0) {
             bmi.setModelType(ModelType.MATERIALIZATION);
         } else {
             bmi.setModelType(ModelType.PHYSICAL);
         }
         bmi.setModelURI("uri");
         bmi.setUuid("uuid");
         bmi.setVersion("1");
         bmi.setDescription("desc");
         bmi.setVersionDate(new Date());
         bmi.setVersionedBy("versionedBY");
         bmi.setVisibility(Model.PUBLIC);
         bmi.enableMutliSourceBindings(false);
         
         
         for (int x=0; x < bindings; x++) {
             bmi.addConnectorBindingByName("uuid" + x);
         }
         
         Map fm = new HashMap(files);
         
         for (int x=0; x < files; x++) {
             fm.put("filename" +x, "filename"+x);
         }
         bmi.setDDLFiles(fm);
         
         return bmi;
     }
     
     
     private void validateModelInfo(BasicModelInfo m, int bindings, int files) {
         assertNotNull(m.getDateVersioned());
         
         assertNotNull(m.getName());
         assertNotNull(m.getUUID());
         
         if (files > 0) {
             assertEquals(ModelType.MATERIALIZATION, m.getModelType());
         } else {
             assertEquals(ModelType.PHYSICAL, m.getModelType());
         }
         
         assertNotNull(m.getModelTypeName());
         assertNotNull(m.getModelURI());
         assertNotNull(m.getName());
         assertNotNull(m.getDescription());
         assertNotNull(m.getVersion());
         assertNotNull(m.getVersionedBy());
         assertNotNull(m.getDateVersioned());

         assertEquals(Model.PUBLIC,  m.getVisibility());
         
         if (m.getConnectorBindingNames().size() != bindings) {
             fail("Model " + m.getName() + " should have had " + bindings + " connector bindings, but found " + m.getConnectorBindingNames().size());
         }
         
         assertEquals(files, m.getDDLFileNames().length);
                
     }
        
 }

