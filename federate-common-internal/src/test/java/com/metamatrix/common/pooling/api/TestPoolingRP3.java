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

package com.metamatrix.common.pooling.api;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ResourceComponentType;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.pooling.api.exception.ResourceWaitTimeOutException;
import com.metamatrix.common.pooling.impl.ResourcePoolMgrImpl;
import com.metamatrix.common.pooling.resource.GenericResourceAdapter;

public class TestPoolingRP3 extends TestCase{

    private static final String USER = "TestPoolingRP3"; //$NON-NLS-1$

    int count;

    ResourceComponentType compType;
    ComponentTypeID compTypeID; 
    ComponentTypeID superID;
    ComponentTypeID parentID = null;
    
    BasicConfigurationObjectEditor  editor ;
    

    public TestPoolingRP3(String name) {
        super(name);
        
        editor = new BasicConfigurationObjectEditor();
        
        
        superID = new ComponentTypeID("ResourceType"); //$NON-NLS-1$
        compTypeID = new ComponentTypeID("GenericResourceType"); //$NON-NLS-1$
        try {
                compType = ConfigUtil.createComponentType("GenericResourceType", superID, parentID); //$NON-NLS-1$
        } catch(Exception e) {
             fail(e.getMessage());
        }
        
    }

    /**
    * Test that the Resource Wait Time Out Exception ocurrs
    * Expanded mode is defaulting to false
    */
    public void testScenario1(){
    // the max_pool_size should be less than thread cnt
    // the difference between the two are the number of expected time out errors
    // note: only one exception per thread can occurr in this test
      int threadCnt = 2;
      int max_size = 1;
      String min = "1"; //$NON-NLS-1$
      String users = "1"; //$NON-NLS-1$
      String max_pool_size = String.valueOf(max_size);

      int expectedNumErrors = threadCnt - max_size;


      ResourcePoolMgrImpl mgr = new ResourcePoolMgrImpl();

      try {
          
                ResourceDescriptor descriptor = ConfigUtil.createDescriptor("RP3Pool ", compTypeID);  //$NON-NLS-1$
                                                                           
        
                Properties props = new Properties();
                
                props.setProperty(ResourcePoolPropertyNames.RESOURCE_ADAPTER_CLASS_NAME, "com.metamatrix.common.pooling.resource.GenericResourceAdapter"); //$NON-NLS-1$
                props.setProperty(ResourcePoolPropertyNames.RESOURCE_POOL_CLASS_NAME, "com.metamatrix.common.pooling.impl.BasicResourcePool"); //$NON-NLS-1$
                
                props.setProperty(GenericResourceAdapter.RESOURCE_CLASS_NAME, "com.metamatrix.common.pooling.NoOpResource"); //$NON-NLS-1$
                props.setProperty(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE, min);
                props.setProperty(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE, max_pool_size);            
                props.setProperty(ResourcePoolPropertyNames.WAIT_TIME_FOR_RESOURCE, "100"); // .1 seconds //$NON-NLS-1$
                
                props.setProperty(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS, users);
                
                editor.modifyProperties(descriptor, props, ConfigurationObjectEditor.ADD);
          
                             
          TimeOutThread[] ts = new TimeOutThread[threadCnt];

          for (count = 0; count < threadCnt; count++) {
              ts[count] = new TimeOutThread(mgr, descriptor);
          }

          for(int k = 0; k < threadCnt; k++){
    			    ts[k].start();
          }

            try {
                  for(int k = 0; k < threadCnt; k++){
                      ts[k].join();
                  }
            } catch (InterruptedException e) {
            }

            int cntEs = 0;
            Exception te = null;
            for(int k = 0; k < threadCnt; k++){
              if (ts[k].hasException()) {
                  Exception e = ts[k].getException();
                  if (e instanceof ResourceWaitTimeOutException) {
                // test should fail with this exception
                      ++cntEs;

                  } else {
                      te = e;
                      break;
                  }

              }
            }


            if (cntEs == expectedNumErrors) {
                // returned the number of timeouts expected
            } else if (cntEs == 0) {
                fail("Testing wait time for resource did not throw a ResourceWaitTimeOutException."); //$NON-NLS-1$
            } else if (cntEs > expectedNumErrors) {
                fail("Testing wait time for resource incurred too many time outs - " + cntEs); //$NON-NLS-1$
                // test should fail with this exception
            } else if (te != null) {
               throw te;
            }


    	}catch(Exception e){
    		fail(e.getMessage());
    	}

    }

    protected class TimeOutThread extends BaseThread{
      private ResourcePoolMgr mgr;
      private ResourceDescriptor descriptor;

      public TimeOutThread(ResourcePoolMgr mgr, ResourceDescriptor rd) {
          super(1);
          this.mgr = mgr;
          this.descriptor = rd;
      }
    	public void run(){
      // DO NOT call resource.close(), all the resources should remain
      // checkedout to cause the next resource request to timeout.
      

        for (int i=0; i < perThreadCnt; i++ ) {

            try {

                
    //            GenericResourceDescriptor grd = new GenericResourceDescriptor(rprops, pprops);
//                Resource resource = null;

//                resource = 
                mgr.getResource(descriptor, USER);
//                System.out.println(objName + " " + i + ": Resource - " + resource.getName());

            } catch (Exception toe) {
                setException(toe);
//                System.out.println(objName + " " + i + ": Resource - " + resource.getName() + " timed out");

            }
            // yield to the other thread to checkout instance, which should timeout
            yield();
        }
    	}	
    }

    protected class BaseThread extends Thread{
    	protected String objName = "Thread " + count; //$NON-NLS-1$
      protected int perThreadCnt = 1;
      private Exception t = null;


      public BaseThread(int iterationCnt) {
          perThreadCnt = iterationCnt;
      }

      public Exception getException() {
          return t;
      }

      public void setException(Exception te) {
          t = te;
      }

      public boolean hasException() {
          return (t==null ? false : true);
      }

    }


} 
