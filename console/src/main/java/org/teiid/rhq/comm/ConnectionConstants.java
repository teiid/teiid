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
package org.teiid.rhq.comm;


/** 
 * These are the Constants that used in conjunction with using the 
 * @since 5.5.3
 */
public interface ConnectionConstants {
	
	
	public static final String VERSION = "6.0.0";
    
    /**
     * These are Environment properties need to create a connection.  They will be exposed via the @see #getEnvironment call.
     */
	
	// The system key is the value used to obtain a connection.   
	// In embedded, its a predefined value
	// In enterprise, its the installation directory
	    public final static String SYSTEM_KEY = "system.key"; //$NON-NLS-1$
        public final static String USERNAME = "username"; //$NON-NLS-1$
        public final static String PASSWORD = "password"; //$NON-NLS-1$
        public final static String URL = "url"; //$NON-NLS-1$
//        public final static String INSTALL_DIR = "install.dir"; //$NON-NLS-1$

    /**
     * These are global properties used by all components
     */
        /*
         * This is the key for the fully qualified identifier. 
         * For Runtime components it should be the deployedcomponent full name
         * For Resource components it should be the Service Defn full name
         * for adding to the value maps for metrics and operations.
         */
        public final static String IDENTIFIER = "identifier"; //$NON-NLS-1$
        

    /**
     * These properties are exposed via the #getProperty method call.
     */
        public static String SYSTEM_NAME = "cluster.name"; //$NON-NLS-1$
        public static String SYSTEM_NAME_IDENTIFIER = "JGroups"; //$NON-NLS-1$
        
        /**
         * Use these component type names when calling Connection related methods
         * that require the type.
         * @since 1.0
         */
        public  interface ComponentType {
            public final static String PLATFORM = "Platform"; //$NON-NLS-1$            
            
        	public interface Runtime {
        		
        		public interface System {
					public final static String TYPE = "Runtime.System"; //$NON-NLS-1$

					public static interface Operations {

						public final static String BOUNCE_SYSTEM = "bounceSystem"; //$NON-NLS-1$					
						public final static String GET_LONGRUNNINGQUERIES = "listLongRunningQueries"; //$NON-NLS-1$
						
					}      
					
					public static interface Metrics {

						public final static String QUERY_COUNT = "queryCount"; //$NON-NLS-1$            
						public final static String SESSION_COUNT = "sessionCount"; //$NON-NLS-1$
						public final static String LONG_RUNNING_QUERIES = "longRunningQueries"; //$NON-NLS-1$     
						
					}
        		}
        		       		
        	
        		
        		public interface Process {

					public final static String TYPE = "Runtime.Process"; //$NON-NLS-1$
					public static interface Operations {
					
					}
       			
        		}
        		
        		
        		public interface Connector {

					public final static String TYPE = "Runtime.Connector"; //$NON-NLS-1$
					public static interface Operations {

						public final static String RESTART_CONNECTOR = "restart"; //$NON-NLS-1$            
						public final static String STOP_CONNECTOR = "stop"; //$NON-NLS-1$ 
						
					}
       			
        		}
        		
//        		public interface Service {
//
//					public final static String TYPE = "Runtime.Service"; //$NON-NLS-1$
//					public static interface Operations {
//
//						public final static String RESTART_SERVICE = "restart"; //$NON-NLS-1$            
//						public final static String STOP_SERVICE = "stop"; //$NON-NLS-1$ 
//						
//					}
//       			
//        		} 
        		
        		public interface Session {

					public final static String TYPE = "Runtime.Sesssion"; //$NON-NLS-1$
					public static interface Query {

						public final static String GET_SESSIONS = "getSessions"; //$NON-NLS-1$
					}
       			

       			
        		} 
        		
        		public interface Queries {

					public final static String TYPE = "Runtime.Queries"; //$NON-NLS-1$
					public static interface Query {

						public final static String GET_QUERIES = "listQueries"; //$NON-NLS-1$
					}
       			

       			
        		} 
        	
        	}
        	public interface Resource {
        		
        		public interface VDB {
					public final static String TYPE = "teiid"; //$NON-NLS-1$
					public final static String SUBTYPE = "vdb"; //$NON-NLS-1$


					public static interface Operations {
											
					}        			
        		}
        		
        		public interface Model {
					public final static String TYPE = "teiid"; //$NON-NLS-1$
					public final static String SUBTYPE = "model"; //$NON-NLS-1$


					public static interface Operations {
											
					}        			
        		}
        		
        		public interface Service {

					public final static String TYPE = "Resource.Service"; //$NON-NLS-1$
					public static interface Operations {
						
					}
					
					public static interface Query {
						
					}
       			
        		}   
        		public interface Connector {

					public final static String TYPE = "Resource.Connector"; //$NON-NLS-1$
					public static interface Operations {
						
					}
       			
        		}
        	}
        	
        	public interface Security {
        		
        	}
            /**
             * Use these metric names when calling getValues() on the connection
             * interface.
             * @since 1.0
             */
            public  interface Metric {
            	public final static String HIGH_WATER_MARK = "highWatermark"; //$NON-NLS-1$
                
            }
            
            /**
             * Use these operation names when calling executeOperation() on the connection
             * interface.
             * @since 1.0
             */
            public static interface Operation {
            	public final static String KILL_REQUEST = "killRequest"; //$NON-NLS-1$
                public final static String GET_VDBS = "listVDBs"; //$NON-NLS-1$
                
                public final static String GET_PROPERTIES = "getProperties"; //$NON-NLS-1$
                 
                /**
                 * Use these value names when calling executeOperation() on the connection
                 * interface. These will correlate with parameters used in operations.
                 * @since 1.0
                 */
                public static interface Value {
                	public final static String STOP_NOW = "stopNow"; //$NON-NLS-1$               
                	public final static String WAIT_UNTIL_FINISHED = "waitUntilFinished"; //$NON-NLS-1$
                	
                	public final static String INCLUDE_SOURCE_QUERIES = "includeSourceQueries"; //$NON-NLS-1$
                	
                	public final static String LONG_RUNNING_QUERY_LIMIT = "longRunningQueryLimit"; //$NON-NLS-1$
                	
                	public final static String FIELD_LIST = "fieldList"; //$NON-NLS-1$
                	
                	public final static String REQUEST_ID = "requestID"; //$NON-NLS-1$
                	
                	public final static String NAME = "Name"; //$NON-NLS-1$
                	public final static String VALUE = "Value"; //$NON-NLS-1$
                    
                }
                
            }
            
        }  
        
   }
