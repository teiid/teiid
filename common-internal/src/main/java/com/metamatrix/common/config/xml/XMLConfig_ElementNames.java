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

package com.metamatrix.common.config.xml;

import com.metamatrix.common.config.util.ConfigurationPropertyNames;

/**
* This class structure mimics the structure of a Configuration XML
* document and defines all of the element and attribute names for that 
* document type.
*/
public interface XMLConfig_ElementNames {
    
    /**
    * This is used as the delimiter of all XML element names.
    */
    public static final String DELIMITER = "."; //$NON-NLS-1$
    
    /**
    * This should be the root Element name for all Configuration XML Documents.
    */
    public static final String ELEMENT = "ConfigurationDocument"; //$NON-NLS-1$
    
        
        
        /**
        * This is the class that represents the Configuration Element which contains
        * all of the XML elements that represent a Configuration object.
        */
        public static class Configuration {
            /**
             * This is the name of the Configurations Element.
             */
             public static final String ELEMENT = "Configuration"; //$NON-NLS-1$
        	 public static class Attributes extends ComponentObject.Attributes{
        	 }
            
  
 
            
            /**
             * This is the class that represents the Host Element which contains
             * all of the XML elements that represent a Host object.
             */
             public static class Host {
             
                 /**
                 * This is the name of the Host Element.
                 */
                 public static final String ELEMENT = "Host"; //$NON-NLS-1$
                 public static class Attributes extends ComponentObject.Attributes{
                

             }
                
            
            /**
            * This is the class that represents the ComponentDefnID Element which contains
            * all of the XML elements that represent a ComponentDefnID object.
            */
//            public static final class ComponentDefnID{
//            
//                /**
//                * This is the name of the ComponentDefnID Element.
//                */
//                public static final String ELEMENT = "ComponentDefnID"; //$NON-NLS-1$
//                
//                /**
//                * This class defines the Attributes of the Element class that 
//                * contains it.  Note that this class just inherits its attributes
//                * from its configuration object superclass.
//                */
//                public static class Attributes extends ID.Attributes {
//                }
//            }
            
            /**
            * This is the class that represents the DeployedComponentID Element which contains
            * all of the XML elements that represent a DeployedComponentID object.
            */
//            public static final class DeployedComponentID{
//            
//                /**
//                * This is the name of the DeployedComponentID Element.
//                */
//                public static final String ELEMENT = "DeployedComponentID"; //$NON-NLS-1$
//                
//                /**
//                * This class defines the Attributes of the Element class that 
//                * contains it.  Note that this class just inherits its attributes
//                * from its configuration object superclass.
//                */
//                public static class Attributes extends ID.Attributes {
//                }
//            }
            
            /**
            * This is the class that represents the ProductServiceConfigID Element which contains
            * all of the XML elements that represent a ProductServiceConfigID object.
            */
//            public static final class ProductServiceConfigID{
//            
//                /**
//                * This is the name of the ProductServiceConfigID Element.
//                */
//                public static final String ELEMENT = "ProductServiceConfigID"; //$NON-NLS-1$
//                
//                /**
//                * This class defines the Attributes of the Element class that 
//                * contains it.  Note that this class just inherits its attributes
//                * from its configuration object superclass.
//                */
//                public static class Attributes extends ID.Attributes {
//                }
//            }
            
            /**
            * This is the class that represents the VMComponentDefnID Element which contains
            * all of the XML elements that represent a VMComponentDefnID object.
            */
//            public static final class VMComponentDefnID{
//            
//                /**
//                * This is the name of the VMComponentDefnID Element.
//                */
//                public static final String ELEMENT = "VMComponentDefnID"; //$NON-NLS-1$
//                
//                /**
//                * This class defines the Attributes of the Element class that 
//                * contains it.  Note that this class just inherits its attributes
//                * from its configuration object superclass.
//                */
//                public static class Attributes extends ID.Attributes {
//                }
//            }
            
            /**
            * This is the class that represents the VMComponentDefnID Element which contains
            * all of the XML elements that represent a VMComponentDefnID object.
            */
//            public static final class ServiceComponentDefnID{
//            
//                /**
//                * This is the name of the ServiceComponentDefnID Element.
//                */
//                public static final String ELEMENT = "ServiceComponentDefnID"; //$NON-NLS-1$
//                
//                /**
//                * This class defines the Attributes of the Element class that 
//                * contains it.  Note that this class just inherits its attributes
//                * from its configuration object superclass.
//                */
//                public static class Attributes extends ID.Attributes {
//                }
//            }
                        

               
        }
    
    /**
       * This is the class that represents the ProductServiceConfigs Element which contains
       * all of the XML elements that represent a ProductServiceConfigs object.
       */
       public static class AuthenticationProviders {
       
           /**
           * This is the name of the ProductServiceConfigs Element.
           */
           public static final String ELEMENT = "AuthenticationProviders"; //$NON-NLS-1$
           
           /**
           * This is the class that represents the ProductServiceConfig Element which contains
           * all of the XML elements that represent a ProductServiceConfig object.
           */
           public static class Provider {
           
               /**
               * This is the name of the ProductServiceConfig Element.
               */
               public static final String ELEMENT = "Provider"; //$NON-NLS-1$
               
                 public static class Attributes extends ComponentObject.Attributes{

               }
                         
           }
       }      

    
    /**
    * This is the class that represents the Resources Element which contains
    * all of the XML elements that represent a Resource object.
    */
    public static class Resources {
    
        /**
        * This is the name of the Hosts Element.
        */
        public static final String ELEMENT = "SharedResources"; //$NON-NLS-1$
        
        /**
        * This is the class that represents the Resource Element which contains
        * all of the XML elements that represent a Resource object.
        */
        public static class Resource {
        
            /**
            * This is the name of the Resource Element.
            */
            public static final String ELEMENT = "Resource"; //$NON-NLS-1$
            
                /**
                * This class defines the Attributes of the Element class that 
                * contains it.  Note that this class just inherits its attributes
                * from its configuration object superclass.
                */
            	public static class Attributes extends ComponentObject.Attributes{
                }
            
            
        }
    }
    
    /**
    * This is the class that represents the ServiceComponentDefns Element which contains
    * all of the XML elements that represent a ServiceComponentDefns object.
    */
    public static class ConnectorComponents {
    
        /**
        * This is the name of the ServiceComponentDefns Element.
        */
        public static final String ELEMENT = "ConnectorBindings"; //$NON-NLS-1$
        
        /**
        * This is the class that represents the ConnectorBinding Element which contains
        * all of the XML elements that represent a ConnectorBinding object.
        */
        public static class ConnectorComponent {
        
            /**
            * This is the name of the ConnectorBinding Element.
            */
            public static final String ELEMENT = "Connector"; //$NON-NLS-1$
        
            public static class Attributes extends ComponentObject.Attributes{
                public static final String ROUTING_UUID = "routingUUID"; //$NON-NLS-1$
            }
        }
    }
            
 
    
    /**
     * This is the class that represents the ServiceComponentDefns Element which contains
     * all of the XML elements that represent a ServiceComponentDefns object.
     */
     public static class Services {
     
         /**
         * This is the name of the ServiceComponentDefns Element.
         */
         public static final String ELEMENT = "Services"; //$NON-NLS-1$
         
         /**
         * This is the class that represents the ServiceComponentDefn Element which contains
         * all of the XML elements that represent a ServiceComponentDefn object.
         */
         public static class Service {
         
             /**
             * This is the name of the ServiceComponentDefn Element.
             */
             public static final String ELEMENT = "Service"; //$NON-NLS-1$
         
             public static class Attributes extends ComponentObject.Attributes{
                 public static final String QUEUED_SERVICE = "QueuedService"; //$NON-NLS-1$
                 public static final String ROUTING_UUID = "routingUUID"; //$NON-NLS-1$
             }
         }
     }
    
   

	/**
	* This is the class that represents the LogConfiguration Element which contains
	* all of the XML elements that represent a LogConfiguration object.
	*/
//	public static class LogConfiguration {
//	
//	    /**
//	    * This is the name of the LogConfiguration Element.
//	    */
//	    public static final String ELEMENT = "LogConfiguration"; //$NON-NLS-1$
//	    
//	}

	/**
	* This is the class that represents the VMComponentDefns Element which contains
	* all of the XML elements that represent a VMComponentDefns object.
	*/
//	public static class VMComponentDefns {
//	
//	    /**
//	    * This is the name of the VMComponentDefns Element.
//	    */
//	    public static final String ELEMENT = Configuration.ELEMENT + DELIMITER +"VMComponentDefns"; //$NON-NLS-1$
//	    
//	    /**
//	    * This is the class that represents the VMComponentDefn Element which contains
//	    * all of the XML elements that represent a VMComponentDefn object.
//	    */
//	    public static class VMComponentDefn {
//	    
//	        /**
//	        * This is the name of the VMComponentDefn Element.
//	        */
//	        public static final String ELEMENT = "VMComponentDefn"; //$NON-NLS-1$
//	        
//	        /**
//	        * This class defines the Attributes of the Element class that 
//	        * contains it.  Note that this class just inherits its attributes
//	        * from its configuration object superclass.
//	        */
//	        public static class Attributes extends ComponentObject.Attributes{
//	        }
//	    }
//	}

	            
	            /**
	            * This is the class that represents the VMComponentDefnID Element which contains
	            * all of the XML elements that represent a VMComponentDefnID object.
	            */
//	            public static final class VMComponentDefnID{
//	            
//	                /**
//	                * This is the name of the VMComponentDefnID Element.
//	                */
//	                public static final String ELEMENT = "VMComponentDefnID"; //$NON-NLS-1$
//	                
//	                /**
//	                * This class defines the Attributes of the Element class that 
//	                * contains it.  Note that this class just inherits its attributes
//	                * from its configuration object superclass.
//	                */
//	                public static class Attributes extends ID.Attributes {
//	                }
//	            }


				/**
				* This is the class that represents the HostID Element which contains
				* all of the XML elements that represent a HostID object.
				*/
//				public static final class HostID {
//				
//				    /**
//				    * This is the name of the HostID Element.
//				    */
//				    public static final String ELEMENT = "HostID"; //$NON-NLS-1$
//				    
//				    /**
//				    * This class defines the Attributes of the Element class that 
//				    * contains it.  Note that this class just inherits its attributes
//				    * from its configuration object superclass.
//				    */
//				    public static class Attributes extends ID.Attributes {
//				    }
//				}

				public static class DeployedService {
				    
				        /**
				        * This is the name of the ServiceComponentDefn Element.
				        */
				        public static final String ELEMENT = "DeployedService"; //$NON-NLS-1$
				    
				        public static class Attributes extends ComponentObject.Attributes{
				            public static final String ROUTING_UUID = "routingUUID"; //$NON-NLS-1$
				        }
				
				
				
				}

				/**
				* This is the class that represents the VMComponentDefns Element which contains
				* all of the XML elements that represent a VMComponentDefns object.
				*/
				public static class Process {
				
				    /**
				    * This is the name of the VMComponentDefns Element.
				    */
				    public static final String ELEMENT = "Process"; //$NON-NLS-1$
				        
				    /**
				    * This class defines the Attributes of the Element class that 
				    * contains it.  Note that this class just inherits its attributes
				    * from its configuration object superclass.
				    */
				    public static class Attributes extends ComponentObject.Attributes{
				    }
				
				}    
    
}



		/**
		* This is the class that represents the ComponentTypeID Element which contains
		* all of the XML elements that represent a ComponentTypeID object.
		*/
//		public static class ComponentTypeID {
//		
//		    /**
//		    * This is the name of the ComponentTypeID Element.
//		    */
//		    public static final String ELEMENT = "ComponentTypeID"; //$NON-NLS-1$
//		    
//		    /**
//		    * This class defines the Attributes of the Element class that 
//		    * contains it.  Note that this class just inherits its attributes
//		    * from its configuration object superclass.
//		    */
//		    public static class Attributes extends ID.Attributes {
//		    }
//		    
//		}



		/**
		* This is the class that represents the ComponentTypes Element which contains
		* all of the XML elements that represent a ComponentTypes object.
		*/
		public static class ComponentTypes {
		
		    /**
		    * This is the name of the ComponentTypes Element.
		    */
		    public static final String ELEMENT = "ComponentTypes"; //$NON-NLS-1$
		    
		    /**
		    * This is the class that represents the ComponentType Element which contains
		    * all of the XML elements that represent a ComponentType object.
		    */
		    public static class ComponentType {
		    
		        /**
		        * This is the name of the ComponentType Element.
		        */
		        public static final String ELEMENT = "ComponentType"; //$NON-NLS-1$
		        
		        /**
		        * This class defines the Attributes of the Element class that 
		        * contains it.
		        */
		        public static class Attributes {
		            public static final String NAME = "Name"; //$NON-NLS-1$
		            public static final String PARENT_COMPONENT_TYPE = "ParentComponentType"; //$NON-NLS-1$
		            public static final String SUPER_COMPONENT_TYPE = "SuperComponentType"; //$NON-NLS-1$
		            public static final String COMPONENT_TYPE_CODE = "ComponentTypeCode"; //$NON-NLS-1$
		            public static final String DEPLOYABLE = "Deployable"; //$NON-NLS-1$
		            public static final String DEPRECATED = "Deprecated"; //$NON-NLS-1$
		            public static final String MONITORABLE = "Monitorable"; //$NON-NLS-1$
		            public static final String DESCRIPTION = "Description"; //$NON-NLS-1$
		        }
		        
		        
		        /**
		        * This is the class that represents the ComponentTypeDefn Element which contains
		        * all of the XML elements that represent a ComponentTypeDefn object.
		        */
		        public static class ComponentTypeDefn {
		        
		            /**
		            * This is the name of the ComponentTypeDefn Element.
		            */
		            public static final String ELEMENT = "ComponentTypeDefn"; //$NON-NLS-1$
		            
		            /**
		            * This class defines the Attributes of the Element class that 
		            * contains it.
		            */
		            public static class Attributes {
		                public static final String DEPRECATED = "Deprecated"; //$NON-NLS-1$
		            }
		            
		            /**
		            * This is the class that represents the PropertyDefinition Element which contains
		            * all of the XML elements that represent a PropertyDefinition object.
		            */
		            public static class PropertyDefinition {
		            
		                /**
		                * This is the name of the PropertyDefinition Element.
		                */
		                public static final String ELEMENT = "PropertyDefinition"; //$NON-NLS-1$
		                
		                /**
		                * This class defines the Attributes of the Element class that 
		                * contains it.
		                */
		                public static class Attributes {
		                    public static final String NAME = "Name"; //$NON-NLS-1$
		                    public static final String DISPLAY_NAME = "DisplayName"; //$NON-NLS-1$
		                    public static final String SHORT_DESCRIPTION ="ShortDescription"; //$NON-NLS-1$
		                    public static final String DEFAULT_VALUE = "DefaultValue"; //$NON-NLS-1$
		                    public static final String MULTIPLICITY = "Multiplicity"; //$NON-NLS-1$
		                    public static final String PROPERTY_TYPE = "PropertyType"; //$NON-NLS-1$
		                    public static final String VALUE_DELIMITER = "ValueDelimiter"; //$NON-NLS-1$
		                    public static final String IS_CONSTRAINED_TO_ALLOWED_VALUES = "IsConstrainedToAllowedValues"; //$NON-NLS-1$
		                    public static final String IS_EXPERT = "IsExpert"; //$NON-NLS-1$
		                    public static final String IS_HIDDEN = "IsHidden"; //$NON-NLS-1$
		                    public static final String IS_MASKED = "IsMasked"; //$NON-NLS-1$
		                    public static final String IS_MODIFIABLE = "IsModifiable"; //$NON-NLS-1$
		                    public static final String IS_REQUIRED = "IsRequired"; //$NON-NLS-1$
		                    public static final String REQUIRES_RESTART = "RequiresRestart"; //$NON-NLS-1$
		                }
		                
		                /**
		                * This is the class that represents the AllowedValue Element which contains
		                * all of the XML elements that represent a AllowedValue object.
		                */
		                public static class AllowedValue {
		                
		                    /**
		                    * This is the name of the AllowedValue Element.
		                    */
		                    public static final String ELEMENT = "AllowedValue"; //$NON-NLS-1$
		                }
		                
		            }
		            
		        }
		    }
		}



		/**
		* This is the class that represents the ComponentObject Element which contains
		* all of the XML elements that represent a ComponentObject object.
		*/
		public static class ComponentObject {
		    public static class Attributes {
		        public static final String NAME = "Name"; //$NON-NLS-1$
		        public static final String COMPONENT_TYPE = "ComponentType"; //$NON-NLS-1$
		    }
		}



		/**
		* This is the class that represents the ChangeHistory Properties Element which contains
		* all of the XML elements that represent the change information for the object.
		*/
		public static class ChangeHistory {
		
		    /**
		    * This is the name of the ChangeHistory Element.
		    */
		    public static final String ELEMENT = "ChangeHistory"; //$NON-NLS-1$
		    
		    /**
		    * This is the class that represents the Property Element which contains
		    * all of the XML elements that represent a Property object.
		    */
		    public static class Property {
		    
		        /**
		        * This is the name of the Property Element.
		        */
		        public static final String ELEMENT = "Property"; //$NON-NLS-1$
		        
		        
		        /**
		        * This class defines the Attributes of the Element class that 
		        * contains it. 
		        */
		        public static class Attributes {
		            public static final String NAME = "Name"; //$NON-NLS-1$
		        }
		        
		        public static class NAMES {
		         	public static final String LAST_CHANGED_DATE = "LastChangedDate"; //$NON-NLS-1$
		         	public static final String LAST_CHANGED_BY = "LastChangedBy"; //$NON-NLS-1$
		          	public static final String CREATION_DATE = "CreationDate"; //$NON-NLS-1$
		          	public static final String CREATED_BY = "CreatedBy"; //$NON-NLS-1$
		
		        }
		    }
		}



		public static class Header {
		
		     /**
		     * This is the name of the Header Element.
		     */
		     public static final String ELEMENT = "Header"; //$NON-NLS-1$
		     
		     /**
		     * This is the class that represents the UserName Element which contains
		     * all of the XML elements that represent a UserName object.
		     */
		     public static class UserCreatedBy {
		     
		         /**
		         * This is the name of the UserName Element.
		         */
		         public static final String ELEMENT = ConfigurationPropertyNames.USER_CREATED_BY; 
		     }
		     
		     /**
		     * This is the class that represents the ApplicationCreatedDate Element which contains
		     * all of the XML elements that represent a ApplicationCreatedDate object.
		     */
		     public static class ApplicationCreatedBy {
		     
		         /**
		         * This is the name of the ApplicationCreatedDate Element.
		         */
		         public static final String ELEMENT = ConfigurationPropertyNames.APPLICATION_CREATED_BY; 
		     }
		     
		     /**
		     * This is the class that represents the ApplicationVersionCreatedBy Element which contains
		     * all of the XML elements that represent a ApplicationVersionCreatedBy object.
		     */
		     public static class ApplicationVersionCreatedBy {
		     
		         /**
		         * This is the name of the ApplicationVersionCreatedBy Element.
		         */
		         public static final String ELEMENT = ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY;            
		     }
		     
		     /**
		     * This is the class that represents the Time Element which contains
		     * all of the XML elements that represent a Time object.
		     */
		     public static class Time {
		     
		         /**
		         * This is the name of the Time Element.
		         */
		         public static final String ELEMENT = ConfigurationPropertyNames.TIME; 
		     }
		     
		     /**
		     * This is the class that represents the DocumentTypeVersion Element which contains
		     * all of the XML elements that represent a DocumentTypeVersion object.
		     */
		     public static class ConfigurationVersion {
		     
		         /**
		         * This is the name of the DocumentTypeVersion Element.
		         */
		         public static final String ELEMENT = ConfigurationPropertyNames.CONFIGURATION_VERSION; 
		     }
		     
		     /**
		     * This is the class that represents the MetaMatrixServerVersion Element which contains
		     * all of the XML elements that represent a ProductServiceConfigs object.
		     */
		     public static class MetaMatrixSystemVersion {
		     
		         /**
		         * This is the name of the MetaMatrixServerVersion Element.
		         */
		         public static final String ELEMENT = ConfigurationPropertyNames.SYSTEM_VERSION; 
		     }
		 }



		/**
		* This is the class that represents the Properties Element which contains
		* all of the XML elements that represent a Properties object.
		*/
		public static class Properties {
		
		    /**
		    * This is the name of the Properties Element.
		    */
		    public static final String ELEMENT = "Properties"; //$NON-NLS-1$
		    
		    /**
		    * This is the class that represents the Property Element which contains
		    * all of the XML elements that represent a Property object.
		    */
		    public static class Property {
		    
		        /**
		        * This is the name of the Property Element.
		        */
		        public static final String ELEMENT = "Property"; //$NON-NLS-1$
		        
		        
		        /**
		        * This class defines the Attributes of the Element class that 
		        * contains it. 
		        */
		        public static class Attributes {
		            public static final String NAME = "Name"; //$NON-NLS-1$
		        }
		        
		    }
		}



		/**
		* This is the class that represents the ID Element which contains
		* all of the XML elements that represent a ID object.
		*/
		public static class ID {
		    // these are the  shared attributes of all ID Elements
		    
		    /**
		    * This class defines the Attributes of the Element class that 
		    * contains it. 
		    */
		    public static class Attributes {
		        public static final String NAME = "Name"; //$NON-NLS-1$
		    }
		}   
}
