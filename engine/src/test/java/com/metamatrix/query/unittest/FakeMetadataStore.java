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

package com.metamatrix.query.unittest;

import java.util.*;

public class FakeMetadataStore {

	private Map models = new HashMap();
	private Map groups = new HashMap();
	private Map elements = new HashMap();
    private Map keys = new HashMap();
    private Map procedures = new HashMap();
    private Map resultSets = new HashMap();

	public FakeMetadataStore() { 			
	}
	
	public void addObject(FakeMetadataObject object) { 
		String lookupName = object.getName().toUpperCase();
		if(object.getType().equals(FakeMetadataObject.ELEMENT)) { 
			elements.put(lookupName, object); 
		} else if(object.getType().equals(FakeMetadataObject.GROUP)) { 
			groups.put(lookupName, object); 
		} else if(object.getType().equals(FakeMetadataObject.MODEL)) { 
			models.put(lookupName, object); 		
        } else if(object.getType().equals(FakeMetadataObject.KEY)) { 
            keys.put(lookupName, object);   
        } else if(object.getType().equals(FakeMetadataObject.PROCEDURE)) {
            procedures.put(lookupName, object);      
        } else if(object.getType().equals(FakeMetadataObject.RESULT_SET)) {
            resultSets.put(lookupName, object);
		} else {
			throw new IllegalArgumentException("Bad FakeMetadataObject type: " + object.getType()); //$NON-NLS-1$
		}
	}

	public void addObjects(Collection objects) { 
		Iterator iter = objects.iterator();
		while(iter.hasNext()) { 
			addObject((FakeMetadataObject) iter.next());
		}
	}
		
	public FakeMetadataObject findObject(String name, String type) { 
		String lookupName = name.toUpperCase();
		if(type.equals(FakeMetadataObject.ELEMENT)) { 
			return (FakeMetadataObject) elements.get(lookupName); 
		} else if(type.equals(FakeMetadataObject.GROUP)) { 
			return (FakeMetadataObject) groups.get(lookupName); 
		} else if(type.equals(FakeMetadataObject.MODEL)) { 
			return (FakeMetadataObject) models.get(lookupName); 		
        } else if(type.equals(FakeMetadataObject.KEY)) { 
            return (FakeMetadataObject) keys.get(lookupName);         
        } else if(type.equals(FakeMetadataObject.PROCEDURE)) {
            return (FakeMetadataObject) procedures.get(lookupName);
        } else if(type.equals(FakeMetadataObject.RESULT_SET)) {
            return (FakeMetadataObject) resultSets.get(lookupName);
		} else {
			throw new IllegalArgumentException("Bad FakeMetadataObject type: " + type); //$NON-NLS-1$
		}
	}

	public List findObjects(String type, String propertyName, Object matchValue) { 
		Map domain = null;
		if(type.equals(FakeMetadataObject.ELEMENT)) { 
			domain = elements; 
		} else if(type.equals(FakeMetadataObject.GROUP)) { 
			domain = groups; 
		} else if(type.equals(FakeMetadataObject.MODEL)) { 
			domain = models; 
        } else if(type.equals(FakeMetadataObject.KEY)) { 
            domain = keys; 
        } else if(type.equals(FakeMetadataObject.PROCEDURE)) {
            domain = procedures;
        } else if(type.equals(FakeMetadataObject.RESULT_SET)) {
            domain = resultSets;
		} else {
			throw new IllegalArgumentException("Bad FakeMetadataObject type: " + type); //$NON-NLS-1$
		}
		
		List found = new ArrayList();
		
		Iterator iter = domain.values().iterator();
		while(iter.hasNext()) { 
			FakeMetadataObject mdobj = (FakeMetadataObject) iter.next();
			if(compareWithNull(mdobj.getProperty(propertyName), matchValue)) { 
				found.add(mdobj);
			}			
		}
		
		return found;
	}


	public boolean compareWithNull(Object obj1, Object obj2) { 
		if(obj1 == null) { 
			if(obj2 == null) { 
				return true;
			}
			return false;
		}
		if(obj2 == null) { 
			return false;
		}
		return obj1.equals(obj2);
	}

}
