package org.teiid.resource.adapter.coherence;

import java.util.Iterator;
import java.util.List;

import org.teiid.core.BundleUtil;
import org.teiid.language.Comparison;
import org.teiid.translator.TranslatorException;

import com.tangosol.util.Filter;
import com.tangosol.util.QueryHelper;

/**
 * 
 * @author vhalbert
 *
 *	TODO:  add the LimitFilter
 */

public class CoherenceFilterUtil {
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CoherenceFilterUtil.class);

    
    public static Filter createFilter(String filterString) throws TranslatorException {
		return QueryHelper.createFilter(filterString); 	
    }  
    
    public static Filter createInFilter(String colName, List<Object> parms, Class<?> type) throws TranslatorException {
		String parm = null;
		for (Iterator<Object> it = parms.iterator(); it.hasNext();) {
			Object t = it.next();
			if (parm != null) {
				parm += ",";
			}
            if(type == String.class) {
            	parm = (String) t;        
            } else if (type == Long.class) {
                parm = String.valueOf(t + "l");
               
            } else {
            	parm = t.toString();
            }

		}
    
		String filterString = colName + " in (" + parm + ")";
		return QueryHelper.createFilter(filterString);
  	
    }
    
    public static Filter createCompareFilter(String colName, Object parm, Comparison.Operator op, Class<?> type) throws TranslatorException {
    	String parmValue = null;
        if(type == String.class) {
        	parmValue = (String) parm;        
        } else if (type == Long.class) {
        	parmValue = String.valueOf(parm + "l");
           
        } else {
        	parmValue = parm.toString();
        }
        
        String opString = " = ";
        
		switch(op) {
	    case NE:
		case EQ:
			break;
		case GT:
			opString = " > ";
			break;
		case GE:
			opString = " >= "; 
			break;
		case LT:
			opString = " < ";
			break;
		case LE:
			opString = " <= "; 
			break;
		default:
            final String msg = UTIL.getString("CoherenceVisitor.criteriaNotSupportedError"); 
			throw new TranslatorException(msg); 
			
		}
        
		String filterString = colName + opString + parmValue ;
		return QueryHelper.createFilter(filterString);
  	
    }  
  

}
