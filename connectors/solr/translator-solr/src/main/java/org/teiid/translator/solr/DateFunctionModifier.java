package org.teiid.translator.solr;

import java.text.SimpleDateFormat;
import java.util.List;
import org.teiid.language.Function;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.FunctionModifier;

public class DateFunctionModifier extends FunctionModifier {
	
	@Override
	public List<?> translate(Function function){
		
		if( (function.getName() != "parsetimestamp" && function.getName() != "formattimestamp") || 
			(function.getParameters() == null || function.getParameters().size() != 2) ) 
		{
			return (List<?>) new Exception();
		}

		String dateFormat = function.getParameters().get(1).toString();
		if( 
			dateFormat.equals("'yyyy'")					|| 
			dateFormat.equals("'yyyy-MM'") 				|| 
			dateFormat.equals("'yyyy-MM-dd'") 			|| 
			dateFormat.equals("'yyyy-MM-dd HH'") 		|| 
			dateFormat.equals("'yyyy-MM-dd HH:mm'") 	|| 
			dateFormat.equals("'yyyy-MM-dd HH:mm:ss'") 	) 
		{
			function.setName("");
			function.getParameters().remove(1);
			return null;
		} else {
			return (List<?>) new Exception();
		}
		
		
		
	}

}