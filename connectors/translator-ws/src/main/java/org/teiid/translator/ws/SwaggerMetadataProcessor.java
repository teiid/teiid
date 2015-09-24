package org.teiid.translator.ws;

import java.util.Map;

import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;

import io.swagger.models.Path;
import io.swagger.models.Swagger;
import io.swagger.parser.SwaggerParser;

public class SwaggerMetadataProcessor {
    
    private Swagger swagger;
    
    public SwaggerMetadataProcessor(String url){
        swagger = new SwaggerParser().read(url);
    }
    
    public void getMetadata(MetadataFactory mf, WSConnection connection) throws TranslatorException {
        
        Map<String, Path> pathMap = swagger.getPaths();
        for(String key : pathMap.keySet()) {
            System.out.println(key + " -> " + pathMap.get(key));
        }
    }
    
    public static void main(String[] args) throws TranslatorException {
        
        SwaggerMetadataProcessor processor = new SwaggerMetadataProcessor("http://localhost:8080/swagger.json");
        processor.getMetadata(null, null);
    }

}
