CREATE FOREIGN PROCEDURE listPets(IN "limit" integer NOT NULL OPTIONS (ANNOTATION 'How many items to return at one time (max 100)', "teiid_rel:default_handling" 'omit', "teiid_rest:COLLECION_FORMAT" 'form', "teiid_rest:PARAMETER_TYPE" 'query')) RETURNS TABLE (code integer, message string)
OPTIONS (ANNOTATION 'List all pets', "teiid_rest:METHOD" 'GET', "teiid_rest:PRODUCES" 'application/json', "teiid_rest:URI" 'http://petstore.swagger.io/v1/pets');

CREATE FOREIGN PROCEDURE createPets() RETURNS TABLE (code integer, message string)
OPTIONS (ANNOTATION 'Create a pet', "teiid_rest:METHOD" 'POST', "teiid_rest:PRODUCES" 'application/json', "teiid_rest:URI" 'http://petstore.swagger.io/v1/pets');

CREATE FOREIGN PROCEDURE showPetById(IN petId string NOT NULL OPTIONS (ANNOTATION 'The id of the pet to retrieve', "teiid_rest:COLLECION_FORMAT" 'simple', "teiid_rest:PARAMETER_TYPE" 'path')) RETURNS TABLE (code integer, message string)
OPTIONS (ANNOTATION 'Info for a specific pet', "teiid_rest:METHOD" 'GET', "teiid_rest:PRODUCES" 'application/json', "teiid_rest:URI" 'http://petstore.swagger.io/v1/pets/{petId}');