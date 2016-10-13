CREATE FOREIGN TABLE "Sample.Asset.ElementAttribute" (
        ID string(36) NOT NULL OPTIONS (ANNOTATION '', NAMEINSOURCE '[ID]', CASE_SENSITIVE FALSE, NATIVE_TYPE 'Cti_Guid'),
        Path string NOT NULL OPTIONS (ANNOTATION '', NAMEINSOURCE '[Path]', CHAR_OCTET_LENGTH 8000, NATIVE_TYPE 'Cti_WString'),
        Name string NOT NULL OPTIONS (ANNOTATION '', NAMEINSOURCE '[Name]', CHAR_OCTET_LENGTH 8000, NATIVE_TYPE 'Cti_WString'));

CREATE FOREIGN TABLE "Sample.Asset.ElementAttributeCategory" (
        ElementAttributeID string(36) NOT NULL OPTIONS (ANNOTATION '', NAMEINSOURCE '[ElementAttributeID]', CASE_SENSITIVE FALSE, NATIVE_TYPE 'Cti_G
uid'),
        CategoryID string(36) NOT NULL OPTIONS (ANNOTATION '', NAMEINSOURCE '[CategoryID]', CASE_SENSITIVE FALSE, NATIVE_TYPE 'Cti_Guid')
) OPTIONS (ANNOTATION '', NAMEINSOURCE '[Sample].[Asset].[ElementAttributeCategory]', UPDATABLE TRUE);

CREATE FOREIGN TABLE "Sample.Asset.ElementCategory" (
        ElementID string(36) NOT NULL OPTIONS (ANNOTATION '', NAMEINSOURCE '[ElementID]', CASE_SENSITIVE FALSE, NATIVE_TYPE 'Cti_Guid'),
        CategoryID string(36) NOT NULL OPTIONS (ANNOTATION '', NAMEINSOURCE '[CategoryID]', CASE_SENSITIVE FALSE, NATIVE_TYPE 'Cti_Guid')
) OPTIONS (ANNOTATION '', NAMEINSOURCE '[Sample].[Asset].[ElementCategory]', UPDATABLE TRUE);

CREATE FOREIGN PROCEDURE "Sample.EventFrame.GetPIPoint"(
    IN EventFrameAttributeID string NOT NULL OPTIONS (NATIVE_TYPE 'Cti_Guid')) RETURNS TABLE
        (Path string(8000) OPTIONS (NATIVE_TYPE 'Cti_WString'), 
        "Server" string(8000) OPTIONS (NATIVE_TYPE 'Cti_WString'), 
        Tag string(8000) OPTIONS (NATIVE_TYPE 'Cti_WString'),
        "Number of Computers" string(23) OPTIONS (NAMEINSOURCE '[Number of Computers]'))
    OPTIONS ("teiid_pi:TVF" 'TRUE', NAMEINSOUCE '[Sample].[EventFrame].[GetPIPoint]');

