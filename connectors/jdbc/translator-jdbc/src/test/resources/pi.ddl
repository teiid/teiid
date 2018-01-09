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

CREATE FOREIGN TABLE SmallA (
	IntKey integer OPTIONS (NATIVE_TYPE 'int32'),
	StringKey string OPTIONS (NATIVE_TYPE 'string'),
	IntNum integer OPTIONS (NATIVE_TYPE 'int32'),
	StringNum string OPTIONS (NATIVE_TYPE 'string'),
	FloatNum float OPTIONS (NATIVE_TYPE 'single'),
	LongNum long OPTIONS (NATIVE_TYPE 'int64'),
	DoubleNum double OPTIONS (NATIVE_TYPE 'double'),
	ByteNum byte OPTIONS (NATIVE_TYPE 'int8'),
	DateValue date OPTIONS (NATIVE_TYPE 'datetime'),
	TimeValue timestamp OPTIONS (NATIVE_TYPE 'datetime'),
	TimestampValue timestamp OPTIONS (NATIVE_TYPE 'timestamp'),
	BooleanValue boolean OPTIONS (NATIVE_TYPE 'boolean'),
	CharValue char OPTIONS (NATIVE_TYPE 'string'),
	ShortValue short OPTIONS (NATIVE_TYPE 'int16'),
	BigIntegerValue biginteger OPTIONS (NATIVE_TYPE 'int64'),
	BigDecimalValue integer OPTIONS (NATIVE_TYPE 'int32'),
	ObjectValue string OPTIONS (NATIVE_TYPE 'string'))
OPTIONS (UPDATABLE 'FALSE', NAMEINSOURCE 'dvqe..SmallA');

CREATE FOREIGN TABLE SmallB (
	IntKey integer OPTIONS (NATIVE_TYPE 'int32'),
	StringKey string OPTIONS (NATIVE_TYPE 'string'),
	IntNum integer OPTIONS (NATIVE_TYPE 'int32'),
	StringNum string OPTIONS (NATIVE_TYPE 'string'),
	FloatNum float OPTIONS (NATIVE_TYPE 'single'),
	LongNum long OPTIONS (NATIVE_TYPE 'int64'),
	DoubleNum double OPTIONS (NATIVE_TYPE 'double'),
	ByteNum byte OPTIONS (NATIVE_TYPE 'int8'),
	DateValue date OPTIONS (NATIVE_TYPE 'datetime'),
	TimeValue timestamp OPTIONS (NATIVE_TYPE 'datetime'),
	TimestampValue timestamp OPTIONS (NATIVE_TYPE 'timestamp'),
	BooleanValue boolean OPTIONS (NATIVE_TYPE 'boolean'),
	CharValue char OPTIONS (NATIVE_TYPE 'string'),
	ShortValue short OPTIONS (NATIVE_TYPE 'int16'),
	BigIntegerValue biginteger OPTIONS (NATIVE_TYPE 'int64'),
	BigDecimalValue integer OPTIONS (NATIVE_TYPE 'int32'),
	ObjectValue string OPTIONS (NATIVE_TYPE 'string'))
OPTIONS (UPDATABLE 'FALSE', NAMEINSOURCE 'dvqe..SmallB');

CREATE FOREIGN TABLE MediumA (
	IntKey integer OPTIONS (NATIVE_TYPE 'int32'),
	StringKey string OPTIONS (NATIVE_TYPE 'string'),
	IntNum integer OPTIONS (NATIVE_TYPE 'int32'),
	StringNum string OPTIONS (NATIVE_TYPE 'string'),
	FloatNum float OPTIONS (NATIVE_TYPE 'single'),
	LongNum long OPTIONS (NATIVE_TYPE 'int64'),
	DoubleNum double OPTIONS (NATIVE_TYPE 'double'),
	ByteNum byte OPTIONS (NATIVE_TYPE 'int8'),
	DateValue date OPTIONS (NATIVE_TYPE 'datetime'),
	TimeValue timestamp OPTIONS (NATIVE_TYPE 'datetime'),
	TimestampValue timestamp OPTIONS (NATIVE_TYPE 'timestamp'),
	BooleanValue boolean OPTIONS (NATIVE_TYPE 'boolean'),
	CharValue char OPTIONS (NATIVE_TYPE 'string'),
	ShortValue short OPTIONS (NATIVE_TYPE 'int16'),
	BigIntegerValue biginteger OPTIONS (NATIVE_TYPE 'int64'),
	BigDecimalValue integer OPTIONS (NATIVE_TYPE 'int32'),
	ObjectValue string OPTIONS (NATIVE_TYPE 'string'))
OPTIONS (UPDATABLE 'FALSE', NAMEINSOURCE 'dvqe..MediumA');

CREATE FOREIGN TABLE MediumB (
	IntKey integer OPTIONS (NATIVE_TYPE 'int32'),
	StringKey string OPTIONS (NATIVE_TYPE 'string'),
	IntNum integer OPTIONS (NATIVE_TYPE 'int32'),
	StringNum string OPTIONS (NATIVE_TYPE 'string'),
	FloatNum float OPTIONS (NATIVE_TYPE 'single'),
	LongNum long OPTIONS (NATIVE_TYPE 'int64'),
	DoubleNum double OPTIONS (NATIVE_TYPE 'double'),
	ByteNum byte OPTIONS (NATIVE_TYPE 'int8'),
	DateValue date OPTIONS (NATIVE_TYPE 'datetime'),
	TimeValue timestamp OPTIONS (NATIVE_TYPE 'datetime'),
	TimestampValue timestamp OPTIONS (NATIVE_TYPE 'timestamp'),
	BooleanValue boolean OPTIONS (NATIVE_TYPE 'boolean'),
	CharValue char OPTIONS (NATIVE_TYPE 'string'),
	ShortValue short OPTIONS (NATIVE_TYPE 'int16'),
	BigIntegerValue biginteger OPTIONS (NATIVE_TYPE 'int64'),
	BigDecimalValue integer OPTIONS (NATIVE_TYPE 'int32'),
	ObjectValue string OPTIONS (NATIVE_TYPE 'string'))
OPTIONS (UPDATABLE 'FALSE', NAMEINSOURCE 'dvqe..MediumB');