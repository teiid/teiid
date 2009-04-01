if exists (select * from dbo.sysobjects where id = object_id(N'[FK_ATHPRMS_ATHPERM]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [AUTHPERMISSIONS] DROP CONSTRAINT FK_ATHPRMS_ATHPERM
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_ATHPRMS_ATHPLCY]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [AUTHPERMISSIONS] DROP CONSTRAINT FK_ATHPRMS_ATHPLCY
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_ATHPLCY_PLCYUID]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [AUTHPRINCIPALS] DROP CONSTRAINT FK_ATHPLCY_PLCYUID
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_ATHPRMS_ATHRLMS]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [AUTHPERMISSIONS] DROP CONSTRAINT FK_ATHPRMS_ATHRLMS
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_LOGENTRIES_MSGTYPES]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [LOGENTRIES] DROP CONSTRAINT FK_LOGENTRIES_MSGTYPES
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_PRNCPLTTRBS_PRPS]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [PRINCIPALATTRIBUTES] DROP CONSTRAINT FK_PRNCPLTTRBS_PRPS
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_PRNCPLTTR_PRNCPL]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [PRINCIPALATTRIBUTES] DROP CONSTRAINT FK_PRNCPLTTR_PRNCPL
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_PRNCPLCRDNTLS_PRNC]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [PRINCIPALCREDENTIALS] DROP CONSTRAINT FK_PRNCPLCRDNTLS_PRNC
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_PRNCPLMMBRSHP_MMBR]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [PRINCIPALMEMBERSHIPS] DROP CONSTRAINT FK_PRNCPLMMBRSHP_MMBR
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_PRNCPLMMBRSHP_PRNCP]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [PRINCIPALMEMBERSHIPS] DROP CONSTRAINT FK_PRNCPLMMBRSHP_PRNCP
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[FK_PRINCIPAL_PRINCIPALTYPE]') and OBJECTPROPERTY(id, N'IsForeignKey') = 1)
ALTER TABLE [PRINCIPALS] DROP CONSTRAINT FK_PRINCIPAL_PRINCIPALTYPE
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[AUDITENTRIES]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [AUDITENTRIES]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[AUTHPERMISSIONS]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [AUTHPERMISSIONS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[AUTHPERMTYPES]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [AUTHPERMTYPES]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[AUTHPOLICIES]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [AUTHPOLICIES]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[AUTHPRINCIPALS]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [AUTHPRINCIPALS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[AUTHREALMS]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [AUTHREALMS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[CS_EXT_FILES]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [CS_EXT_FILES]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[IDTABLE]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [IDTABLE]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[LOGENTRIES]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [LOGENTRIES]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[LOGMESSAGETYPES]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [LOGMESSAGETYPES]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[PRINCIPALTYPES]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [PRINCIPALTYPES]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[RT_MDLS]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [RT_MDLS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[RT_MDL_PRP_NMS]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [RT_MDL_PRP_NMS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[RT_MDL_PRP_VLS]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [RT_MDL_PRP_VLS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[RT_VDB_MDLS]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [RT_VDB_MDLS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[RT_VIRTUAL_DBS]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [RT_VIRTUAL_DBS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[TX_MMXCMDLOG]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [TX_MMXCMDLOG]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[TX_SQL]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [TX_SQL]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[TX_SRCCMDLOG]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [TX_SRCCMDLOG]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[MMSCHEMAINFO_CA]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
DROP TABLE [MMSCHEMAINFO_CA]
GO

