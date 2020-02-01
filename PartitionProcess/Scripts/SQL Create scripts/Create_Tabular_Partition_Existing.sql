CREATE TABLE [partitioning].[Tabular_Partitions_Existing](
	[Audit_Key] [int] NULL,
	[Tabular_Database_Name] [varchar](100) NULL,
	[Table_Name] [varchar](100) NULL,
	[Partition_Name] [varchar](100) NULL,
	[Partition_RefreshedTime] [datetime] NULL,
	[Partition_State] [varchar](100) NULL
) ON [PRIMARY]