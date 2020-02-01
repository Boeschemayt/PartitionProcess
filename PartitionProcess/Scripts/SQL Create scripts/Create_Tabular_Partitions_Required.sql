CREATE TABLE [partitioning].[Tabular_Partitions_Required](
	[Audit_Key] [int] NULL,
	[Tabular_Database_Name] [varchar](100) NULL,
	[Table_Name] [varchar](100) NULL,
	[Partition_Name] [varchar](100) NULL,
	[Partition_Filter] [varchar](max) NULL,
	[Partition_Action_Flag] [varchar](50) NULL,
	[Partition_Periods] [varchar](15) NULL,
	[Process_State] [int] NULL,
	[Start_Process] [datetime] NULL,
	[End_Process] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]