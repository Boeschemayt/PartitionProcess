CREATE TABLE [partitioning].[Tabular_Partition_Config](
	[Tabular_Partition_Config_Id] [int] IDENTITY(1,1) NOT NULL,
	[Tabular_Database_Name] [varchar](100) NULL,
	[Table_Name] [varchar](100) NULL,
	[Partition_Name_Prefix] [varchar](100) NULL,
	[Partitioning_Column] [varchar](100) NULL,
	[Source_Object] [varchar](100) NULL,
	[Stage_Table] [varchar](100) NULL,
	[Stage_Column] [varchar](100) NULL,
	[Partition_Grain] [varchar](100) NULL,
	[Partitions_Start_Date_SK] [int] NULL,
	[Partitions_End_Date_SK] [int] NULL,
	[Partitions_Start_Date] [date] NULL,
	[Partitions_End_Date] [date] NULL,
	[Process_Grain] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[Tabular_Partition_Config_Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
