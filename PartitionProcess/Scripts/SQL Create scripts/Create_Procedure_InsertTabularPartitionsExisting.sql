
CREATE PROCEDURE [partitioning].[InsertTabularPartitionsExisting]
	@Audit_Key INT,
	@Tabular_Database_Name varchar(100),
	@Table_Name varchar(100),
	@Partition_Name varchar(100),
	@Partition_RefreshedTime varchar(100),
	@Partition_State varchar(100)
AS
    ------------------
    -- Local variables
    -- ---------------
    DECLARE @lcl_Audit_Key INT = @Audit_Key
	DECLARE @lcl_Tabular_Database_Name varchar(100) = @Tabular_Database_Name
	DECLARE @lcl_Table_Name varchar(100) = @Table_Name
	DECLARE @lcl_Partition_Name varchar(100) = @Partition_Name
	DECLARE @lcl_Partition_RefreshedTime datetime = TRY_CONVERT(DATETIME, @Partition_RefreshedTime, 103)
	DECLARE @lcl_Partition_State varchar(100) = @Partition_State


	BEGIN TRY
		BEGIN TRAN;

		INSERT INTO partitioning.Tabular_Partitions_Existing ([Audit_Key], [Tabular_Database_Name], [Table_Name], [Partition_Name], [Partition_RefreshedTime], [Partition_State])
		VALUES (@lcl_Audit_Key, @lcl_Tabular_Database_Name, @lcl_Table_Name, @lcl_Partition_Name, @lcl_Partition_RefreshedTime, @lcl_Partition_State)

		IF @@TRANCOUNT > 0
			COMMIT TRAN;

	END TRY

		BEGIN CATCH
			IF @@TRANCOUNT > 0
				ROLLBACK TRAN;
			THROW; 
		END CATCH;
GO