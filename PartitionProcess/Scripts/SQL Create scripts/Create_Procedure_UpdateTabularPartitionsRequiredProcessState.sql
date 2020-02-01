CREATE procedure [partitioning].[UpdateTabularPartitionsRequiredProcessState]
@Tabular_Database_Name nvarchar(250),
@State int
as

update r
	set	r.Process_State = @State,
	Start_Process = case when @State = 1 then GETDATE() ELSE Start_process end,
	End_Process = case when @State = 0 then GETDATE() end
		from partitioning.Tabular_Partitions_Required r
			where Tabular_Database_Name = @Tabular_Database_Name 

