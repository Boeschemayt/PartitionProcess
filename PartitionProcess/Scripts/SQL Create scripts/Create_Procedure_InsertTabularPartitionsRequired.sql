
CREATE PROCEDURE [partitioning].[InsertTabularPartitionsRequired]
	@Audit_Key int,
	@Tabular_Database_Name varchar(100),
	@Table_Name varchar(100)
AS
    ---------------------
    -- Local variables --
    ---------------------
    DECLARE @lcl_Audit_Key INT = @Audit_Key
	DECLARE @lcl_Tabular_Database_Name varchar(100) = @Tabular_Database_Name
	DECLARE @lcl_Table_Name varchar(100) = @Table_Name

	
	
	---------------------------------------------------------------------------------
	-- Dynamically create a list of partitioning periods based on the config value --
	---------------------------------------------------------------------------------
	DECLARE @Partitioning_Dim_Date_Column AS VARCHAR(100)
	DECLARE @Stage_Table AS VARCHAR(100)
	DECLARE @Stage_Column AS VARCHAR(100)
	DECLARE @Partition_Name_Prefix AS VARCHAR(100)
	DECLARE @Partition_Process_Grain as INT

		SELECT  @Partitioning_Dim_Date_Column = gm.Partitioning_Dim_Date_Column,
				@Stage_Table = pc.Stage_Table,
				@Stage_Column = pc.Stage_Column,
				@Partition_Name_Prefix = pc.Partition_Name_Prefix,
				@Partition_Process_Grain = pc.Process_Grain
		FROM	partitioning.Tabular_Partition_Config AS pc
		JOIN	partitioning.Tabular_Partition_Grain_Mapping AS gm
			ON	pc.Partition_Grain = gm.Partition_Grain
		WHERE	pc.Tabular_Database_Name = @lcl_Tabular_Database_Name
			AND	pc.Table_Name = @lcl_Table_Name
	----------------------------------------------------
	-- Define temp table with yearmonth's for data exists in stage_table --
	----------------------------------------------------
	DECLARE @Stage_table_Periods nvarchar(512)

	DROP TABLE IF exists #Stage_Table_Periods
	create table #Stage_Table_Periods (DataMonths varchar(6), NextMonths varchar(6))

	set @Stage_Table_Periods = 
		'insert into #Stage_Table_Periods(
			DataMonths,
			NextMonths
		)
					select distinct 
		cast(YEAR([Uppdaterad datum]) as nvarchar(4)) +''''+ format([Uppdaterad datum], ''MM'')  ,
		cast(year(DATEADD(mm,1,[Uppdaterad datum])) as nvarchar(4)) +''''+ FORMAT(DATEADD(mm,1,[Uppdaterad datum]),''MM'') 
			from '+@stage_table+'
		'

	exec sp_executesql @Stage_Table_Periods


	DECLARE @sql_List_Partitioning_Periods AS NVARCHAR(MAX)

	DROP TABLE IF EXISTS #List_Partitioning_Periods
	CREATE TABLE #List_Partitioning_Periods (Partitioning_Periods INT, Partition_Name_Suffix VARCHAR(10), Partition_Start_Date_SK INT, Partition_End_Date_SK INT)

	SET @sql_List_Partitioning_Periods =
	N'	
		INSERT INTO #List_Partitioning_Periods (Partitioning_Periods, Partition_Name_Suffix, Partition_Start_Date_SK, Partition_End_Date_SK)
		SELECT	DISTINCT
				dd.' + @Partitioning_Dim_Date_Column + ' AS Partitioning_Periods,
				CAST(dd.' + @Partitioning_Dim_Date_Column + ' AS VARCHAR(10)) AS Partition_Name_Suffix,
				MIN(Datekey) AS Partition_Start_Date_SK,
				MAX(dd.Datekey) AS Partition_End_Date_SK
		FROM	partitioning.vDatumPart AS dd inner join #Stage_Table_Periods p ON (dd.YearMonth_Code = p.DataMonths or dd.YearMonth_Code = p.NextMonths)
		WHERE	EXISTS (
					SELECT	1
					FROM	partitioning.Tabular_Partition_Config AS c 
					WHERE	c.Partitions_Start_Date_SK <= dd.DateKey
						AND	dd.DateKey <= c.Partitions_End_Date_SK
						AND	c.Tabular_Database_Name = ''' + @lcl_Tabular_Database_Name + '''
						AND	c.Table_Name = ''' + @lcl_Table_Name + '''
				)
		GROUP BY dd.' + @Partitioning_Dim_Date_Column + ',
				CAST(dd.' + @Partitioning_Dim_Date_Column + ' AS VARCHAR(10))
	'
	
	EXECUTE sp_executesql @sql_List_Partitioning_Periods


	
	----------------------------------------------------
	-- Define required partitions with date filtering --
	----------------------------------------------------
	;WITH
	cte_partition_config AS (
		SELECT  pc.Tabular_Database_Name,
				pc.Table_Name,
				pc.Partition_Name_Prefix,
				pc.Source_Object,
				pc.Partitioning_Column,
				pc.Partition_Grain,
				gm.Partitioning_Dim_Date_Column,
				pc.Stage_Table,
				pc.Stage_Column,
				pc.Partitions_Start_Date_SK,
				pc.Partitions_End_Date_SK
		FROM	partitioning.Tabular_Partition_Config AS pc
		JOIN	partitioning.Tabular_Partition_Grain_Mapping AS gm
			ON	pc.Partition_Grain = gm.Partition_Grain

		WHERE	pc.Tabular_Database_Name = @lcl_Tabular_Database_Name
			AND	pc.Table_Name = @lcl_Table_Name
	)
	,
	cte_partition_periods AS (
		SELECT	Partitioning_Periods, Partition_Name_Suffix, Partition_Start_Date_SK, Partition_End_Date_SK
		FROM	#List_Partitioning_Periods
	)
	,
	cte_partition_required AS (
		SELECT	c.Tabular_Database_Name,
				c.Table_Name,
				c.Partition_Name_Prefix + p.Partition_Name_Suffix AS Partition_Name,
				p.Partition_Name_Suffix,
				p.Partitioning_Periods,
				Partition_Start_Date_SK,
				Partition_End_Date_SK
		FROM    cte_partition_config AS c
		CROSS JOIN	cte_partition_periods AS p
	)

	----------------------------------------------
	-- Populate ETL_Tabular_Partitions_Required --
	----------------------------------------------	
	INSERT INTO partitioning.Tabular_Partitions_Required ([Audit_Key], [Tabular_Database_Name], [Table_Name], [Partition_Name], [Partition_Filter], [Partition_Action_Flag], [Partition_Periods])
	SELECT	@lcl_Audit_Key AS Audit_Key,
			ISNULL(p.Tabular_Database_Name, e.Tabular_Database_Name) AS Tabular_Database_Name,
			ISNULL(p.Table_Name, e.Table_Name) AS Table_Name,
			ISNULL(p.Partition_Name, e.Partition_Name) AS Partition_Name,
			--'WHERE Transaction_Date_SK >= ' + CAST(p.Partition_Start_Date_SK AS char(8)) + ' AND Transaction_Date_SK <= ' + CAST(p.Partition_End_Date_SK AS char(8)) AS Partition_Filter, --sql
			'[Data], #"Filtered Rows" = Table.SelectRows('+ replace(@Stage_Table,'.','_') +', each DateTime.Date([Uppdaterad datum]) >= #date('+ LEFT(p.Partition_Start_Date_SK,4)+','+ RIGHT(LEFT(p.Partition_Start_Date_SK,6),2)+','+ RIGHT(p.Partition_Start_Date_SK,2)+') and 
			DateTime.Date([Uppdaterad datum]) <= #date('+LEFT(p.Partition_End_Date_SK,4) +','+ RIGHT(LEFT(p.Partition_End_Date_SK,6),2)+ ',' +RIGHT(p.Partition_End_Date_SK,2)+')) in #"Filtered Rows"' as test, -- Power Query
			CASE
				WHEN e.Partition_Name IS NULL THEN 'Create'
				WHEN p.Partition_Name IS NULL THEN 'Leave' 
				WHEN e.Partition_Name = p.Partition_Name THEN 'Existing'
				ELSE 'Malfunction'
			END AS Partition_Action_Flag,
			Partitioning_Periods
	FROM    cte_partition_required AS p
	FULL OUTER JOIN	partitioning.Tabular_Partitions_Existing AS e
				ON	(e.Partition_Name = p.Partition_Name and e.Tabular_Database_Name = p.Tabular_Database_Name)
            where isnull(p.Tabular_Database_Name, e.Tabular_Database_Name) = @lcl_Tabular_Database_Name 
		order by Partition_Name
	

	
				
	-- flip flag to Process from Existing if there is data for that partition in staging
	DECLARE @sql_List_Staging_Periods AS NVARCHAR(MAX)
	
	DROP TABLE IF EXISTS #List_Staging_Periods
	CREATE TABLE #List_Staging_Periods (Partitioning_Periods INT, Partition_Name_Prefix VARCHAR(100), Partition_Name_Suffix VARCHAR(10))

	SET @sql_List_Staging_Periods =
	
	N'	
		INSERT INTO #List_Staging_Periods (Partitioning_Periods, Partition_Name_Prefix, Partition_Name_Suffix)
		SELECT	DISTINCT
				dd.' + @Partitioning_Dim_Date_Column + ' AS Partitioning_Periods,
				''' + @Partition_Name_Prefix + ''',
				CAST(dd.' + @Partitioning_Dim_Date_Column + ' AS VARCHAR(10)) AS Partition_Name_Suffix
		FROM	partitioning.vDatumPart AS dd
		WHERE	EXISTS (
					SELECT	1
					FROM	' + @Stage_Table + ' AS s
					WHERE	s.' + @Stage_Column + ' = dd.Datum
				)
		GROUP BY dd.' + @Partitioning_Dim_Date_Column + ',
				CAST(dd.' + @Partitioning_Dim_Date_Column + ' AS VARCHAR(10))
	'
	
	EXECUTE sp_executesql @sql_List_Staging_Periods
	
	-- set Proces flag on partitions that Exists and have data in stage-table for that period. (Only flag partition in current grain (month, year etc) and process_grain back (1 month back etc)) 
	UPDATE	p
		SET	p.Partition_Action_Flag = 'Process'
	FROM	partitioning.Tabular_Partitions_Required p 
				
	WHERE	EXISTS (
				SELECT	1
				FROM	#List_Staging_Periods AS s
				WHERE	(s.Partition_Name_Prefix + s.Partition_Name_Suffix) = p.Partition_Name
				and Partitioning_Periods >= FORMAT(dateadd(month, -@Partition_Process_Grain,GETDATE()), 'yyyyMM')  and Partitioning_Periods <= FORMAT(GETDATE(), 'yyyyMM')
			)
		AND	p.Partition_Action_Flag IN ('Existing')	
	

	-- Set delete flag on partitions that have a post in tabular_partitions_config
	UPDATE r
	set Partition_Action_Flag = 'Delete'
		from partitioning.Tabular_Partitions_Required r 
		inner join partitioning.Tabular_Partition_Config c on (r.Tabular_Database_Name = c.Tabular_Database_Name)
			where r.Partition_Action_Flag = 'Leave'

	-- Set delete flag on partitions that exists in analysis services but not have data in stage_table.
	UPDATE r
		set Partition_Action_Flag = 'Delete'
			from partitioning.Tabular_Partitions_Required r left join #Stage_Table_Periods p
				on (r.Partition_Periods = p.DataMonths or r.Partition_Periods = p.NextMonths)
			where 
			p.DataMonths is null
			and Partition_Action_Flag like 'Existing'
			and Tabular_Database_Name = @Tabular_database_Name



			
GO