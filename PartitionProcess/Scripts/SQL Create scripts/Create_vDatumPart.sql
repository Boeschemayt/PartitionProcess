CREATE VIEW [partitioning].[vDatumPart] AS
SELECT [DateKey]
      ,[Date]							AS [Datum]
      ,[Day]							AS [Dag av m�nad]
	  ,[Day]							AS [Dag av m�nad sortering]
	  ,[Weekday]						AS [Veckodagsnummer]
	  ,[WeekdayName]					AS [Veckodag]
	  ,[IsWeekend]						AS [Helg]
	  ,[IsSwedishHoliday]				AS [Helgdag]
	  ,[SwedishHolidayName]				AS [Helgdagsnamn]
	  ,[DayOfYear]						AS [Dag av �r]
      ,[Week]							AS [Vecka av �r]

	  ,CASE WHEN [Week] %2=0 THEN concat('TV',([Week]-1)) ELSE concat('TV',[Week]) END AS [Tv�veckor av �r]
	  
	  ,[Month]							AS [M�nadsnummer]
	  ,[MonthName]						AS [M�nad]
	  ,[MonthNameShort]					as [M�nadsnamn]

      ,[Quarter]						AS [Kvartal]
	  ,[QuarterName]					AS [Kvartalsnamn]

      ,[Year]							AS [�r]
	  ,[YearQuarter]					AS [�r och kvartal]
	  ,[YearMonth]						AS [�r och m�nad]
      ,[MonthYear]						AS [M�nad och �r]
	  ,CONCAT(LEFT([DateKey],4),SUBSTRING(CAST([DateKey] AS VARCHAR),5,2)) AS [YearMonth_Code]

	  ,MonthNO							AS [M�nad och �r sortering]

	  ,[YearWeek]						AS [�r och vecka]
	  
	  ,[FirstDayOfMonth]				AS [F�rsta dagen i m�naden]
	  ,[LastDayOfMonth]					AS [Sista dagen i m�naden]
	  ,[FirstDayOfQuarter]				AS [F�rsta dagen i kvartalet]
	  ,[LastDayOfQuarter]				AS [Sista dagen i kvartalet]
	  ,[FirstDayOfYear]					AS [F�rsta dagen p� �ret]
	  ,[LastDayOfYear]					AS [Sista dagen p� �ret]
	  ,[FirstDayOfNextMonth]			AS [F�rsta dagen i n�sta m�nad]
	  ,[FirstDayOfNextYear]				AS [F�rsta dagen p� n�sta �r]

	  ,[MonthNO]						AS [L�pnummer m�nad]
	  ,[CurrentMonthNO]					AS [L�pnummer m�nad aktuell]

FROM [gem_dim].[Datum]