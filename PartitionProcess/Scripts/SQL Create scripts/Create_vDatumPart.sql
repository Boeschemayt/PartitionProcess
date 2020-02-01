CREATE VIEW [partitioning].[vDatumPart] AS
SELECT [DateKey]
      ,[Date]							AS [Datum]
      ,[Day]							AS [Dag av månad]
	  ,[Day]							AS [Dag av månad sortering]
	  ,[Weekday]						AS [Veckodagsnummer]
	  ,[WeekdayName]					AS [Veckodag]
	  ,[IsWeekend]						AS [Helg]
	  ,[IsSwedishHoliday]				AS [Helgdag]
	  ,[SwedishHolidayName]				AS [Helgdagsnamn]
	  ,[DayOfYear]						AS [Dag av år]
      ,[Week]							AS [Vecka av år]

	  ,CASE WHEN [Week] %2=0 THEN concat('TV',([Week]-1)) ELSE concat('TV',[Week]) END AS [Tvåveckor av år]
	  
	  ,[Month]							AS [Månadsnummer]
	  ,[MonthName]						AS [Månad]
	  ,[MonthNameShort]					as [Månadsnamn]

      ,[Quarter]						AS [Kvartal]
	  ,[QuarterName]					AS [Kvartalsnamn]

      ,[Year]							AS [År]
	  ,[YearQuarter]					AS [År och kvartal]
	  ,[YearMonth]						AS [År och månad]
      ,[MonthYear]						AS [Månad och år]
	  ,CONCAT(LEFT([DateKey],4),SUBSTRING(CAST([DateKey] AS VARCHAR),5,2)) AS [YearMonth_Code]

	  ,MonthNO							AS [Månad och år sortering]

	  ,[YearWeek]						AS [År och vecka]
	  
	  ,[FirstDayOfMonth]				AS [Första dagen i månaden]
	  ,[LastDayOfMonth]					AS [Sista dagen i månaden]
	  ,[FirstDayOfQuarter]				AS [Första dagen i kvartalet]
	  ,[LastDayOfQuarter]				AS [Sista dagen i kvartalet]
	  ,[FirstDayOfYear]					AS [Första dagen på året]
	  ,[LastDayOfYear]					AS [Sista dagen på året]
	  ,[FirstDayOfNextMonth]			AS [Första dagen i nästa månad]
	  ,[FirstDayOfNextYear]				AS [Första dagen på nästa år]

	  ,[MonthNO]						AS [Löpnummer månad]
	  ,[CurrentMonthNO]					AS [Löpnummer månad aktuell]

FROM [Partitioning].[Datum]