﻿CREATE TABLE [partitioning].[Datum] (
    [DateKey]             INT          NOT NULL,
    [Date]                DATE         NULL,
    [Day]                 TINYINT      NOT NULL,
    [Weekday]             TINYINT      NOT NULL,
    [WeekdayName]         VARCHAR (10) NOT NULL,
    [IsWeekend]           BIT          NOT NULL,
    [DayOfYear]           SMALLINT     NOT NULL,
    [Week]                TINYINT      NOT NULL,
    [Month]               TINYINT      NOT NULL,
    [MonthName]           VARCHAR (10) NOT NULL,
    [MonthNameShort]      VARCHAR (3)  NOT NULL,
    [Quarter]             TINYINT      NOT NULL,
    [QuarterName]         VARCHAR (6)  NOT NULL,
    [Year]                INT          NOT NULL,
    [YearQuarter]         VARCHAR (6)  NOT NULL,
    [YearMonth]           VARCHAR (7)  NOT NULL,
    [MonthYear]           VARCHAR (8)  NOT NULL,
    [YearWeek]            VARCHAR (7)  NOT NULL,
    [FirstDayOfMonth]     DATE         NULL,
    [LastDayOfMonth]      DATE         NULL,
    [FirstDayOfQuarter]   DATE         NULL,
    [LastDayOfQuarter]    DATE         NULL,
    [FirstDayOfYear]      DATE         NULL,
    [LastDayOfYear]       DATE         NULL,
    [FirstDayOfNextMonth] DATE         NULL,
    [FirstDayOfNextYear]  DATE         NULL,
    [MonthNO]             INT          NOT NULL,
    [CurrentMonthNO]      INT          NOT NULL,
    [IsSwedishHoliday]    BIT          NOT NULL,
    [SwedishHolidayName]  VARCHAR (30) NULL,
    CONSTRAINT [PK_Datum] PRIMARY KEY CLUSTERED ([DateKey] ASC)
);


