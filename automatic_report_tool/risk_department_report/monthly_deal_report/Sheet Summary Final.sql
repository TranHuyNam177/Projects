DECLARE @t0Date DATETIME
SET @t0Date = '2024-05-21';

WITH

[RMR9004.GroupBy] AS (
	SELECT
		[sub_account]
		, [ticker] [Stock]
		, SUM([trading_volume]) + SUM([receiving_volume]) [Quantity]
	FROM [DWH-CoSo].[dbo].[vmr9004]
	WHERE [date] = @t0Date
	GROUP BY [ticker], [sub_account]
)

, [DataClosedPrice] AS (
	--- đảm bảo luôn lấy giá trị mới nhất
	SELECT
		[ticker] [Stock]
		, [Close] [ClosedPrice]
	FROM (
		SELECT 
			MAX([Date]) OVER (PARTITION BY [ticker]) [MaxDate]
			, [Date]
			, [ticker]
			, [Close]
		FROM [DWH-ThiTruong].[dbo].[DuLieuGiaoDichNgay]
		-- WHERE [Date] <= @t0Date
	) [t]
	WHERE [t].[MaxDate] = [t].[Date]
)

, [RMR0062.Filter] AS (
	SELECT
		[account_code]
		, SUM([cash]) [TotalCash]
	FROM [DWH-CoSo].[dbo].[RMR0062]
	WHERE [RMR0062].[date] = @t0Date
		AND [loan_type] = 1
	GROUP BY [account_code]
)

, [RMR0015.FilterByStock] AS(
    SELECT
		[sub_account]
		, [ticker] [Stock]
		, SUM([market_value]) [MarketValueByStock]
    FROM [DWH-CoSo].[dbo].[rmr0015]
    WHERE [date] = @t0Date
    GROUP BY [sub_account], [ticker]
)

, [RMR0015.FilterBySubAccount] AS(
    SELECT
		[sub_account]
		, SUM([market_value]) [TotalMarketValueAllStock]
    FROM [DWH-CoSo].[dbo].[rmr0015]
    WHERE [date] = @t0Date
    GROUP BY [sub_account]
)

, [230006_DanhSachTieuKhoanMargin] AS (
	SELECT
		[230006_DanhSachTieuKhoan].[Ngay] [Date]
		, [230006_DanhSachTieuKhoan].[TieuKhoan] [SubAccount]
		, [230006_DanhSachTieuKhoan].[MaHieuRoom] [RoomCode]
	FROM [DWH-CoSo].[dbo].[230006_DanhSachTieuKhoan]
	LEFT JOIN [DWH-CoSo].[dbo].[vcf0051]
		ON [vcf0051].[sub_account] = [230006_DanhSachTieuKhoan].[TieuKhoan]
		AND [vcf0051].[date] = [230006_DanhSachTieuKhoan].[Ngay]
	WHERE [230006_DanhSachTieuKhoan].[Ngay] = @t0Date
		AND [vcf0051].[contract_type] LIKE '%MR%'
)

, [230006.Filter] AS (
	SELECT
		-- ưu tiên lấy mã room nên bảng 230006_DanhSachChungKhoan được để trước trong hàm COALESCE
		COALESCE([230006_DanhSachChungKhoan].[Ngay], [230006_DanhSachTieuKhoanMargin].[Date]) [Date230006]
		, [230006_DanhSachTieuKhoanMargin].[SubAccount]
		, COALESCE([230006_DanhSachChungKhoan].[MaHieuRoom], [230006_DanhSachTieuKhoanMargin].[RoomCode]) [RoomCode]
		, [230006_DanhSachChungKhoan].[ChungKhoan] [Stock]
	FROM [230006_DanhSachTieuKhoanMargin]
	FULL OUTER JOIN [DWH-CoSo].[dbo].[230006_DanhSachChungKhoan]
		ON [230006_DanhSachTieuKhoanMargin].[RoomCode] = [230006_DanhSachChungKhoan].[MaHieuRoom]
		AND [230006_DanhSachTieuKhoanMargin].[Date] = [230006_DanhSachChungKhoan].[Ngay]
	WHERE [230006_DanhSachTieuKhoanMargin].[Date] = @t0Date
		OR [230006_DanhSachChungKhoan].[Ngay] = @t0Date
)

, [230006.Final] AS (
	SELECT
		[230006.Filter].*
	FROM [230006.Filter]
	LEFT JOIN [DWH-CoSo].[dbo].[230006_ThongTinChung]
		ON [230006_ThongTinChung].[Ngay] = [230006.Filter].[Date230006]
		AND [230006_ThongTinChung].[MaHieuRoom] = [230006.Filter].[RoomCode]
	WHERE [230006_ThongTinChung].[NgayHetHieuLuc] >= @t0Date
)

, [CustomerInfo] AS (
	SELECT
		[230006.Final].[Date230006]
		, [230006.Final].[RoomCode]
		, ISNULL([relationship].[account_code], '') [Account]
		, ISNULL([230006.Final].[SubAccount], '') [SubAccount]
		, ISNULL([230006.Final].[Stock], '') [Stock]
		, ISNULL(CASE [relationship].[branch_id]
			WHEN '0301' THEN 'Hai Phong'
			WHEN '0105' THEN 'Tan Binh'
			WHEN '0201' THEN 'Ha Noi'
            WHEN '0117' THEN 'District 1'
			WHEN '0101' THEN 'District 3'
			WHEN '0202' THEN 'Thanh Xuan'
            WHEN '0102' THEN 'Phu My Hung'
            WHEN '0104' THEN 'District 7'
            WHEN '0113' THEN 'Internet Broker'
            WHEN '0116' THEN 'AMD01'
            WHEN '0120' THEN 'AMD05'
            WHEN '0111' THEN 'Institutional Business 01'
            WHEN '0119' THEN 'Institutional Business 02'
			ELSE [branch].[branch_name]
		END, '') [Location]
	FROM [230006.Final]
	LEFT JOIN [DWH-CoSo].[dbo].[relationship]
		ON [relationship].[sub_account] = [230006.Final].[SubAccount]
		AND [relationship].[date] = [230006.Final].[Date230006]
	LEFT JOIN [DWH-CoSo].[dbo].[branch]
		ON [branch].[branch_id] = [relationship].[branch_id]
)

, [RootTablePartition] AS (
	SELECT
		[CustomerInfo].*
		, ISNULL([RMR9004.GroupBy].[Quantity], 0) [Quantity]
		, ISNULL([DataClosedPrice].[ClosedPrice], 0) * 1000 [ClosedPrice]
		, ISNULL([RMR0062.Filter].[TotalCash], 0) [TotalCash]
		, ISNULL([RMR0015.FilterByStock].[MarketValueByStock], 0) [MarketValueStock]
		, ISNULL([RMR0015.FilterBySubAccount].[TotalMarketValueAllStock], 0) [TotalMarketValueAllStock]
	FROM [CustomerInfo]
	LEFT JOIN [RMR9004.GroupBy]
		ON [RMR9004.GroupBy].[sub_account] = [CustomerInfo].[SubAccount]
		AND [RMR9004.GroupBy].[Stock] = [CustomerInfo].[Stock]
	LEFT JOIN [DataClosedPrice]
		ON [DataClosedPrice].[Stock] = [CustomerInfo].[Stock]
	LEFT JOIN [RMR0062.Filter]
		ON [RMR0062.Filter].[account_code] = [CustomerInfo].[Account]
	LEFT JOIN [RMR0015.FilterByStock]
		ON [RMR0015.FilterByStock].[sub_account] = [CustomerInfo].[SubAccount]
		AND [RMR0015.FilterByStock].[Stock] = [CustomerInfo].[Stock]
	LEFT JOIN [RMR0015.FilterBySubAccount]
		ON [RMR0015.FilterBySubAccount].[sub_account] = [CustomerInfo].[SubAccount]
)

, [TableJoinCommitment] AS (
	SELECT
		[RootTablePartition].*
		, C.[FromDate][FromDate_RLN0006]
		, C.[FromDate] [FromDate_ROD0040]
		, C.[Months]
		, C.[MATV] [MATV_File]
	FROM [RootTablePartition]
	LEFT JOIN [DWH-CoSo].[dbo].[CommitmentMonthlyDeal] C
		ON C.[RoomCode] = [RootTablePartition].[RoomCode]
		AND C.[Stock] = [RootTablePartition].[Stock]
)

, [CommitmentGroupBy] AS (
	SELECT
		T.[RoomCode]
		, T.[Location]
		, T.[Months]
		, T.[Account]
		, T.[SubAccount]
		, MIN([FromDate_RLN0006]) [FromDate_RLN0006]
		, MIN([FromDate_ROD0040]) [FromDate_ROD0040]
	FROM [TableJoinCommitment] T
	WHERE T.[MATV_File] IS NOT NULL
	GROUP BY T.[RoomCode], T.[Months], T.[Location], T.[Account], T.[SubAccount]
)

, [RoomStockUnique] AS (
	SELECT DISTINCT
		[RoomCode]
		, GRP.[Months]
		, GRP.[Location]
		, GRP.[Account]
		, GRP.[SubAccount]
		, GRP.[FromDate_RLN0006]
		, @t0Date [ToDate_RLN0006]
		, GRP.[FromDate_ROD0040]
		, @t0Date [ToDate_ROD0040]
	FROM [CommitmentGroupBy] GRP
)

, [RLN0006.SUM] AS (
	SELECT
		[margin_outstanding].[date]
		, [RoomStockUnique].[Account]
		, SUM([principal_outstanding]) [principal_outstanding]
		, SUM([interest_outstanding]) [interest_outstanding]
		, SUM([principal_outstanding] + [interest_outstanding]) [TotalOutstanding]
	FROM [RoomStockUnique]
	LEFT JOIN [DWH-CoSo].[dbo].[margin_outstanding]
		ON [margin_outstanding].[account_code] = [RoomStockUnique].[Account]
		AND [margin_outstanding].[date] BETWEEN [RoomStockUnique].[FromDate_RLN0006] AND [RoomStockUnique].[ToDate_RLN0006]
	LEFT JOIN [DWH-CoSo].[dbo].[Date]
		ON [Date].[Date] = [margin_outstanding].[date]
	WHERE [type] IN (N'Margin', N'Trả chậm', N'Bảo lãnh')
		AND [Date].[Work] = 1
	GROUP BY [margin_outstanding].[date], [Account]
)

, [RLN0006.AVG] AS (
	SELECT
		[RoomStockUnique].[Account]
		, AVG([principal_outstanding]) [principal_outstanding]
		, AVG([interest_outstanding]) [interest_outstanding]
		, AVG([principal_outstanding] + [interest_outstanding]) [TotalOutstanding]
	FROM [DWH-CoSo].[dbo].[margin_outstanding]
	RIGHT JOIN [RoomStockUnique]
		ON [RoomStockUnique].[Account] = [margin_outstanding].[account_code]
		AND [margin_outstanding].[date] BETWEEN [RoomStockUnique].[FromDate_RLN0006] AND [RoomStockUnique].[ToDate_RLN0006]
	WHERE [type] IN (N'Margin', N'Trả chậm', N'Bảo lãnh')
	GROUP BY [Account]
)

, [ROD0040BySubAccount] AS (
	SELECT
		[sub_account]
		, SUM([value]) [TradingValueBySubAccount]
		, SUM([fee]) [TradingFee]
	FROM [RoomStockUnique]
	LEFT JOIN [DWH-CoSo].[dbo].[trading_record]
		ON [trading_record].[sub_account] = [RoomStockUnique].[SubAccount]
		AND [trading_record].[date] BETWEEN [RoomStockUnique].[FromDate_ROD0040] AND [RoomStockUnique].[ToDate_ROD0040]
	GROUP BY [sub_account], [Months]
)

, [ROD0040ByTicker] AS (
	SELECT
		[sub_account]
		, [trading_record].[ticker] [Stock]
		, SUM([value]) [TradingValueByStock]
	FROM [RoomStockUnique]
	LEFT JOIN [DWH-CoSo].[dbo].[trading_record]
		ON [trading_record].[sub_account] = [RoomStockUnique].[SubAccount]
		AND [trading_record].[date] BETWEEN [RoomStockUnique].[FromDate_ROD0040] AND [RoomStockUnique].[ToDate_ROD0040]
	GROUP BY [sub_account], [ticker]
)

SELECT
	[TableJoinCommitment].*
	, ISNULL([RLN0006.AVG].[principal_outstanding], 0) [PrincipalOutstanding]
	, ISNULL([RLN0006.AVG].[interest_outstanding], 0) [InterestOutstanding]
	, ISNULL([RLN0006.AVG].[TotalOutstanding], 0) [TotalOutstanding]
	, ISNULL([ROD0040BySubAccount].[TradingValueBySubAccount], 0) [TradingValueBySubAccount]
	, ISNULL([ROD0040BySubAccount].[TradingFee], 0) [TradingFee]
	, ISNULL([ROD0040ByTicker].[TradingValueByStock], 0) [TradingValueByStock]
FROM [TableJoinCommitment]
LEFT JOIN [RLN0006.AVG]
	ON [RLN0006.AVG].[Account] = [TableJoinCommitment].[Account]
LEFT JOIN [ROD0040BySubAccount]
	ON [ROD0040BySubAccount].[sub_account] = [TableJoinCommitment].[SubAccount]
LEFT JOIN [ROD0040ByTicker]
	ON [ROD0040ByTicker].[sub_account] = [TableJoinCommitment].[SubAccount]
	AND [ROD0040ByTicker].[Stock] = [TableJoinCommitment].[Stock]
ORDER BY [TableJoinCommitment].[RoomCode], [Stock], [TableJoinCommitment].[Account] -- Theo yêu cầu của QLRR, sort theo Room code