DECLARE @t0Date VARCHAR(20)
SET @t0Date = '2024-06-20';

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
	-- đảm bảo luôn lấy giá trị mới nhất
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
		-- WHERE [Date] <= @t0Date - không setup điều kiện chỗ này vì QLRR muốn lấy giá đóng cửa mới nhất dù xuất báo cáo ngày cũ
	) [t]
	WHERE [t].[MaxDate] = [t].[Date]
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

, [Sector] AS (
	SELECT
		[MaChungKhoan]
		, [Nganh]
	FROM [DWH-CoSo].[dbo].[DataHop]
)

, [VPR0109.MR] AS (
	SELECT
		[ticker_code] [Stock]
		, [margin_ratio] / 100 [R_MR]
		, [margin_max_price] [MaxPrice]
	FROM [DWH-CoSo].[dbo].[vpr0109]
	 WHERE [room_code] = 'CL01_PHS' 
		AND [date] = @t0Date
)

, [VPR0109.DP] AS (
	SELECT
		[ticker_code] [Stock]
		, [margin_ratio] / 100 [R_DP]
	FROM [DWH-CoSo].[dbo].[vpr0109]
	 WHERE [room_code] = 'TC01_PHS' 
		AND [date] = @t0Date
)

, [VPR0108.Flex] AS (
	SELECT
		[room_code]
		, [ticker]
		, [total_volume]
		, [used_volume]
	FROM [DWH-CoSo].[dbo].[vpr0108]
	WHERE [date] = @t0Date
		AND [total_volume] <> 0
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
		COALESCE([230006_DanhSachChungKhoan].[Ngay], [230006_DanhSachTieuKhoanMargin].[Date]) [Date]
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
		ON [230006_ThongTinChung].[Ngay] = [230006.Filter].[Date]
		AND [230006_ThongTinChung].[MaHieuRoom] = [230006.Filter].[RoomCode]
	WHERE [230006_ThongTinChung].[NgayHetHieuLuc] >= @t0Date
)

, [CustomerInfo] AS (
	SELECT
		[230006.Final].[Date]
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
		AND [relationship].[date] = [230006.Final].[Date]
	LEFT JOIN [DWH-CoSo].[dbo].[branch]
		ON [branch].[branch_id] = [relationship].[branch_id]
)

, [RootTable] AS (
	SELECT
		[VPR0108.Flex].[room_code] [RoomCode]
		, [VPR0108.Flex].[ticker] [Stock]
		, [CustomerInfo].[Location]
		, [CustomerInfo].[Account]
		, [CustomerInfo].[SubAccount]
		, ISNULL([VPR0108.Flex].[total_volume], 0) [Setup]
		, ISNULL([VPR0109.MR].[R_MR], 0) [MR_Ratio]
		, ISNULL([VPR0109.DP].[R_DP], 0) [DP_Ratio]
		, ISNULL([VPR0109.MR].[MaxPrice], 0) [MaxPrice]
		, ISNULL([DataClosedPrice].[ClosedPrice], 0) [ClosedPrice]
		, ISNULL([VPR0108.Flex].[used_volume], 0) [UsedQuantity]
		, ISNULL([RMR9004.GroupBy].[Quantity], 0) [Quantity]
		, ISNULL([RMR0015.FilterByStock].[MarketValueByStock], 0) [MarketValueStock]
		, ISNULL([RMR0015.FilterBySubAccount].[TotalMarketValueAllStock], 0) [TotalMarketValueAllStock]
	FROM [VPR0108.Flex]  -- lấy bảng VPR0108 làm gốc
	LEFT JOIN [CustomerInfo]
		ON [VPR0108.Flex].[room_code] = [CustomerInfo].[RoomCode]
		AND [VPR0108.Flex].[ticker] = [CustomerInfo].[Stock]
	LEFT JOIN [RMR9004.GroupBy]
		ON [RMR9004.GroupBy].[sub_account] = [CustomerInfo].[SubAccount]
		AND [RMR9004.GroupBy].[Stock] = [VPR0108.Flex].[ticker]
	LEFT JOIN [DataClosedPrice]
		ON [DataClosedPrice].[Stock] = [VPR0108.Flex].[ticker]
	LEFT JOIN [RMR0015.FilterByStock]
		ON [RMR0015.FilterByStock].[sub_account] = [CustomerInfo].[SubAccount]
		AND [RMR0015.FilterByStock].[Stock] = [VPR0108.Flex].[ticker]
	LEFT JOIN [RMR0015.FilterBySubAccount]
		ON [RMR0015.FilterBySubAccount].[sub_account] = [CustomerInfo].[SubAccount]
	LEFT JOIN [VPR0109.MR]
		ON [VPR0109.MR].[Stock] = [VPR0108.Flex].[ticker]
	LEFT JOIN [VPR0109.DP]
		ON [VPR0109.DP].[Stock] = [VPR0108.Flex].[ticker]
)

, [RootTablePartition] AS (
	SELECT
		[RoomCode]
		, [Stock]
		, [Location]
		, [Account]
		, [SubAccount]
		, MAX([Setup]) OVER(PARTITION BY [RoomCode], [Stock], [Location]) [Setup] 
		, MAX([MR_Ratio]) OVER(PARTITION BY [RoomCode], [Stock], [Location]) [MR_Ratio]
		, MAX([DP_Ratio]) OVER(PARTITION BY [RoomCode], [Stock], [Location]) [DP_Ratio]
		, MAX([MaxPrice]) OVER(PARTITION BY [RoomCode], [Stock], [Location]) [MaxPrice]
		, MAX([ClosedPrice]) OVER(PARTITION BY [RoomCode], [Stock], [Location]) [ClosedPrice]
		, MAX([UsedQuantity]) OVER(PARTITION BY [RoomCode], [Stock], [Location]) [UsedQuantity]
		, [MarketValueStock]
		, [TotalMarketValueAllStock]
		, [TotalMarketValueAllStock] - [MarketValueStock] [MarketValueOtherStocks]
		, SUM([Quantity]) OVER(PARTITION BY [RoomCode], [Stock], [Location]) [Quantity]
		, [Quantity] [QuantityByAccount]
	FROM [RootTable]
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

, [RLN0006.AtDate] AS (
	SELECT
		[account_code]
		, SUM([principal_outstanding] + [interest_outstanding]) [TotalOutstanding]
	FROM [DWH-CoSo].[dbo].[margin_outstanding]
	WHERE [date] = @t0Date
		AND [type] IN (N'Margin', N'Trả chậm', N'Bảo lãnh')
	GROUP BY [account_code]
)

, [RLN0006.Filter] AS (
	SELECT
		[margin_outstanding].[date]
		, [RoomStockUnique].[Account]
		, [Months]
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
		AND [RoomStockUnique].[RoomCode] = '0095'
	GROUP BY [margin_outstanding].[date], [Account], [Months]
)

, [RMR0062.AtDate] AS (
	SELECT
		[account_code]
		, SUM([cash]) [TotalCash]
	FROM [DWH-CoSo].[dbo].[RMR0062]
	WHERE [RMR0062].[date] = @t0Date
		AND [loan_type] = 1
	GROUP BY [account_code]
)

, [RMR0062.Filter] AS (
	SELECT
		[date]
		, [RoomStockUnique].[Account]
		, [Months]
		, SUM([cash]) [TotalCash]
	FROM [RoomStockUnique]
	LEFT JOIN [DWH-CoSo].[dbo].[RMR0062]
		ON [RMR0062].[account_code] = [RoomStockUnique].[Account]
		AND [RMR0062].[date] BETWEEN [RoomStockUnique].[FromDate_RLN0006] AND [RoomStockUnique].[ToDate_RLN0006]
	WHERE [loan_type] = 1
	GROUP BY [date], [Account], [Months]
)

, [RLN0006JoinRMR0062] AS (
	SELECT
		COALESCE([RLN0006.Filter].[date], [RMR0062.Filter].[date]) [date]
		, COALESCE([RLN0006.Filter].[Account], [RMR0062.Filter].[Account]) [Account]
		, COALESCE([RLN0006.Filter].[Months], [RMR0062.Filter].[Months]) [Months]
		, ISNULL([RLN0006.Filter].[TotalOutstanding], 0) - ISNULL([RMR0062.Filter].[TotalCash], 0) [P.OutsNetCash_Account]
	FROM [RLN0006.Filter]
	FULL OUTER JOIN [RMR0062.Filter]
		ON [RMR0062.Filter].[date] = [RLN0006.Filter].[date]
		AND [RMR0062.Filter].[Account] = [RLN0006.Filter].[Account]
		AND [RMR0062.Filter].[Months] = [RLN0006.Filter].[Months]
	WHERE ISNULL([RLN0006.Filter].[TotalOutstanding], 0) - ISNULL([RMR0062.Filter].[TotalCash], 0) > 0
)

, [TableP.OutNetCashSum] AS (
	SELECT
		[RLN0006JoinRMR0062].[date]
		, [RoomStockUnique].[RoomCode]
		, [RoomStockUnique].[Months] -- test
		, [RoomStockUnique].[Location]
		, SUM([RLN0006JoinRMR0062].[P.OutsNetCash_Account]) [P.OutsNetCash_RoomStock]
	FROM [RoomStockUnique]
	LEFT JOIN [RLN0006JoinRMR0062]
		ON [RLN0006JoinRMR0062].[Account] = [RoomStockUnique].[Account]
		AND [RLN0006JoinRMR0062].[Months] = [RoomStockUnique].[Months]
	WHERE [date] IS NOT NULL
	GROUP BY [date], [RoomStockUnique].[RoomCode], [RoomStockUnique].[Months], [Location]
)

, [TableP.OutNetCashAVG] AS (
	SELECT
		[RoomCode]
		, [Months]
		, [Location]
		, AVG([P.OutsNetCash_RoomStock]) [P.OutsNetCash_RoomStock]
	FROM [TableP.OutNetCashSum]
	GROUP BY [RoomCode], [Months], [Location]
)

, [ROD0040BySubAccount] AS (
	SELECT
		[sub_account]
		, [Months]
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

, [SummaryTable] AS (
	SELECT
		T.*
		, CASE
			-- Nếu tính ra nhỏ hơn 0 thì lấy 0
			WHEN (ISNULL([RLN0006.AtDate].[TotalOutstanding], 0) - ISNULL([RMR0062.AtDate].[TotalCash], 0) - [MarketValueOtherStocks]) / NULLIF([QuantityByAccount], 0) <= 0 THEN 0
			ELSE (ISNULL([RLN0006.AtDate].[TotalOutstanding], 0) - ISNULL([RMR0062.AtDate].[TotalCash], 0) - [MarketValueOtherStocks]) / NULLIF([QuantityByAccount], 0)
		END [BreakevenPrice]
		, [TableP.OutNetCashAVG].[P.OutsNetCash_RoomStock] [P.OutsNetCash]
		, SUM(ISNULL([ROD0040BySubAccount].[TradingValueBySubAccount], 0)) OVER (PARTITION BY T.[RoomCode], T.[Months], T.[Location], T.[Stock]) [TradingValueByAccount]
		, SUM(ISNULL([ROD0040ByTicker].[TradingValueByStock], 0)) OVER(PARTITION BY T.[RoomCode], T.[Location], T.[Stock]) [TradingValueByStock]
	FROM [TableJoinCommitment] T
	LEFT JOIN [ROD0040BySubAccount]
		ON [ROD0040BySubAccount].[sub_account] = T.[SubAccount]
		AND [ROD0040BySubAccount].[Months] = T.[Months]
	LEFT JOIN [ROD0040ByTicker]
		ON [ROD0040ByTicker].[sub_account] = T.[SubAccount]
		AND [ROD0040ByTicker].[Stock] = T.[Stock]
	LEFT JOIN [RMR0062.AtDate]
		ON [RMR0062.AtDate].[account_code] = T.[Account]
	LEFT JOIN [RLN0006.AtDate]
		ON [RLN0006.AtDate].[account_code] = T.[Account]
	LEFT JOIN [TableP.OutNetCashAVG]
		ON [TableP.OutNetCashAVG].[RoomCode] = T.[RoomCode]
		AND [TableP.OutNetCashAVG].[Location] = T.[Location]
		AND [TableP.OutNetCashAVG].[Months] = T.[Months]
)

, [FullColumn] AS (
	SELECT
		[SummaryTable].*
		, MAX(ISNULL([BreakevenPrice], 0)) OVER(PARTITION BY [RoomCode], [Stock]) [BreakevenPrice.Max]
		, MIN(ISNULL([BreakevenPrice], 0)) OVER(PARTITION BY [RoomCode], [Stock]) [BreakevenPrice.Min]
	FROM [SummaryTable]
)

, [AccountGroup] AS (
	SELECT
		[RoomCode]
		, [Location]
		, STRING_AGG(SUBSTRING([Account],PATINDEX('%[^0]%',SUBSTRING([Account],5,6))+4,LEN([Account])-PATINDEX('%[^0]%',SUBSTRING([Account],5,6))),', ') [AccountGroup]
	FROM (
		SELECT DISTINCT
			[Account]
			, [RoomCode]
			, [Location]
		FROM [SummaryTable]
	) [t]
	GROUP BY [RoomCode], [Location]
)

, [FinalResult] AS (
	SELECT DISTINCT
		[FullColumn].[RoomCode]
		, [FullColumn].[Stock]
		, [FullColumn].[Months]
		, [Sector].[Nganh] [Sector]
		, [FullColumn].[Location]
		, [AccountGroup].[AccountGroup]
		, [FullColumn].[Setup]
		, [FullColumn].[MR_Ratio]
		, [FullColumn].[DP_Ratio]
		, [FullColumn].[MaxPrice] [MaxPrice]
		, [FullColumn].[ClosedPrice] * 1000 [ClosedPrice]
		, CASE
			WHEN [FullColumn].[BreakevenPrice.Min] - [FullColumn].[BreakevenPrice.Max] <= 0 THEN 0
			ELSE [FullColumn].[BreakevenPrice.Min] - [FullColumn].[BreakevenPrice.Max]
		END [BreakevenPrice.Min] -- rule này của Hoàng Anh, không phải rule của chị vì rule chị phức tạp hơn và cần con người đánh giá
		, [FullColumn].[BreakevenPrice.Max]
		, [FullColumn].[UsedQuantity]
		, CASE
			WHEN [FullColumn].[P.OutsNetCash] < 0 THEN 0
			ELSE [FullColumn].[P.OutsNetCash] / 1e6
		END [P.OutsNetCash]
		, [FullColumn].[TradingValueByAccount] / 1e6 [TradingValueByAccount] -- cột này ý nghĩa là giá trị giao dịch của tất cả tài khoản được group theo RoomCode và Location
		, [FullColumn].[TradingValueByStock] / 1e6 [TradingValueByStock]
		, [FullColumn].[Quantity]
		, ISNULL([FullColumn].[MATV_File], 0) [Note]  -- MATV trong file
	FROM [AccountGroup]
	LEFT JOIN [FullColumn]
		ON [FullColumn].[RoomCode] = [AccountGroup].[RoomCode]
		AND [FullColumn].[Location] = [AccountGroup].[Location]
	LEFT JOIN [Sector]
		ON [Sector].[MaChungKhoan] = [FullColumn].[Stock]
)

SELECT
	[FinalResult].*
	, ISNULL([TradingValueByAccount] / (NULLIF([P.OutsNetCash], 0)), 0) [MATV] -- ko ISNULL chỗ này vì sẽ có những case khác cũng có thể ra 0
	, CASE
		WHEN [MR_Ratio] >= [DP_Ratio] THEN [MR_Ratio]
		ELSE [DP_Ratio]
	END [MaxRatio]
	, CASE
		WHEN ISNULL([TradingValueByAccount] / (NULLIF([P.OutsNetCash], 0)), 0) < [Note] THEN 'Y'
		ELSE ''
	END [Check]
FROM [FinalResult]
-- ORDER BY [RoomCode], [Stock]  -- sort theo yêu cầu QLRR
ORDER BY [Location], [RoomCode], [Months], [Stock] -- Ms Thu Anh yêu cầu đổi 01/12/2023