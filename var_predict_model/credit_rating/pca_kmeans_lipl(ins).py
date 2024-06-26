from request.stock import *

# destination_dir = join(dirname(realpath(__file__)), 'result')
# Nam test 29/02/2024
p = '2023q4'
destination_dir = join(dirname(realpath(__file__)), f'result - {p}')

# Parameters:
centroids = 3
min_tickers = 3
nstd_bound = 2

start_time = time.time()
pd.options.mode.chained_assignment = None

agg_data = fa.all('ins')  # expensive

# periods = fa.periods[-13:]  # trailing 3 years
# trailing Nam test
periods = fa.periods
periods = periods[0:periods.index(p) + 1]

converted_idx = (agg_data.index.get_level_values(0).map(str) + 'q' + agg_data.index.get_level_values(1).map(str))
agg_data = agg_data.loc[converted_idx.isin(periods)]

quantities = [
    'net_premium', 'gross_premium', 'ins_revenue', 'net_claim',
    'net_income', 'equity', 'cash', 'st_invst', 'ins_reserve',
    'cur_asset', 'cur_lib', 'inv', 'liability', 'st_debt', 'lt_debt',
    'asset', 'pbt', 'int_expense', 'cmisn_expense', 'sell_expense',
    'ga_expense', 're_profit', 'fin_profit', 'other_profit',
    'invst_prop', 'lt_invst', 'ins_gprofit'
]

tickers = fa.tickers('ins')

years = list()
quarters = list()
for period in periods:
    years.append(int(period[:4]))
    quarters.append(int(period[-1]))

period_tuple = list(zip(years, quarters))
inds = [x for x in itertools.product(period_tuple, tickers)]

for i in range(len(inds)):
    inds[i] = inds[i][0] + tuple([inds[i][1]])

index = pd.MultiIndex.from_tuples(inds, names=['year', 'quarter', 'ticker'])
col = pd.Index(quantities, name='quantity')

df = pd.DataFrame(columns=col, index=index)

for year, quarter in period_tuple:
    for ticker in tickers:
        if ticker not in agg_data.loc[pd.IndexSlice[year, quarter, :], :].index.get_level_values(2):
            continue
        for quantity in quantities:
            if quantity == 'net_premium':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '11.')]
            elif quantity == 'gross_premium':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '2.')]
            elif quantity == 'ins_revenue':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '19.')]
            elif quantity == 'net_claim':
                df.loc[(year, quarter, ticker), quantity] = -agg_data.loc[(year, quarter, ticker), ('is', '34.')]
            elif quantity == 'net_income':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '82.')]
            elif quantity == 'equity':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '135.')]
            elif quantity == 'cash':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '2.')]
            elif quantity == 'st_invst':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '5.')]
            elif quantity == 'ins_reserve':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '102.')]
            elif quantity == 'cur_asset':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '1.')]
            elif quantity == 'cur_lib':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '84.')]
            elif quantity == 'inv':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '21.')]
            elif quantity == 'liability':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '83.')]
            elif quantity == 'st_debt':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '85.')]
            elif quantity == 'lt_debt':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '119.')]
            elif quantity == 'asset':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '82.')]
            elif quantity == 'pbt':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '77.')]
            elif quantity == 'int_expense':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('cfi', '8.')]
            elif quantity == 'cmisn_expense':
                df.loc[(year, quarter, ticker), quantity] = -agg_data.loc[(year, quarter, ticker), ('is', '39.')]
            elif quantity == 'sell_expense':
                df.loc[(year, quarter, ticker), quantity] = -agg_data.loc[(year, quarter, ticker), ('is', '44.')]
            elif quantity == 'ga_expense':
                df.loc[(year, quarter, ticker), quantity] = -agg_data.loc[(year, quarter, ticker), ('is', '65.')]
            elif quantity == 're_profit':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '54.')]
            elif quantity == 'fin_profit':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '57.')]
            elif quantity == 'other_profit':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '61.')]
            elif quantity == 'invst_prop':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '62.')]
            elif quantity == 'lt_invst':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('bs', '68.')]
            elif quantity == 'ins_gprofit':
                df.loc[(year, quarter, ticker), quantity] = agg_data.loc[(year, quarter, ticker), ('is', '50.')]
            else:
                pass

# fill những thằng nan
df = df.fillna(0)  # 29/02/2024
# replace 0 values with 1000 VND to avoid 0 denominator
df = df.loc[~(df == 0).all(axis=1)]  # remove no-data companies first
df = df.replace(to_replace=0, value=1e3)
# remove negative revenue companies
df = df.loc[df['ins_revenue'] > 0]

df['asset_'] = df['asset']
df['equity_'] = df['equity']
df['revenue_'] = df['ins_revenue']
# df['(-)risk_retention'] = -df['net_premium'] / df['gross_premium']
df['(-)loss_ratio'] = -df['net_claim'] / df['ins_revenue']
df['roe'] = df['net_income'] / df['equity']
df['liq/reserve'] = (df['cash'] + df['st_invst']) / df['ins_reserve']
df['oper_leverage'] = df['net_premium'] / df['equity']
df['cur_ratio'] = df['cur_asset'] / df['cur_lib']
df['acid_test'] = (df['cur_asset'] - df['inv']) / df['cur_lib']
df['cash_ratio'] = df['cash'] / df['cur_lib']
df['asset_turnover'] = df['ins_revenue'] / df['asset']
df['curasset_turnover'] = df['ins_revenue'] / df['cur_asset']
df['equity_turnover'] = df['ins_revenue'] / df['equity']
df['lib_ratio'] = df['liability'] / df['asset']
# df['st/total_debt'] = df['st_debt'] / (df['lt_debt'] + df['st_debt'])
df['roa'] = df['net_income'] / df['asset']
df['roca'] = df['net_income'] / df['cur_asset']
df['rotc'] = (df['pbt'] - df['int_expense']) \
             / (df['lt_debt'] + df['st_debt'] + df['equity'])
df['(-)expense_ratio'] = -(df['cmisn_expense']
                           + df['ga_expense']
                           + df['sell_expense']) \
                         / df['ins_revenue']
df['(-)combined_ratio'] = df['(-)loss_ratio'] + df['(-)expense_ratio']
df['invst_yield'] = (df['re_profit'] + df['fin_profit'] + df['other_profit']) \
                    / (df['st_invst'] + df['invst_prop'] + df['lt_invst'])
df['(-)lt_debt/equity'] = -df['lt_debt'] / df['equity']
df['(-)lt_debt/asset'] = -df['lt_debt'] / df['asset']
df['(-)debt/equity'] = -(df['st_debt'] + df['lt_debt']) / df['equity']
df['(-)debt/asset'] = -(df['st_debt'] + df['lt_debt']) / df['asset']
df['(-)st_debt/equity'] = -df['st_debt'] / df['equity']
df['(-)st_debt/asset'] = -df['st_debt'] / df['asset']
df['lib/equity'] = df['liability'] / df['equity']
df['gross_margin'] = df['ins_gprofit'] / df['ins_revenue']
df['net_margin'] = df['net_income'] / df['ins_revenue']

df = df.drop(columns=quantities)
df.sort_index(axis=1, inplace=True)
df.replace([np.inf, -np.inf], np.nan, inplace=True)

quantities_new = df.columns.to_list()

df.dropna(inplace=True, how='all')

kmeans_index = pd.Index(['insurance'])
benmrks = pd.DataFrame(index=kmeans_index, columns=periods)
kmeans = pd.DataFrame(index=kmeans_index, columns=periods)
labels = pd.DataFrame(index=kmeans_index, columns=periods)
centers = pd.DataFrame(index=kmeans_index, columns=periods)
kmeans_tickers = pd.DataFrame(index=kmeans_index, columns=periods)
kmeans_coord = pd.DataFrame(index=kmeans_index, columns=periods)

for year, quarter in zip(years, quarters):
    # cross section
    tickers = fa.fin_tickers(True)['ins']
    try:
        df_xs = df.loc[(year, quarter, tickers), :]
    except KeyError:
        continue
    df_xs.dropna(axis=0, how='any', inplace=True)

    for quantity in quantities_new:
        # remove outliers (Interquartile Range Method)
        ## (have to ensure symmetry)
        df_xs_median = df_xs.loc[:, quantity].median()
        df_xs_75q = df_xs.loc[:, quantity].quantile(q=0.75)
        df_xs_25q = df_xs.loc[:, quantity].quantile(q=0.25)
        cut_off = (df_xs_75q - df_xs_25q) * 1.5
        for ticker in df_xs.index.get_level_values(2):
            df_xs.loc[(year, quarter, ticker), quantity] \
                = max(df_xs.loc[(year, quarter, ticker), quantity], df_xs_25q - cut_off)
            df_xs.loc[(year, quarter, ticker), quantity] \
                = min(df_xs.loc[(year, quarter, ticker), quantity], df_xs_75q + cut_off)

        # standardize to mean=0
        df_xs_mean = df_xs.loc[:, quantity].mean()
        for ticker in df_xs.index.get_level_values(2):
            df_xs.loc[(year, quarter, ticker), quantity] \
                = (df_xs.loc[(year, quarter, ticker), quantity] - df_xs_mean)

        # standardize to range (-1,1)
        df_xs_min = df_xs.loc[:, quantity].min()
        df_xs_max = df_xs.loc[:, quantity].max()
        if df_xs_max == df_xs_min:
            df_xs.drop(columns=quantity, inplace=True)
        else:
            for ticker in df_xs.index.get_level_values(2):
                df_xs.loc[(year, quarter, ticker), quantity] \
                    = -1 + (df_xs.loc[(year, quarter, ticker), quantity] - df_xs_min) / (df_xs_max - df_xs_min) * 2

    # PCA algorithm
    X = df_xs.values
    benmrk_vector = np.array([-1 for n in range(df_xs.shape[1])])
    benmrk_vector = benmrk_vector.reshape((1, benmrk_vector.shape[0]))
    X = np.append(benmrk_vector, X, axis=0)
    PCs = PCA(n_components=0.9).fit_transform(X)

    idx = df_xs.index.to_list()
    idx.insert(0, (year, quarter, 'BM_'))
    idx = pd.MultiIndex.from_tuples(idx)
    cols = ['PC_' + str(i + 1) for i in range(PCs.shape[1])]
    df_xs = pd.DataFrame(PCs, index=idx, columns=cols)

    benmrks.loc['insurance', f'{year}q{quarter}'] = PCs[0]

    # Kmeans algorithm
    kmeans.loc['insurance', f'{year}q{quarter}'] = KMeans(
        n_clusters=centroids,
        init='k-means++',
        n_init=10,
        max_iter=1000,
        tol=1e-6,
        random_state=1
    ).fit(df_xs.dropna(axis=0, how='any'))

    kmeans_tickers.loc['insurance', f'{year}q{quarter}'] = df_xs.index.get_level_values(2).tolist()
    kmeans_coord.loc['insurance', f'{year}q{quarter}'] = df_xs.values
    labels.loc['insurance', f'{year}q{quarter}'] = kmeans.loc['insurance', f'{year}q{quarter}'].labels_.tolist()
    centers.loc['insurance', f'{year}q{quarter}'] = kmeans.loc[
        'insurance', f'{year}q{quarter}'].cluster_centers_.tolist()

del df_xs  # for memory saving

radius_centers = pd.DataFrame(index=kmeans_index, columns=periods)
for col in range(centers.shape[1]):
    if centers.iloc[0, col] is None:
        radius_centers.iloc[0, col] = None
    else:
        distance = np.zeros(centroids)
        for center in range(centroids):
            # origin at (-1,-1,-1,...) whose dimension varies by PCA
            distance[center] = ((np.array(centers.iloc[0, col][center]) - np.array(
                benmrks.iloc[0, col])) ** 2).sum() ** (1 / 2)
        radius_centers.iloc[0, col] = distance

center_scores = pd.DataFrame(index=kmeans_index, columns=periods)
for col in range(centers.shape[1]):
    if radius_centers.iloc[0, col] is None:
        center_scores.iloc[0, col] = None
    else:
        center_scores.iloc[0, col] = rankdata(radius_centers.iloc[0, col])
        for n in range(1, centroids + 1):
            center_scores.iloc[0, col] \
                = np.where(
                center_scores.iloc[0, col] == n,
                100 / (centroids + 1) * n,
                center_scores.iloc[0, col]
            )

radius_tickers = pd.DataFrame(index=kmeans_index, columns=periods)
for col in range(labels.shape[1]):
    if labels.iloc[0, col] is None:
        radius_tickers.iloc[0, col] = None
    else:
        distance = np.zeros(len(labels.iloc[0, col]))
        for ticker in range(len(labels.iloc[0, col])):
            # origin at (-1,-1,-1,...) whose dimension varies by PCA
            distance[ticker] \
                = (((np.array(kmeans_coord.iloc[0, col][ticker]))
                    - np.array(benmrks.iloc[0, col])) ** 2).sum() ** (1 / 2)
        radius_tickers.iloc[0, col] = distance

ticker_raw_scores = pd.DataFrame(index=kmeans_index, columns=periods)  # not used
for col in range(labels.shape[1]):
    if labels.iloc[0, col] is None:
        ticker_raw_scores.iloc[0, col] = None
    else:
        raw = np.zeros(len(labels.iloc[0, col]))
        for n in range(len(labels.iloc[0, col])):
            raw[n] = center_scores.iloc[0, col][labels.iloc[0, col][n]]
        ticker_raw_scores.iloc[0, col] = raw

ticker_scores = pd.DataFrame(index=kmeans_index, columns=periods)
for col in range(radius_tickers.shape[1]):
    if radius_tickers.iloc[0, col] is None:
        ticker_scores.iloc[0, col] = None
    else:
        min_ = min(radius_centers.iloc[0, col])
        max_ = max(radius_centers.iloc[0, col])
        range_ = max_ - min_
        f = interp1d(np.sort(np.append(
            radius_centers.iloc[0, col],
            [min_ - range_ / (centroids - 1),
             max_ + range_ / (centroids - 1)])
        ), np.sort(np.append(center_scores.iloc[0, col], [0, 100])),
            kind='linear', bounds_error=False, fill_value=(0, 100)
        )
        ticker_scores.iloc[0, col] = f(radius_tickers.iloc[0, col])
        for n in range(len(ticker_scores.iloc[0, col])):
            ticker_scores.iloc[0, col][n] = int(ticker_scores.iloc[0, col][n])

result_table = pd.DataFrame(index=pd.Index(tickers, name='ticker'))
# fillna những thằng bị nan ở 1 vài cột
result_table = result_table.fillna(0)  # 29/02/2024

for period in periods:
    try:
        for n in range(len(kmeans_tickers.loc['insurance', period])):
            result_table.loc[
                kmeans_tickers.loc['insurance', period][n], period
            ] = ticker_scores.loc['insurance', period][n]
    except TypeError:
        continue

# ==============================================================================

component_filename = 'component_table_ins'


def export_component_table():
    df.to_csv(join(destination_dir, component_filename + '.csv'))


export_component_table()

df = pd.read_csv(
    join(destination_dir, component_filename + '.csv')
)
tickers_check = set(df['ticker'].to_list())
df = df.set_index(['year', 'quarter', 'ticker'])

result_filename = 'result_table_ins'


def export_result_table():
    result_table.to_csv(join(destination_dir, result_filename + '.csv'))


export_result_table()
result_table = pd.read_csv(join(destination_dir, result_filename + '.csv'), index_col=['ticker'])


def graph_tickers(tickers: list):
    table = result_table.loc[tickers]
    for ticker in tickers:
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(8, 6))
        ax.set_title(ticker, fontsize=15, fontweight='bold', color='darkslategrey', fontfamily='Times New Roman')

        xloc = np.arange(table.shape[1])  # label locations
        rects = ax.bar(
            xloc,
            table.loc[ticker],
            width=0.8,
            color='tab:blue',
            label='Credit Score',
            edgecolor='black'
        )
        for rect in rects:
            height = rect.get_height()
            ax.annotate(
                '{:.0f}'.format(height),
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha='center',
                va='bottom',
                fontsize=11
            )
        ax.set_xticks(np.arange(len(xloc)))
        ax.set_xticklabels(
            table.columns.tolist(), rotation=45, x=xloc,
            fontfamily='Times New Roman', fontsize=11
        )
        ax.set_yticks(np.array([0, 25, 50, 75, 100]))
        ax.tick_params(axis='y', labelcolor='black', labelsize=11)

        Acolor = 'green'
        Bcolor = 'olivedrab'
        Ccolor = 'darkorange'
        Dcolor = 'firebrick'

        ax.axhline(100, ls='--', linewidth=0.5, color=Acolor)
        ax.axhline(75, ls='--', linewidth=0.5, color=Bcolor)
        ax.axhline(50, ls='--', linewidth=0.5, color=Ccolor)
        ax.axhline(25, ls='--', linewidth=0.5, color=Dcolor)
        ax.fill_between([-0.4, xloc[-1] + 0.4], 100, 75, color=Acolor, alpha=0.2)
        ax.fill_between([-0.4, xloc[-1] + 0.4], 75, 50, color=Bcolor, alpha=0.25)
        ax.fill_between([-0.4, xloc[-1] + 0.4], 50, 25, color=Ccolor, alpha=0.2)
        ax.fill_between([-0.4, xloc[-1] + 0.4], 25, 0, color=Dcolor, alpha=0.2)

        plt.xlim(-0.6, xloc[-1] + 0.6)

        ax.set_ylim(top=110)
        midpoints = np.array([87.5, 62.5, 37.5, 12.5]) / 110
        labels = ['Group A', 'Group B', 'Group C', 'Group D']
        colors = [Acolor, Bcolor, Ccolor, Dcolor]
        for loc in zip(midpoints, labels, colors):
            ax.annotate(
                loc[1],
                xy=(-0.1, loc[0]),
                xycoords='axes fraction',
                textcoords="offset points",
                xytext=(0, -5),
                ha='center',
                va='bottom',
                color=loc[2],
                fontweight='bold',
                fontsize='large'
            )
        ax.legend(loc='best', framealpha=0.5)
        ax.margins(tight=True)
        plt.subplots_adjust(left=0.15, bottom=0.1, right=0.95, top=0.9)
        plt.savefig(join(destination_dir, 'Result', f'{ticker}_result.png'))
        plt.close('all')


def compare_industry(tickers: list):
    components = {
        'asset_': 'Assets',
        'equity_': 'Equity',
        'revenue_': 'Revenue',
        '(-)loss_ratio': 'Loss Ratio',
        'roe': 'Return on Equity',
        'liq/reserve': 'Liquid Assets on Reserves',
        'oper_leverage': 'Net Premiums Written to Equity',
        'cur_ratio': 'Current Ratio',
        'acid_test': 'Quick Ratio',
        'cash_ratio': 'Cash Ratio',
        'asset_turnover': 'Asset Turnover',
        'curasset_turnover': 'Current Asset Turnover',
        'equity_turnover': 'Equity Turnover',
        'lib_ratio': 'Total Liabilities to Assets Ratio',
        'roa': 'Return on Assets',
        'roca': 'Return on Current Assets',
        'rotc': 'Return on Total Capital',
        '(-)expense_ratio': 'Expense Ratio',
        '(-)combined_ratio': 'Combined Ratio',
        'invst_yield': 'Investment Yield ',
        '(-)lt_debt/equity': 'Long Term Debt to Equity',
        '(-)lt_debt/asset': 'Long-Term Debt to Total Assets Ratio',
        '(-)debt/equity': 'Debt to Equity Ratio',
        '(-)debt/asset': 'Debt to Assets Ratio',
        '(-)st_debt/equity': 'Short-term Debt to Equity',
        '(-)st_debt/asset': 'Short-term Debt to Assets',
        'lib/equity': 'Total Liabilities to Equity',
        'gross_margin': 'Gross Margin',
        'net_margin': 'Net Margin'
    }

    df.dropna(axis=0, how='all', inplace=True)
    median = df.groupby(axis=0, level=[0, 1]).median()

    for ticker in tickers:
        if ticker not in tickers_check:
            continue
        # to avoid cases of missing data right at the first period, result in mis-shaped
        quantities = pd.DataFrame(
            np.zeros_like(median),
            index=median.index,
            columns=median.columns
        )
        ref_table = df.xs(ticker, axis=0, level=2)
        quantities = pd.concat([quantities, ref_table], axis=0)
        quantities = quantities.groupby(level=[0, 1], axis=0).sum()

        comparison = pd.concat([quantities, median], axis=1, join='outer',
                               keys=[ticker, 'median'])

        periods = [f'{q[0]}q{q[1]}' for q in comparison.index]
        variables = comparison.columns.get_level_values(1).drop_duplicates().to_numpy()

        chartsperrow = 4
        rowsrequired = int(np.ceil(len(variables) / chartsperrow))
        variables = np.append(variables, [None] * (chartsperrow * rowsrequired - len(variables)))
        variables = np.reshape(variables, (rowsrequired, chartsperrow))
        fig, ax = plt.subplots(
            rowsrequired,
            chartsperrow,
            figsize=(chartsperrow * 6, rowsrequired * 4.5),
            tight_layout=True
        )
        fig.set_size_inches(24, 22)
        for row in range(rowsrequired):
            for col in range(chartsperrow):
                w = 0.35
                l = np.arange(len(periods))  # the label locations
                if variables[row, col] is None:
                    ax[row, col].axis('off')
                else:
                    ax[row, col].bar(
                        l - w / 2,
                        quantities.iloc[:, row * chartsperrow + col],
                        width=w,
                        label=ticker,
                        color='tab:orange',
                        edgecolor='black'
                    )
                    ax[row, col].bar(
                        l + w / 2,
                        median.iloc[:, row * chartsperrow + col],
                        width=w,
                        label='Industry\'s Average',
                        color='tab:blue',
                        edgecolor='black'
                    )
                    plt.setp(ax[row, col].xaxis.get_majorticklabels(), rotation=45)
                    ax[row, col].set_xticks(l)
                    ax[row, col].set_xticklabels(periods, fontsize=13)
                    ax[row, col].set_yticks([])
                    ax[row, col].set_autoscaley_on(True)
                    ax[row, col].set_title(components[variables[row, col]], fontsize=18)

        fig.subplots_adjust(top=0.9)
        fig.suptitle(
            f'{ticker}\n'
            f'Comparison With The Industry\'s Average \n'
            f'All {len(quantities_new)} Variables In Use \n',
            fontweight='bold',
            color='darkslategrey',
            fontfamily='Times New Roman',
            fontsize=30,
            y=0.99
        )
        handles, labels = ax[0, 0].get_legend_handles_labels()
        fig.legend(
            handles,
            labels,
            loc='upper left',
            bbox_to_anchor=(0.01, 0.98),
            ncol=2,
            fontsize=18,
            markerscale=0.7
        )
        plt.savefig(join(destination_dir, 'Compare with Industry', f'{ticker}_compare_industry.png'))
        plt.close('all')

def compare_rs(tickers: list):
    global result_filename
    rs_file = join(dirname(realpath(__file__)), 'research_rating.xlsx')
    rs_rating = pd.read_excel(rs_file, sheet_name='summary',
                              index_col='ticker', engine='openpyxl')

    def scoring(rating: str) -> int:
        mapping = {
            'AAA': 95,
            'AA': 85,
            'A': 77.5,
            'BBB': 72.5,
            'BB': 67.5,
            'B': 62.5,
            'CCC': 57.5,
            'CC': 52.5,
            'C': 47.5,
            'DDD': 40,
            'DD': 30,
            'D': 20
        }
        try:
            return mapping[rating]
        except KeyError:
            return np.nan

    rs_rating = rs_rating.applymap(scoring)
    for i in range(rs_rating.shape[0]):
        for j in range(1, rs_rating.shape[1] - 1):
            before = rs_rating.iloc[i, j - 1]
            after = np.nan
            k = 0
            if not np.isnan(before) and np.isnan(rs_rating.iloc[i, j]):
                while np.isnan(after) and j + k < rs_rating.shape[1] - 1:
                    k += 1
                    after = rs_rating.iloc[i, j + k]
                rs_rating.iloc[i, j] = before + (after - before) / (k + 1)

    model_file = join(dirname(realpath(__file__)), 'result', result_filename + '.csv')
    model_rating = pd.read_csv(model_file, index_col='ticker')

    for ticker in tickers:
        try:
            fig, ax = plt.subplots(1, 1, figsize=(8, 6))
            periods = [q for q in model_rating.columns]
            w = 0.35
            xloc = np.arange(len(periods))  # the label locations
            ax.bar(
                xloc - w / 2,
                model_rating.loc[ticker, :],
                width=w,
                label='K-Means',
                color='tab:blue',
                edgecolor='black'
            )
            ax.bar(
                xloc + w / 2,
                rs_rating.loc[ticker, :],
                width=w,
                label='Research\'s Rating',
                color='tab:gray',
                edgecolor='black'
            )
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            ax.set_xticks(xloc)
            ax.set_xticklabels(periods, fontsize=11)

            ax.set_yticks(np.array([0, 25, 50, 75, 100]))
            ax.tick_params(axis='y', labelcolor='black', labelsize=11)

            Acolor = 'green'
            Bcolor = 'olivedrab'
            Ccolor = 'darkorange'
            Dcolor = 'firebrick'

            ax.axhline(100, ls='--', linewidth=0.5, color=Acolor)
            ax.axhline(75, ls='--', linewidth=0.5, color=Bcolor)
            ax.axhline(50, ls='--', linewidth=0.5, color=Ccolor)
            ax.axhline(25, ls='--', linewidth=0.5, color=Dcolor)
            ax.fill_between(
                [-0.4, xloc[-1] + 0.4], 100, 75, color=Acolor, alpha=0.2
            )
            ax.fill_between(
                [-0.4, xloc[-1] + 0.4], 75, 50, color=Bcolor, alpha=0.25
            )
            ax.fill_between(
                [-0.4, xloc[-1] + 0.4], 50, 25, color=Ccolor, alpha=0.2
            )
            ax.fill_between(
                [-0.4, xloc[-1] + 0.4], 25, 0, color=Dcolor, alpha=0.2
            )
            plt.xlim(-0.6, xloc[-1] + 0.6)
            ax.set_ylim(top=110)
            midpoints = np.array([87.5, 62.5, 37.5, 12.5]) / 110
            labels = ['Group A', 'Group B', 'Group C', 'Group D']
            colors = [Acolor, Bcolor, Ccolor, Dcolor]
            for loc in zip(midpoints, labels, colors):
                ax.annotate(
                    loc[1],
                    xy=(-0.1, loc[0]),
                    xycoords='axes fraction',
                    textcoords="offset points",
                    xytext=(0, -5),
                    ha='center',
                    va='bottom',
                    color=loc[2],
                    fontweight='bold',
                    fontsize='large'
                )
            ax.legend(loc='best', framealpha=0.5)
            ax.margins(tight=True)
            plt.subplots_adjust(left=0.15, bottom=0.1, right=0.95, top=0.9)
            ax.set_title(
                ticker + '\n' + "Comparison with Research's Rating",
                fontsize=15,
                fontweight='bold',
                color='darkslategrey',
                fontfamily='Times New Roman'
            )
            plt.savefig(join(destination_dir, 'Compare with RS', f'{ticker}_compare_rs.png'))
        except KeyError:
            print(f'{ticker} has KeyError')


def mlist_group(year: int, quarter: int) -> dict:
    file = join(dirname(realpath(__file__)), 'result', result_filename + '.csv')
    table = pd.read_csv(file, index_col='ticker')

    series = table[str(year) + 'q' + str(quarter)]
    mlist = internal.mlist('all')
    ticker_list = fa.fin_tickers(True)['ins']
    # some tickers in margin list do not have enough data to run K-Means
    model_tickers = table.index.to_list()
    ticker_list = list(set(ticker_list).intersection(model_tickers).intersection(mlist))
    series = series.loc[ticker_list]

    def f(score):
        if score <= 25:
            return 'D'
        elif score <= 50:
            return 'C'
        elif score <= 75:
            return 'B'
        elif score <= 100:
            return 'A'
        else:
            return np.nan

    series = series.map(f).dropna()
    groups = series.drop_duplicates().to_numpy()
    d = dict()
    for group in groups:
        tickers = series.loc[series == group].index.to_list()
        d[group] = tickers

    return d


graph_tickers(fa.tickers('ins'))
compare_industry(fa.tickers('ins'))

execution_time = time.time() - start_time
print(f"The execution time is: {int(execution_time)}s seconds")
