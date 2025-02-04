import yfinance as yf

from stateprocessor import StateProcessor

# globals
TIME_PERIOD = "5y" # yahoo finance format
FILENAME = "5y-Analysis-MW-T"
COMPARE_COL = "Close"
# LOR = 3
DATASETS = [
    'VOX Communication Services',
    'VCR Consumer Discretionary',
    'VDC Consumer Staples',
    'VDE Energy',
    'VFH Finacials',
    'VHT Health Car',
    'VIS Industrials',
    'VGT Information Technology',
    'VAW Materials',
    'VNQ Real Estate',
    'VPU Utilities'
] # 11 sector etfs

# load the 11 vanguard sector etfs
vox = yf.Ticker("VOX")
vcr = yf.Ticker("VCR")
vdc = yf.Ticker("VDC")
vde = yf.Ticker("VDE")
vfh = yf.Ticker("VFH")
vht = yf.Ticker("VHT")
vis = yf.Ticker("VIS")
vgt = yf.Ticker("VGT")
vaw = yf.Ticker("VAW")
vnq = yf.Ticker("VNQ")
vpu = yf.Ticker("VPU")

# list_sector_etf_tps = [] # list of dataframes

# value = 3 # 95th percentile
# state = "flowstate"
# value = -1 # 95th percentile
# vox_tp = vox.history(period=TIME_PERIOD)
# # list_sector_etf_tps.append(vox_tp)
# state_processor_vox_tp = StateProcessor(vox_tp, "VOX", "1y")
# state_processor_vox_tp.calculate_all_test_states(compare_col=COMPARE_COL)
# state_processor_vox_tp.calculate_benchmarks()
# state_processor_vox_tp.write_csv_header()

csv_data = {
        'FileName': FILENAME,
        'TimePeriod': TIME_PERIOD,
        'Ticker': 0,
        'LOR': 0,
        'State': 0,
        'Value': 0
    }
tickers = ['VOX','VCR','VDC','VDE','VFH','VHT','VIS','VGT','VAW','VNQ','VPU']
# tickers = ['VOX']
state_processor_tp = StateProcessor("asset_tp")
state_processor_tp.write_csv_header(FILENAME)

for ticker in tickers:
    print(f'analyzing {ticker} ...')
    csv_data['Ticker'] = ticker
    asset = yf.Ticker(ticker)
    asset_tp = asset.history(period=TIME_PERIOD)
    # new object of the state processor for a specific ticker
    state_processor_tp = StateProcessor(asset_tp)
    # calculate all the states in steps
    # state_processor_tp.calculate_all_test_states(compare_col=COMPARE_COL)
    # state_processor_tp.calculate_benchmarks()
    # state_processor_tp.write_csv_header()

    # state = "downstate"
    # print('calculating downstate analysis ...')
    # values = [1,2,3,4]
    # lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    # for value in values:
    #     for lor in lors:
    #         state_processor_tp.calculate_all_test_states(
    #             compare_col=COMPARE_COL, lor=lor) # calculate all the states in steps
    #         # benchmarks are based on length of return calculations
    #         state_processor_tp.calculate_lor(lor)
    #         state_processor_tp.calculate_benchmarks()
    #         state_processor_tp.calculate_test_benchmarks(state, value)

    lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    state_processor_tp.calculate_all_test_states(
            compare_col=COMPARE_COL) # calculate all the states in steps

    # state_processor_tp.set_maxmin_values()
    for lor in lors:
        csv_data['LOR'] = lor
        # benchmarks are based on length of return calculations
        state_processor_tp.calculate_lor(lor)
        benchmarks = state_processor_tp.calculate_benchmarks()
        csv_data['LOR_Returns'] = benchmarks['LOR_Returns']
        csv_data['LOR_Returns_Per'] = benchmarks['LOR_Returns_Per']
        csv_data['Total_Returns'] = benchmarks['Total_Returns']
        csv_data['Total_Returns_Per'] = benchmarks['Total_Returns_Per']

        # state = "downstate"
        print('magnitude analysis ...')
        # np.quantile(.97, returns_dataset)

        state = "downstate"
        print('downstate analysis ...')
        csv_data['State'] = state
        # downstate means nothing when it is 0 (it is an upstate)
        # for value in range(1, state_processor_tp.max_downstate+1):
        for value in range(1, max(state_processor_tp.pdf['downstate'])+1):
            print(f'calculating downstate analysis for value {value} ...')
            csv_data['Value'] = value
            test_benchmarks = state_processor_tp.calculate_test_benchmarks(lor, state, value)
            csv_data['T-P-Value'] = state_processor_tp.calculate_ttest(lor, state, value)
            csv_data['MW-P-Value'] = state_processor_tp.calculate_mannwhitneyu(lor, state, value)
            beat_benchmark = csv_data['Total_Returns'] < test_benchmarks['Test_Total_Returns']
            csv_data['Test_LOR_Returns'] = test_benchmarks['Test_LOR_Returns']
            csv_data['Test_LOR_Returns_Per'] = test_benchmarks['Test_LOR_Returns_Per']
            csv_data['Test_Total_Returns'] = test_benchmarks['Test_Total_Returns']
            csv_data['Test_Total_Returns_Per'] = test_benchmarks['Test_Total_Returns_Per']
            state_processor_tp.append_to_csv(csv_data, beat_benchmark)

        state = "upstate"
        print('upstate analysis ...')
        csv_data['State'] = state
        # upstate means nothing when it is 0 (it is an downstate)
        # for value in range(1, state_processor_tp.max_upstate+1):
        for value in range(1, max(state_processor_tp.pdf['upstate'])+1):
            print(f'calculating upstate analysis for value {value} ...')
            csv_data['Value'] = value
            test_benchmarks = state_processor_tp.calculate_test_benchmarks(lor, state, value)
            csv_data['T-P-Value'] = state_processor_tp.calculate_ttest(lor, state, value)
            csv_data['MW-P-Value'] = state_processor_tp.calculate_mannwhitneyu(lor, state, value)
            beat_benchmark = csv_data['Total_Returns'] < test_benchmarks['Test_Total_Returns']
            csv_data['Test_LOR_Returns'] = test_benchmarks['Test_LOR_Returns']
            csv_data['Test_LOR_Returns_Per'] = test_benchmarks['Test_LOR_Returns_Per']
            csv_data['Test_Total_Returns'] = test_benchmarks['Test_Total_Returns']
            csv_data['Test_Total_Returns_Per'] = test_benchmarks['Test_Total_Returns_Per']
            state_processor_tp.append_to_csv(csv_data, beat_benchmark)

        state = "totalstate"
        print('calculating totalstate analysis ...')
        csv_data['State'] = state
        # resets when the streak ends for up or down
        # for value in range(state_processor_tp.min_totalstate, state_processor_tp.max_totalstate+1):
        for value in range(min(state_processor_tp.pdf['totalstate']), max(state_processor_tp.pdf['totalstate']+1)):
            print(f'calculating totalstate analysis for value {value} ...')
            csv_data['Value'] = value
            test_benchmarks = state_processor_tp.calculate_test_benchmarks(lor, state, value)
            csv_data['T-P-Value'] = state_processor_tp.calculate_ttest(lor, state, value)
            csv_data['MW-P-Value'] = state_processor_tp.calculate_mannwhitneyu(lor, state, value)
            beat_benchmark = csv_data['Total_Returns'] < test_benchmarks['Test_Total_Returns']
            csv_data['Test_LOR_Returns'] = test_benchmarks['Test_LOR_Returns']
            csv_data['Test_LOR_Returns_Per'] = test_benchmarks['Test_LOR_Returns_Per']
            csv_data['Test_Total_Returns'] = test_benchmarks['Test_Total_Returns']
            csv_data['Test_Total_Returns_Per'] = test_benchmarks['Test_Total_Returns_Per']
            state_processor_tp.append_to_csv(csv_data, beat_benchmark)

        state = "volumestate"
        print('calculating volumestate analysis ...')
        csv_data['State'] = state
        # resets when the streak ends for up or down
        # for value in range(state_processor_tp.min_totalstate, state_processor_tp.max_totalstate+1):
        for value in range(min(state_processor_tp.pdf[state]), max(state_processor_tp.pdf[state]+1)):
            print(f'calculating volumestate analysis for value {value} ...')
            csv_data['Value'] = value
            test_benchmarks = state_processor_tp.calculate_test_benchmarks(lor, state, value)
            csv_data['T-P-Value'] = state_processor_tp.calculate_ttest(lor, state, value)
            csv_data['MW-P-Value'] = state_processor_tp.calculate_mannwhitneyu(lor, state, value)
            beat_benchmark = csv_data['Total_Returns'] < test_benchmarks['Test_Total_Returns']
            csv_data['Test_LOR_Returns'] = test_benchmarks['Test_LOR_Returns']
            csv_data['Test_LOR_Returns_Per'] = test_benchmarks['Test_LOR_Returns_Per']
            csv_data['Test_Total_Returns'] = test_benchmarks['Test_Total_Returns']
            csv_data['Test_Total_Returns_Per'] = test_benchmarks['Test_Total_Returns_Per']
            state_processor_tp.append_to_csv(csv_data, beat_benchmark)

    # state = "upstate"
    # print('calculating upstate analysis ...')
    # values = [1,2,3,4]
    # lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    # for value in values:
    #     for lor in lors:
    #         state_processor_tp.calculate_all_test_states(
    #             compare_col=COMPARE_COL, lor=lor) # calculate all the states in steps
    #         # benchmarks are based on length of return calculations
    #         state_processor_tp.calculate_lor(lor)
    #         state_processor_tp.calculate_benchmarks()
    #         state_processor_tp.calculate_test_benchmarks(state, value)

    # resets when the streak ends for up or down
    # state = "totalstate"
    # print('calculating totalstate analysis ...')
    # values = [3,2,1,0,
    #         -1,-2,-3]
    # lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    # for value in values:
    #     for lor in lors:
    #         state_processor_tp.calculate_all_test_states(
    #             compare_col=COMPARE_COL, lor=lor) # calculate all the states in steps
    #         # benchmarks are based on length of return calculations
    #         state_processor_tp.calculate_lor(lor)
    #         state_processor_tp.calculate_benchmarks()
    #         state_processor_tp.calculate_test_benchmarks(state, value)


# vox_tp = vox.history(period=TIME_PERIOD)
# # list_sector_etf_tps.append(vox_tp)
# state_processor_vox_tp = StateProcessor(vox_tp, "VOX", "1y")
# state_processor_vox_tp.calculate_all_test_states(compare_col=COMPARE_COL)
# state_processor_vox_tp.calculate_benchmarks()
# state_processor_vox_tp.write_csv_header()

# state = "downstate"
# values = [1,2,3,4]
# lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
# for value in values:
#     for lor in lors:
#         state_processor_vox_tp.calculate_lor(lor)
#         state_processor_vox_tp.calculate_test_benchmarks(state, value)

# state = "upstate"
# values = [1,2,3,4]
# lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
# for value in values:
#     for lor in lors:
#         state_processor_vox_tp.calculate_lor(lor)
#         state_processor_vox_tp.calculate_test_benchmarks(state, value)

# state = "totalstate"
# values = [3,2,1,0,
#           -1,-2,-3]
# lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
# for value in values:
#     for lor in lors:
#         state_processor_vox_tp.calculate_lor(lor)
#         state_processor_vox_tp.calculate_test_benchmarks(state, value)

# vcr_tp = vcr.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vcr_tp)
# state_processor_vcr_tp = StateProcessor(vcr_tp)

# vdc_tp = vdc.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vdc_tp)
# state_processor_vdc_tp = StateProcessor(vdc_tp)

# vde_tp = vde.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vde_tp)
# state_processor_vde_tp = StateProcessor(vde_tp)

# vfh_tp = vfh.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vfh_tp)
# state_processor_vfh_tp = StateProcessor(vfh_tp)

# vht_tp = vht.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vht_tp)
# state_processor_vht_tp = StateProcessor(vht_tp)

# vis_tp = vis.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vis_tp)
# state_processor_vis_tp = StateProcessor(vis_tp)

# vgt_tp = vgt.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vgt_tp)
# state_processor_vgt_tp = StateProcessor(vgt_tp)

# vaw_tp = vaw.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vaw_tp)
# state_processor_vaw_tp = StateProcessor(vaw_tp)

# vnq_tp = vnq.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vnq_tp)
# state_processor_vnq_tp = StateProcessor(vnq_tp)

# vpu_tp = vpu.history(period=TIME_PERIOD)
# list_sector_etf_tps.append(vpu_tp)
# state_processor_vpu_tp = StateProcessor(vpu_tp)

# list_sector_etf_idx_state = [] # list of dataframes

# state_processor = StateProcessor()

# for sec_etf_tp in list_sector_etf_tps:
#     sec_etf_tp_state = state_processor.calculate_allstates(sec_etf_tp, compare_col="Close", lor=LOR)
#     list_sector_etf_idx_state.append(sec_etf_tp_state)
