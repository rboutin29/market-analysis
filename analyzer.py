import yfinance as yf

from stateprocessor import StateProcessor

# globals
TIME_PERIOD = "1y" # yahoo finance format
COMPARE_COL = "Close"
LOR = 3
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

tickers = ['VOX','VCR','VDC','VDE','VFH','VHT','VIS','VGT','VAW','VNQ','VPU']
state_processor_tp = StateProcessor("asset_tp", "ticker", TIME_PERIOD, filename='full')
state_processor_tp.write_csv_header(filename='full')
for ticker in tickers:
    asset = yf.Ticker(ticker)
    asset_tp = asset.history(period=TIME_PERIOD)
    state_processor_tp = StateProcessor(asset_tp, ticker, TIME_PERIOD, filename='full')
    state_processor_tp.calculate_all_test_states(compare_col=COMPARE_COL)
    state_processor_tp.calculate_benchmarks()
    # state_processor_tp.write_csv_header()

    state = "downstate"
    values = [1,2,3,4]
    lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    for value in values:
        for lor in lors:
            state_processor_tp.calculate_lor(lor)
            state_processor_tp.calculate_test_benchmarks(state, value)

    state = "upstate"
    values = [1,2,3,4]
    lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    for value in values:
        for lor in lors:
            state_processor_tp.calculate_lor(lor)
            state_processor_tp.calculate_test_benchmarks(state, value)

    state = "totalstate"
    values = [3,2,1,0,
            -1,-2,-3]
    lors = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    for value in values:
        for lor in lors:
            state_processor_tp.calculate_lor(lor)
            state_processor_tp.calculate_test_benchmarks(state, value)


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
