'''
Calculates states of a yahoo finance dataframe
'''
import csv
import pingouin as pg

class StateProcessor():
    '''
    Calculating states of time series data
    '''

    def __init__(self, df):
        # self.asset = asset
        # self.time_period = time_period
        self.df = df # df from yahoo finance
        self.pdf = 0 # df for processing
        # self.lor = 0

        # self.trail_avg_return_per = 0
        # self.trail_avg_return = 0
        # self.lead_avg_return_per = 0
        # self.lead_avg_return = 0

        # self.max_downstate = 0
        # self.max_upstate = 0
        # self.max_totalstate = 0
        # self.min_totalstate = 0

        # benchmark variables
        # self.total_return_per = 0
        # self.total_return = 0
        # self.lor_returns = 0
        # self.lor_returns_per = 0

        # testing variables
        # self.test_avg_return_per = 0
        # self.test_avg_return = 0
        # self.test_total_return_per = 0
        # self.test_total_return = 0
        self.occurences = 0
        # self.pvalue = 0
        # self.trail_pvalue = 0
        # self.lead_pvalue = 0
        # self.filename = filename

    # def reset_params(self):
    #     self.pdf = 0 # df for processing, which is overwritten in functions
    #     # self.trail_avg_return_per = 0
    #     # self.trail_avg_return = 0
    #     # self.lead_avg_return_per = 0
    #     # self.lead_avg_return = 0
    #     self.total_return_per = 0
    #     self.total_return = 0
    #     self.test_avg_return_per = 0
    #     self.test_avg_return = 0
    #     self.test_total_return_per = 0
    #     self.test_total_return = 0
    #     self.lor = 0
    #     # self.trail_pvalue = 0
    #     # self.lead_pvalue = 0
    #     self.filename = 'example'

    def calculate_all_test_states(self, compare_col):
        '''
        calculate states
            upstate: value represents number of consecutive days where
                close price > previous day's close price
            downstate: value represents number of consecutive days where
                close price < previous day's close price
            totalstate: upstate and downstate combined where downstate
                values are negative, zeroes will exist at state switch
            flowstate: running tally where upstate gives plus 1 and downstate gives minus 1
            volumestate: represents number of consecutive days where
                volume of current day > volume of previous day
        '''
        # self.reset_params()
        df = self.df.reset_index()
        df['upstate'] = 0
        df['downstate'] = 0
        df['totalstate'] = 0
        df['flowstate'] = 0
        df['volumestate'] = 0
        # # returns based on previous day
        # df['trail_returns'] = 0
        # df['trail_returns_per'] = 0
        # # returns based on next day
        # df['lead_returns'] = 0
        # df['lead_returns_per'] = 0

        prev_row = df.iloc[0]
        for index, curr_row in df.iterrows():
            if index > 0:
                # volumestate flow
                # volume is increasing
                if prev_row['Volume'] < curr_row['Volume']:
                    # if volume switches (from down to up), restart volumestate at 0
                    if prev_row["volumestate"] < 0:
                        df.at[index, 'volumestate'] = 0
                    # increment streak of daily increasing volume
                    else:
                        df.at[index, 'volumestate'] = prev_row['volumestate'] + 1
                # volume is decreasing
                else:
                    # if volume switches (from up to down), restart volumestate at 0
                    if prev_row["volumestate"] > 0:
                        df.at[index, 'volumestate'] = 0
                    # increment streak of daily decreasing volume
                    else:
                        df.at[index, 'volumestate'] = prev_row['volumestate'] - 1

                # upstate flow
                if prev_row[compare_col] < curr_row[compare_col]:
                    # downstate will be 0 everytime we increment upstate
                    df.at[index, 'upstate'] = prev_row['upstate'] + 1
                    df.at[index, 'flowstate'] = prev_row['flowstate'] + 1
                    # if state switches (from down to up), restart total count
                    if prev_row["downstate"] != 0:
                        df.at[index, 'totalstate'] = 0
                    else:
                        df.at[index, 'totalstate'] = prev_row['totalstate'] + 1
                # downstate flow
                else:
                    df.at[index, 'downstate'] = prev_row['downstate'] + 1
                    df.at[index, 'flowstate'] = prev_row['flowstate'] - 1
                    # if state switches (from up to down), restart count
                    if prev_row["upstate"] != 0:
                        df.at[index, 'totalstate'] = 0
                    else:
                        df.at[index, 'totalstate'] = prev_row['totalstate'] - 1
                # calculate trailing returns based on previous day
                # df.at[index, 'trail_returns'] = curr_row['Close'] - prev_row['Close']
                # df.at[index, 'trail_returns_per'] = (
                #     curr_row['Close'] - prev_row['Close']) / prev_row['Close'] * 100
                # if (index < len(df)-1):
                #     # calculate leading returns based on next day
                #     next_row = df.iloc[index+1]
                #     df.at[index, 'lead_returns'] = next_row['Close'] - curr_row['Close']
                #     df.at[index, 'lead_returns_per'] = (
                #         next_row['Close'] - curr_row['Close']) / curr_row['Close'] * 100
                # calculate returns based on parameter, ;ength of returns
                # ex: lor = 5
                # if index < len(df)-self.lor:
                #     lor_row = df.iloc[index + self.lor] #5th day
                #     # 5th day close - curr close = return over 5 days? Yes
                #     # can we use df.pct_chg()?
                #     df.at[index, 'b_lor_returns'] = (
                #         lor_row['Close'] - curr_row['Close'])
                #     df.at[index, 'b_lor_returns_per'] = (
                #             lor_row['Close'] - curr_row['Close']) / curr_row['Close'] * 100

                prev_row = df.iloc[index] # use df and index bc 'curr_row' is a copy
        self.pdf = df
        return df


    # def set_maxmin_values(self):
    #     '''
    #     set max value
    #     '''
    #     self.max_downstate = max(self.pdf['downstate'])
    #     self.max_upstate = max(self.pdf['upstate'])
    #     self.max_totalstate = max(self.pdf['totalstate'])
    #     self.min_totalstate = min(self.pdf['totalstate'])


    def calculate_lor(self, lor):
        '''
        calculate returns based on a length parameter and append these
        results to the processing dataframe
        '''
        # self.lor = lor
        df = self.pdf
        df['lor_returns'] = 0
        df['lor_returns_per'] = 0
        for index, curr_row in df.iterrows():
            if index > 0:
                # calculate returns based on parameter, length of returns
                # ex: lor = 5
                if index < len(df)-lor:
                    lor_row = df.iloc[index + lor] #5th day
                    # 5th day close - curr close = return over 5 days? Yes
                    # can we use df.pct_chg()?
                    df.at[index, 'lor_returns'] = (
                        lor_row['Close'] - curr_row['Close'])
                    df.at[index, 'lor_returns_per'] = (
                            lor_row['Close'] - curr_row['Close']) / curr_row['Close'] * 100
        self.pdf = df
        # return df


    def calculate_benchmarks(self):
        '''
        calculates daily returns for everyday, regardless of state and
        first row doesn't have return data
        '''
        # b = vox_tp_idx_state['returns_per'].iloc[1:len(vox_tp_idx_state)]
        # self.trail_avg_return_per = self.pdf['trail_returns_per'].iloc[1:].mean()
        # self.trail_avg_return = self.pdf['trail_returns'].iloc[1:].mean()
        # self.lead_avg_return_per = self.pdf['lead_returns_per'].iloc[:len(self.pdf)-1].mean()
        # self.lead_avg_return = self.pdf['lead_returns'].iloc[:len(self.pdf)-1].mean()
        # self.lor_returns = self.pdf['lor_returns'].iloc[:len(self.pdf)-1].mean()
        # self.lor_returns_per = self.pdf['lor_returns_per'].iloc[:len(self.pdf)-1].mean()
        # self.total_return_per = (
        #     self.pdf.at[
        #         len(self.pdf)-1, 'Close'] - self.pdf.at[0, 'Close']) / self.pdf.at[0, 'Close'] * 100
        # self.total_return = self.pdf.at[len(self.pdf)-1, 'Close'] - self.pdf.at[0, 'Close']
        benchmarks = {
            'LOR_Returns_Per': 0,
            'LOR_Returns': 0,
            'Total_Returns': 0,
            'Total_Returns_Per': 0
        }
        benchmarks['LOR_Returns'] = self.pdf['lor_returns'].iloc[:len(self.pdf)-1].mean()
        benchmarks['LOR_Returns_Per'] = self.pdf['lor_returns_per'].iloc[:len(self.pdf)-1].mean()
        benchmarks['Total_Returns_Per'] = (
            self.pdf.at[
                len(self.pdf)-1, 'Close'] - self.pdf.at[0, 'Close']) / self.pdf.at[0, 'Close'] * 100
        benchmarks['Total_Returns'] = self.pdf.at[
            len(self.pdf)-1, 'Close'] - self.pdf.at[0, 'Close']
        return benchmarks


    def calculate_test_benchmarks(self, lor, state, value):
        '''
        calculate returns based on a testing state and value, then write to a csv file
        '''
        lordf = self.pdf[1:len(self.pdf)-lor]
        tdf = lordf.drop(
            lordf[lordf[state] != value].index) # drop all rows that are not the target state

        # self.test_avg_return_per = tdf['lor_returns_per'].mean()
        # self.test_avg_return = tdf['lor_returns'].mean()
        # # estimate: percent return of the sum of returns, using the strategy,
        # # to the initial close price
        # self.test_total_return_per = sum(tdf['lor_returns']) / self.pdf.at[0, 'Close'] * 100
        # self.test_total_return = sum(tdf['lor_returns'])
        self.occurences = len(tdf)
        test_benchmarks = {
            'Test_LOR_Returns_Per': 0,
            'Test_LOR_Returns': 0,
            'Test_Total_Returns': 0,
            'Test_Total_Returns_Per': 0
        }
        test_benchmarks['Test_LOR_Returns_Per'] = tdf['lor_returns_per'].mean()
        test_benchmarks['Test_LOR_Returns'] = tdf['lor_returns'].mean()
        # estimate: percent return of the sum of returns, using the strategy,
        # to the initial close price
        test_benchmarks['Test_Total_Returns_Per'] = sum(tdf['lor_returns']) / self.pdf.at[0, 'Close'] * 100
        test_benchmarks['Test_Total_Returns'] = sum(tdf['lor_returns'])
        return test_benchmarks


    def write_csv_header(self, filename):
        '''
        overwrite csv file with only a header
        '''
        # Open the file in write mode
        # self.filename = filename
        with open(f'{filename}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            # Write a header row
            # writer.writerow(
            #     ['Asset','Time Period','Length of Return','State','Value','Avg % Return',
            #      'Avg Return','Total % Return','Total Return','Trail p-value',
            #      'B-Trail Avg % Return','B-Trail Avg Return','Lead p-value',
            #      'B-Lead Avg % Return','B-Lead Avg Return','B-Total % Return',
            #      'B-Total Return','B-LOR Avg % Return','B-LOR Avg Return','Beat Benchmark?'
            #      ]
            # )
            writer.writerow(
                ['Asset','Time Period','Length of Return','State','Value','Avg % Return',
                 'Avg Return','Total % Return','Total Return','p-value','B-Total % Return',
                 'B-Total Return','B-LOR Avg % Return','B-LOR Avg Return','Beat Benchmark?',
                 'Occurences'
                 ]
            )


    def append_to_csv(self, csv_data, beat_benchmark=False):
        '''
        append data to csv file
        '''
        # Open the file in write mode
        fileName = csv_data['FileName']
        with open(f'{fileName}.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            # beat_benchmark = self.total_return<self.test_total_return
            # Write multiple rows
            # rows = [
            #     [asset,time_period,self.lor,state,value,self.test_avg_return_per,
            #      self.test_avg_return,self.test_total_return_per,
            #      self.test_total_return,self.trail_pvalue,self.trail_avg_return_per,
            #      self.trail_avg_return,self.lead_pvalue,self.lead_avg_return_per,
            #      self.lead_avg_return,self.total_return_per,self.total_return,
            #      self.b_lor_returns_per,self.b_lor_returns,beat_benchmark
            #      ]
            # ]
            # rows = [
            #     [
            #         csv_data['Ticker'],csv_data['TimePeriod'],csv_data['LOR'],
            #         csv_data['State'],csv_data['Value'],self.test_avg_return_per,
            #         self.test_avg_return,self.test_total_return_per,
            #         self.test_total_return,self.pvalue,
            #         self.total_return_per,self.total_return,
            #         self.lor_returns_per,self.lor_returns,beat_benchmark,
            #         self.occurences
            #     ]
            # ]
            rows = [
                [
                    csv_data['Ticker'], csv_data['TimePeriod'], csv_data['LOR'],
                    csv_data['State'], csv_data['Value'], csv_data['Test_LOR_Returns_Per'],
                    csv_data['Test_LOR_Returns'], csv_data['Test_Total_Returns_Per'],
                    csv_data['Test_Total_Returns'], csv_data['P-Value'],
                    csv_data['Total_Returns'], csv_data['Total_Returns_Per'],
                    csv_data['LOR_Returns_Per'], csv_data['LOR_Returns'],
                    beat_benchmark, self.occurences
                ]
            ]
            writer.writerows(rows)


    def calculate_ttest(self, lor, state, value):
        '''
        perform a t-test and save the p-value
        '''
        lordf = self.pdf[1:len(self.pdf)-lor]
        # drop all rows that are not the target state
        tdf = lordf.drop(lordf[lordf[state] != value].index)
        # need to drop target state rows for independent t-test
        # drop all rows that ARE the target state
        bdf = self.pdf.drop(self.pdf[self.pdf[state] == value].index)
        # t-test against returns (not based on state)
        b = bdf['lor_returns_per'].iloc[:len(self.pdf)-1]
        # degrees of freedom (or sample size - 1) cannot be 0
        if len(tdf) != 1:
            results = pg.ttest(
                tdf['lor_returns_per'], b, alternative = 'two-sided', paired=False, correction=True)
            # self.pvalue = results.loc[results.index[0], 'p-val']
            pvalue = results.loc[results.index[0], 'p-val']
        else:
            print('throwing out case because sample size is 1')
            # self.pvalue = 1
            pvalue = 1
        return pvalue
        # # t-test against trailing returns (based on previous day)
        # b_trail = bdf['trail_returns_per'].iloc[1:] # returns for everyday excluding target state
        # trail_results = pg.ttest(tdf['lor_returns_per'], b_trail, alternative = 'greater', paired=False, correction=True)
        # self.trail_pvalue = trail_results.loc[trail_results.index[0], 'p-val']
        # # t-test against leading returns (based on next day)
        # b_lead = bdf['lead_returns_per'].iloc[:len(self.pdf)-1]
        # lead_results = pg.ttest(tdf['lor_returns_per'], b_lead, alternative = 'greater', paired=False, correction=True)
        # self.lead_pvalue = lead_results.loc[lead_results.index[0], 'p-val']
