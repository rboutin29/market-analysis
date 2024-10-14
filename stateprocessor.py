'''
Calculates states of a yahoo finance dataframe
'''
import csv
import pingouin as pg

class StateProcessor():

    def __init__(self, df, asset, time_period, filename='example'):
        self.asset = asset
        self.time_period = time_period
        self.df = df # df from yahoo finance
        self.pdf = 0 # df for processing, which is overwritten in functions
        self.trail_avg_return_per = 0
        self.trail_avg_return = 0
        self.lead_avg_return_per = 0
        self.lead_avg_return = 0
        self.total_return_per = 0
        self.total_return = 0
        self.test_avg_return_per = 0
        self.test_avg_return = 0
        self.test_total_return_per = 0
        self.test_total_return = 0
        self.lor = 0
        self.trail_pvalue = 0
        self.lead_pvalue = 0
        self.filename = filename

    def reset_params(self):
        self.pdf = 0 # df for processing, which is overwritten in functions
        self.trail_avg_return_per = 0
        self.trail_avg_return = 0
        self.lead_avg_return_per = 0
        self.lead_avg_return = 0
        self.total_return_per = 0
        self.total_return = 0
        self.test_avg_return_per = 0
        self.test_avg_return = 0
        self.test_total_return_per = 0
        self.test_total_return = 0
        self.lor = 0
        self.trail_pvalue = 0
        self.lead_pvalue = 0
        self.filename = 'example'

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
        '''
        # self.reset_params()
        df = self.df.reset_index()
        df['upstate'] = 0
        df['downstate'] = 0
        df['totalstate'] = 0
        df['flowstate'] = 0
        # returns based on previous day
        df['trail_returns'] = 0
        df['trail_returns_per'] = 0
        # returns based on next day
        df['lead_returns'] = 0
        df['lead_returns_per'] = 0
        prev_row = df.iloc[0]
        for index, curr_row in df.iterrows():
            if index > 0:
                # upstate flow
                if prev_row[compare_col] < curr_row[compare_col]:
                    df.at[index, 'upstate'] = prev_row['upstate'] + 1
                    df.at[index, 'flowstate'] = prev_row['flowstate'] + 1
                    # if state switches (fron down to up), restart count
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
                df.at[index, 'trail_returns'] = curr_row['Close'] - prev_row['Close']
                df.at[index, 'trail_returns_per'] = (
                    curr_row['Close'] - prev_row['Close']) / prev_row['Close'] * 100
                if (index < len(df)-1):
                    # calculate leading returns based on next day
                    next_row = df.iloc[index+1]
                    df.at[index, 'lead_returns'] = next_row['Close'] - curr_row['Close']
                    df.at[index, 'lead_returns_per'] = (
                        next_row['Close'] - curr_row['Close']) / curr_row['Close'] * 100
                prev_row = df.iloc[index] # use df and index bc 'curr_row' is a copy
        self.pdf = df
        return df

    def calculate_lor(self, lor):
        '''
        calculate returns based on a length parameter and append these
        results to the processing dataframe
        '''
        self.lor = lor
        df = self.pdf
        df['lor_returns'] = 0
        df['lor_returns_per'] = 0
        for index, curr_row in df.iterrows():
            if index > 0:
                # calculate returns based on parameter, ;ength of returns
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
        return df

    def calculate_benchmarks(self):
        '''
        calculates daily returns for everyday, regardless of state and
        first row doesn't have return data
        '''
        # b = vox_tp_idx_state['returns_per'].iloc[1:len(vox_tp_idx_state)]
        self.trail_avg_return_per = self.pdf['trail_returns_per'].iloc[1:].mean()
        self.trail_avg_return = self.pdf['trail_returns'].iloc[1:].mean()
        self.lead_avg_return_per = self.pdf['lead_returns_per'].iloc[:len(self.pdf)-1].mean()
        self.lead_avg_return = self.pdf['lead_returns'].iloc[:len(self.pdf)-1].mean()
        self.total_return_per = (
            self.pdf.at[len(self.pdf)-1, 'Close'] - self.pdf.at[0, 'Close']) / self.pdf.at[0, 'Close'] * 100
        self.total_return = self.pdf.at[len(self.pdf)-1, 'Close'] - self.pdf.at[0, 'Close']

    def calculate_test_benchmarks(self, state, value, write_to_csv=True):
        '''
        calculate returns based on a testing state and value, then write to a csv file
        '''
        lordf = self.pdf[1:len(self.pdf)-self.lor]
        tdf = lordf.drop(
            lordf[lordf[state] != value].index) # drop all rows that are not the target state

        self.test_avg_return_per = tdf['lor_returns_per'].mean()
        self.test_avg_return = tdf['lor_returns'].mean()
        # estimate: percent return of the sum of returns, using the strategy,
        # to the initial close price
        self.test_total_return_per = sum(tdf['lor_returns']) / self.pdf.at[0, 'Close'] * 100
        self.test_total_return = sum(tdf['lor_returns'])

        if write_to_csv is True:
            self.append_to_csv(self.asset, self.time_period, state, value)

    def write_csv_header(self, filename):
        '''
        overwrite csv file with only a header
        '''
        # Open the file in write mode
        self.filename = filename
        with open(f'{filename}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            # Write a header row
            writer.writerow(
                ['Asset','Time Period','Length of Return','State','Value','Avg % Return',
                 'Avg Return','Total % Return','Total Return','Trail p-value',
                 'B-Trail Avg % Return','B-Trail Avg Return','Lead p-value',
                 'B-Lead Avg % Return','B-Lead Avg Return','B-Total % Return',
                 'B-Total Return','Beat Benchmark?'
                 ]
            )

    def append_to_csv(self, asset, time_period, state, value):
        '''
        append data to csv file
        '''
        # Open the file in write mode
        with open(f'{self.filename}.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            beat_benchmark = self.total_return<self.test_total_return
            self.calculate_ttest(state, value)
            # Write multiple rows
            rows = [
                [asset,time_period,self.lor,state,value,self.test_avg_return_per,
                 self.test_avg_return,self.test_total_return_per,
                 self.test_total_return,self.trail_pvalue,self.trail_avg_return_per,
                 self.trail_avg_return,self.lead_pvalue,self.lead_avg_return_per,
                 self.lead_avg_return,self.total_return_per,self.total_return,
                 beat_benchmark
                 ]
            ]
            writer.writerows(rows)

    def calculate_ttest(self, state, value):
        '''
        perform a t-test and save the p-value
        '''
        lordf = self.pdf[1:len(self.pdf)-self.lor]
        # drop all rows that are not the target state
        tdf = lordf.drop(lordf[lordf[state] != value].index)
        # need to drop target state rows for independent t-test
        # drop all rows that ARE the target state
        bdf = self.pdf.drop(self.pdf[self.pdf[state] == value].index)
        # t-test against trailing returns (based on previous day)
        b_trail = bdf['trail_returns_per'].iloc[1:] # returns for everyday excluding target state
        trail_results = pg.ttest(tdf['lor_returns_per'], b_trail, alternative = 'greater', paired=False, correction=True)
        self.trail_pvalue = trail_results.loc[trail_results.index[0], 'p-val']
        # t-test against leading returns (based on next day)
        b_lead = bdf['lead_returns_per'].iloc[:len(self.pdf)-1]
        lead_results = pg.ttest(tdf['lor_returns_per'], b_lead, alternative = 'greater', paired=False, correction=True)
        self.lead_pvalue = lead_results.loc[lead_results.index[0], 'p-val']

