import pandas as pd
import numpy as np

def show_df(df: pd.DataFrame, rows=None) -> None:
    with pd.option_context('display.max_rows', rows, 
                           'display.max_columns', None):
        display(df) # or print()
           
def show_groupby(df: pd.DataFrame.groupby) -> None:
    for group, items in df:
        print('*'*60)
        print(f"\nGroup -> Week starting {group.strftime('%B %d, %Y')}\n")
        show_df(items.reset_index(drop=True))
        print()

def show_dates(df):
    new_dates = df.Date.map(lambda date: date.strftime('%A, %B %d, %Y'))
    df.insert(loc=1, column='new_dates', value=new_dates)
    return df
        
RENAME_ITEMS = {'7-Eleven': 'Auto & Transport',
                 'Sc state treasur': 'Income',
                 'Shell': 'Auto & Transport',
                 'Scraps, LLC': 'Bills & Utilities',
                 'Circle K': 'Auto & Transport',
                 'Ncdmv Reg 078 Charlott': 'Auto & Transport',
                 'Venmo': 'Shopping',
                 'QuikTrip': 'Auto & Transport'}

transactions = (pd
                .read_csv('transactions.csv', 
                          usecols=[0,2,3,4,6,8,9,10,11],
                          parse_dates=['Date'],
                          converters={'Amount': lambda x: -float(x)},
                          dtype_backend='numpy_nullable',
                          )
                )

for find, replace in RENAME_ITEMS.items():
    transactions.Category = np.where(transactions.Name == find, replace, transactions.Category)

transactions.Category = (np
			 .where(
				     (transactions.Category == 'Cash & Checks') & (transactions.Amount >= 400.),
				     'Bills & Utilities',
				     transactions.Category,
				     )    
                         )

filter_date_and_accounts: bool = (
					(transactions.Date.between('2024-02-01','2024-04-30')) &
					(transactions['Description'] != 'Card Payment from Secured Account')
					)

filter_transactions: bool = (
				~transactions['Category'].isin(['Internal Transfers', 'Savings Transfer']) &
				~transactions.Name.str.contains('Transfer|Pending',case=False, regex=True) &
				~transactions['Account Name'].isin(['Rainy Day', 'Chime Savings'])
                            	)
                         
last_quarter: pd.DataFrame = (transactions
                .loc[filter_date_and_accounts & filter_transactions]
                .dropna(how='all', axis=1)
                .reset_index(drop=True)
                )

weekly_bills = (last_quarter 
                .groupby(pd.Grouper(key='Date', freq='W-WED', label='left'))
                .apply(lambda x: (x
                                  .sort_values(by='Amount', ascending=False)
                                  .groupby('Category')
                                  .agg({'Amount': 'sum'})
                                  .sort_values('Amount', ascending=False)
                                  )
                       )
                )

monthly_bills = (last_quarter
                 .groupby(pd.Grouper(key='Date', freq='M', label='left'))
                 .apply(lambda x: (x
                                   .groupby('Category')
                                   .agg({'Amount': 'sum'})
                                   .sort_values('Amount', ascending=False)
                                   )
                        )
                 )

average_monthly_income = (last_quarter
                          .loc[last_quarter['Category'] == 'Income']
                          .groupby(pd.Grouper(key='Date', freq='M'))
                          .apply(lambda x: (x
                                            .groupby('Category')
                                            .agg({'Amount': 'sum'})
                                            .sort_values('Amount', ascending=False)
                                            )
                                 )
                           .mean()
                          )

weekly_transactions_df = (last_quarter
			  .pipe(show_dates)
			  .groupby(pd.Grouper(key='Date', freq='W-WED'))
			  .apply(lambda x: (x
								.drop(columns=['Date'])
								.reset_index(drop=True)
							   )
					)
			 )
