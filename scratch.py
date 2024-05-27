import pandas as pd # For constructing DataFrame objects
import numpy as np # For nan values
from collections import OrderedDict # For keeping a dict specified later in order it was created
from string import ascii_uppercase # For the dead count alpha characters
from itertools import cycle # To cycle dead count alpha characters
from pprint import pprint

def rename_columns(df: pd.DataFrame, new_names: list):
        df.columns = df.iloc[0]
        df.drop(df.index[0], inplace=True)
        return (df
                .rename(columns=dict(zip(df.columns, new_names)))
                .dropna(axis=1, how='all')
                )

def main():
    PON_FIBER = pd.read_excel('FlexNap Material Calculator.xlsm', sheet_name='Assignment Tool', header=None, dtype='object').iloc[1,7]
    DEAD_CHARS = cycle([alpha for alpha in ascii_uppercase if alpha not in ['B','G','I','O','P','Q','R','S','T','Z']])

    def create_tether_attributes(**tether_info):
        tether_attributes = ''

        for key, value in tether_info.items():
            if key == 'tether_type' and value in ['FST', 'TCF']:
                tail_length = tether_info['long_tail'] if tether_info['long_tail'] is not None else 10
                tether_environment = '845C' if tether_info['term_loc'] == 'HH' else '822C'
                tether_attributes = f"{tether_info['term_addr']}\nEVOLV{tether_info['tether_size']}/{tail_length}OTIP (PPA) {tether_environment}\n{PON_FIBER},{tether_info['low_count']}-{tether_info['high_count']}\n"

        return tether_attributes

    def create_reel_attributes(reel_num: str,
                        dead_char: str='',
                        cable_type=None,
                        cable_size=0,
                        dead_counts: int=0,
                        count_start: int=0):

        environment = ''
        tail_footage = 0 # Come back to this when we incorporate the 'Cable Calculator' data along with this function
        cable_footage = 0 # Come back to this when we incorporate the 'Cable Calculator' data along with this function
        cable_material = ''
        attributes = ''

        if cable_size and count_start != 0:
            if cable_type is not None:
                if cable_type == 'RPX-T' and cable_size <= 144:
                    environment = '845C'
                    cable_material = f'FNAP-CBL-{cable_size:03}-T'
                elif cable_type == 'EUC' and (cable_size <= 144 or cable_size in [216,432]):
                    environment = '845C'
                    cable_material = f'FNAP-CBL-{cable_size:03}EUC'
                elif cable_type == 'RPX' and cable_size != 216 and cable_size < 288:
                    environment = '822C'
                    cable_material = f'FNAP-CBL-{cable_size:03}'

                pon_counts = f"{PON_FIBER},{count_start}-{count_start-1 + cable_size-dead_counts}"
                cable_counts = f"{dead_char},1-{dead_counts}\n{pon_counts}" if dead_counts != 0 else pon_counts
                attributes = f"{reel_num}\n(PPA) {environment} {cable_footage}'\n{cable_material}\n{cable_counts}\n"
            else:
                environment = '845C' # Placeholder to get the function working for now, come back to this later.
                cable_material = f"5BQ2M{'T' if cable_size <= 144 else 'R'}-{cable_size:03}" # Set formatting of reel material to be 5B@2MT-### for reels less than or equal to 144, otherwise set it to 5B@2MR-### for large sizes
                fibers_used = cable_size-dead_counts # Specify the number of fibers used
                pon_counts = f"{PON_FIBER},{count_start}-{count_start-1 + fibers_used}" # Specify the PON counts for the SR cable
                cable_counts = f"{pon_counts}\n{dead_char},{fibers_used+1}-{cable_size}" if dead_counts != 0 else {pon_counts} # SR cables get the PON counts shown on the first row with the dead counts shown on the second row
                attributes = f"{reel_num}\n(PPA) {environment} {cable_footage}'\n{cable_material}\n{cable_counts}\n" # Construct the final attributes from variables determined by the statements

        return attributes

    def get_reel_lengths():
        df = (pd
                .read_excel('FlexNap Material Calculator.xlsm',
                            sheet_name='Cable Calculator',
                            header=None,
                            skiprows=1,
                            dtype='object',
                            )
                .replace({'Y': True, 'N': False, np.nan: None}))

        reel_column_names = ['cut_length', 'final_length', 'flexnap', 'rev_tether', 'endsplice', 'cable_type']
        span_column_names = ['buried_spans', 'buried_loops', 'aerial_spans', 'aerial_loops']
        for i in range(0, len(df.columns), 5):
            cable_params = (df
                            .iloc[0:6, [i, i+3]]
                            .transpose())
            cable_params = rename_columns(cable_params, reel_column_names)
            cable_spans = (df
                            .iloc[6:, i:i+4]
                            .dropna(how='all')
                            ).reset_index(drop=True)
            cable_spans = rename_columns(cable_spans, span_column_names)
            if not cable_params.empty and not cable_params.iloc[0].isna().all() and not cable_params.iloc[0].eq(False).all():
                yield {'info': cable_params.iloc[0].to_dict(), 'spans': cable_spans}

    def get_reel_materials():
        df = (pd
                .read_excel('FlexNap Material Calculator.xlsm',
                            sheet_name='Assignment Tool',
                            header=None,
                            skiprows=6,
                            na_values=[0, '-', ''],
                            usecols='C:F,H,I,K,M')).replace({np.nan: None})

        cable_info_columns = ['cable_type', 'cable_size', 'dead_counts', 'count_start']
        tether_info_columns = ['tether_type', 'term_addr', 'tether_size',
                            'term_loc', 'low_count', 'high_count' , 'long_tail', 'notes']
        reel_dicts = list()
        for i in range(0, len(df), 41):
            cable_info = df.iloc[i:i+4, 1:3].transpose().reset_index(drop=True)
            cable_info = rename_columns(cable_info, cable_info_columns)
            tether_info = df.iloc[i+8:i+39, :].dropna(how='all').reset_index(drop=True)
            tether_info = rename_columns(tether_info, tether_info_columns)
            if not cable_info.empty and not tether_info.empty:
                reel_dicts.append({'reel_info': cable_info.iloc[0].to_dict(), 'tethers': tether_info.to_dict('records')})

        reel_info = OrderedDict()
        reel_lengths = get_reel_lengths() # Create instance of the generator function to use in the following for-loop
        for reel, material in enumerate(reel_dicts):
            material.update(next(reel_lengths))
            reel_info[f'REEL #{reel+1}'] = material

        return reel_info

    reels = get_reel_materials()
    reels
    # return pprint([reel.keys() for reel in reels.values()])
    pprint(reels)

if __name__ == '__main__':
    main()
