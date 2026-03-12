import pandas as pd
import warnings

# suppressing warnings concerning openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# configuring xls sheet
config_df_bf = {"sheet_name" : "1.21", "header" : 11, "footer" : 7}  #business flows
config_df_nol = {"sheet_name" : "1.32", "header" : 11, "footer" : 8} # nature of loan
config_df_pol = {"sheet_name" : "1.33", "header" : 11, "footer" : 9} # purpose of loan
config_list = [config_df_bf, config_df_nol, config_df_pol]

# function responsible for preprocessing data fron xls
def preprocess(config):

    df = pd.read_excel(
        "../../data/mlar/mlar-longrun-detailed.XLSX",
        sheet_name=config["sheet_name"],
        engine='openpyxl',
        header=None,  # do not use header
        skiprows=config["header"],  # skip first 11 rows
        skipfooter=config["footer"]     # skip last 9 rows
    )

    years = df.iloc[0].ffill()      # forward fill row with years
    quarters = df.iloc[1]           # row with quarters
    cols = [f"{y}{q}" for y,q in zip(years, quarters)]
    df.columns = cols               # setting columns names
    df = df.iloc[2:]                # deleting first 2 rows with years and quarters

    sheet_name = config["sheet_name"]       # get currently processed sheet name
    mapping_file = f"mappings/{sheet_name.replace('.', '_')}.csv"   # construct path
    mapping_df = pd.read_csv(mapping_file)  # read mapping to pandas dataframe

    mapping = {}    # create and then fill the dictionary
    for _, row in mapping_df.iterrows():
        mapping[(row['section'], row['id'])] = row['label']

    new_rows = []           # list as a placeholder
    current_section = None  # just initializing
    for _, row in df.iterrows():
        label = str(row.iloc[0]).strip()    # value in first column in current row
        if label in ['A', 'B', 'C']:
            current_section = label
            continue
        if label.isdigit():
            num = int(label)
            key = (current_section, num)
            if key in mapping:
                category = mapping[key]
                new_row = {'category': category}
                for col in df.columns[4:]:
                    new_row[col] = row[col]
                new_rows.append(new_row)
            else:
                print(f"Warning! No mapping for key: {key}")
        # other rows are ignored

    df_result = pd.DataFrame(new_rows)
    print(f"Processed {len(df_result)} rows for worksheet {sheet_name}")
    return df_result

for conf in config_list:
    df_result = preprocess(conf)
    if df_result is not None and not df_result.empty:
        sheet_name = conf['sheet_name'].replace('.', '_')
        output_path = f"../../data/mlar/mlar_{sheet_name}.csv"
        df_result.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")