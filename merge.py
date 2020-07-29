import os, glob, time, argparse
import pandas as pd
from sklearn.model_selection import train_test_split


def merge_gesture_by_index(data_path, gesture_name, index):
    data_files = glob.glob(os.path.join(data_path, f"{gesture_name}{index}_*.csv"))

    all_df = []

    for f in data_files:
        # print(f"File merging -> {f}")
        df = pd.read_csv(f, sep=',', skiprows=4)
        df = df.sort_values('HostTimestamp') # Make sure rightly sorted
        all_df.append(df)

    # Error checking
    if len(all_df) < 2:
        print(f"ERROR @ merge_gesture_by_index - {gesture_name} -> data_files count: {len(data_files)} and df_count: {len(all_df)} ")
        exit()

    # https://www.datacamp.com/community/tutorials/joining-dataframes-pandas
    df_merge_asof = pd.merge_asof(all_df[0], all_df[1],
              on='HostTimestamp',
              by='NodeName')

    df_merge_asof = pd.merge_asof(df_merge_asof, all_df[2],
              on='HostTimestamp',
              by='NodeName')

    return df_merge_asof

def merge_and_concat_gesture_types_single(gesture_name):
    global train_all_df, test_all_df

    gesture_concatunated_reg = pd.DataFrame()

    for i in range(1, 16):
        # Grab the input data path & Generate full path
        input_data_path = os.getcwd() + '/input_data'
        data_path_single = input_data_path + f'/{gesture_name}_gestures'
        data_single = merge_gesture_by_index(data_path_single, gesture_name, i)

        # Concat the single data
        gesture_concatunated_reg = pd.concat([gesture_concatunated_reg, data_single])

    # Error checking
    if gesture_concatunated_reg.size < 100:
        print(f"ERROR single @ merge_and_concat_gesture_types - {gesture_name} -> all_movements_of_gesture count: {gesture_concatunated_reg.size} ")
        exit()

    # Add the column to differentiate between types
    gesture_id = gesture_name[0].upper() # E.g. D, L, R
    gesture_concatunated_reg['move_type'] = gesture_id

    # Split the data into training and testing data sets 70/30
    train_regular_df, test_regular_df = train_test_split(gesture_concatunated_reg, shuffle = False, test_size=0.2)

    # Append the seperate train and test data sets to the main ones.
    # To maintain a 80/20 ratio per sequence
    train_all_df = pd.concat([train_all_df, train_regular_df])
    test_all_df = pd.concat([test_all_df, test_regular_df])

    # Save intermediate data to csv
    gesture_concatunated_reg = clean_data(gesture_concatunated_reg)
    gesture_concatunated_reg.to_csv( f"output_data/gestures/all_{gesture_name}_merged.csv")

    print(f"Amount of merged files: {len(gesture_concatunated_reg)}")
    return gesture_concatunated_reg


def merge_and_concat_gesture_types(gesture_name):
    global train_all_df, test_all_df

    gesture_concatunated_reg = pd.DataFrame()
    gesture_concatunated_g = pd.DataFrame()

    for i in range(1, 16):
        # Grab the input data path & Generate full paths
        input_data_path = os.getcwd() + '/input_data'
        data_path_normal = input_data_path + f'/{gesture_name}_gestures/{gesture_name}_files'
        data_path_g = data_path_normal + '_g'

        # Get the data
        data_normal = merge_gesture_by_index(data_path_normal, gesture_name, i)
        data_g = merge_gesture_by_index(data_path_g, gesture_name, i)

        # Append data, concat is faster then append I believe
        # See: https://stackoverflow.com/a/15822811/8970591
        gesture_concatunated_reg = pd.concat([gesture_concatunated_reg, data_normal])
        gesture_concatunated_g = pd.concat([gesture_concatunated_g, data_g])

    # Error checking
    if gesture_concatunated_reg.size < 100 or gesture_concatunated_g.size < 100:
        print(f"ERROR regular @ merge_and_concat_gesture_types - {gesture_name} -> regular count: {gesture_concatunated_reg.size} & g count: {gesture_concatunated_g.size} ")
        exit()

    # Add the column to differentiate between types
    gesture_id = gesture_name[0].upper() # E.g. D, L, R
    gesture_concatunated_reg['move_type'] = gesture_id
    gesture_concatunated_g['move_type'] = gesture_id

    # Split the data into training and testing data sets 70/30
    train_regular_df, test_regular_df = train_test_split(gesture_concatunated_reg, shuffle = False, test_size=0.2)
    train_g_df, test_g_df = train_test_split(gesture_concatunated_g, shuffle = False, test_size=0.2)

    # Append the seperate train and test data sets to the main ones.
    # To maintain a 80/20 ratio per sequence
    train_all_df = pd.concat([train_all_df, train_regular_df])
    test_all_df = pd.concat([test_all_df, test_regular_df])
    train_all_df = pd.concat([train_all_df, train_g_df])
    test_all_df = pd.concat([test_all_df, test_g_df])

    # Concatunate all movement data for 1 gesture
    all_gestures_concatunated = pd.concat([gesture_concatunated_reg, gesture_concatunated_g])

    # Save intermediate data to csv
    all_gestures_clean = clean_data(all_gestures_concatunated)
    all_gestures_clean.to_csv( f"output_data/gestures/all_{gesture_name}_merged.csv")

    print(f"Amount of merged files: {len(all_gestures_concatunated)}")
    return all_gestures_clean


def clean_data(raw_data_df):
    # Clear empty rows
    clean_df = raw_data_df.dropna()

    # Drop irrelevant columns
    clean_df = clean_df.drop(['Date_x', 'NodeName', 'RawData_x', 'Date_y',
        'NodeTimestamp_y', 'RawData_y', 'Date', 'NodeTimestamp', 'RawData'],
        axis=1)

    # Rename columns
    clean_df.index.name = "sr_no"
    clean_df.rename(columns={'X (mg)': 'X_mg', 'Y (mg)': 'Y_mg', 'Z (mg)': 'Z_mg',
        'X (mGa)': 'X_mGa','Y (mGa)': 'Y_mGa', 'Z (mGa)': 'Z_mGa',
        'X (dps)': 'X_dps','Z (dps)': 'Z_dps', 'Z (dps)': 'Z_dps',
        'NodeTimestamp_x': 'NodeTimestamp'},
        inplace=True)

    return clean_df


def main():
    global train_all_df, test_all_df
    print(f"Merge starting inlude su: {include_single}.... \n")

    # Track excecution time
    start_time = time.time()

    # To add the df's of all possible movements
    all_gestures_clean = pd.DataFrame()

    for gesture in gestures:
        print(f"---- Start merging data for -> {gesture} ------")

        # Grabs both data measuresments normal and "g" and merges the data
        gesture_data_clean = merge_and_concat_gesture_types(gesture)

        # Concactunate to the main file for non-test train dataset, so all data points
        all_gestures_clean = pd.concat([all_gestures_clean, gesture_data_clean])

        print(f"Done adding test and train regular for -> {gesture} \n")

    # Special case when only 1 person has mesured its data
    if include_single:
        for single_gesture in gesture_single:
            print(f"---- Start merging single data for -> {gesture} ------")

            # Runs merge and concatunation for 1 person's data files
            single_gesture_clean = merge_and_concat_gesture_types_single(single_gesture)

            # Concactunate to the main file for non-test train dataset, so all data points
            all_gestures_clean = pd.concat([all_gestures_clean, single_gesture_clean])
            print(f"Done adding test and train single for -> {single_gesture} \n")

    # Clean all train and test data
    train_all_df = clean_data(train_all_df)
    test_all_df = clean_data(test_all_df)

    # Save all the cleaned data to csv
    keyword_single = 'unbalanced' if include_single else 'all'
    all_gestures_clean.to_csv( f"output_data/{keyword_single}_merged.csv")

    # Save test and train data
    train_all_df.to_csv( f"output_data/{keyword_single}_train.csv")
    test_all_df.to_csv( f"output_data/{keyword_single}_test.csv")

    # Stats
    data_rows_amount = all_gestures_clean.size
    data_train_amount = train_all_df.size
    data_test_amount = test_all_df.size
    ratio_train = data_train_amount / (data_train_amount + data_test_amount)
    ratio_test = 1 - ratio_train
    time_elapsed = time.time() - start_time
    print(f"---- Stats ------")
    print(f"Merge done with {data_rows_amount} data entries")
    print(f"Split: train amount: {data_train_amount} ({'{:.1%}'.format(ratio_train)}) & test amount: {data_test_amount} ({'{:.1%}'.format(ratio_test)})")
    print(f"The merge took in {time_elapsed} seconds")

# Add single flag which defaults to false if not added with the "store_true" for the action method
# From: https://stackoverflow.com/a/8259080/8970591
parser = argparse.ArgumentParser()
parser.add_argument('-su', action='store_true', help="Include dataset from 1 person")
args = parser.parse_args()

# Set global variable to use when the script runs
include_single = args.su

# Recorded movements for 1 person
gesture_single = ['wave', 'spiral']

# Recorded movements for 2 person recordings
gestures = ['down', 'left', 'right', 'up']

# Final test, train set
train_all_df = pd.DataFrame()
test_all_df = pd.DataFrame()

# Call the main function to start merging, contactunationg and cleaning the data
main()
