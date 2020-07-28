import os, glob, time, argparse
import pandas as pd
from sklearn.model_selection import train_test_split


def merge_gesture_by_index(data_path, gesture_name, index):
    data_files = glob.glob(os.path.join(data_path, f"{gesture_name}{index}_*.csv"))

    all_df = []

    for f in data_files:
        print(f"File merging -> {f}")
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


def merge_and_concat_gesture_types(gesture_name):
    print(f"merge_gesture_types called for -> {gesture_name}")
    all_movements_of_gesture = []

    for i in range(1, 16):
        #Grab the input data path
        input_data_path = os.getcwd() + '/input_data'

        # Special case when only 1 person has mesured its data
        if include_single and gesture_name in gesture_single:
            print(f"Merging single data for -> {gesture_name}")
            data_path_single = input_data_path + f'/{gesture_name}_gestures'
            data_single = merge_gesture_by_index(data_path_single, gesture_name, i)

            # Append data
            all_movements_of_gesture.append(data_single)

        # When to different datasets are available
        else:
            # Generate full path
            data_path_normal = input_data_path + f'/{gesture_name}_gestures/{gesture_name}_files'
            data_path_g = data_path_normal + '_g'

            # Get the data
            data_normal = merge_gesture_by_index(data_path_normal, gesture_name, i)
            data_g = merge_gesture_by_index(data_path_g, gesture_name, i)

            # Append data
            all_movements_of_gesture.append(data_normal)
            all_movements_of_gesture.append(data_g)

    # Error checking
    if len(all_movements_of_gesture) < 2:
        print(f"ERROR @ merge_and_concat_gesture_types - {gesture_name} -> all_movements_of_gesture count: {len(all_movements_of_gesture)} ")
        exit()

    # Concactunate all data for 1 movements
    gesture_concatunated = pd.concat(all_movements_of_gesture)

    # Add the column to differentiate between types
    gesture_id = gesture_name[0].upper() # E.g. D, L, R
    gesture_concatunated['move_type'] = gesture_id

    # Save intermediate data to csv
    gesture_concatunated.to_csv( f"output_data/gestures/all_{gesture_name}_merged.csv")

    print(f"Amount of merged files: {len(all_movements_of_gesture)}")

    return gesture_concatunated


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
    print(f"Merge starting inlude su: {include_single}....")

    # Track excecution time
    start_time = time.time()

    # gestures = ['down']
    gestures = ['down', 'left', 'right', 'up']
    # gestures = ['down', 'left', 'right', 'spiral', 'up']

    all_merged_and_contact_data = []

    for gesture in gestures:
        # Grabs both data measuresments normal and "g" and merges the data
        gesture_data = merge_and_concat_gesture_types(gesture)
        all_merged_and_contact_data.append(gesture_data)

    # Special case when only 1 person has mesured its data
    if include_single:
        for single_gesture in gesture_single:
            # Runs merge and concatunation for 1 person's data files
            gesture_data_single = merge_and_concat_gesture_types(single_gesture)
            all_merged_and_contact_data.append(gesture_data_single)

    # Concactunate all data to 1 file
    all_gestures_raw = pd.concat(all_merged_and_contact_data)

    # Clear data set
    all_gestures_clean = clean_data(all_gestures_raw)

    # Save all the cleaned data to csv
    keyword_single = 'unbalanced' if include_single else 'all'
    all_gestures_clean.to_csv( f"output_data/{keyword_single}_merged.csv")

    # Split the data into training and testing data sets 70/30
    train_df, test_df = train_test_split(all_gestures_clean, test_size=0.3)

    # Save test and train data
    train_df.to_csv( f"output_data/{keyword_single}_train.csv")
    test_df.to_csv( f"output_data/{keyword_single}_test.csv")

    # Stats
    column_amount = all_gestures_clean['HostTimestamp'].count()
    time_elapsed = time.time() - start_time

    print(f"Merge done with {column_amount} data entries in {time_elapsed} seconds")


# Add single flag which defaults to false if not added with the "store_true" for the action method
# From: https://stackoverflow.com/a/8259080/8970591
parser = argparse.ArgumentParser()
parser.add_argument('-su', action='store_true', help="Include dataset from 1 person")
args = parser.parse_args()

# Set global variable to use when the script runs
include_single = args.su
gesture_single = ['wave', 'spiral']

# Call the main function to start merging, contactunationg and cleaning the data
main()
