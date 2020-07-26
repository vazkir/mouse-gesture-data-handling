import os, glob, time
import pandas as pd

data_path = '/Users/vascowerk/Harvard/DGMD/merge-mouse-data/data'

def merge_gesture_by_index(data_path, gesture_name, index):
    data_files = glob.glob(os.path.join(data_path, f"{gesture_name}{index}_*.csv"))

    all_df = []

    for f in data_files:
        print(f"File merging -> {f}")
        df = pd.read_csv(f, sep=',', skiprows=4)

        # Make sure rightly sorted
        df = df.sort_values('HostTimestamp')

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
        data_path_normal = data_path + f'/{gesture_name}_gestures/{gesture_name}_files'
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

    # Merge all items per gesture
    gesture_concatunated.to_csv( f"output/gestures/all_{gesture_name}_merged.csv")

    print(f"Amount of merged files: {len(all_movements_of_gesture)}")

    return gesture_concatunated



def main():
    print("Merge starting..")

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

    # Concactunate all data to 1 file
    gesture_concatunated = pd.concat(all_merged_and_contact_data)

    # Merge all items per gesture
    gesture_concatunated.to_csv( f"output/all_merged.csv")

    # Stats
    column_amount = gesture_concatunated['HostTimestamp'].count()
    time_elapsed = time.time() - start_time

    print(f"Merge done with {column_amount} data entries in {time_elapsed} seconds")


main()




    # down_path = data_path + '/down_gestures/down_files'
    # down_files = glob.glob(os.path.join(down_path, "down1_*.csv"))
    #
    # all_df = []
    # for f in down_files:
    #     print(f"File merging -> {f}")
    #     df = pd.read_csv(f, sep=',')
    #     df['file'] = f.split('/')[-1]
    #     all_df.append(df)
    #
    # # df_merged = pd.concat(all_df, ignore_index=True, axis='columns').drop_duplicates().reset_index(drop=True)
    # # df_merged.to_csv( "merged.csv")
    #
    # # df_merge_asof = reduce(lambda  left,right: pd.merge(left,right,on=['HostTimestamp'], how='outer'), all_df)
    # # https://www.datacamp.com/community/tutorials/joining-dataframes-pandas
    # df_merge_asof = pd.merge_asof(all_df[0], all_df[1],
    #           on='HostTimestamp',
    #           by='NodeName')
    #
    # df_merge_asof = pd.merge_asof(df_merge_asof, all_df[2],
    #           on='HostTimestamp',
    #           by='NodeName')
    #
    # df_merge_asof.to_csv( "merged.csv")

    # down_path = data_path + '/down_gestures/down_files'
    # down_files = glob.glob(os.path.join(down_path, "down1_*.csv"))
    #
    # all_df = []
    # for f in down_files:
    #     print(f"File-> {f}")
    #     df = pd.read_csv(f, sep=',')
    #     # f['file'] = f.split('/')[-1]
    #     all_df.append(df)
    #
    # merged_df = pd.concat(all_df, ignore_index=True, sort=True)
    #
    # # df_merged = (pd.read_csv(f, sep=',') for f in down_files)
    # # df_merged   = pd.concat(df_from_each_file, ignore_index=True)
    # df_merged.to_csv( "merged.csv")









# def main2():
#     # From: https://stackoverflow.com/questions/14530748/combine-columns-from-several-csv-files-into-a-single-file
#     down_path = data_path + '/down_gestures/down_files'
#     down_files = glob.glob(os.path.join(down_path, "down1_*.csv"))
#
#     dfs = []
#     for filename in down_files:
#         # read the csv, making sure the first two columns are str
#         df = pd.read_csv(filename, header=None, converters={0: str, 1: str})
#         # throw away all but the first two columns
#         df = df.ix[:,:1]
#         # change the column names so they won't collide during concatenation
#         df.columns = [filename + str(cname) for cname in df.columns]
#         dfs.append(df)
#
#     # concatenate them horizontally
#     merged = pd.concat(dfs,axis=1)
#     # write it out
#     merged.to_csv("merged.csv", header=None, index=None)
