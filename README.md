# Mouse Gesture Project: Data handling and cleaning

This is a small project created to support our project
[Computer mouse replication with gestures](https://github.com/hidai25/mouserepwithgestures). It takes in the separated all our recorded data on the bases f accelerometer, gyroscope and magnitude does the following:
1. Merges the each recorded movement's data from the accelerometer, gyroscope and magnitude axis to 1 file
2. Concatenates this data for all similar movements
3. Repeats this process until all the separate movements are concatenated and merged, to eventually concatenate all movements together
4. Than cleans the dataset
5. Finally splits the dataset into test and train data

## Installation

Use [pipenv](https://pipenv-fork.readthedocs.io/en/latest/basics.html) to create the venv and install the dependencies with:
```python
pipenv install
```

Or alternatively you can the package manager [pip](https://pip.pypa.io/en/stable/) to create the venv and install the dependencies with
```bash
pip install -r requirements.txt
```

## Usage
The project uses the input data files from the project itself. The naming of these directories and filenames is crucial for the project to function, since it matches the files based on some predetermined names. See "Custom Input Data", for more info on custom input

Call this to merge a regular balanced dataset. A regular dataset is when there is recorded data available from 2 persons:
```python
python merge.py
```

Use the -su, single user, flag when you also want to include data of 1 person, leading to an unbalanced dataset:
```python
python merge.py -su
```

## Custom input data
If you want to run the project for your own files, you'll need to generate similar files yourself and add them with a similar file and directory naming convention, similar column names and you need to add the names of these movements to the gestures array(s) right above the main function call:
```python
# Recorded movements for 1 person
gesture_single = ['wave', 'spiral']

# Recorded movements for 2 person recordings
gestures = ['down', 'left', 'right', 'up']

# Call the main function to start merging, contactunationg and cleaning the data
main()
```



## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
