# lma-data-pipeline

General to dos:
- error handling
- performance optimization
- coding style
- logging
- tests
- sanity checks

# Testing data

Testing data is a file that has the same structure as the data dump we receive from the LMA and is based on the real errors found in the datasets.
Although the data resembles real data and may contain real company names, the fields are randomised, therefore no actual information is revealed.
Testing dataset should contain all known errors and deficiencies of data, therefore every time a new inconsistency has been found,
that hasn't been handled earlier, it should be added as a new line to the testing dataset.

# Input / output

Each module takes the full dataframe, creates new columns with the necessary enhancements, adds them to the end of the original dataframe and returns it.
At the end of each module, a check needs to be made if the length of the input dataframe is the same as the length of the output dataframe.
This rule doesn't apply to the filtering module which reduces the number of rows.
