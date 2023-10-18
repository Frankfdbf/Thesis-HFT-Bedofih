# Thesis-HFT-Bedofih
Code to handle and analyse a high frequency database for my thesis.
Because .parquet file are not easily readable. I suggest having a jupiter notebook on the side of the project to go through the data if you need to.

## Setup
- Make sure you have all the packages downloaded to run the project.
- Navigate to .constants/constants.py.
- There, change the path associated with root. It should be the main directory where all sub directories of data will be created. Make that, in this root directory lies a directory called 'raw' with all bedofih data unzipped (this project will not do it for you).
- Run the files starting with a number, one after the other, to start the analysis. 
    - 01: Create folder stucture, only take necessary isins, preprocess all the files, save them as .parquet (much lighter and faster, readable with pandas). Careful this step will last for long time and require a lot of computing memory.
    - 02: Get removed orders. Info on orders that were removed from the book, the reason (trade, cancellation) and the time and date.
    - 03: Get auction times (open and close), as well as the fixing prices (from the trade files). These prices are compared, during testing, to prices obtained when recreating the limit order books (LOBs).
    - 04: IN PROGRESS, LOB. Running the file will print the fixing price and the table for its determination process (for debugging).


## Information on project advancement:

Statistical_summary.py does not work yet.

Constants are nearly finalised. For now, the folder is only missing the list of stocks for each market segment (small, mid, large).

Preprocessing should be finished.

