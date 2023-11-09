# Thesis-HFT-Bedofih

DISCLAIMER - This is an ongoing project. 

Context: As a 2nd year Master student, I analyse a high frequency database (Eurofidai-Bedofih) to conduct my master thesis. The orderbooks are not provided, the main objective of this code is to retrieve them.

## Methodology

The Bedofih dataset comes with, among other files, trade, order updates, event, and history files. However, the dataset does not provide the limit order books. Consequently, we developed an algorithm to generate them. These are indispensable for calculating liquidity measures, assessing the presence of High-Frequency Traders (HFTs) in the limit order book, and deriving additional statistical metrics that facilitate the analysis of HFT behavior and its impact on the market. All the code is written in Python.

Once the dataset is obtained from Eurofidai, we create a more efficient folder structure to navigate through. We then copy and organise all the files in a new manner, simultaneously eliminating data related to the ISINs we do not intend to analyse. During this automated process, each file we copy is in CSV format. We preprocess and save them in Apache Parquet format, enabling fast read and write access as well as significantly reducing file size compared to CSV. We establish several main repositories (orders, trades, histories, removed orders, etc.). Each of these repositories has folders corresponding to the ISINs, and each of those folders contains a file for each day.

To keep track of when orders are removed (either filled or cancelled), we introduce a new file type that we refer to as 'removed_orders.' We find this approach efficient because there are no rows indicating when an order departs from the order book or when it is partially filled. Nevertheless, we do know the date and time of removal for each order and we use this information to create our new file type.

The objective is also to regenerate the trades for comparison with the trades observed by the data provider. We simulate auctions and compare the fixing prices and traded orders to the actual figures. This verification process ensures the correctness of the order books and the proper functioning of the algorithm. Because they are randomly decided by the exchange, we extract fixing date-time objects from the trade files. The opening fixing occurs at a random time between 9:00:00 am and 9:00:30 am. For the closing auction, the fixing occurs at a random time between 5:30:00 pm and 5:30:30 pm.

The program should have three main functions: add, modify, and remove an order. Additionally, the program should perform tasks such as adding or removing a limit level and returning the price and quantity of several limit levels given a depth. It should also handle different order types (limit, market, market-to-limit, stop-limit, stop-market, and pegged orders). For market and market-to-limit orders, we need to keep track of the best bid and ask limit levels. Each time a new message is processed, we should check for order cancellations between the last and new message, determine if trades should be executed, and trigger stop orders if needed.

## Setup
- Make sure you have all the packages downloaded to run the project.
- Navigate to .src/constants/constants.py.
- There, change the path associated with root. It should be the main directory where all sub directories of data will be created. Make that, in this root directory lies a directory called 'raw' with all bedofih data unzipped (this project will not do it for you).
- Run the files starting with a number, one after the other, to start the analysis. 
    - 01: Create folder stucture, only take necessary isins, preprocess all the files, save them as .parquet (much lighter and faster, readable with pandas). Careful this step will last for long time and require a lot of computing memory.
    - 02: Get removed orders. Info on orders that were removed from the book, the reason (trade, cancellation) and the time and date.
    - 03: Get auction times (open and close), as well as the fixing prices (from the trade files). These prices are compared, during testing, to prices obtained when recreating the limit order books (LOBs).
    - 04: [IN PROGRESS], LOB.





