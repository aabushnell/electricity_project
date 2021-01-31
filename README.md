# electricity_project

This is the code I wrote for a data project at UC Davis analyzing bid data from the Australian Energy Market. I set up and maintained a MySQL server on my computer and used the AEMO_ElectricityMarket_write file to clean and upload data from the raw data files to the MySQL server. Then I used the AEMO extraction file to read month by month bid data from the server (multiple bids could be placed for a time period but only the last bid would be binding so I had to filter only those binding bids).
