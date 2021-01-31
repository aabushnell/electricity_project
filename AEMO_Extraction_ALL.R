# Sets working directory to output files to
setwd("/Users/aaron/Documents/Australia_Project/Raw_Data/2017-2019")

# Loops through all files uploaded from working directory
for (i in MonthsZip) {
  
  print(i)
  
  # Generates matching db name for MySQL query
  dbName <- paste("biddayoffer", substr(i,7,13), sep="_")
  # Generats MySQL query with proper db name
  query <- paste("SELECT * FROM", dbName,"WHERE BIDTYPE LIKE 'ENERGY'", sep = " ")
  # Writes result of query to R
  CSVtemp <- dbGetQuery(AEMO.NEM, query)
  
  print("Table Read")
  
  # To change csv file names change text inside quotes below. The substr part adds proper date
  fileName <- paste("biddayoffer", substr(i,7,13), "vmax.csv", sep = "_")
  # Saves table to filee 
  write.csv(CSVtemp, fileName)
  
  print("Table Saved to File")
}

# Same thing but for bidperoffer this time
for (i in MonthsZip) {
  
  print(i)
  
  # Generates matching db name for MySQL query
  dbName <- paste("bidperoffer", substr(i,7,13), sep="_")
  # Generats MySQL query with proper db name
  query <- paste("SELECT * FROM", dbName, "WHERE BIDTYPE LIKE 'ENERGY'", sep = " ")
  # Writes result of query to R
  CSVtemp <- dbGetQuery(AEMO.NEM, query)
  
  print("Table Read")
  
  # To change csv file names change text inside quotes below. The substr part adds proper date
  fileName <- paste("bidperoffer", substr(i,7,13), "vmax.csv", sep = "_")
  # Saves table to filee 
  write.csv(CSVtemp, fileName)
  
  print("Table Written to File")
}
