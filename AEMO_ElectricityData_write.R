
## Introduction
## -----------------------
## This program synthesizes all available AEMO electricity files.  It converts csv files from AEMO into MySQL.

## There are two modes:
##  (1) new --> creates a new AEMO electricity database in MySQL
##  (2) update --> addes new data to the existing AEMO electricity database in MySQL


### START CODE ###


## (1) Extract Market Data
## ---------------------------------

## Create list of zipfiles located in the working directory

setwd("/Users/aaron/Documents/Australia_Project/Raw_Data/2017-2019")

MonthsZip <- list.files(pattern="\\.zip$")

## FORLOOP 1 -- over year-months
for (i in MonthsZip) {
  print(i)
  
  ## Create dataframe listing of all files in current zip
  FILELIST <- unzip(i, list=TRUE)

  
  ## ---------------  
  ## BIDDAYOFFER -- Day-ahead price bids
  ## Create location and name strings for nested zip
  FileZip <- grep("PUBLIC_DVD_BIDDAYOFFER_[0-9]{12}\\.zip", FILELIST$Name, value = TRUE)
  FileCSV <- str_extract(FileZip, "PUBLIC_DVD_BIDDAYOFFER_[0-9]{12}")
  print(paste(FileCSV, ".csv", sep=""))
  
  ## Unzip year-month file (first level)
  unzip(i, files=FileZip)
  
  ## Unzip data file (second level)
  unzip(FileZip)
  
  print("Unzipped Data")
  
  ## Import and clean data
  BIDDAYOFFER <- read.csv(paste(FileCSV, ".csv", sep=""), skip = 1, header = TRUE, stringsAsFactors = FALSE,
                          colClasses = c("NULL","NULL","NULL","NULL",NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,
                                         NA,NA,NA,NA,NA,NA,"NULL","NULL","NULL","NULL","NULL","NULL","NULL",NA))
  
  print("Read Data")
  
  
  ## Export data to MySQL
  dbName <- paste("biddayoffer", substr(i,7,13), sep="_")
  
  dbWriteTable(AEMO.NEM, name=dbName, value=BIDDAYOFFER, row.names=FALSE, append=FALSE)
  
  print("Data Written to DB")
  
  ## Delete data file
  file.remove(paste(FileCSV, ".csv", sep=""))
  
  
  ## ---------------
  ## BIDPEROFFER -- Day-of quantity bids
  ## Create location and name strings for nested zip
  FileZip <- grep("PUBLIC_DVD_BIDPEROFFER_[0-9]{12}\\.zip", FILELIST$Name, value = TRUE)
  FileCSV <- str_extract(FileZip, "PUBLIC_DVD_BIDPEROFFER_[0-9]{12}")
  print(paste(FileCSV, ".csv", sep=""))
  
  ## Unzip year-month file (first level)
  unzip(i, files=FileZip)
  
  ## Unzip data file (second level)
  unzip(FileZip)
  
  print("Unzipped Data")
  
  ## Import and clean data
  BIDPEROFFER <- read.csv(paste(FileCSV, ".csv", sep=""), skip = 1, header = TRUE, stringsAsFactors = FALSE,
                          colClasses = c("NULL","NULL","NULL","NULL",NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,"NULL",
                                         "NULL","NULL","NULL",NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,"NULL","NULL","NULL"))
  
  print("Read Data")
  
  
  ## Export data to MySQL
  dbName <- paste("bidperoffer", substr(i,7,13), sep="_")
  
  dbWriteTable(AEMO.NEM, name=dbName, value=BIDPEROFFER, row.names=FALSE, append=FALSE)
  
  print("Data Written to DB")
  
  ## Delete data file
  file.remove(paste(FileCSV, ".csv", sep=""))


  ## Delete year-month directory
  Month <- str_extract(i, "^MMSDM_[0-9]{4}_[0-9]{2}")
  unlink(Month, recursive=TRUE)
}


## (2) Extract Participant Data???
## ---------------------------------

## (2a) Create file listing and unzip latest monthly dataset
## Create dataframe listing of all files most current zip
FILELIST <- unzip(max(MonthsZip), list=TRUE)

## (2b) Extract DUDETAILSUMMARY
## Create location and name strings for nested zip
FileZip <- grep("PUBLIC_DVD_DUDETAILSUMMARY_[0-9]{12}\\.zip", FILELIST$Name, value = TRUE)
FileCSV <- str_extract(FileZip, "PUBLIC_DVD_DUDETAILSUMMARY_[0-9]{12}")

## Unzip year-month file (first level)
unzip(max(MonthsZip), files=FileZip)

## Unzip data file (second level)
unzip(FileZip)

## Import and clean data
DUDETAILSUMMARY <- read.csv(paste(FileCSV, ".csv", sep=""), skip = 1, header = TRUE, stringsAsFactors = FALSE)
DUDETAILSUMMARY <- DUDETAILSUMMARY %>%
  filter(I=="D") %>%
  select(DUID, START_DATE, END_DATE, REGIONID, CONNECTIONPOINTID, STATIONID, PARTICIPANTID, DISPATCHTYPE, 
    SCHEDULE_TYPE, STARTTYPE)
colnames(DUDETAILSUMMARY) <- c("DUID", "STARTDATE", "ENDDATE", "REGIONID", "CONNECTIONPOINTID", "STATIONID", 
  "PARTICIPANTID", "DISPATCHTYPE", "SCHEDULETYPE", "STARTTYPE")
DUDETAILSUMMARY$STARTDATE <- as.POSIXct(DUDETAILSUMMARY$STARTDATE, tz = "Etc/GMT-10", "%Y/%m/%d %H:%M:%S")
DUDETAILSUMMARY$ENDDATE <- as.POSIXct(DUDETAILSUMMARY$ENDDATE, tz = "Etc/GMT-10", "%Y/%m/%d %H:%M:%S")
DUDETAILSUMMARY <- 
  DUDETAILSUMMARY %>%
  group_by(DUID, REGIONID, CONNECTIONPOINTID, STATIONID, PARTICIPANTID, DISPATCHTYPE, SCHEDULETYPE, STARTTYPE) %>%
  mutate(STARTDATEMIN = min(STARTDATE), ENDDATEMAX = max(ENDDATE)) %>%
  select(DUID, STARTDATEMIN, ENDDATEMAX, REGIONID, CONNECTIONPOINTID, STATIONID, PARTICIPANTID, DISPATCHTYPE, 
    SCHEDULETYPE, STARTTYPE)
colnames(DUDETAILSUMMARY)[2:3] <- c("STARTDATE", "ENDDATE")
DUDETAILSUMMARY <- unique(DUDETAILSUMMARY)
DUDETAILSUMMARY <- arrange(DUDETAILSUMMARY, DUID, STARTDATE)

## Delete data file
file.remove(paste(FileCSV, ".csv", sep=""))

## (2c) Extract DUDETAIL
## Create location and name strings for nested zip
FileZip <- grep("PUBLIC_DVD_DUDETAIL_[0-9]{12}\\.zip", FILELIST$Name, value = TRUE)
FileCSV <- str_extract(FileZip, "PUBLIC_DVD_DUDETAIL_[0-9]{12}")

## Unzip year-month file (first level)
unzip(max(MonthsZip), files=FileZip)

## Unzip data file (second level)
unzip(FileZip)

## Import and clean data
DUDETAIL <- read.csv(paste(FileCSV, ".csv", sep=""), skip = 1, header = TRUE, stringsAsFactors = FALSE)
DUDETAIL <- DUDETAIL %>%
  filter(I=="D") %>%
  select(DUID, EFFECTIVEDATE, REGISTEREDCAPACITY, MAXCAPACITY, SPINNINGRESERVEFLAG, INTERMITTENTFLAG,
    SEMISCHEDULE_FLAG)
colnames(DUDETAIL) <- c("DUID", "EFFECTIVEDATE", "REGCAP", "MAXCAP", "SPINRESERVE", "INTERMITTENT",
  "SEMISCHEDULE")
DUDETAIL$EFFECTIVEDATE <- as.POSIXct(DUDETAIL$EFFECTIVEDATE, tz = "Etc/GMT-10", "%Y/%m/%d %H:%M:%S")
DUDETAIL <- 
  DUDETAIL %>%
  group_by(DUID, REGCAP, MAXCAP, SPINRESERVE, INTERMITTENT, SEMISCHEDULE) %>%
  mutate(EFFECTIVEDATEMIN = min(EFFECTIVEDATE)) %>%
  select(DUID, EFFECTIVEDATEMIN, REGCAP, MAXCAP, SPINRESERVE, INTERMITTENT, SEMISCHEDULE)
colnames(DUDETAIL)[2] <- c("EFFECTIVEDATE")
DUDETAIL <- unique(DUDETAIL)
DUDETAIL <- arrange(DUDETAIL, DUID, EFFECTIVEDATE)

## Delete data file
file.remove(paste(FileCSV, ".csv", sep=""))

## (2d) Extract PARTICIPANT
## Create location and name strings for nested zip
FileZip <- grep("PUBLIC_DVD_PARTICIPANT_[0-9]{12}\\.zip", FILELIST$Name, value = TRUE)
FileCSV <- str_extract(FileZip, "PUBLIC_DVD_PARTICIPANT_[0-9]{12}")

## Unzip year-month file (first level)
unzip(max(MonthsZip), files=FileZip)

## Unzip data file (second level)
unzip(FileZip)

## Import and clean data
PARTICIPANT <- read.csv(paste(FileCSV, ".csv", sep=""), skip = 1, header = TRUE, stringsAsFactors = FALSE)
PARTICIPANT <- PARTICIPANT %>%
  filter(I=="D") %>%
  select(PARTICIPANTID, NAME, PARTICIPANTCLASSID, PRIMARYBUSINESS)
colnames(PARTICIPANT)[2] <- c("PARTICIPANT")
PARTICIPANT <- unique(PARTICIPANT)
PARTICIPANT <- arrange(PARTICIPANT, PARTICIPANTID)

## Delete data file
file.remove(paste(FileCSV, ".csv", sep=""))

## (2e) Merge DUDETAILSUMMARY, DUDETAIL & PARTICIPANT
## Merge DUDETAILSUMMARY & DUDETAIL
DUDETAILtemp <- merge(DUDETAILSUMMARY, DUDETAIL, by=c("DUID"))
## Keep only those observations where the DUDETAIL$EFFECTIVEDATE is less than the DUDETAILSUMMARY$ENDDATE
DUDETAILtemp <- DUDETAILtemp %>%
  filter(EFFECTIVEDATE<ENDDATE)
## Keep only those ovservations which are the most recent DUDETAIL$EFFECTIVEDATE within 
## DUDETAILSUMMARY$STARTDATE and DUDETAILSUMMARY$ENDDATE
DUDETAILtemp <- DUDETAILtemp %>%
  group_by(DUID, STARTDATE, ENDDATE) %>%
  mutate(EFFECTIVEDATEMAX = max(EFFECTIVEDATE)) %>%
  filter(EFFECTIVEDATE==EFFECTIVEDATEMAX) %>%
  select(-EFFECTIVEDATEMAX)

## Merge DUDETAILtemp & PARTICIPANT
DUDETAILtemp <- merge(DUDETAILtemp, PARTICIPANT, by=c("PARTICIPANTID"), all.x=TRUE)

## Rename resultant dataframe and remove unecessary ones
DUDETAIL <- DUDETAILtemp
rm(DUDETAILSUMMARY, DUDETAILtemp, PARTICIPANT)

## Sort and order resultant dataframe
DUDETAIL <- DUDETAIL %>%
  select(DUID, STARTDATE, ENDDATE, REGIONID, CONNECTIONPOINTID, STATIONID, PARTICIPANTID, PARTICIPANT,
    PARTICIPANTCLASSID, PRIMARYBUSINESS, DISPATCHTYPE, SCHEDULETYPE, STARTTYPE, EFFECTIVEDATE, REGCAP,
    MAXCAP, SPINRESERVE, INTERMITTENT, SEMISCHEDULE) %>%
  arrange(DUID, STARTDATE)

## (2f) Export to MySQL and delete unzipped directory
## Export data to MySQL
dbWriteTable(AEMO.NEM, name="dudetail", value=DUDETAIL, row.names=FALSE, overwrite=TRUE)

## Delete year-month directory
Month <- str_extract(max(MonthsZip), "^MMSDM_[0-9]{4}_[0-9]{2}")
unlink(Month, recursive=TRUE)
  

### END CODE ###