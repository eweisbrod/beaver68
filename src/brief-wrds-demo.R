#load packages from FARR book Chapter 6
library(dplyr)
library(ggplot2)
library(DBI)
library(farr)

#load helper scripts
#similar to "include" statement in SAS.
source("src/-Global-Parameters.R")
source("src/utils.R")


#set up connection to WRDS
#This will work if you have set the environment variables following the
#example in the FARR book. You do not need to set the password because 
#the below code will prompt for it.
#You may wish to see the alternate setup in my example project.
db <- dbConnect(RPostgres::Postgres(), 
                password = rstudioapi::askForPassword(),
                bigint = "integer")

#set up connection to Compustat company table on the WRDS server
company <- tbl(db, Id(schema = "comp", table = "company"))

#preview the data
company