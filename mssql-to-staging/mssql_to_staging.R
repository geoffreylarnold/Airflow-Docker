#!/usr/bin/env Rscript
require(snakecase)
require(DBI)
require(dplyr)
library(stringi)
library(tibble)

# dotenv::load_dot_env()

username <- Sys.getenv('USER')
password <- Sys.getenv('PASS')
host <- Sys.getenv('HOST')
database <- Sys.getenv('DATABASE')
sql <- Sys.getenv('SQL')

schema <- Sys.getenv('SCHEMA', 'dbo')
  
dept <- Sys.getenv("DEPT")
tables <- Sys.getenv('TABLES') # List/Dict

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')
sa_user <- Sys.getenv('SA_USER', wh_user)
sa_pass <- Sys.getenv('SA_PASS', wh_pass)
append_col <- Sys.getenv('APPEND_COL')
append_type <- Sys.getenv('APPEND_TYPE', "MAX")
append_sign <- Sys.getenv('APPEND_SIGN', ">")

max_cols_load <- Sys.getenv("MAX_COLS")
max_cols <- unlist(strsplit(max_cols_load, ","))
  
# DB Connection String
con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = host, DATABSE = database, UID = username, pwd = password)
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = sa_user, pwd = sa_pass)

tables <- unlist(strsplit(tables, ","))

for (table in tables) {
  # Import SQL
  if (sql != '') {
    query <- sql
  } else {
    query <- paste0("SELECT * FROM ", database, ".", schema, ".", table)
  }
  
  master_table <- paste0("Master.", paste(dept, database, table, sep = "_"))

  # Append Value and Query
  if (append_col != '' && dbExistsTable(wh_con, SQL(master_table))) { 
    value_sql <- paste0("SELECT ", append_type, "(", append_col, ") as value FROM ", master_table)
    value <- dbGetQuery(wh_con, value_sql)$value
    if (sql != '') { 
      query <- gsub("VALUE", value, query)
    } else {
      query <- paste0(query, " WHERE ", append_col, " ", append_sign, " '", value, "'")
    }
  }
  
  # Grab Data
  temp <- dbGetQuery(con, query)
  
  if (max_cols_load == "") { 
    dbWriteTable(wh_con, SQL(paste0("Staging.", paste(dept, database, table, sep = "_"))), temp, overwrite = TRUE)
  } else {
    if (any(max_cols %in% colnames(temp))) {
      # ID Max Cols for this table
      cols <- colnames(temp)[which(colnames(temp) %in% max_cols)]
      
      # Create Max Cols Type List
      types <- data.frame(cols = cols) %>%
        mutate(names = "varchar(max)") %>%
        deframe() %>%
        as.list()
      
      # Move Max columns to end of table
      temp <- select(temp, c(-all_of(cols), everything()))
      
      # Transfer Data to Warehouse
      dbWriteTable(wh_con, SQL(paste0("Staging.", paste(dept, database, table, sep = "_"))), temp, field.type = types, overwrite = TRUE)
    } else {
      dbWriteTable(wh_con, SQL(paste0("Staging.", paste(dept, database, table, sep = "_"))), temp, overwrite = TRUE)
    }
  }
}

dbDisconnect(con)
dbDisconnect(wh_con)