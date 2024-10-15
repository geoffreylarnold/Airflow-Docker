require(DBI)
require(dplyr)
require(RPostgres)
require(odbc)

# dotenv::load_dot_env()
options(odbc.batch_rows = 1024)

# Warehouse variables
schema <- Sys.getenv('SCHEMA', 'dbo')

dept <- Sys.getenv("DEPT")
tables <- Sys.getenv('TABLES') # List/Dict

dev <- Sys.getenv('DEV')
wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')
wh_schema <- Sys.getenv('WH_SCHEMA', "Staging")

append_col <- Sys.getenv('APPEND_COL')
append_type <- Sys.getenv('APPEND_TYPE', "MAX")
append_sign <- Sys.getenv('APPEND_SIGN', ">")

max_cols_load <- Sys.getenv("MAX_COLS")
max_cols <- unlist(strsplit(max_cols_load, ","))

# Postgres Variralbes
pg_host <- Sys.getenv('PG_HOST')
database <- Sys.getenv('PG_DB')
pg_user <- Sys.getenv('PG_USER')
pg_pass <- Sys.getenv('PG_PASS')
pg_port <- Sys.getenv('PG_PORT')
sql <- Sys.getenv('SQL')

pg_con <- RPostgres::dbConnect(
  Postgres(),
  host = pg_host,
  dbname = database,
  port = pg_port,
  user = pg_user,
  password = pg_pass
)

if (dev != ""){
  wh_con <- dbConnect(
    odbc::odbc(), 
    driver = "{ODBC Driver 17 for SQL Server}",
    server = wh_host, 
    database = wh_db, 
    Trusted_Connection = "YES")
}else{
  wh_con <- dbConnect(
    odbc::odbc(), 
    driver = "{ODBC Driver 17 for SQL Server}",
    server = wh_host, 
    database = wh_db, 
    UID = wh_user, 
    pwd = wh_pass)
}

tables <- unlist(strsplit(tables, ","))

for (tablet in tables) {
  # Import SQL
  if (sql != '') {
    query <- sql
  } else {
    query <- paste0("SELECT * FROM ", database, ".", schema, ".", tablet)
  }
  
  master_table <- paste0("Master.", paste(dept, database, tablet, sep = "_"))
  
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
  temp <- DBI::dbGetQuery(pg_con, query)
  print(paste(tablet, "read"))
  
  if(grepl("^_", tablet)){
    table_nx <- gsub("^_", "", tablet)
  } else {
    table_nx <- tablet
  }
  
  table_name <- DBI::Id(catalog = wh_db, schema = wh_schema, table = paste(dept, database, table_nx, sep = "_"))
  
  if (max_cols_load == "") { 
    dbWriteTable(wh_con, table_name, temp, overwrite = TRUE)
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
      dbWriteTable(wh_con, table_name, temp, field.type = types, overwrite = TRUE)
    } else {
      dbWriteTable(wh_con, table_name, temp, overwrite = TRUE)
    }
  }
  print(paste(tablet, "written"))
}

dbDisconnect(pg_con)
dbDisconnect(wh_con)



