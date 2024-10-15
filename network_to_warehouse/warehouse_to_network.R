require(DBI)
require(dplyr)
require(readxl)
require(lubridate)
require(janitor)
library(tools)
library(openxlsx)
library(data.table)

#dotenv::load_dot_env("./network_to_warehouse/.env")


wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')


source <- Sys.getenv("SOURCE")
schema <- Sys.getenv('SCHEMA', 'Master')
table <- Sys.getenv("TABLE")
filepath <- Sys.getenv('FILEPATH')
filename <- Sys.getenv('FILENAME', NA)
fileext <- Sys.getenv("FILEEXT", "csv")
sheet_name <- Sys.getenv("SHEETNAME", "Sheet 1")
overwrite <- Sys.getenv("OVERWRITE", "TRUE")
if (overwrite != "TRUE"){
  overwrite = FALSE
}else{
  overwrite = TRUE
}

if (!fileext %in% c("excel", "csv")){
  stop("Specified file extension not valid: must be either excel or csv (default)")
}

# Read warehouse table
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db
                    #, trusted_connection="YES"
                    , UID = wh_user, pwd = wh_pass)

table_name <- paste(schema, table, sep = ".")
warehouse_export <- dbReadTable(wh_con, SQL(table_name))
if (is.na(filename)){
  filename <- table_name
}

if (fileext == "excel"){
  wb_final <- buildWorkbook(warehouse_export)
  saveWorkbook(wb_final, paste0(filepath,"/",filename,".xlsx"), overwrite = overwrite)
}else{
  fwrite(warehouse_export, file = paste0(filepath,"/",filename,".csv"), dateTimeAs = "write.csv")
} 
