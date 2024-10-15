require(DBI)
require(dplyr)
require(readxl)
require(lubridate)
require(janitor)
library(tools)

#dotenv::load_dot_env()

source <- Sys.getenv("SOURCE")
dept <- Sys.getenv("DEPT")
table <- Sys.getenv('TABLE')
schema <- Sys.getenv('SCHEMA', 'Staging')
append <- Sys.getenv('APPEND', 'FALSE')
filepath <- Sys.getenv('FILEPATH')
workbook <- Sys.getenv('WORKBOOK', NA)
coltypes <- Sys.getenv('COLTYPES')
coltypes <- unlist(as.list(strsplit(coltypes, ",")))
sheet <- Sys.getenv('SHEET', 1)

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

# Read File
if(!is.na(workbook)){
  if(!grepl("\\.xlsx$", workbook)){
    stop(paste0("Workbook specified, but file extension not .xlsx: ", file_ext(workbook)))
  }
}else{
  stop("No workbook specified")
}

new_table <- read_excel(paste0(filepath,"/", workbook),
                        col_types = coltypes,
                        sheet = sheet) %>% 
  janitor::clean_names()

# Connection to Warehouses


wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db
                    # , trusted_connection="YES"
                    , UID = wh_user, pwd = wh_pass)

# Write to Staging
table_name <- paste(schema, paste(dept, source, table, sep = "_"), sep = ".")

if (append) {
  dbWriteTable(wh_con, SQL(table_name), new_table, append = T)
} else {
  dbWriteTable(wh_con, SQL(table_name), new_table, overwrite = T)
}

