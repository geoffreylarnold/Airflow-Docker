require(DBI)
require(tidyverse)

source("./alco_geocoder.R")

# Load Credentials
# dotenv::load_dot_env(".env")
# source('https://raw.githubusercontent.com/Allegheny-CountyStats/ALCO-Geocoder/master/alco_geocoder.R')

# Load Env Variables
source <- Sys.getenv("SOURCE")
dept <- Sys.getenv("DEPT")
table <- Sys.getenv('TABLE')

full_table <- paste0("Master.", dept, "_", source, "_", table)

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

id_col <- Sys.getenv("ID_COL")
full_address <- Sys.getenv("FULL_ADDRESS")
where <- Sys.getenv("WHERE", unset = "TRUE")
endpoint <- Sys.getenv("ENDPOINT", unset = "verification")

# Connection to Warehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

query <- paste("SELECT", paste(id_col, full_address, sep = ", "), "AS FULL_ADDRESS FROM", full_table)

# New Geocoded Table
geo_table <- paste0("Master.", dept, "_", source, "_", table, "_G")

geo_exists <- dbExistsTable(wh_con, SQL(geo_table))

# Add Where statement
if (where == 'TRUE' & geo_exists) {
  query <- paste(query, "WHERE", id_col, "NOT IN (SELECT", id_col, "FROM", geo_table, ")")
  append <- TRUE
} else {
  append <- FALSE
}

clean_id <- gsub("\\[|]", "", id_col)

# Grab Rows to geocode
df <- dbGetQuery(wh_con, query) %>%
  distinct_at(c(clean_id, "FULL_ADDRESS"), .keep_all = T)

# If new rows geocode
if (nrow(df) > 0) {
  geo <- df %>%
    mutate_countyGeo(FULL_ADDRESS, endpoint = endpoint)
  
  if (append) {
    # Append Table to Warehouse
    dbWriteTable(wh_con, SQL(geo_table), geo, append = TRUE)
  } else {
    # Action for First Table
    dbWriteTable(wh_con, SQL(geo_table), geo, overwrite = TRUE)
  }
}

# Disconnect DB
dbDisconnect(wh_con)