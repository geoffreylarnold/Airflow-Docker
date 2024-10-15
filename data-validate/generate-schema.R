require(DBI)
require(dplyr)
require(jsonlite)

dotenv::load_dot_env()

database <- Sys.getenv('DATABASE')
database <- gsub("\\.COUNTY\\.ALLEGHENY\\.LOCAL", "", database)
source <- Sys.getenv("SOURCE", unset = database)

dept <- Sys.getenv("DEPT")
schema <- Sys.getenv("SCHEMA", "Staging")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')
na_false <- Sys.getenv('NA_FALSE')

na_list <- unlist(strsplit(na_false, ","))

table <- Sys.getenv("TABLE")

sql <- paste0("SELECT COLUMN_NAME, DATA_TYPE
FROM ", wh_db, ".INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '", paste(dept, source, table, sep = "_"), "' AND TABLE_SCHEMA = '", schema, "'")

wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass, Trusted_Connection= "yes")

preload <- dbGetQuery(wh_con, sql) %>%
  mutate(NAs = case_when(COLUMN_NAME %in% na_list ~ FALSE,
                    TRUE ~ NA))
dbDisconnect(wh_con)

schema_name <- paste(table, "schema", sep = "_")
filename <- paste0("./schemas/", schema_name, ".json")

# Create Folder if its not there
if (!dir.exists("./schemas")) {
  dir.create("./schemas")
}

# Generate Schema
write_json(preload, filename, unbox = TRUE)

print(schema_name)

# Verify the file
file.edit(filename)

