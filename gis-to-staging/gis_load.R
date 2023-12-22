require(httr)
require(jsonlite)
require(sf)
require(dplyr)
require(lubridate)
require(DBI)

# dotenv::load_dot_env()

# Load Warehouse Credentials
wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

# Connect to Warehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Pull Table Variables
service <- Sys.getenv("SERVICE")
table <- Sys.getenv("TABLE")
dept <- Sys.getenv("DEPT")
update_col <- Sys.getenv("UPDATE_COL")

table_name <- paste0("GIS.", dept, "_GISOnline_", table)

if (!dbExistsTable(wh_con, SQL(table_name))) {
  where <- "1%3D1"
} else if (update_col != "") {
  query <- paste0("SELECT MAX(", update_col, ") max FROM ", table_name)
  max <- as.Date(dbGetQuery(wh_con, query)$max)
  where <- URLencode(paste0(update_col, " > '", max, "'"))
} else {
  where <- "1%3D1"
}

# Generate Esri Token
headers <- list("username" = Sys.getenv("LOGIN"),
                "password" = Sys.getenv("PASSWORD"),
                "referer" = "https://www.arcgis.com",
                "f" = "json")
p <- POST("https://www.arcgis.com/sharing/generateToken", body = headers)
token <- content(p, "parsed")$token

offset_orig <- as.numeric(Sys.getenv("OFFSET", 1000))
offset <- offset_orig

# Get First Page
url_2 <- paste0(service, "query?where=", where, "&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&resultRecordCount=", offset, "&f=pgeojson&token=", token)

temp <- read_sf(GET(url_2)) %>%
  mutate_at(vars(contains("date")), function(x) {as.POSIXct(as.numeric(x) / 1000, origin = "1970-01-01")})

# Load more Pages
while (nrow(temp) %% offset_orig == 0) {
  url_2 <- paste0(service, "query?where=", where, "&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&resultOffset=", offset, "&resultRecordCount=2000&f=pgeojson&token=", token)
  
  temp <- read_sf(GET(url_2)) %>%
    mutate_at(vars(contains("date")), function(x) {as.POSIXct(as.numeric(x) / 1000, origin = "1970-01-01")}) %>%
    bind_rows(temp)
  
  offset <- offset + offset_orig
}

# Make SQL Server friendly table
final <- temp %>%
  mutate(geometry_wkt = st_as_text(geometry)) %>%
  st_drop_geometry()

# Load to Warehouse
load_table <- paste0("Staging.", dept, "_GISOnline_", table)
dbWriteTable(wh_con, SQL(load_table), final, overwrite = TRUE, field.type = list("geometry_wkt" = "varchar(max)"))

dbDisconnect(wh_con)