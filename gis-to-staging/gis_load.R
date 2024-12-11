require(httr)
require(jsonlite)
require(sf)
require(dplyr)
require(lubridate)
require(DBI)

# dotenv::load_dot_env("./gis-to-staging/.env")

# Load Warehouse Credentials
wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

# Connect to Warehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, 
                    UID = wh_user, pwd = wh_pass
                    # , Trusted_Connection="YES"
                    )

# Pull Table Variables
service <- Sys.getenv("SERVICE")
table <- Sys.getenv("TABLE")
dept <- Sys.getenv("DEPT")
update_col <- Sys.getenv("UPDATE_COL")
int_col_list <- Sys.getenv("INT_COL", NA)
if(!is.na(int_col_list)){
  int_col <- unlist(as.list(strsplit(int_col_list, ",")))
}

float_col_list <- Sys.getenv("FLOAT_COL", NA)
if(!is.na(float_col_list)){
  float_col <- unlist(as.list(strsplit(float_col_list, ",")))
}

table_name <- Id(schema="GIS", table=paste0(dept, "_GISOnline_", table))
table_name_text <- paste0("GIS.",dept,'_GISOnline_',table)

if (!dbExistsTable(wh_con, table_name)) {
  where <- "1%3D1"
} else if (update_col != "") {
  query <- paste0("SELECT MAX(", update_col, ") max FROM ", table_name_text)
  max <- as.Date(dbGetQuery(wh_con, query)$max)
  where <- URLencode(paste0(update_col, " > '", max, "'"))
} else {
  where <- "1%3D1"
}

login <- Sys.getenv("LOGIN")
pwd <- Sys.getenv("PASSWORD")

# Generate Esri Token
headers <- list("username" = login,
                "password" = pwd,
                "referer" = "https://www.arcgis.com",
                "f" = "json")
p <- POST("https://www.arcgis.com/sharing/generateToken", body = headers)
token <- content(p, "parsed")$token

offset_orig <- as.numeric(Sys.getenv("OFFSET", 1000))
offset <- offset_orig

# Get First Page
url_2 <- paste0(service, "query?where=", where, "&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&resultRecordCount=", offset, "&f=pgeojson&token=", token)

temp <- read_sf(RETRY("GET", url_2)) %>%
  mutate(across(contains("date") & where(is.numeric) | contains("date") & where(~sum(!is.na(.)) < 1), 
                ~as.POSIXct(as.numeric(.) / 1000, origin = "1970-01-01")))

if (!is.na(int_col_list)) {
  temp <- temp %>% 
    mutate(across(any_of(int_col), function(x) { as.integer(x)}))
}

if (!is.na(float_col_list)) {
  temp <- temp %>% 
    mutate(across(any_of(float_col), function(x) { as.integer(x)}))
}

# Load more Pages
while (nrow(temp) %% offset_orig == 0) {
  url_2 <- paste0(service, "query?where=", where, "&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&resultOffset=", offset, "&resultRecordCount=2000&f=pgeojson&token=", token)
  
  temp2 <- read_sf(RETRY("GET", url_2)) %>%
    mutate(across(contains("date") & where(is.numeric) | contains("date") & where(~sum(!is.na(.)) < 1), 
                  ~as.POSIXct(as.numeric(.) / 1000, origin = "1970-01-01")))
  
  # Mutate Int
  if (!is.na(int_col_list)) {
    temp2 <- temp2 %>% 
      mutate(across(any_of(int_col), function(x) { as.integer(x)}))
  }
  
  if (!is.na(float_col_list)) {
    temp2 <- temp2 %>% 
      mutate(across(any_of(float_col), function(x) { as.integer(x)}))
  }
  
  temp <- temp2 %>%
    bind_rows(temp)
  
  offset <- offset + offset_orig
}

# Make SQL Server friendly table
final <- temp %>%
  mutate(geometry = st_as_text(geometry))%>%
  mutate(geometry_wkt = format(geometry)) %>%
  st_drop_geometry()

# Load to Warehouse
load_table <- paste0("Staging.", dept, "_GISOnline_", table)
# Waiting on DIT to fix permissions in database
dbWriteTable(wh_con, SQL(load_table), final, overwrite = TRUE, field.type = list("geometry_wkt" = "varchar(max)"))

# Make Geometry Column GIS Friendly
dbExecute(wh_con, paste0("ALTER TABLE ", load_table, "
            ADD geometry AS geography::STGeomFromText(geometry_wkt, 4326).MakeValid()"))

dbDisconnect(wh_con)