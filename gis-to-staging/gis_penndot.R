require(httr)
require(jsonlite)
require(sf)
require(dplyr)
require(lubridate)
require(DBI)

# dotenv::load_dot_env("./.env")

# Load Warehouse Credentials
wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

# Connect to Warehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, 
                    database = wh_db, UID = wh_user, pwd = wh_pass)

# Pull Table Variables
service <- Sys.getenv("SERVICE")
table <- Sys.getenv("TABLE")
dept <- Sys.getenv("DEPT")
update_col <- Sys.getenv("UPDATE_COL")
int_col <- Sys.getenv("INT_COL")
int_col <- unlist(strsplit(int_col, ","))
where_state <- Sys.getenv("WHERE_STATE")
pretext <- Sys.getenv("PRETEXT","query?timeRelation=esriTimeRelationOverlaps&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&units=esriSRUnit_Foot&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&returnIdsOnly=false&returnCountOnly=false&returnZ=false&returnM=false&returnDistinctValues=false&returnExtentOnly=false&sqlFormat=none&featureEncoding=esriDefault&f=geojson&where=")

table_name <- paste0("GIS.", dept, "_GISOnline_", table)

offset_orig <- as.numeric(Sys.getenv("OFFSET", 1000))
offset <- offset_orig


if (where_state != ""){
  if (!dbExistsTable(wh_con, SQL(table_name))) {
    where <- where_state
  } else if (update_col != "") {
    query <- paste0("SELECT MAX(", update_col, ") max FROM ", table_name)
    max <- as.Date(dbGetQuery(wh_con, query)$max)
    where <- URLencode(paste0(update_col, " > '", max, "'"))
    where <- paste0(where_state,"&",where)
  } else {
    where <- where_state
  }
} else {
  if (!dbExistsTable(wh_con, SQL(table_name))) {
    where <- "1%3D1"
  } else if (update_col != "") {
    query <- paste0("SELECT MAX(", update_col, ") max FROM ", table_name)
    max <- as.Date(dbGetQuery(wh_con, query)$max)
    where <- URLencode(paste0(update_col, " > '", max, "'"))
  } else {
    where <- "1%3D1"
  }
}

# Get First Page
url_2 <- paste0(service, pretext, where,"&resultRecordCount=",offset_orig)
temp <- read_sf(RETRY("GET", url_2)) %>%
  mutate(across(contains("date") & where(is.numeric) | contains("date") & where(~sum(!is.na(.)) < 1), 
                ~as.POSIXct(as.numeric(.) / 1000, origin = "1970-01-01")))

# Load more Pages
while ((nrow(temp) %% offset_orig) == 0) {
  print(paste(offset, "through next offset"))
  url_2 <- paste0(service, pretext, where, "&resultOffset=", offset, "&resultRecordCount=",offset_orig)
  
  temp2 <- read_sf(RETRY("GET", url_2)) %>%
    mutate(across(contains("date") & where(is.numeric) | contains("date") & where(~sum(!is.na(.)) < 1), 
                  ~as.POSIXct(as.numeric(.) / 1000, origin = "1970-01-01")))
  
  # Mutate Int
  if (length(int_col)>0) {
    temp2 <- temp2 %>% 
      mutate_at(vars(int_col), function(x) { as.integer(x)})
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
load_table <- paste0("Staging.", dept, "_PennDOT_", table)
dbWriteTable(wh_con, SQL(load_table), final, overwrite = TRUE, field.type = list("geometry_wkt" = "varchar(max)"))

dbDisconnect(wh_con)