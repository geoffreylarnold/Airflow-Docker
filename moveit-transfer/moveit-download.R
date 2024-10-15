require(moveitAPI)
require(dplyr)
require(DBI)
require(snakecase)
require(tibble)

# dotenv::load_dot_env()

dept <- Sys.getenv("DEPT")
source <- Sys.getenv('SOURCE')

user <- Sys.getenv('MI_USER')
pass <- Sys.getenv('MI_PASS')

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

max_cols <- unlist(strsplit(Sys.getenv("MAX_COLS"), ","))

filename <- Sys.getenv("FILENAME")
file_id <- Sys.getenv("FILE_ID")
snakecase <- Sys.getenv("SNAKECASE", unset = "FALSE")

file_type <- tolower(tools::file_ext(filename))

# Table Fix
pattern <- paste0(".", file_type)
# Table Name for Preload
table_def <- gsub(pattern, "", filename)
table <- Sys.getenv("TABLE", unset = table_def)

# Set URL
moveit_url <- Sys.getenv("BASE_URL", unset = "alleghenycounty.us")

# Auth MoveIt API
auth <- paste0("grant_type=password&username=", user, "&password=", pass)
tokens <- authMoveIt(moveit_url, auth)

# Look for file ID if name not provided
if (file_id == "" & filename != "") {
  # Look for File
  file <- availableFiles(moveit_url, tokens) %>%
    filter(name == filename)
  
  # Grab File ID
  if (nrow(file) == 1) {
    file_id <- file$id[1]
  } else if (nrow(file) > 1) {
    stop("More than 1 file name found, please pass file ID")
  } else {
    stop("No file with that name found, please pass a file name that exists on the Moveit server")
  }
} else if (file_id == "" & filename == "" ) {
  stop("You did not provide a filename or file id, please pass one of these to download a file")
}

file_type <- ifelse(file_type %in% c("xlsx", "xls"), "excel", file_type)

# Download File
temp <- readMoveItFile(moveit_url, tokens, file_id, file_type)

if (nrow(temp) > 0 ) {
  temp <- temp %>%
    mutate_if(is.character, iconv, from = "WINDOWS-1252", to = "UTF-8")
}

# Snake Case col conversion
if (snakecase == "TRUE") {
  colnames(temp) <- to_snake_case(colnames(temp))
}

# Connect to DataWarehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass)

# Write Table
table_name <- paste(dept, source, table, sep = "_")
# Create Max Cols Type List
if (any(max_cols %in% colnames(temp)) & nrow(temp) > 0) {
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
  dbWriteTable(wh_con, SQL(paste("Staging", table_name, sep =".")), temp, field.type = types, overwrite = TRUE)
} else if (any(max_cols == "auto")) {
  list <- lapply(temp, function(x) max(nchar(x), na.rm = T)) 
  max_cols <- temp[sapply(temp, function(x) max(nchar(x), na.rm = T))> 255]
  
  types <- data.frame(cols = colnames(max_cols)) %>%
    mutate(names = "varchar(max)") %>%
    deframe() %>%
    as.list()
  
  # Transfer Data to Warehouse
  dbWriteTable(wh_con, SQL(paste("Staging", table_name, sep =".")), temp, field.type = types, overwrite = TRUE)
} else {
  dbWriteTable(wh_con, SQL(paste("Staging", table_name, sep =".")), temp, overwrite = TRUE)
}

# Warehouse Disconnect
dbDisconnect(wh_con)
