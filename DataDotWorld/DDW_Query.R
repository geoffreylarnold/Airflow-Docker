require(tidyverse, quietly = T)
require(jsonlite)
require(httr)
require(DBI)

dev <- Sys.getenv('DEV', "no")# Change to yes when testing locally

if (dev == "yes") {
  dotenv::load_dot_env("./.env")
}

query_name <- Sys.getenv("QUERY_NAME")
query_id <- Sys.getenv("QUERY_ID", NA)
query_return <- Sys.getenv("QUERY_RETURN", "text/csv")
dept <- Sys.getenv("DEPT")
source <- Sys.getenv('SOURCE')
table <- Sys.getenv("TABLE")

wh_host <- Sys.getenv('WH_HOST')
wh_db <- Sys.getenv('WH_DB')
wh_user <- Sys.getenv('WH_USER')
wh_pass <- Sys.getenv('WH_PASS')

accept_null_return <- Sys.getenv("ACCEPT_NULL_RETURN", "yes") # Specifies if null returns from query should be accepted
ddw_org <- Sys.getenv("DDW_ORG", "alleghenycounty")
ddw_id <- Sys.getenv("DDW_ID", "alco-metadata-reporting")
replace_grep <- Sys.getenv("LOOP_VAR",NA)
Auth_Token <- Sys.getenv('DW_AUTH_TOKEN')

# Import query from env variable

if(is.na(query_id) & !is.na(query_name)){
  query_DDW_QUERIES <- function(Auth_Token, ddw_owner, ddw_ID) {
    url <- paste0("https://api.data.world/v0/projects/",ddw_owner,"/",ddw_ID,"/queries")
    response <- VERB("GET", url,
                     add_headers('Authorization' = paste('Bearer',Auth_Token)),
                     content_type("application/octet-stream"),
                     accept("application/json")
    )
    raise <- suppressMessages(httr::content(response, "text"))
    result <- jsonlite::fromJSON(raise)
    dfs <- lapply(result$records, data.frame, stringsAsFactors = FALSE)
    FINAL_set <- do.call(cbind.data.frame, dfs)
    new_names <- names(result$records)[-9]
    colnames(FINAL_set) <- new_names
    return(FINAL_set)
  }
  
  queries_list <- query_DDW_QUERIES(Auth_Token, ddw_org, ddw_id)
  query_id <- queries_list[queries_list$name == query_name,"id"]
  if(is_empty(query_id)){
    stop("No Query ID returned by QUERY_NAME specified")
  }
}else if (is.na(query_name)){
  stop("No SQL_QUERY_ID supplied and QUERY_NAME is NA")
}

# DDW Query Functions
user_agent <- function() {
  ret <- sprintf("dwapi-R - %s", "X.X.X")
  ret
}

next_page <- function(next_token, Auth_Token, accept_type) { #Funciton to paginate multi-page responses
  NextPage <- 
    httr::GET(
      paste0("https://api.data.world/v0/",next_token),
      httr::add_headers(
        Accept = accept_type,
        Authorization = sprintf("Bearer %s", Auth_Token)
      ),
      httr::user_agent(user_agent())
    )
  raise <- suppressMessages(httr::content(NextPage, "text"))
  result <- read.table(text = raise, sep = ",", header = TRUE, stringsAsFactors = FALSE)
  return(result)
}

next_page_add <- function (api_result, Auth_Token, accept_type) {
  if(any(grepl("^next",names(api_result),ignore.case = TRUE))){
    TokentDetect <- 0
    Return_Frame <- api_result$records%>%
      jsonlite::flatten()
    while(TokentDetect == 0){
      NextPage <- next_page(api_result[grep("^next", names(api_result))],Auth_Token,accept_type)
      if (!any(grepl("^next",names(NextPage),ignore.case = TRUE))){
        TokentDetect <- 1
      }
      api_result <- NextPage #overrides the api_result variable to prevent infinity loop
      Return_Frame <- bind_rows(Return_Frame, NextPage$records %>% jsonlite::flatten())
    }
  }else{
    Return_Frame <- api_result
  }
  return(Return_Frame)
}

query_DDW <- function(Auth_Token, AcceptNullReturn, query_ID, accept_type) {
  url <- paste0("https://api.data.world/v0/queries/",URLencode(query_ID),"/results")
  response <- VERB("GET", url,
                   add_headers('Authorization' = paste('Bearer',Auth_Token)), 
                   content_type("application/octet-stream"), 
                   accept(accept_type)
  )
  print("response generated")
  raise <- suppressMessages(httr::content(response, "text"))
  print("text response formatted")
  result <- read.table(text = raise, sep = ",", header = TRUE, stringsAsFactors = FALSE)
  print("result generated, doing next page add")
  Collections <- next_page_add(result,Auth_Token,accept_type)#check for next page in api return set
  if (is.null(nrow(Collections)) & AcceptNullReturn == "yes"){
  }else if (is.null(nrow(Collections)) & AcceptNullReturn == "no"){
    stop("No rows returned (no results$bindings) and ACCEPT_NULL_RETURN is set to 'no'")
  }else{
    print("producing final set")
    FINAL_set <- Collections %>% 
      tidyr::unnest(result$head$vars, names_sep = "_")%>%
      dplyr::select(-contains("_type"))
    print("Returned query as Final Set")
    return(FINAL_set)
  }
}

QueryReturn <- query_DDW(Auth_Token, query_ID = query_id, 
                         AcceptNullReturn = accept_null_return, accept_type = query_return)
print("Query complete")

# Connect to DataWarehouse
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", 
                    server = wh_host,
                    database = wh_db, 
                    UID = wh_user, 
                    pwd = wh_pass)

# Write Table
table_name <- paste(dept, source, table, sep = "_")
if (!is.null(QueryReturn)){
  if(!is.na(replace_grep)){
    new_table <- paste0("Staging.", paste(dept, source, table, sep = "_"))
    if (dbExistsTable(wh_con, SQL(new_table))) {
      prel_table <- paste0("Staging.NEW_", table_name)
      dbWriteTable(wh_con, SQL(prel_table), QueryReturn, overwrite = TRUE)
      cols <- paste0("SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '", table_name, "' AND TABLE_SCHEMA = 'Staging'")
      col_names <- dbGetQuery(wh_con, cols)$COLUMN_NAME %>%
        paste(collapse = "], [")
      
      # Append to Master Table
      sql_insert <- paste0("WITH NewData AS (SELECT * FROM ", prel_table, ")
                          INSERT INTO ", new_table, " ([", col_names, "]) SELECT * FROM NewData")
      y <- dbExecute(wh_con, sql_insert)
      print(paste(y, "records added to", new_table))
      # Drop Staging.NEW_ Table
      sql_drop <- paste('DROP TABLE IF EXISTS', prel_table)
      dbExecute(wh_con, sql_drop)
    }else{
      print("writing table")
      dbWriteTable(wh_con, SQL(new_table), QueryReturn)
    }
  }else{
    print("writing table overwrite NO LOOP REPLACE DETECTED")
    dbWriteTable(wh_con, SQL(paste("Staging", table_name, sep =".")), QueryReturn, overwrite = TRUE)
  }
}else if (is.null(QueryReturn) & accept_null_return == "yes"){
  if (!is.na(replace_grep)){
    print(paste("No records to add from", replace_grep))
  }else{
    print("No records to add and ACCEPT_NULL_RETURN is set to `yes`")  
  }
}else if (is.null(QueryReturn) & accept_null_return == "no"){
  stop("Query Return is Null and ACCEPT_NULL_RETURN is `no`: Error with Query as well since line 97 should prevent this message")
}
