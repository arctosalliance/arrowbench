ensure_custom_duckdb <- function() {
  result <- tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    DBI::dbExecute(con, "LOAD tpch;")
    DBI::dbGetQuery(con, "select scale_factor, query_nr from tpch_answers() LIMIT 1;")
  },
  error = function(e) {
    error_is_from_us <- grepl(
      paste0(c(
        "(name tpch_answers is not on the catalog)",
        "(name tpch_answers does not exist)",
        "(tpch.duckdb_extension\" not found)"
      ),
      collapse = "|"
      ),
      conditionMessage(e)
    )

    if (error_is_from_us) {
      NULL
    } else {
      rlang::abort(
        "An unexpected error occured whilst querying TPC-H enabled duckdb",
        parent = e
      )
    }
  }
  )

  # Check that the result has a query in it
  if (identical(result$query_nr, 1L)) {
    return(invisible(NULL))
  }


  install_duckdb_tpch()
  result <- try(
    ensure_custom_duckdb(),
    silent = FALSE
  )

  if (!inherits(result, "try-error")) {
    return(invisible(NULL))
  }

  stop("Could not load the DuckDB TPC-H extension.")
}

query_custom_duckdb <- function(sql, dbdir = ":memory:") {
  ensure_custom_duckdb()

  con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbdir))
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "LOAD tpch;")
  DBI::dbGetQuery(con, sql)
}

export_custom_duckdb <- function(sql, sink, dbdir = ":memory:") {
  ensure_custom_duckdb()

  con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbdir))
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "LOAD tpch;")
  res <- DBI::dbSendQuery(con, sql, arrow = TRUE)

  # this could be streamed in the future when the parquet writer
  # in R supports streaming
  reader <- duckdb::duckdb_fetch_record_batch(res)
  table <- reader$read_table()
  arrow::write_parquet(table, sink)

  sink
}

install_duckdb_tpch <- function() {
  # First, clear any stale cached extensions that might cause version mismatch
  duckdb_ext_dir <- file.path(Sys.getenv("HOME"), ".duckdb", "extensions")
  if (dir.exists(duckdb_ext_dir)) {
    unlink(duckdb_ext_dir, recursive = TRUE)
    message("Cleared stale DuckDB extensions cache")
  }
  
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  
  # Force update from the correct repository for this DuckDB version
  tryCatch({
    DBI::dbExecute(con, "FORCE INSTALL tpch;")
    DBI::dbExecute(con, "LOAD tpch;")
  }, error = function(e) {
    # If FORCE INSTALL fails, try regular install
    # (works when extension is bundled with DuckDB)
    tryCatch({
      DBI::dbExecute(con, "INSTALL tpch;")
      DBI::dbExecute(con, "LOAD tpch;")
    }, error = function(e2) {
      rlang::abort(
        paste0(
          "Failed to install DuckDB TPCH extension. ",
          "This may be due to a DuckDB version mismatch. ",
          "Try installing the latest stable DuckDB: install.packages('duckdb')\n",
          "Original error: ", conditionMessage(e2)
        )
      )
    })
  })
}
