library(data.table)
library(dplyr)

Airports <- read.csv("airports.csv", stringsAsFactors = F)
aircraft <- fread("aircrafts.txt")
aircraft$V4 <- as.numeric(aircraft$V4)
Routes = read.csv("routes.csv", stringsAsFactors = F)

Airports <- filter(Airports, type == "large_airport")

cap_check <- function(Airport_code, source = T, capacity) {
  
  if (source == T) {
  City_routes <- filter(Routes, destination.apirport == Airport_code) %>%
    left_join(Airports[, c("iso_country", "municipality", "iata_code")], 
              by = c("source.airport" = "iata_code"))
  } else {
    City_routes <- filter(Routes, source.airport == Airport_code) %>%
      left_join(Airports[, c("iso_country", "municipality", "iata_code")], 
                by = c("destination.apirport" = "iata_code"))
  }
  
  ref_ind <- c()
  above_capacity <- aircraft$V3[which(aircraft$V4 >= capacity)]
  for (i in 1:length(City_routes$equipment)){
    planes <- strsplit(City_routes$equipment[i], split = " ")
    planes <- unlist(planes)
    for (j in 1:length(planes)){
      if (planes[j] %in% above_capacity){
        ref_ind <- append(ref_ind, i)
        break
      }
    }
  }
  table(as.character(City_routes$municipality[ref_ind]))
}




