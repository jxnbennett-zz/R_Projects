#Using Datadr ----------------

# Read in data, filter for most recent 10 years, convert to ddf
Temps <- read.csv("GlobalLandTemperaturesByCountry.csv")
Temps$record_year <- year(Temps$dt)
To_split <- filter(Temps, record_year >= 2004)
TempDdf <- ddf(To_split)

# Divide by country, take average temperature and recombine
byCountry <- divide(TempDdf, by = "Country", update = T)
Avg_Count_Temp <- addTransform(byCountry, function(x) mean(x$AverageTemperature, na.rm = T))
Country_Temp_df <- recombine(Avg_Count_Temp, combRbind)

# Using Hadoop/RHIPE -----------------------
byCountry <- rhwatch(
  map      = map_byCountry,
  reduce   = reduce_byCountry,
  input    = rhfmt("/user/benne105/eaps595/final/Happiness_Data.txt", type = "text"),
  output   = rhfmt("/user/benne105/eaps595/final/byCountry", type = "sequence"),
  readback = FALSE
)

map_byCountry <- expression({
  lapply(seq_along(map.keys), function(r) {
    line = strsplit(map.values[[r]], "\t")[[1]]
    outputkey <- line[1]
    outputvalue <- data.frame(
      year = as.numeric(line[3]),
      h_score =  as.numeric(line[4]),
      GDP = as.numeric(line[5]),
      Social = as.numeric(line[6]),
      Life_expt = as.numeric(line[7]),
      free =  as.numeric(line[8]),
      gen = as.numeric(line[9]),
      corrupt = as.numeric(line[10]),
      stringsAsFactors = FALSE
    )
    rhcollect(outputkey, outputvalue)
  })
})

reduce_byCountry <- expression(
  pre = {
    reduceoutputvalue <- data.frame()
  },
  reduce = {
    reduceoutputvalue <- rbind(reduceoutputvalue, do.call(rbind, reduce.values))
  },
  post = {
    reduceoutputkey <- reduce.key[1]
    attr(reduceoutputvalue, "location") <- reduce.key[1]
    names(attr(reduceoutputvalue, "location")) <- c("Country")
    rhcollect(reduceoutputkey, reduceoutputvalue)
  }
)

CountryStats <- rhwatch(
  map      = map_stats,
  reduce   = reduce_stats,
  input    = rhfmt("/user/benne105/eaps595/final/byCountry", type = "sequence"),
  output   = rhfmt("/user/benne105/eaps595/final/CountryStats2", type = "text"),
  readback = TRUE
)

map_stats <- expression({
  lapply(seq_along(map.keys), function(r) {
    outputvalue <- data.frame(
      Country = map.keys[[r]],
      h_score_avg = mean(map.values[[r]]$h_score, na.rm = TRUE),
      GDP_avg = mean(map.values[[r]]$GDP, na.rm = TRUE),
      Social_avg = mean(map.values[[r]]$Social, na.rm = TRUE),
      Life_expt_avg = mean(map.values[[r]]$Life_expt, na.rm = TRUE),
      free_avg = mean(map.values[[r]]$free, na.rm = TRUE),
      gen_avg = mean(map.values[[r]]$gen, na.rm = TRUE),
      corrupt_avg = mean(map.values[[r]]$corrupt, na.rm = TRUE),
      stringsAsFactors = FALSE
    )
    outputkey <- attr(map.values[[r]], "location")["Country"]
    rhcollect(outputkey, outputvalue)
  })
})


reduce_stats <- expression(
  pre = {
    reduceoutputvalue <- data.frame()
  },
  reduce = {
    reduceoutputvalue <- rbind(reduceoutputvalue, do.call(rbind, reduce.values))
  },
  post = {
    rhcollect(reduce.key, reduceoutputvalue)
  }
)

# Transform key-value data frame into standard data frame
Country_results <- data.frame(matrix(nrow = 0, ncol = 8))
for (i in 1:length(Country_Stats)) {
  Country_results <- rbind(Country_results, Country_Stats[[i]][[2]])
}

# Save results
write.table(Country_results, file = "results.csv", row.names = F, col.names = F, append = F)
