library(data.table)
library(plyr)
library(dplyr)
library(pipeR)
#library(parallel)
library(lubridate)
#library(doParallel)

# cl <- makeCluster(8)
# clusterEvalQ(cl, {
#   library(data.table)
#   library(plyr)
#   library(dplyr)
#   library(pipeR)
#   library(lubridate)
# })
#
# registerDoParallel(cl)
args <- commandArgs(trailingOnly = TRUE)
year <- args[1]

filepaths <- list.files(path = paste0("D:/Data/CNSTOCK/CNSTOCK/",year,"/"),
  pattern = ".+\\.gz",full.names = T,recursive = T) %>>% sort
matches <- ldply(stringr::str_match_all(sapply(strsplit(filepaths,"/"),last),
  ".+-(\\d{4})-(\\d{2})-(.+)\\.csv\\.gz"))
colnames(matches) <- c("filename","year","month","code")
matches <- cbind(filepath = filepaths,matches) %>>%
  mutate(filepath = as.character(filepath),
    filename = as.character(filename),
    year = as.numeric(as.character(year)),
    month = as.numeric(as.character(month)),
    code = as.character(code)) %>>%
  arrange(code,year,month)

system.time(d_ply(matches,"code",.parallel = F,.fun = function(df) {
  d_ply(df,"year",.fun = function(ddf) {
    code <- ddf$code[1]
    year <- ddf$year[1]
    datetime_morning_open <- as.POSIXct(strptime("20000101 01:25:00",format = "%Y%m%d %H:%M:%S",tz = "GMT"))
    datetime_morning_start <- as.POSIXct(strptime("20000101 01:30:00",format = "%Y%m%d %H:%M:%S",tz = "GMT"))
    datetime_morning_end <- as.POSIXct(strptime("20000101 03:30:00",format = "%Y%m%d %H:%M:%S",tz = "GMT"))
    datetime_afternoon_start <- as.POSIXct(strptime("20000101 05:00:00",format = "%Y%m%d %H:%M:%S",tz = "GMT"))
    datetime_afternoon_end <- as.POSIXct(strptime("20000101 07:00:00",format = "%Y%m%d %H:%M:%S",tz = "GMT"))

    data <- ddf$filepath %>>%
      llply(function(file)
        read.csv(file,header = T,colClasses = rep("character",23),na.strings = "")) %>>%
      rbindlist() %>>%
      select(-GMT.Offset,-Type) %>>%
      mutate(datetime = as.POSIXct(strptime(paste("20000101",Time.G.),format = "%Y%m%d %H:%M:%S",tz = "GMT"))) %>>%
      group_by(Date.G.) %>>%
      #filter(Date.G. != "20140710") %>>%
      filter(!all(!complete.cases(Open,High,Low,Last,Volume))) %>>%
      ungroup() %>>%
      filter(((datetime == datetime_morning_open) |
          (datetime >= datetime_morning_start & datetime <= datetime_morning_end) |
          (datetime >= datetime_afternoon_start & datetime <= datetime_afternoon_end))) %>>%
      select(-datetime)
    dir <- paste0("D:/Data/CNSTOCK/DataFeed/",year)
    if(!file.exists(dir))dir.create(dir)
    output_filepath <- paste0(dir,"/",year,"-",code,".csv")
    write.table(data,output_filepath,col.names=F,row.names=F,quote=F,sep=",",append=T,na = "")
    message(output_filepath)
  })
}))

# stopCluster(cl)
