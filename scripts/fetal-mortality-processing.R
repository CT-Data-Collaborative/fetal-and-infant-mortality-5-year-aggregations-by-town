library(dplyr)
library(datapkg)
library(data.table)
library(stringr)
library(reshape2)

##################################################################
#
# Processing Script for Fetal Mortality
# Created by Jenna Daly
# On 07/07/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
raw_data_location <- grep("raw", sub_folders, value=T)
data_location <- grep("data$", sub_folders, value=T)
path_to_data <-  (paste0(getwd(), "/", data_location))
path_to_raw_data <- (paste0(getwd(), "/", raw_data_location))
all_csvs <- dir(path_to_raw_data, recursive=T, pattern = ".csv")

# Read in table 02a data
tab02A_data <- dir(path_to_raw_data, recursive=T, pattern = "02A")

tab02a <- data.frame(stringsAsFactors = F)
for (i in 1:length(tab02A_data)) {
  current_file <- read.csv(paste0(path_to_raw_data, "/", tab02A_data[i]), stringsAsFactors=F, header=T )
  current_file <- current_file[current_file$GEOG_level != 3,]
  tab02a <- rbind(tab02a, current_file)
}

# Read in table 02b data
tab02B_data <- dir(path_to_raw_data, recursive=T, pattern = "02B")

tab02b <- data.frame(stringsAsFactors = F)
for (i in 1:length(tab02B_data)) {
  current_file <- read.csv(paste0(path_to_raw_data, "/", tab02B_data[i]), stringsAsFactors=F, header=T )
  current_file <- current_file[current_file$GEOG_level != 3,]
  tab02b <- rbind(tab02b, current_file)
}

#write csv raw files with all years
write.table(
  tab02a,
  sep = ",",
  file.path(raw_data_location, "CT_RR_TAB02A_9914.csv"),
  row.names = F
)

write.table(
  tab02b,
  sep = ",",
  file.path(raw_data_location, "CT_RR_TAB02B_9914.csv"),
  row.names = F
)


#combine 02a and 02b
tab02 <- merge(tab02a, tab02b, by = c("AGGREGATION", "RR_YR", "GEOG_level", "GEOG_ID", "GEOG_name", "POP"))

#Parse out race, observation type, and measure type from each row
setDT(tab02)

tab02 <- melt(
  tab02,
  id.vars=1:6,
  Variable.factor=F,
  Value.factor=F
)

names(tab02)[names(tab02) == 'variable'] <- 'Variable'
names(tab02)[names(tab02) == 'value'] <- 'Value'



tab02$Race <- "All"
tab02$Race[grep("black", tab02$Variable, ignore.case = T)] <- "Black or African American" 
tab02$Race[grep("hisp", tab02$Variable, ignore.case = T)] <- "Hispanic or Latino"
tab02$Race[grep("white", tab02$Variable, ignore.case = T)] <- "White"
tab02$Race[grep("other", tab02$Variable, ignore.case = T)] <- "Other"

tab02$Type <- "All"
tab02$Type[grep("Bth", tab02$Variable, ignore.case = T)] <- "Birth"
tab02$Type[grep("Ftl", tab02$Variable, ignore.case = T)] <- "Fetal"
tab02$Type[grep("Inf", tab02$Variable, ignore.case = T)] <- "Infant"
tab02$Type[grep("Neon", tab02$Variable, ignore.case = T)] <- "Neonatal"
tab02$Type[grep("PNeo", tab02$Variable, ignore.case = T)] <- "Postneonatal"

tab02$Measure.Type <- NA
tab02$Measure.Type[grep("Num", tab02$Variable, ignore.case = T)] <- "Number"
tab02$Measure.Type[grep("Rate", tab02$Variable, ignore.case = T)] <- "Rate"

# Drop unnecessary columns
tab02$Variable <- NULL

# Make connecticut geog name title case
tab02$GEOG_name <- str_to_title(tab02$GEOG_name)

# Fix format of aggregation span and years
tab02$AGGREGATION <- gsub("([0-9])", "\\1-", tab02$AGGREGATION)
tab02$AGGREGATION <- gsub(fixed("YR"), "Year", tab02$AGGREGATION)
tab02$RR_YR <- gsub(fixed(":"), "-", tab02$RR_YR)

#Remove ',' from numeric columns
tab02[] <- lapply(tab02, gsub, pattern=',', replacement='')

tab02$POP <- as.numeric(tab02$POP)
tab02$Value <- as.numeric(tab02$Value)
##############################################################################################################################################################

#Separate out into 2 different parts

##Part 1: Race (All births by race, all deaths by race, all fetal/infant deaths by race)
race.totals <- tab02[tab02$Race != "All" & tab02$Type != "Birth" & tab02$Type != "All",]
race.totals <- unique(race.totals)
race.totals$Measure.Type <- "Number"

##Part 2a: Total Births by Type (total births, total birth rate, totals for all races)
births <- tab02[tab02$Type == "Birth",]
births <- unique(births)
births <- births[!(births$Race == "All" & is.na(births$Measure.Type)),]
births$Measure.Type[is.na(births$Measure.Type)] <- "Number"
names(births)[names(births) == "Value"] <- "Total Births"
births$Type <- NULL

##Part 2b: Calculate rate of each type by total births
calc_rate <- tab02[tab02$Type != "Birth",]
calc_rate <- unique(calc_rate)
calc_rate$Measure.Type[is.na(calc_rate$Measure.Type)] <- "Number"

calc_rate <- data.frame(calc_rate)
births <- data.frame(births)

#Merge in births so rate can be calculated
calc_rate <- merge(calc_rate, births, 
  by = c("AGGREGATION", "RR_YR", "GEOG_level", 
         "GEOG_ID", "GEOG_name", "POP", "Race"))

calc_rate <- calc_rate[!(calc_rate$Measure.Type.x == "Rate" | calc_rate$Measure.Type.y == "Rate"),]
calc_rate$Death.Rate <- NA
calc_rate$Death.Rate <- round(calc_rate$Value*1000/calc_rate$Total.Births,2)
calc_rate_final <- calc_rate[calc_rate$Race == "All" & calc_rate$Type != "All",]
calc_rate_final <- unique(calc_rate_final)
calc_rate_final$Measure.Type.x <- NULL
calc_rate_final$Measure.Type.y <- NULL
calc_rate_final$Total.Births <- NULL

#convert wide to long
cols_to_stack <- c("Value", 
                   "Death.Rate")

long_row_count = nrow(calc_rate_final) * length(cols_to_stack)

#table has number and rate values for when Race = All
calc_rate_final_long <- reshape(calc_rate_final,
                            varying = cols_to_stack,
                            v.names = "Value",
                            timevar = "Measure.Type",
                            times = cols_to_stack,
                            new.row.names = 1:long_row_count,
                            direction = "long"
)

calc_rate_final_long$id <- NULL

calc_rate_final_long$Measure.Type[calc_rate_final_long$Measure.Type == "Value"] <- "Number"
calc_rate_final_long$Measure.Type[calc_rate_final_long$Measure.Type == "Death.Rate"] <- "Rate"


complete_fm <- rbind(calc_rate_final_long, race.totals)
complete_fm$GEOG_level <- NULL
complete_fm$GEOG_ID <- NULL
complete_fm$POP <- NULL
complete_fm$Variable <- "Fetal Mortality"

complete_fm <- complete_fm %>% 
  arrange(AGGREGATION, RR_YR, GEOG_name, Race, Type) 

complete_fm <- unique(complete_fm)

##Suppression: All number values btn 1,5 and all corresponding rates for those values
#Find all the values that are 1 <= x <= 5, set to -9999
complete_fm$Value[complete_fm$Measure.Type == "Number" & complete_fm$Value <= 5 & complete_fm$Value >=1] <- -9999

#Isolate those values to a df, to find all scenarios (combinations of columns) that are suppressed
complete_fm_9999 <- complete_fm[complete_fm$Value == -9999,]

complete_fm_9999 <- complete_fm_9999[complete.cases(complete_fm_9999),]

#Edit suppressed df so we can left join with original df 
complete_fm_9999 <- select(complete_fm_9999, -c(Measure.Type, Value, Variable))

#Left join rows with -9999 & MT=Number back with original df, so we can find corresponding MT=Rate when number is btn 1,5
all_9999 <- merge(complete_fm_9999, complete_fm, by = c("AGGREGATION", "RR_YR", "GEOG_name", "Race", "Type"), all.x=T)
all_9999$Value[all_9999$Measure.Type == "Rate"] <- -9999

#Create anti_join table for -9999 (find all rows from original df that wouldnt have -9999)
all_9999_join <- select(all_9999, -c(Measure.Type, Value, Variable)) 

complete_fm_complete <- complete_fm %>% 
  anti_join(all_9999_join)

#combine all_9999 and complete_fm_complete
final_fm <- rbind(all_9999, complete_fm_complete)

# FIPS
# first get one table of all town AND county fips - make sure to deduplicate CT value

town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
town_fips <- (town_fips_dp$data[[1]])

county_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-county-list/master/datapackage.json'
county_fips_dp <- datapkg_read(path = county_fips_dp_URL)
county_fips <- (county_fips_dp$data[[1]])

setnames(town_fips, "Town", "Town/County")
setnames(county_fips, "County", "Town/County")
fips <- rbind(town_fips, county_fips)
fips <- unique(fips)
names(final_fm)[names(final_fm) == "GEOG_name"] <- "Town/County"
final_fm_fips <- merge(final_fm, fips, by = "Town/County", all.x=T)


final_fm_fips_arrange <- final_fm_fips %>% 
  rename("Year" = "RR_YR", "Measure Type" = "Measure.Type") %>% 
  select("AGGREGATION", "Town/County", "FIPS", "Year", "Race", "Type", "Measure Type", "Variable", "Value") %>% 
  arrange(`AGGREGATION`, `Town/County`, Year, `Race`, `Type`, `Measure Type`)

write.table(
  final_fm_fips_arrange,
  sep = ",",
  file.path(path_to_data, "all_years_FM.csv"),
  row.names = F
)

counties <- c("Connecticut", "Fairfield County", "Hartford County",
              "Litchfield County", "Middlesex County", "New Haven County",
              "New London County", "Tolland County", "Windham County")

# write tables by agg level
for (y in c(1,3,5)) {
  agg <- paste(y, "Year", sep = "-")
  aggData <- final_fm_fips_arrange[final_fm_fips_arrange$AGGREGATION == agg,]
  
  # county data
  county <- aggData[aggData$`Town/County` %in% counties,]
  names(county)[names(county) == "Town/County"] <- "County"
  county <- select(county, -(AGGREGATION))
  write.table(
    county,
    file.path(path_to_data, paste0("fetal_infant_mortality-", tolower(agg), "-county.csv")),
    sep = ",",
    row.names = F,
    na = "-9999"
  )
  
  # town data
  town <- aggData[!(aggData$`Town/County` %in% counties),]
  names(town)[names(town) == "Town/County"] <- "Town"
  town <- select(town, -(AGGREGATION))
  
  write.table(
    town,
    file.path(path_to_data, paste0("fetal_infant_mortality-", tolower(agg), "-town.csv")),
    sep = ",",
    row.names = F,
    na = "-9999"
  )
}


