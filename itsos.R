# The State and Trends of Barcode, RFID, Biometric and Pharmacy Automation Technologies in US Hospitals
# Raymonde Uy, MD, MBA, Fabricio Kury, MD, Paul Fontelo, MD, MPH
# ------
# Codename: itsos
# http://github.com/fabkury/itsos
# Project start: January 2015.
# 

#
## Globals
data_dir <- "../../../Data/"
output_dir <- paste0("Output/", format(Sys.Date(), format="%y-%m-%d"))
years_to_analyze <- c(2008, 2009, 2010, 2011, 2012)

Region_switch <-
"switch(State in ('AK', 'WA', 'OR', 'CA', 'HI'), 'Pacific',
State in ('MT', 'WY', 'ID', 'UT', 'CO', 'NV', 'AZ', 'NM'), 'Mountain',
State in ('ND', 'SD', 'NE', 'KS', 'MO', 'IA', 'MN'), 'West North Central',
State in ('WI', 'IL', 'IN', 'MI', 'OH'), 'East North Central',
State in ('NY', 'PA', 'NJ'), 'Middle Atlantic',
State in ('ME', 'NH', 'VT', 'MA', 'CT', 'RI'), 'New England',
State in ('OK', 'AR', 'LA', 'TX'), 'West South Central',
State in ('KY', 'TN', 'AL', 'MS'), 'East South Central',
State in ('WV', 'MD', 'DE', 'DC', 'VA', 'NC', 'SC', 'GA', 'FL'), 'South Atlantic',
true, State) as Region"

NofBeds_switch <-
"switch(NofBeds > 499, '500+',
NofBeds > 399, '400-499',
NofBeds > 299, '300-399',
NofBeds > 199, '200-299',
NofBeds > 99, '100-199',
NofBeds <= 99, '0-99',
true, '?') as `NofBeds stratum`"

NofStaffedBeds_switch <-
"switch(NofStaffedBeds > 499, '500+',
NofStaffedBeds > 399, '400-499',
NofStaffedBeds > 299, '300-399',
NofStaffedBeds > 199, '200-299',
NofStaffedBeds > 99, '100-199',
NofStaffedBeds <= 99, '0-99',
true, '?') as `NofStaffedBeds stratum`"

NofIntensiveCareBeds_switch <-
"switch(NofIntensiveCareBeds > 499, '500+',
NofIntensiveCareBeds > 399, '400-499',
NofIntensiveCareBeds > 299, '300-399',
NofIntensiveCareBeds > 199, '200-299',
NofIntensiveCareBeds > 99, '100-199',
NofIntensiveCareBeds <= 99, '0-99',
true, '?') as `NofIntensiveCareBeds stratum`"

TotalOperExpense_switch <-
"switch(TotalOperExpense >= 200000000, '200+ million',
TotalOperExpense >= 100000000, '100-199 million',
TotalOperExpense >= 40000000, '040-99 million',
TotalOperExpense >= 20000000, '020-39 million',
TotalOperExpense < 20000000, '00-19 million',
true, '?') as `TotalOperExpense stratum`"

standard_names <- c('NofBeds stratum', 'NofStaffedBeds stratum', 'NofIntensiveCareBeds stratum',
  'Region', 'State', 'ProfitStatus')
standard_switches <- c(NofBeds_switch, NofStaffedBeds_switch, NofIntensiveCareBeds_switch, Region_switch,
  'State', 'ProfitStatus')
AutoIdentification_types <- c('BarCoding', 'RFID')
Pharmacy_columns_of_interest <- c("DeptMedical", "DeptED", "DeptOR", "DeptOther", "Robot", "Carousels",
  "ADM")
##


as.numeric.factor <- function(x) {as.numeric(levels(x))[x]}


guarantee.output.dir <- function() {
	if(!file.exists(output_dir)) {
		message(paste0("Creating output directory '", output_dir, "'..."))
		dir.create(output_dir)
		if(file.exists(output_dir))
			message("...sucess.")
		else
			stop("...error.")
	}
	output_dir <<- paste0(output_dir, "/")
}


guarantee.removal <- function(file) {
	if(file.exists(file))
		file.remove(file)
	if(file.exists(file))
		stop(paste0("Unable to erase file '", file, "'."))
}


load.libraries <- function(libraries_needed) {
	for(library_needed in libraries_needed)
		if(!library(library_needed, quietly=TRUE, logical.return=TRUE, character.only=TRUE)) {
			install.packages(library_needed)
			if(!library(library_needed, quietly=TRUE, logical.return=TRUE, character.only=TRUE))
				stop(paste("Unable to load library '", library_needed, "'.", sep=""))
		}
}


get.mdb <- function(year) odbcConnectAccess(paste0(data_dir, 'HADB ', year, '.mdb'))


make.AutoIdentification.Names <- function(years, types, names, name_switches) {
  for(type in types) {
    output_xlsx <- paste0(output_dir, type, '.xlsx')
    guarantee.removal(output_xlsx)
    for(i in 1:length(names)) {
      name <- names[i]
      name_switch <- name_switches[i]
      ret <- data.frame()
	  for(year in years) {
	    mdb <- get.mdb(year)
	    data <- sqlQuery(mdb,
paste0("select * from
(select a.`", name, "`, round(NOfHospitals*100/NInStratum, 2) as `", year, "`
  from (select count(*) as NOfHospitals, `", name, "`
    from (select distinct a.HAEntityId, ", name_switch, "
      from AutoIdentification a, HAEntity b
      where a.Type='", type, "' and InUseFlag = -1 and DepartmentName = 'Laboratory' and a.HAEntityId=b.HAEntityId)
    group by `", name, "`) a,
(select count(*) as NInStratum, `", name, "`
  from (select distinct a.HAEntityId, ", name_switch, "
    from AutoIdentification a, HAEntity b
    where a.Type='", type, "' and DepartmentName = 'Laboratory' and a.HAEntityId=b.HAEntityId)
  group by `", name, "`) b
where a.`", name, "` = b.`", name, "`
order by a.`", name, "`)
union all
(select 'All' as `", name, "`, round(NOfHospitals*100/NInStratum, 2) as `", year, "`
  from (select count(*) as NOfHospitals
    from (select distinct HAEntityId from AutoIdentification
    where Type='", type, "' and InUseFlag = -1 and DepartmentName = 'Laboratory')) a,
(select count(*) as NInStratum
  from (select distinct HAEntityId from AutoIdentification
  where Type='", type, "' and InUseFlag is not null and DepartmentName = 'Laboratory')) b);"))
	    close(mdb)
	    if(class(data) == "character") {
          warning(paste0("Unable to calculate year ", year, "."))
	      next }
	  
	    ret <- if(nrow(ret)==0) data else merge(ret, data, by=name, all=TRUE, sort=TRUE, suffixes=c('', ''))
	  }
	  write.xlsx(ret, output_xlsx, sheetName=paste0('By ', gsub(' stratum', '', name)),
	    col.names=TRUE, row.names=FALSE, append=TRUE, showNA=FALSE)
    }
  }
}


make.medadministration <- function(years=c(2008,2009,2010,2011,2012)) {
  ret <- data.frame()
  for(year in years) {
    mdb <- get.mdb(year)
    dat <- sqlQuery(mdb,
"select count(*) as TotalN
FROM
	(select distinct HAEntityId from MedAdministration)")
    total_n <- dat$TotalN[1]
    dat <- sqlQuery(mdb, paste0(
"SELECT ProcessName, count(*) as N
FROM MedAdministration
GROUP BY ProcessName"))
    if(length(dat) == 0) {
      warning(paste0("Unable to calculate year ", year, "."))
      next
    }
    close(mdb)
    dat <- cbind(dat, TotalN=total_n)
    dat <- mutate(dat, PercentN=round((N/total_n)*100, 2))
    dat <- cbind(dat, Year=year)
    dat <- cbind(Type='Medication Administration', dat)
    ret <- rbind(ret, dat)
  }
  ret
}


make.MedAdministration.Names <- function(years, names, name_switches) {
  output_xlsx <- paste0(output_dir, "Medication Administration.xlsx")
  guarantee.removal(output_xlsx)
  for(i in 1:length(names)) {
  	name <- names[i]
  	name_switch <- name_switches[i]
    ret <- data.frame()
    for(year in years) {
      mdb <- get.mdb(year)
      data <- sqlQuery(mdb, paste0(
"select a.`", name, "`, round(N*100/`Total in Stratum`, 2) as `", year, "`
from
(select `", name, "`, count(*) as N
    from (select a.HAEntityId, ", name_switch, "
      from (select distinct HAEntityId from MedAdministration) a, HAEntity b
      where a.HAEntityId = b.HAEntityId)
    group by `", name, "`) a,
  (select `", name, "`, count(*) as `Total in Stratum`
    from (select HAEntityId, `", name, "`
      from (select distinct HAEntityId, ", name_switch, " from HAEntity where HAEntityType='Hospital'))
    group by `", name, "`) b
  where a.`", name, "` = b.`", name, "`
union all
select 'All' as `", name, "`, round(N*100/`Total in Stratum`, 2) as `", year, "`
from (select count(*) as N from (select distinct HAEntityId from MedAdministration)) a,
  (select count(*) as `Total in Stratum` from (select distinct HAEntityId from HAEntity where HAEntityType='Hospital')) b
order by `", name, "`;"))
      close(mdb)
      if(class(data) == "character") {
        warning(paste0("Unable to calculate year ", year, "."))
        next
      }

  	  ret <- if(nrow(ret)==0) data else merge(ret, data, by=name, all=TRUE, sort=TRUE, suffixes=c('', ''))
	}

	write.xlsx(ret, output_xlsx, sheetName=paste0('By ', gsub(' stratum', '', name)),
	  col.names=TRUE, row.names=FALSE, append=TRUE, showNA=FALSE)
  }
}


make.BiometricTechnology.Name <- function(years, names, name_switches) {
  output_xlsx <- paste0(output_dir, paste0("Biometrics.xlsx"))
  guarantee.removal(output_xlsx)
  for(i in 1:length(names)) {
  	name <- names[i]
    name_switch <- name_switches[i]
    ret <- data.frame()
    for(year in years) {
      mdb <- get.mdb(year)
      data <- sqlQuery(mdb, paste0(
"(select a.TechnologyName, a.Type, a.`", name, "`, round(N*100/TotalN, 2) as `", year, "`
from
(select TechnologyName, Type, `", name, "`, count(*) as N
from (select TechnologyName, a.Type, ", name_switch, "
  from BiometricTechnology a, HAEntity b
  where a.HAEntityId = b.HAEntityId and TechnologyName not like 'Identity Management')
  group by TechnologyName, a.Type, `", name, "`) a,
(select `", name, "`, count(*) as TotalN
  from (select ", name_switch, "
  from HAEntity
  where HAEntityType='Hospital')
  group by `", name, "`) b
where a.`", name, "`=b.`", name, "`
order by a.TechnologyName, a.Type, a.`", name, "`)
union all
(select a.TechnologyName, Type, 'All' as `", name, "`, round(N*100/TotalN, 2) as `", year, "`
  from (select TechnologyName, Type, count(*) as N
      from (select TechnologyName, Type from BiometricTechnology
          where TechnologyName not like 'Identity Management')
      group by TechnologyName, Type) a,
  (select count(*) as TotalN from HAEntity where HAEntityType='Hospital') b)
order by TechnologyName, Type, `", name, "`;"))
      close(mdb)
      if(class(data) == "character") {
        warning(paste0("Unable to calculate year ", year, "."))
        next
      }

      if(length(ret)==0)
        ret <- data
      else
        ret <- merge(ret, data, by=c('TechnologyName', 'Type', name),
          all = TRUE, sort=TRUE, suffixes=c('', ''))
    }
    write.xlsx(ret, output_xlsx, sheetName=paste0('By ', gsub(' stratum', '', name)),
      col.names=TRUE, row.names=FALSE, append=TRUE, showNA=FALSE)
  }
}



make.Pharmacy.Names <- function(years, names, name_switches, columns_of_interest) {
	for(i in 1:length(names)) {
	  name <- names[i]
	  name_switch <- name_switches[i]
	  output_xlsx <- paste0(output_dir, 'Pharmacy Automation by ', gsub(' stratum', '', name), '.xlsx')
	  guarantee.removal(output_xlsx)		
	  for(column in columns_of_interest) {
	    ret <- data.frame()
	    for(year in years) {
		  mdb <- get.mdb(year)
		  dat <- sqlQuery(mdb, paste0(
"(select a.`", name, "`, round(N*100/TotalN, 2) as `", year, "` from
(select `", name, "`, count(*) as N FROM
(select distinct a.HAEntityId, ", name_switch, "
from Pharmacy a, HAEntity b
WHERE a.", column, "=-1 and a.HAEntityId=b.HAEntityId)
group by `", name, "`) a,
(select `", name, "`, count(*) as TotalN
FROM (select distinct HAEntityId, ", name_switch, "
from HAEntity WHERE HAEntityType='Hospital')
group by `", name, "`) b
where a.`", name, "` = b.`", name, "`)
union all
(select 'All' as `", name, "`, round(N*100/TotalN, 2) as `", year, "` from
(select count(*) as N FROM (select distinct a.HAEntityId
from Pharmacy a, HAEntity b
WHERE a.", column, "=-1 and a.HAEntityId=b.HAEntityId)) a,
(select count(*) as TotalN
FROM (select distinct HAEntityId from HAEntity WHERE HAEntityType='Hospital')) b);"))
          close(mdb)
          if(class(dat) == "character") {
            warning(paste0("Unable to calculate column ", column," of year ", year, "."))
		    next
		  }

		  ret <- if(nrow(ret)==0) dat else merge(ret, dat, by=name, all=TRUE, sort=TRUE, suffixes=c('', ''))
		}
		write.xlsx(ret, output_xlsx, sheetName=gsub('\\W', '', column),
  	  col.names=TRUE, row.names=FALSE, append=TRUE, showNA=FALSE)
	  }
	}
}



make.Pharmacy.TotalOperExpense <- function(years, columns_of_interest) {
	output_xlsx <- paste0(output_dir, "Pharmacy Automation by TotalOperExpense.xlsx")
	guarantee.removal(output_xlsx)
	for(column in columns_of_interest) {
		retv <- data.frame()
		for(year in years) {
			mdb <- get.mdb(year)
			dat <- sqlQuery(mdb, paste0(
"(select a.`TotalOperExpense stratum`, round(N*100/TotalN, 2) as `", year, "` from
(select `TotalOperExpense stratum`, count(*) as N FROM
(select distinct a.HAEntityId, ", TotalOperExpense_switch, "
from Pharmacy a, AcuteInfo b
WHERE a.", column, "=-1 and a.HAEntityId=b.HAEntityId)
group by `TotalOperExpense stratum`) a,
(select `TotalOperExpense stratum`, count(*) as TotalN
FROM (select distinct a.HAEntityId, ", TotalOperExpense_switch, "
from AcuteInfo a, HAEntity b WHERE HAEntityType='Hospital' and a.HAEntityId=b.HAEntityId)
group by `TotalOperExpense stratum`) b
where a.`TotalOperExpense stratum` = b.`TotalOperExpense stratum`)
union all
(select 'All' as `TotalOperExpense stratum`, round(N*100/TotalN, 2) as `", year, "` from
(select count(*) as N FROM (select distinct a.HAEntityId
from Pharmacy a, HAEntity b
WHERE a.", column, "=-1 and a.HAEntityId=b.HAEntityId)) a,
(select count(*) as TotalN
FROM (select distinct HAEntityId from HAEntity WHERE HAEntityType='Hospital')) b);"))
			close(mdb)
			if(class(dat) == "character") {
				warning(paste0("Unable to calculate column ", column," of year ", year, "."))
				next
			}

			if(length(retv)==0)
      			retv <- dat
    		else
      			retv <- merge(retv, dat, by='TotalOperExpense stratum', all=TRUE, sort=TRUE, suffixes=c('', ''))
		}
		write.xlsx(retv, output_xlsx, sheetName=column, 
  			col.names=TRUE, row.names=FALSE, append=TRUE, showNA=FALSE)
	}
}


#
## Execution starts here.
message("The State and Trends of Barcode, RFID, Biometric and Pharmacy Automation Technologies in US Hospitals")
message("Raymonde Uy, MD, MBA, Fabricio Kury, MD, Paul Fontelo, MD, MPH")
message(" ------ ")
message("Codename: itsos")
message("http://github.com/fabkury/itsos")
message("Project start: January 2015.\n")

message("Loading libraries...")
load.libraries(c('RODBC', 'plyr', 'xlsx'))

message("Processing...")
guarantee.output.dir()
message(paste0("Output directory: ", output_dir))

View(make.medadministration())

make.AutoIdentification.Names(years_to_analyze, AutoIdentification_types, standard_names, standard_switches)
make.MedAdministration.Names(years_to_analyze, standard_names, standard_switches)
make.BiometricTechnology.Name(years_to_analyze, standard_names, standard_switches)
make.Pharmacy.Names(years_to_analyze, standard_names, standard_switches, Pharmacy_columns_of_interest)
make.Pharmacy.TotalOperExpense(years_to_analyze, Pharmacy_columns_of_interest)

message("Script execution completed.")