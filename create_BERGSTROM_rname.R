###############################################################################################################################
###### This file creates B_TS.csv, a list with all Rnames names binned within Bergström et al. (2009) stage slices  ###
###### same as http://rnames.luomus.fi/index.php/search-bergstroem-stage-slices #######################################
###############################################################################################################################
###### before running specify work directory ######
##### run needs several tens of minutes ###

### Clears everything!
rm(list=ls())
gc()

library(RMySQL)
source("/Users/bkroger/Documents/r/rnames/rname_finals/rname_functions.r");
setwd("/Users/bkroger/Documents/r/rnames/rname_finals/");

##############################################################
##############################################################
### Rule 1: Direct relations between names with Biostratigraphy Qualifiers (short: bio*) and Stage Slices (Bergstrom et al. 2009) (short: TS*) are selected with compromise.selector()
### Rules 1 and 2 create temp_opinions_a file in RNames

rule_id <- 1
rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con," SET NAMES utf8")
dbGetQuery(rn.con,"delete from temp_opinions_a")

bio.try <- dbGetQuery(rn.con,"
                        select r.reference_id, r.name_1, name_2
                      from view_cron_relations r
                      where r.strat_qualifier_1 = 'Biostratigraphy'
                      and r.qualifier_name_2 = 'TimeSlice_Bergstrom'
                      group by 1, 2
                      order by 1, 2;
                      ")
bio.out <- subset(bio.try, bio.try$name_1 == "not specified"|bio.try$name_2 == "not specified")
ref.out <- unique(bio.out$reference_id)
name.out <- unique(rbind(bio.out$name_1, bio.out$name_2))
name.out <- unique(name.out[name.out != "not specified"])

bio.names <- dbGetQuery(rn.con,"
                        select r.reference_id, r.name_1, GROUP_CONCAT(r.name_2) name_2
                        from view_cron_relations r
                        where r.strat_qualifier_1 = 'Biostratigraphy'
                        and r.qualifier_name_2 = 'TimeSlice_Bergstrom'
                        and r.name_1 not in (select name from temp_opinions_a)
                        group by 1, 2
                        order by 1, 2;
                        ")

out.1 <- bio.names[grepl(paste(ref.out, collapse="|"), bio.names$reference_id), ]
out.2 <- out.1[grepl(paste(name.out, collapse="|"), out.1$name_1), ]
if (length(ref.out)>0) {bio.names <- bio.names[!(out.2$reference_id==bio.names$reference_id),]}

### We select all unique name_1 values and loop them to find out the related time slices and references for them
bio.names.unique <- as.matrix(unique(bio.names$name_1))

if (length(bio.names.unique) > 0){
  for (i in 1:NROW(bio.names.unique)){
    result <- compromise.selector(ts_used = 'B', subset( bio.names, name_1 == bio.names.unique[i]))
    query <- paste0("INSERT INTO temp_opinions_a (`rule_id`, `name`, `oldest`, `youngest`, `ts_count`, `reference`) VALUES ( ", rule_id, ",\"", result[1,1],"\", '",result[1,2],"', '",result[1,3],"', '",result[1,4], "', \"", result[1,5],"\")")
    # Run insert into SQL ...  
    dbGetQuery(rn.con, query) 
  }  
}
dbDisconnect(rn.con)

##################################################################################
### Rule 2: Direct relations between bio* and bio* that have a direct TS* relation are selected with compromise.selector()

rule_id <- 2;
rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con," SET NAMES utf8")

bio.try <- dbGetQuery(rn.con,"
                      select r.reference_id, r.name_1, name_2
                      from view_cron_relations r
                      where r.strat_qualifier_1 = 'Biostratigraphy'
                      and r.strat_qualifier_2 = 'Biostratigraphy'
                      group by 1, 2
                      order by 1, 2;
                      ")
bio.out <- subset(bio.try, bio.try$name_1 == "not specified"|bio.try$name_2 == "not specified")
ref.out <- unique(bio.out$reference_id)
name.out <- unique(rbind(bio.out$name_1, bio.out$name_2))
name.out <- unique(name.out[name.out != "not specified"])

repeat
{
  bio.names <- dbGetQuery(rn.con,"
                          select concat(r.reference_id, ' ', r.name_2) as reference_id, r.name_1, CONCAT(r2.`oldest`, ', ', r2.`youngest`) name_2
                          from view_cron_relations r
                          join temp_opinions_a r2 on r.name_2 = r2.name
                          left join temp_opinions_a dublicates on r.name_1 = dublicates.name
                          where r.strat_qualifier_1 = 'Biostratigraphy'
                          and r.qualifier_name_1 != 'TimeSlice_Bergstrom'
                          and r.strat_qualifier_2 = 'Biostratigraphy'
                          and dublicates.`name` is null
                          group by 1, 2
                          order by 1, 2;
                          ")
  
  out.1 <- bio.names[grepl(paste(ref.out, collapse="|"), bio.names$reference_id), ]
  out.2 <- out.1[grepl(paste(name.out, collapse="|"), out.1$name_1), ]
  if (length(ref.out)>0) {bio.names <- bio.names[!(out.2$reference_id==bio.names$reference_id),]}
  
  
  if (NROW(bio.names) == 0)
  {
    print("No more relations!");
    break
  } else { 
    rule_id <- rule_id + 0.1       
    bio.names.unique <- as.matrix(unique(bio.names$name_1))
    
    for (i in 1:NROW(bio.names.unique)){
      result <- compromise.selector(ts_used = 'B', subset(bio.names, name_1 == bio.names.unique[i]))
      query <- paste0("INSERT INTO temp_opinions_a (`rule_id`, `name`, `oldest`, `youngest`, `ts_count`, `reference`) VALUES ( ", rule_id, ",\"", result[1,1],"\", '",result[1,2],"', '",result[1,3],"', '",result[1,4], "', \"", result[1,5],"\")")
      # Run insert into SQL ...
      dbGetQuery(rn.con, query)
    }
  }  
}
# We close this connection
dbDisconnect(rn.con)

##############################################################
### Rule 3: Direct relations between bio* and TS* are selected with compromise.selector()
### Rule 3 creates temp_opinions_b file in RNames

rule_id <- 3
rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con,"delete from temp_opinions_b")
dbGetQuery(rn.con," SET NAMES utf8")


bio.try <- dbGetQuery(rn.con,"
                      select r.reference_id, r.name_1, name_2
                      from view_cron_relations r
                      where r.strat_qualifier_1 != 'Biostratigraphy'
                      and r.qualifier_name_2 = 'TimeSlice_Bergstrom'
                      group by 1, 2
                      order by 1, 2;
                      ")
bio.out <- subset(bio.try, bio.try$name_1 == "not specified"|bio.try$name_2 == "not specified")
ref.out <- unique(bio.out$reference_id)
name.out <- unique(rbind(bio.out$name_1, bio.out$name_2))
name.out <- unique(name.out[name.out != "not specified"])

bio.names <- dbGetQuery(rn.con,"
                        select r.reference_id, r.name_1, GROUP_CONCAT(r.name_2) name_2
                        from view_cron_relations r
                        left join temp_opinions_b dublicates on r.name_1 = dublicates.name
                        where r.strat_qualifier_1 != 'Biostratigraphy'
                        and r.qualifier_name_2 = 'TimeSlice_Bergstrom'
                        and dublicates.`name` is null
                        group by 1, 2
                        order by 1, 2;
                        ")

out.1 <- bio.names[grepl(paste(ref.out, collapse="|"), bio.names$reference_id), ]
out.2 <- out.1[grepl(paste(name.out, collapse="|"), out.1$name_1), ]
if (length(ref.out)>0) {bio.names <- bio.names[!(out.2$reference_id==bio.names$reference_id),]}

bio.names.unique <- as.matrix(unique(bio.names$name_1))

if (length(bio.names.unique) > 0){
  for (i in 1:NROW(bio.names.unique)){
    result <- compromise.selector(ts_used = 'B', subset( bio.names, name_1 == bio.names.unique[i]))
    query <- paste0("INSERT INTO temp_opinions_b (`rule_id`, `name`, `oldest`, `youngest`, `ts_count`, `reference`) VALUES ( ", rule_id, ",\"", result[1,1],"\", '",result[1,2],"', '",result[1,3],"', '",result[1,4], "', \"", result[1,5],"\")")
    # Run insert into SQL ...  
    dbGetQuery(rn.con, query)      
  }  
}
# We close this connection
dbDisconnect(rn.con)

#######################################################
### Rule 4: Direct relations between non-bio* names and bio* with the youngest.selector()
### This creates temp_opinions_c

rule_id <- 4
rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con,"delete from temp_opinions_c")
dbGetQuery(rn.con," SET NAMES utf8")

bio.try <- dbGetQuery(rn.con,"
                      select r.reference_id, r.name_1, name_2
                      from view_cron_relations r
                      where r.strat_qualifier_1 != 'Biostratigraphy'
                      and r.strat_qualifier_2 = 'Biostratigraphy'
                      and r.qualifier_name_1 != 'TimeSlice_Bergstrom'
                      group by 1, 2
                      order by 1, 2;
                      ")

bio.out <- subset(bio.try, bio.try$name_1 == "not specified"|bio.try$name_2 == "not specified")
ref.out <- unique(bio.out$reference_id)
name.out <- unique(rbind(bio.out$name_1, bio.out$name_2))
name.out <- unique(name.out[name.out != "not specified"])

bio.names <- dbGetQuery(rn.con,"
                        select r.reference_id, r.name_1, concat(GROUP_CONCAT(bio.`youngest`),',',GROUP_CONCAT(bio.`oldest`)) name_2
                        from view_cron_relations r
                        inner join `temp_opinions_a` bio
                        on r.`name_2`=bio.`name`
                        where r.strat_qualifier_1 != 'Biostratigraphy'
                        and r.strat_qualifier_2 = 'Biostratigraphy'
                        and r.qualifier_name_1 != 'TimeSlice_Bergstrom'
                        group by 1, 2;
                        ")

out.1 <- bio.names[grepl(paste(ref.out, collapse="|"), bio.names$reference_id), ]
out.2 <- out.1[grepl(paste(name.out, collapse="|"), out.1$name_1), ]
if (length(ref.out)>0) {bio.names <- bio.names[!(out.2$reference_id==bio.names$reference_id),]}
bio.names.unique <- as.matrix(unique(bio.names$name_1))

if (length(bio.names.unique) > 0){
  for (i in 1:NROW(bio.names.unique)){
    result <- youngest.selector(ts_used = 'B', subset( bio.names, name_1 == bio.names.unique[i]))
    query <- paste0("INSERT INTO temp_opinions_c (`rule_id`, `name`, `oldest`, `youngest`, `ts_count`, `reference`) VALUES ( ", rule_id, ",\"", result[1,1],"\", '",result[1,2],"', '",result[1,3],"', '",result[1,4], "', \"", result[1,5],"\")")
    # Run insert into SQL ...  
    dbGetQuery(rn.con, query)      
  }  
}
# We close this connection
dbDisconnect(rn.con)

##################################################################################
### Rule 5:  relations of non-bio* to non-bio* with link to bio* (route via temp-opinions_c) are selected via youngest.selector()

rule_id <- 5
rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con,"delete from temp_opinions_d")
dbGetQuery(rn.con," SET NAMES utf8")

bio.try <- dbGetQuery(rn.con,"
                      select r.reference_id, r.name_1, name_2
                      from view_cron_relations r
                      where r.strat_qualifier_1 != 'Biostratigraphy'
                      and r.strat_qualifier_1 != 'Chronostratigraphy'
                      and r.strat_qualifier_2 != 'Biostratigraphy'
                      and r.qualifier_name_1 != 'TimeSlice_Bergstrom'
                      group by 1, 2
                      order by 1, 2;
                      ")
bio.out <- subset(bio.try, bio.try$name_1 == "not specified"|bio.try$name_2 == "not specified")
ref.out <- unique(bio.out$reference_id)
name.out <- unique(rbind(bio.out$name_1, bio.out$name_2))
name.out <- unique(name.out[name.out != "not specified"])

bio.names_a <- dbGetQuery(rn.con,"
                        select r.reference_id, r.name_1, concat(GROUP_CONCAT(nme.oldest), ',', GROUP_CONCAT(nme.youngest)) name_2
                        from view_cron_relations r
                        inner join `temp_opinions_c` nme
                        on r.`name_2`=nme.`name`
                        and r.strat_qualifier_1 != 'Chronostratigraphy'
                        and r.strat_qualifier_1 != 'Biostratigraphy'
                        and r.strat_qualifier_2 != 'Biostratigraphy'
                        and r.qualifier_name_1 != 'TimeSlice_Bergstrom'
                        -- and r.name_1 = 'Pontyfenni'                       
                        group by 1, 2
                        order by 1, 2
                        ")

bio.names_b <- dbGetQuery(rn.con,"
                        select r.reference_id, r.name_1, concat(GROUP_CONCAT(nme.oldest), ',', GROUP_CONCAT(nme.youngest)) name_2
                          from view_cron_relations r
                          inner join `temp_opinions_c` nme
                          on r.`name_2`=nme.`name`
                          and r.strat_qualifier_1 != 'Lithostratigraphy'
                          and r.strat_qualifier_1 != 'Biostratigraphy'
                          and r.strat_qualifier_2 != 'Lithostratigraphy'
                          and r.strat_qualifier_2 != 'Biostratigraphy' 
                          and r.qualifier_name_1 != 'TimeSlice_Bergstrom'                     
                          group by 1, 2
                          order by 1, 2
                          ")

bio.names <- rbind(bio.names_b, bio.names_a)
out.1 <- bio.names[grepl(paste(ref.out, collapse="|"), bio.names$reference_id), ]
out.2 <- out.1[grepl(paste(name.out, collapse="|"), out.1$name_1), ]
if (length(ref.out)>0) {bio.names <- bio.names[!(out.2$reference_id==bio.names$reference_id),]}
bio.names.unique <- as.matrix(unique(bio.names$name_1))

if (length(bio.names.unique) > 0){
  for (i in 1:NROW(bio.names.unique)){
    result <- youngest.selector(ts_used = 'B', subset( bio.names, name_1 == bio.names.unique[i]))
    query <- paste0("INSERT INTO temp_opinions_d (`rule_id`, `name`, `oldest`, `youngest`, `ts_count`, `reference`) VALUES ( ", rule_id, ",\"", result[1,1],"\", '",result[1,2],"', '",result[1,3],"', '",result[1,4], "', \"", result[1,5],"\")")
    # Run insert into SQL ...  
    dbGetQuery(rn.con, query)      
  }  
}


repeat
{
  bio.names <- dbGetQuery(rn.con,"
                          select r.reference_id, r.name_1, concat(GROUP_CONCAT(bio.`youngest`),',',GROUP_CONCAT(bio.`oldest`)) name_2
                          from view_cron_relations r
                          inner join `temp_opinions_d` bio
                          on r.`name_2`=bio.`name`
                          where r.strat_qualifier_1 != 'Biostratigraphy'
                          and r.strat_qualifier_1 != 'Chronostratigraphy'
                          and r.strat_qualifier_2 != 'Biostratigraphy'
                          and r.qualifier_name_1 != 'TimeSlice_Bergstrom'
                          and r.`name_1` not in (select name from temp_opinions_d)
                          group by 1, 2;
                          ")
  out.1 <- bio.names[grepl(paste(ref.out, collapse="|"), bio.names$reference_id), ]
  out.2 <- out.1[grepl(paste(name.out, collapse="|"), out.1$name_1), ]
  if (length(ref.out)>0) {bio.names <- bio.names[!(out.2$reference_id==bio.names$reference_id),]}

  if (NROW(bio.names) == 0)
  {
    print("No more relations!");
    break
  } else { 
    rule_id <- rule_id + 0.1       
    bio.names.unique <- as.matrix(unique(bio.names$name_1))
    
    for (i in 1:NROW(bio.names.unique)){
      result <- compromise.selector(ts_used = 'B', subset( bio.names, name_1 == bio.names.unique[i]))
      query <- paste0("INSERT INTO temp_opinions_d (`rule_id`, `name`, `oldest`, `youngest`, `ts_count`, `reference`) VALUES ( ", rule_id, ",\"", result[1,1],"\", '",result[1,2],"', '",result[1,3],"', '",result[1,4], "', \"", result[1,5],"\")")
      # Run insert into SQL ...
      dbGetQuery(rn.con, query)
    }
  }  
}
# We close this connection
dbDisconnect(rn.con)

##################################################################################
### Rule 6: Relations of non-bio* to non-bio* with link to TS* (route via temp-opinions_b) are selected with compromise.selector()


rule_id <- 6
rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con,"delete from temp_opinions_e")
dbGetQuery(rn.con," SET NAMES utf8")

bio.names_a <- dbGetQuery(rn.con,"
                          select r.reference_id, r.name_1, concat(GROUP_CONCAT(nme.oldest), ',', GROUP_CONCAT(nme.youngest)) name_2
                          from view_cron_relations r
                          inner join `temp_opinions_b` nme
                          on r.`name_2`=nme.`name`
                          and r.strat_qualifier_1 != 'Chronostratigraphy'
                          and r.strat_qualifier_1 != 'Biostratigraphy'
                          and r.strat_qualifier_2 != 'Biostratigraphy'
                          and r.qualifier_name_1 != 'TimeSlice_Bergstrom'
                          -- and r.name_1 = 'Pontyfenni'
                          group by 1, 2
                          order by 1, 2
                          ")

bio.names_b <- dbGetQuery(rn.con,"
                          select r.reference_id, r.name_1, concat(GROUP_CONCAT(nme.oldest), ',', GROUP_CONCAT(nme.youngest)) name_2
                          from view_cron_relations r
                          inner join `temp_opinions_b` nme
                          on r.`name_2`=nme.`name`
                          and r.strat_qualifier_1 != 'Lithostratigraphy'
                          and r.strat_qualifier_1 != 'Biostratigraphy'
                          and r.strat_qualifier_2 != 'Lithostratigraphy'
                          and r.strat_qualifier_2 != 'Biostratigraphy' 
                          and r.qualifier_name_1 != 'TimeSlice_Bergstrom'                     
                          group by 1, 2
                          order by 1, 2
                          ")
bio.names <- rbind(bio.names_b, bio.names_a)
out.1 <- bio.names[grepl(paste(ref.out, collapse="|"), bio.names$reference_id), ]
out.2 <- out.1[grepl(paste(name.out, collapse="|"), out.1$name_1), ]
if (length(ref.out)>0) {bio.names <- bio.names[!(out.2$reference_id==bio.names$reference_id),]}
bio.names.unique <- as.matrix(unique(bio.names$name_1))

if (length(bio.names.unique) > 0){
  for (i in 1:NROW(bio.names.unique)){
    result <- youngest.selector(ts_used = 'B', subset( bio.names, name_1 == bio.names.unique[i]))
    query <- paste0("INSERT INTO temp_opinions_e (`rule_id`, `name`, `oldest`, `youngest`, `ts_count`, `reference`) VALUES ( ", rule_id, ",\"", result[1,1],"\", '",result[1,2],"', '",result[1,3],"', '",result[1,4], "', \"", result[1,5],"\")")
    # Run insert into SQL ...  
    dbGetQuery(rn.con, query)      
  }  
}

repeat
{
  bio.names <- dbGetQuery(rn.con,"
                          select r.reference_id, r.name_1, concat(GROUP_CONCAT(bio.`youngest`),',',GROUP_CONCAT(bio.`oldest`)) name_2
                          from view_cron_relations r
                          inner join `temp_opinions_e` bio
                          on r.`name_2`=bio.`name`
                          where r.strat_qualifier_1 != 'Biostratigraphy'
                          and r.strat_qualifier_1 != 'Chronostratigraphy'
                          and r.strat_qualifier_2 != 'Biostratigraphy'
                          and r.qualifier_name_1 != 'TimeSlice_Bergstrom'
                          and r.`name_1` not in (select name from temp_opinions_e)
                          group by 1, 2;
                          ")
    out.1 <- bio.names[grepl(paste(ref.out, collapse="|"), bio.names$reference_id), ]
    out.2 <- out.1[grepl(paste(name.out, collapse="|"), out.1$name_1), ]
    if (length(ref.out)>0) {bio.names <- bio.names[!(out.2$reference_id==bio.names$reference_id),]}
  
  if (NROW(bio.names) == 0)
  {
    print("No more relations!");
    break
  } else { 
    rule_id <- rule_id + 0.1       
    bio.names.unique <- as.matrix(unique(bio.names$name_1))
    
    for (i in 1:NROW(bio.names.unique)){
      result <- compromise.selector(ts_used = 'B', subset( bio.names, name_1 == bio.names.unique[i]))
      query <- paste0("INSERT INTO temp_opinions_e (`rule_id`, `name`, `oldest`, `youngest`, `ts_count`, `reference`) VALUES ( ", rule_id, ",\"", result[1,1],"\", '",result[1,2],"', '",result[1,3],"', '",result[1,4], "', \"", result[1,5],"\")")
      # Run insert into SQL ...
      dbGetQuery(rn.con, query)
    }
  }  
}
# We close this connection
dbDisconnect(rn.con)

##################################################################################
##################################################################################
#### We combine the five temp_opinion files and again search for the best correlation

rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con," SET NAMES utf8")
opba <- dbGetQuery(rn.con,"select * from temp_opinions_a"); ## bio to TS
opbb <- dbGetQuery(rn.con,"select * from temp_opinions_b"); ## non-bio to TS
opbc <- dbGetQuery(rn.con,"select * from temp_opinions_c"); ## non-bio to bio to TS
opbd <- dbGetQuery(rn.con,"select * from temp_opinions_d"); ## non-bio to non-bio to bio to TS
opbe <- dbGetQuery(rn.con,"select * from temp_opinions_e"); ## non-bio to non-bio to TS
on.exit(dbDisconnect(rn.con))

### search for shortest
coa <- combib.temp(opbb, opbe)
cob <- combib.temp(opbc, opbd)

com <- merge (coa, cob, by.x = "name", by.y = "name")
cax <- subset(com, ts_count.x < ts_count.y, select = c("name", "id.x", "date_time.x", "rule_id.x", "oldest.x", "youngest.x", "ts_count.x", "reference.x")); ## these are the shorter
cay <- subset(com, ts_count.x > ts_count.y, select = c("name", "id.y", "date_time.y", "rule_id.y", "oldest.y", "youngest.y", "ts_count.y", "reference.y")); ## these are the shorter indirectly linked names 
cas <- subset(com, ts_count.x == ts_count.y, select = c("name", "id.x", "date_time.y", "rule_id.y", "oldest.y", "youngest.y", "ts_count.y", "reference.x", "reference.y")); ## these are the names with identical length
#cs <- cbind(cas[,1:7], paste(cas$reference.x, cas$reference.y, sep= ", ")) # with all references
cs <- cas[,c(1:7,9)] # more consequent only with y references
in.b <- subset(cob, !cob$name %in% com$name, select = c("name", "id", "date_time", "rule_id", "oldest", "youngest", "ts_count", "reference")); ## the ones in b that are not overlapping
in.a <- subset(coa, !coa$name %in% com$name, select = c("name", "id", "date_time", "rule_id", "oldest", "youngest", "ts_count", "reference")); ## the ones in a that are not overlapping
combi.names <- rbind(in.a, in.b, setNames(cax, names(in.a)), setNames(cay, names(in.a)), setNames(cs, names(in.a)))
combi.names <- subset(combi.names, combi.names$ts_count<21)

####### clean reference column
ron <- array(, dim=c(NROW(combi.names),1))
for (i in 1:NROW(combi.names)){
    ra1  <- as.character(combi.names$reference)
   ra <- as.data.table(strsplit(ra1[i], ", "))
   rax <- paste(unique(ra$V1), collapse= ", ")
   ron[i] <- rbind(rax)
} 
colnames(ron) <- "reference"
oai <- opba[,c(1:7, 12)]
obi <- cbind(combi.names[, c(2, 3, 4, 1, 5:7)], ron)

B_TS <- rbind(oai, obi)
B_TS$id <- 1:nrow(B_TS)
rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con," SET NAMES utf8")
dbGetQuery(rn.con,"delete from opinions_bergstrom")
dbWriteTable(rn.con, "opinions_bergstrom", B_TS, row.names=FALSE, append=TRUE)
dbGetQuery(rn.con,"delete from opinions_bergstrom_reference")
dbGetQuery(rn.con,re_query_B)
dbDisconnect(rn.con)

write.csv(B_TS, file = "B_TS.csv")
