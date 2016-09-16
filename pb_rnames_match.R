#########################################################################################################################################
###### This file creates a list of time binned Paleobiology Database occurrences based on opinions tables in Rnames ################################
###### currently three options are available for time binning: Global Stages, Bergström et al. (2009), and Webby et al. (2004) TS, #####
###### the result will be written into a csv file ######################################################################################
########################################################################################################################################
###### before running specify work directory, time resolution, and data sources ######
##### !run needs several hours! ###

library(DBI)
library(RMySQL)
library(gdata)
source("/Users/bkroger/Documents/r/rnames/rname_finals/rname_functions.r");
setwd("/Users/bkroger/Documents/r/rnames/rname_finals/");

###########################
#### select time resolution 
selection <- "b" ## b - bergström stage slices, c - chronostrat. stages, w - webby time slices


############################
### optional: download from PBDB Data from original sources or use previously downloaded files

http<-paste("http://paleobiodb.org/data1.2/occs/list.txt?interval=Ordovician&show=stratext,geo,loc")
http2<-paste("http://paleobiodb.org/data1.2/colls/list.txt?interval=Ordovician&show=ref,loc,stratext,paleoloc")

ordo_tot <-read.table(http, sep=',', header=T)
coll_tot <- read.table(http2, sep=',', header=T)
coll_tots <- coll_tot[,c(1,36)]

write.csv(ordo_tot, file="pbdb_ds.csv")
write.csv(coll_tots, file="coll_tots.csv")

### This is a shortcut if files are already downloaded from PBDB
#ordo_tot <-read.table(file="pbdb_ds.csv", sep=',', header=T)
#coll_tots <-read.table(file="coll_tots.csv", sep=',', header=T)

############################
### optional: download opinion tables from Rnames DB or use previously downloaded files

# rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
# dbGetQuery(rn.con," SET NAMES utf8")
# opb <- dbGetQuery(rn.con,"select * from opinions_bergstrom")
# opb <- cbind(opb[,1], opb)
# opc <- dbGetQuery(rn.con,"select * from opinions_chrono")
# opc <- cbind(opc[,1], opc)
# opw <- dbGetQuery(rn.con,"select * from opinions_webby")
# opw <- cbind(opw[,1], opw)
# dbDisconnect(rn.con)

opb <-read.table(file="B_TS.csv", sep=',', header=T) # load Bergstroem ts data
opc <-read.table(file="C_TS.csv", sep=',', header=T) # load stage slice data
opw <-read.table(file="W_TS.csv", sep=',', header=T) # load Webby slice data

bts <- cbind(Bergst_ts,row.names(Bergst_ts))
wts <- cbind(Webby_ts,row.names(Webby_ts))
cts <- cbind(Chrono_ts,row.names(Chrono_ts))

#########################
########## additional preparation of input data

opc[(nrow(opc)+1),] <- c("","","","0", "Katian", "Katian", "Katian", "1","0")
levels(opc[,5])[nrow(opc)] <- "Katian"
opc[(nrow(opc)),5] <- "Katian"
ordo_tot <- subset(ordo_tot, ordo_tot$identified_rank == "genus" | ordo_tot$identified_rank == "species" ||  ordo_tot$identified_no>0)
ordo_tot <- subset(ordo_tot, ordo_tot$identified_no>0)

os <- as.data.frame(ordo_tot[,c("early_interval", "late_interval", "stratgroup", "formation", "member", "zone", "localbed", "regionalbed", "stratcomments")])
colnames(os) <- c("early_interval", "late_interval", "stratgroup", "formation", "member", "zone", "localbed", "regionalbed", "stratcomments")

osu <- unique(os)

os.c <- os
os.c <- as.data.frame(os.c)
os.c$fmm <- apply(format(os.c[,4:5]), 1, paste, collapse=" ")
os.c$flb <- apply(format(os.c[,c(4,7)]), 1, paste, collapse=" ")
os.c$id  <- 1:nrow(os.c)

#############################
#### load manually matched PBDB names from Rnames (= names in Rnames table "pbdb_names" which are linked to structured_names in Rnames

rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con," SET NAMES utf8")
pb.m <- dbGetQuery(rn.con,"select * from pbdb_name where structured_name_id is not null")
dbDisconnect(rn.con)
pb.m <- pb.m[,4:6]
pb.u <- pb.m[!(duplicated(pb.m[c("pbdb_name","structured_name_id")]) | duplicated(pb.m[c("pbdb_name","structured_name_id")])), ]

rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con," SET NAMES utf8")
q.r <- dbGetQuery(rn.con,"select * from qualifier")
dbDisconnect(rn.con)

rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con," SET NAMES utf8")
n.r <- dbGetQuery(rn.con,"select * from name")
dbDisconnect(rn.con)

rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)
dbGetQuery(rn.con," SET NAMES utf8")
sn.r <- dbGetQuery(rn.con,"select * from structured_name")
dbDisconnect(rn.con)

########################
####### merge PBDB  data with Rnames: os.c = prepared PBDB occurrences; opx = time frames, either: opb= bergs, opw=webby, opc=chronostages
####### x_ts: ts scheme from rname_functions.R either Bergstr_ts, or Webby_ts, or Chrono_ts
####### xts: ts scheme from above, either: bts, wts, or cts

######### here we define resolution of analysis
if (selection == "b") {opx <- opb;
                      x_ts <- "Bergst_ts";
                      xts <- bts
                      }
if (selection == "c") {opx <- opc;
                      x_ts <- "Chrono_ts";
                      xts <- cts
                      }
if (selection == "w") {opx <- opw;
                      x_ts <- "Webby_ts";
                      xts <- wts
                      }

######### here we combine PBDB data with xts
#########
sn.1 <- merge(x=sn.r, y=n.r, by.x = "name_id", by.y = "id", all.x=TRUE)
sn.1 <- merge(x=sn.1, y=q.r, by.x = "qualifier_id", by.y = "id", all.x=TRUE)
sn <- sn.1[,c(3, 11, 18, 19)]
pbdb.sn <-  merge(x=pb.u, y=sn, by.x = "structured_name_id", by.y = "id", all.x=TRUE)
op.f <-  merge(x=pbdb.sn, y=opx, by.x = "name", by.y = "name", all.x=TRUE)

######### here we create subset matches for each pbdb_qualifier

ea <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="early_interval"), by.x = "early_interval", by.y = "pbdb_name", all.x=TRUE)
ea <- ea[with(ea, order(ea$id.x)), ]

la <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="late_interval"), by.x = "late_interval", by.y = "pbdb_name", all.x=TRUE)
la <- la[with(la, order(la$id.x)), ]

sa <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="stratgroup"), by.x = "stratgroup", by.y = "pbdb_name", all.x=TRUE)
sa <- sa[with(sa, order(sa$id.x)), ]

fa <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="formation"), by.x = "formation", by.y = "pbdb_name", all.x=TRUE)
fa <- fa[with(fa, order(fa$id.x)), ]

ma <- merge(x=os.c, y=unique(subset(op.f, op.f$pbdb_qualifier=="member")), by.x = "member", by.y = "pbdb_name", all.x=TRUE)
ma <- ma[with(ma, order(ma$id.x)), ]

za <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="zone"), by.x = "zone", by.y = "pbdb_name", all.x=TRUE)
za <- za[with(za, order(za$id.x)), ]

lla <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="localbed"), by.x = "localbed", by.y = "pbdb_name", all.x=TRUE)
lla <- lla[with(lla, order(lla$id.x)), ]

ra <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="regionalbed"), by.x = "regionalbed", by.y = "pbdb_name", all.x=TRUE)
ra <- ra[with(ra, order(ra$id.x)), ]

sta <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="stratcomments"), by.x = "stratcomments", by.y = "pbdb_name", all.x=TRUE)
sta <- sta[with(sta, order(sta$id.x)), ]
sta <- sta[!(duplicated(sta[c("id.x","stratcomments")]) | duplicated(sta[c("id.x","stratcomments")])), ]
#xl <- as.data.frame(duplicated(sta$id.x))

fma <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="fmm"), by.x = "fmm", by.y = "pbdb_name", all.x=TRUE)
fma <- fma[with(fma, order(fma$id.x)), ]

fba <- merge(x=os.c, y=subset(op.f, op.f$pbdb_qualifier=="flb"), by.x = "flb", by.y = "pbdb_name", all.x=TRUE)
fba <- fba[with(fba, order(fba$id.x)), ]

eat <- merge(x=ea, y=xts, by.x = "oldest", by.y = x_ts, all.x=TRUE)
eat <- eat[with(eat, order(eat$id.x)), ]
colnames(eat)[18] <- "reference"
colnames(eat)[19] <- "ts_number"

lat <- merge(x=la, y=xts, by.x = "youngest", by.y = x_ts, all.x=TRUE)
lat <- lat[with(lat, order(lat$id.x)), ]
colnames(lat)[19] <- "reference"
colnames(lat)[19] <- "ts_number"

########################
######### we find shortest interval within different PBDB options for each occurrence and combine this with ordo_res this takes several hours to calculate

lelo <- nrow(ra)
ordo_res <- array()
for (i in 1:nrow(ra)) {
  iresult <- matrix(0,1,5, dimnames=list(c(),c("oldest", "youngest", "ts_count", "rules", "refs")))
  
  ## cont.a collects interval length in different resolutions
  cont.a <- cbind((as.numeric(as.character(lat$ts_number[i]))-as.numeric(as.character(eat$ts_number[i]))), 
                  as.numeric(as.character(ea$ts_count[i])),
                  as.numeric(as.character(la$ts_count[i])),
                  as.numeric(as.character(sa$ts_count[i])), 
                  as.numeric(as.character(fa$ts_count[i])), 
                  as.numeric(as.character(ma$ts_count[i])), 
                  as.numeric(as.character(za$ts_count[i])), 
                  as.numeric(as.character(lla$ts_count[i])),
                  as.numeric(as.character(sta$ts_count[i])),
                  as.numeric(as.character(fma$ts_count[i])),
                  as.numeric(as.character(fba$ts_count[i])),
                  as.numeric(as.character(ra$ts_count[i])))
  cont.a[is.na(cont.a)] <- 1111
  
  ## interval, eai, lai, etc. concentenate all necassary fields for iresult
  interval <- as.data.frame(c(as.character(eat$oldest[i]), as.character(lat$youngest[i]), cont.a[1], as.character(lat$rule_id[i]), as.character(lat$reference[i])))
  eai <- as.data.frame(c(as.character(ea$oldest[i]), as.character(ea$youngest[i]), cont.a[2], as.character(ea$rule_id[i]), as.character(ea$reference[i])))
  lai <- as.data.frame(c(as.character(la$oldest[i]), as.character(la$youngest[i]), cont.a[3], as.character(la$rule_id[i]), as.character(la$reference[i])))
  sai <- as.data.frame(c(as.character(sa$oldest[i]), as.character(sa$youngest[i]), cont.a[4], as.character(sa$rule_id[i]), as.character(sa$reference[i])))
  fai<- as.data.frame(c(as.character(fa$oldest[i]), as.character(fa$youngest[i]), cont.a[5], as.character(fa$rule_id[i]), as.character(fa$reference[i])))
  mai<- as.data.frame(c(as.character(ma$oldest[i]), as.character(ma$youngest[i]), cont.a[6], as.character(ma$rule_id[i]), as.character(ma$reference[i])))
  zai<- as.data.frame(c(as.character(za$oldest[i]), as.character(za$youngest[i]), cont.a[7], as.character(za$rule_id[i]), as.character(za$reference[i])))
  llai<- as.data.frame(c(as.character(lla$oldest[i]), as.character(lla$youngest[i]), cont.a[8], as.character(lla$rule_id[i]), as.character(lla$reference[i])))
  stai<- as.data.frame(c(as.character(sta$oldest[i]), as.character(sta$youngest[i]), cont.a[9], as.character(sta$rule_id[i]), as.character(sta$reference[i])))
  fmai<- as.data.frame(c(as.character(fma$oldest[i]), as.character(fma$youngest[i]), cont.a[10], as.character(fma$rule_id[i]), as.character(fma$reference[i])))
  fbai<- as.data.frame(c(as.character(fba$oldest[i]), as.character(fba$youngest[i]), cont.a[11], as.character(fba$rule_id[i]), as.character(fba$reference[i])))
  rai<- as.data.frame(c(as.character(ra$oldest[i]), as.character(ra$youngest[i]), cont.a[12], as.character(ra$rule_id[i]), as.character(ra$reference[i])))
  
  ## cont.b combines all necessary fields from each resolution
  cont.b <- cbind(interval, eai, lai, sai, fai, mai, zai, llai, stai, fmai, fbai, rai)
  cont.b <- t(cont.b)
  
  ## we select either one interval or interval range
  if(cont.b[3,3]=="1111"){cont.b<- cont.b[-3,]} else {cont.b<- cont.b[c(-1,-2),]}
  
  ## we select the highest resolution of the occurrence
  initi <- unique(subset(cont.b,cont.b[,3]== min(as.numeric(as.character(cont.b[,3])))))
  colnames(initi) <- c("a", "b", "c", "d", "e")
  
  ## in case there are more than one equally long resolutions we select the most direct correlation (lowest rule)
  if(nrow(initi)>1){
    initi <- subset(initi,initi[,4] == min(as.numeric(as.character(initi[,4]))))
  } 
  
  ## in case there are more than one equally long most direct correlation (lowest rule), we select with the best theoretical solution
  if(nrow(initi)>1){
    initi <- initi[max(nrow(initi)),]
  } 
  
  ## in case of complete nonmatch we create iresult with NAs
  if(unique(cont.b[,3])=="1111"&&length(unique(cont.b[,3]))==1){iresult[1,]<- c("NA", "NA", "NA", "NA", "NA")}
  
  iresult[1,1] <- as.character(initi[1])
  iresult[1,2] <- as.character(initi[2])
  iresult[1,3] <- as.character(initi[3])
  iresult[1,4] <- as.character(initi[4])
  iresult[1,5] <- as.character(initi[5])
  
  #rbind for each i
  ordo.line <- cbind(ordo_tot[i,], iresult)
  ordo_res <- rbind(ordo_res, ordo.line)
}
ordo_res <- ordo_res[-1,]

################# write out result

if (selection == "b") {write.csv(ordo_res, file = "ordo_match_bergst.csv")}
if (selection == "c") {write.csv(ordo_res, file = "ordo_match_chrono.csv")}
if (selection == "w") {write.csv(ordo_res, file = "ordo_match_webby.csv")}
