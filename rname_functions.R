### functions for rnames

library(DBI)
library(RMySQL)
library(gdata)
library(splitstackshape)

##### Variable for rnames
m.drv <- dbDriver("MySQL")
db.url <- "mysql.luomus.fi"
db.port <- "3306"  
db.user <- "xxx" ############ ---> insert you user name here
db.pw <- "xxx" ############ ---> insert you user password here
db.db <- "rnames"
rn.con <- dbConnect(m.drv, dbname = db.db, username = db.user, password = db.pw, host = db.url)

Bergst_ts <- c("Camb", "Tr1", "Tr2", "Tr3", "Fl1", "Fl2", "Fl3", "Dp1", "Dp2", "Dp3", "Dw1", "Dw2", "Dw3", "Sa1", "Sa2", "Ka1", "Ka2", "Ka3", "Ka4", "Hi1", "Hi2", "Sil")
names(Bergst_ts) <- 1:22
Bergst_ts <- as.data.frame(Bergst_ts)

Webby_ts <- c("Camb","1a", "1b", "1c", "1d", "2a", "2b", "2c", "3a", "3b", "4a", "4b", "4c", "5a", "5b", "5c", "5d", "6a", "6b", "6c", "Sil")
names(Webby_ts) <- 1:21
Webby_ts <- as.data.frame(Webby_ts)

Kroger_ts <- c("Camb", "Tr1", "Tr2", "Fl1", "Fl2", "Fl3", "Dp1", "Dp2", "Dw1", "Dw2", "Dw3", "Sa1", "Sa2", "Ka1", "Ka2", "Ka3", "Ka4", "Hi1", "Hi2", "Sil")
names(Kroger_ts) <- 1:20
Kroger_ts <- as.data.frame(Kroger_ts)

Chrono_ts <- c("Cambrian", "Tremadocian", "Floian", "Dapingian", "Darriwilian", "Sandbian", "Katian", "Hirnantian", "Silurian")
names(Chrono_ts) <- 1:9
Chrono_ts <- as.data.frame(Chrono_ts)

Bergst_abs <- c("497.0","485.4", "480.3", "480.0", "477.7", "473.7", "472.4", "470.0", "468.6", "468.2", "467.3", "465.3", "462.5", "458.4", "456.6", "453.0", "450.4", "448.9", "448.5", "445.2", "444.5", "443.8","440.8") # gives bases of StS, based on Cooper & Sadler (2012)
names(Bergst_abs) <- 1:23
Bergst_abs <- as.data.frame(Bergst_abs)
b.diff <-  -as.numeric(as.character(Bergst_abs[-1,]))+as.numeric(as.character(Bergst_abs[1:nrow(Bergst_abs)-1,]))

Chrono_abs <- c("497.0","485.4", "477.7", "470.0", "467.3", "458.4", "453.0", "445.2", "443.8","440.8") # gives bases of stages, based on Cooper & Sadler (2012)
names(Chrono_abs) <- 1:10
Chrono_abs <- as.data.frame(Chrono_abs)
c.diff <-  -as.numeric(as.character(Chrono_abs[-1,]))+as.numeric(as.character(Chrono_abs[1:nrow(Chrono_abs)-1,]))

Webby_abs <- c("497.0","485.4", "480.3", "480.0", "479.0", "477.7", "476.0", "473.4", "470.0", "468.7", "467.3", "465.3", "462.5", "458.4", "456.7", "453.8", "450.3", "448.4", "447.0", "445.2", "443.8", "440.8")# based on Cooper & Sadler (2012)
names(Webby_abs) <- 1:22
Webby_abs <- as.data.frame(Webby_abs)
w.diff <-  -as.numeric(as.character(Webby_abs[-1,]))+as.numeric(as.character(Webby_abs[1:nrow(Webby_abs)-1,]))


########################################		
########################################
######## This function is used in CREATE_script. It selects timeslices out of name_table based on compromise algorithm

compromise.selector <- function (ts_used, name_table_to_ts) {
  
	if (ts_used == "B") {nts <- Bergst_ts}
	if (ts_used == "W") {nts <- Webby_ts}
	if (ts_used == "K") {nts <- Kroger_ts}
  if (ts_used == "C") {nts <- Chrono_ts};
	
bios <- name_table_to_ts
ebios <- cSplit(bios, "name_2", ",", direction = "long");
ele <- unique(bios$name_1)
lele <- length(ele)
bio.sel <- array(, dim=c(0,5))
colnames(bio.sel) <- c("name", "oldest", "youngest", "tscount", "refs");
		
for (i in 1:lele) {
		bio_set <- subset(ebios, ebios$name_1 == ele[i])
			bio_out <- subset(bio_set, bio_set$name_2 == "not specified")
      			refout <- unique(bio_out$reference_id)
      			bio_set <- subset(bio_set, !(bio_set$reference_id %in% refout))
		i.n <- as.character(ele[i])
		u.ref <- unique(bio_set$reference_id)
		rle <- length(u.ref)
		nle <- nrow(nts)
    counts.per_ts <- array(, dim=c(0,5)) 
		colnames(counts.per_ts) <- c("oldest", "youngest", "refs", "refcount", "tscount")
			for (l in 1:nle) {				
				for (m in l:nle) { 
	 							name.c <-vector();
								refnamesa <- subset(bio_set, as.character(bio_set$name_2) == as.character(nts[m,]));
								name.c <-refnamesa$reference_id;
	 							counts.per_ts <- rbind (counts.per_ts, c(l, m, paste(as.character(unique(name.c)), collapse=", ") , length(unique(name.c)), m-l+1)); ## builds array with interval max/min (=l, m), list of containing refs, length of that lisd, and length of interval; check with any number for l m
								}	
							}
			######## this is selection core #############	
		
			cpts <- data.frame(counts.per_ts)
			cpts.refmax <- subset(cpts, as.numeric(levels(cpts$refcount))[cpts$refcount] == max(as.numeric(levels(cpts$refcount))[cpts$refcount])) ## selects interval with max nr of references per name
			cpts.tsmin <- subset(cpts.refmax, as.numeric(levels(cpts.refmax$tscount))[cpts.refmax$tscount] == min(as.numeric(levels(cpts.refmax$tscount))[cpts.refmax$tscount])) ## selects interval with min nr of time bins
			
			## this builds the output array #############
			i.on <- min(as.numeric(levels(cpts.tsmin$oldest))[cpts.tsmin$oldest])
			i.yn <- max(as.numeric(levels(cpts.tsmin$youngest))[cpts.tsmin$youngest])
			i.r <- cSplit(cpts.tsmin, "refs", ",", direction = "long")
			i.r <- paste(as.character(unique(i.r$refs)), collapse=", ") #??
			i.c <- as.character(levels(cpts.tsmin$refcount))[cpts.tsmin$refcount]
			i.c <- i.c[1]
			i.o <- nts[i.on,1]
			i.o <- as.character(levels(i.o))[i.o]
			i.y <- nts[i.yn,1]
			i.y <- as.character(levels(i.y))[i.y]
			bio.sel <- rbind(bio.sel,c(i.n, i.o, i.y, i.yn-i.on+1, i.r))			
		}
		return(bio.sel)	
		}

########################################
########################################
######## This function is used in CREATE_script. It selects timeslices out of name_table based on youngest algorithm

youngest.selector <- function (ts_used, name_table_to_ts) {
  if (ts_used == "B") {nts <- Bergst_ts}
  if (ts_used == "W") {nts <- Webby_ts}
  if (ts_used == "K") {nts <- Kroger_ts}
  if (ts_used == "C") {nts <- Chrono_ts}
  
  bios <- name_table_to_ts
  ebios <- cSplit(bios, "name_2", ",", direction = "long");
  
  ele <- unique(bios$name_1)
  lele <- length(ele)
  
  bio.sel <- array(, dim=c(0,5));
  colnames(bio.sel) <- c("name", "oldest", "youngest", "tscount", "refs");
  
  for (i in 1:lele) {
    bio_set <- subset(ebios, ebios$name_1 == ele[i])
	  bio_out <- subset(bio_set, bio_set$name_2 == "not specified")
      	  refout <- unique(bio_out$reference_id)
      	  bio_set <- subset(bio_set, !(bio_set$reference_id %in% refout))
    i.n <- as.character(ele[i])
    i.d <- as.vector(strsplit(as.character(bio_set$reference_id), " "))
    i.d <- as.data.table(i.d)
    i.d <- as.character(i.d[1,])
    bio_set <-cbind(i.d, bio_set)
    u.ref <- unique(bio_set$i.d)
    refu <- reftab[reftab$id %in% u.ref,] ### the next three lines select youngest refs
    ####
    refy <- subset(refu, refu$year == max(refu$year))
    bio_set<- bio_set[bio_set$i.d %in% refy$id,]
    rle <- length(refy)
    nle <- nrow(nts)
    counts.per_ts <- array(, dim=c(0,5)) 
    colnames(counts.per_ts) <- c("oldest", "youngest", "refs", "refcount", "tscount")
    for (l in 1:nle) {					# l starting value of interval
      for (m in l:nle) { 				# m starting value of interval, runs through ts
        name.c <-vector();
        refnamesa <- subset(bio_set, as.character(bio_set$name_2) == as.character(nts[m,])); #test use 17
        name.c <-refnamesa$reference_id; # hier nur die refnames
        counts.per_ts <- rbind (counts.per_ts, c(l, m, paste(as.character(unique(name.c)), collapse=", ") , length(unique(name.c)), m-l+1)); ## builds array with interval max/min (=l, m), list of containing refs, length of that lisd, and length of interval; check with any number for l m
      }	
    }	
    ## this is selection core #############	
    cpts <- data.frame(counts.per_ts)
    cpts.refmax <- subset(cpts, as.numeric(levels(cpts$refcount))[cpts$refcount] == max(as.numeric(levels(cpts$refcount))[cpts$refcount])) ## selects interval with max nr of references / formations
    cpts.tsmin <- subset(cpts.refmax, as.numeric(levels(cpts.refmax$tscount))[cpts.refmax$tscount] == min(as.numeric(levels(cpts.refmax$tscount))[cpts.refmax$tscount])) ## selects interval with min nr of ts
    
    ## this builds the output array #############
    i.on <- min(as.numeric(levels(cpts.tsmin$oldest))[cpts.tsmin$oldest])
    i.yn <- max(as.numeric(levels(cpts.tsmin$youngest))[cpts.tsmin$youngest])
    i.r <- cSplit(cpts.tsmin, "refs", ",", direction = "long")
    i.r <- paste(as.character(unique(i.r$refs)), collapse=", ")
    i.c <- as.character(levels(cpts.tsmin$refcount))[cpts.tsmin$refcount]
    i.c <- i.c[1]
    i.o <- nts[i.on,1]
    i.o <- as.character(levels(i.o))[i.o]
    i.y <- nts[i.yn,1]
    i.y <- as.character(levels(i.y))[i.y]
    bio.sel <- rbind(bio.sel,c(i.n, i.o, i.y, i.yn-i.on+1, i.r))			
  }
  return(bio.sel)	
}


########################################
#######################################
###function for combining two temp_opinion data.frames in CREATE scripts
combi.temp <- function(tempa,tempb){
  com <- merge (tempa, tempa, by.x = "name", by.y = "name")
  cax <- subset(com, ts_count.x < ts_count.y, select = c("name", "id.x", "date_time.x", "rule_id.x", "oldest.x", "youngest.x", "ts_count.x", "reference.x")); ## these are the shorter
  cay <- subset(com, ts_count.x > ts_count.y, select = c("name", "id.y", "date_time.y", "rule_id.y", "oldest.y", "youngest.y", "ts_count.y", "reference.y")); ## these are the shorter indirectly linked names 
  cas <- subset(com, ts_count.x == ts_count.y, select = c("name", "id.x", "date_time.y", "rule_id.y", "oldest.y", "youngest.y", "ts_count.y", "reference.x", "reference.y")); ## these are the names with identical length
  cxa <- merge (cas, cax, by.x = "name", by.y = "name"); # just a comparison
  cya <- merge (cas, cay, by.x = "name", by.y = "name"); # just a comparison
  cs <- cbind(cas[,1:7], paste(cas$reference.x, cas$reference.y, sep= ", "))
  in.b <- subset(tempb, !tempb$name %in% com$name, select = c("name", "id", "date_time", "rule_id", "oldest", "youngest", "ts_count", "reference")); ## the ones in b that are not overlapping
  in.a <- subset(tempa, !tempa$name %in% com$name, select = c("name", "id", "date_time", "rule_id", "oldest", "youngest", "ts_count", "reference")); ## the ones in a that are not overlapping
  combi.names <- rbind(in.a, in.b, setNames(cax, names(in.a)), setNames(cay, names(in.a)), setNames(cs, names(in.a)))
  return(combi.names)
}

combib.temp <- function(tempa,tempb){
  com <- merge (tempa, tempa, by.x = "name", by.y = "name")
  cas <- subset(com, ts_count.x == ts_count.y, select = c("name", "id.x", "date_time.y", "rule_id.y", "oldest.y", "youngest.y", "ts_count.y", "reference.x", "reference.y")); ## these are the names with identical length
  cs <- cbind(cas[,1:7], paste(cas$reference.x, cas$reference.y, sep= ", "))
  in.b <- subset(tempb, !tempb$name %in% tempa$name, select = c("name", "id", "date_time", "rule_id", "oldest", "youngest", "ts_count", "reference")); ## the ones in b that are not in a
  in.a <- subset(tempa, !tempa$name %in% cas$name, select = c("name", "id", "date_time", "rule_id", "oldest", "youngest", "ts_count", "reference")); ## the ones in a that are not overlapping
  combi.names <- rbind(in.a, in.b, setNames(cs, names(in.a)))
  return(combi.names)
}
