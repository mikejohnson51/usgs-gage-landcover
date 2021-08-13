library(dplyr)
library(sf)
library(units)

collect_lc_stats = function(ID, rast, basins){
  n   = names(rast) 
  bas = vect(filter(basins, GAGE_ID == ID ))
  
  mask(crop(rast, bas), bas) %>% 
    terra::freq() %>% 
    mutate(layer = n[layer]) %>% 
    select(layer, landcover = value, count) %>% 
    tidyr::pivot_wider(id_cols = c(landcover),
                       names_from = layer, values_from = count) %>% 
    mutate(landcover = substr(landcover,1,1)) %>% 
    group_by(landcover) %>% 
    summarise(lc2001 = sum(get(n[1])), lc2019 = sum(get(!!n[2]))) %>% 
    mutate(GAGE_ID = ID,
           delta = get(n[2]) - get(n[1]), 
           perD  = round((100 * delta / sum(get(n[1]))), 3))
}

# Get the Basins! ----------------------------------------------------------

# url <- "https://water.usgs.gov/GIS/dsdl/boundaries_shapefiles_by_aggeco.zip"
# out <- tempdir(check = TRUE)
# out_f <- file.path(out, basename(url))
# if(!file.exists(file.path(out, basename(url)))) { download.file(url, destfile = out_f) }
# outdir <- gsub(".zip", "", out_f)
# try(zip::unzip(out_f, overwrite = FALSE, exdir = outdir))
# 
# f = list.files(outdir, recursive = TRUE, pattern = "shp", full.names = TRUE)
# 
# o = lapply(1:length(f), function(x){
#   read_sf(f[x]) %>%
#     mutate(AREA = drop_units(set_units(st_area(.), "km2"))) %>%
#     filter(between(AREA, 0, 25000))
# })
# 
# out = bind_rows(o)
# 
# mat = data.frame(from = seq(1,nrow(out), by = 500), to = c(seq(500,nrow(out), by = 500), nrow(out))) 
# 
# for(i in 1:nrow(mat)){
# file = paste0("data/sites_",mat$from[i], "-", mat$to[i] ,".csv")
# 
# if(!file.exists(file)){
#   obs = dataRetrieval::whatNWISdata(sites=out$GAGE_ID[mat$from[i]:mat$to[i]], 
#                                     service = "dv", 
#                                     parameterCd = c("00060"))
#   
#   obs = dplyr::select(obs, GAGE_ID = site_no, huc_cd, begin_date, end_date, count_nu)
#   
#   write.csv(obs, file = file)
# }
# message(i)
# }
# }
# 
# file = list.files("./data/", pattern = "sites_", full.names = TRUE)
# meta = bind_rows(lapply(file, read.csv)) %>% 
#   mutate(GAGE_ID = sprintf("%08s", GAGE_ID), X = NULL)
# 
# tt = left_join(out, meta, by = "GAGE_ID") %>% 
#   filter(!duplicated(.)) %>% 
#   group_by(GAGE_ID) %>% 
#   slice_max(as.Date(end_date), n = 1) %>% 
#   slice_max(count_nu, n = 1) %>% 
#   ungroup()

# sfarrow::st_write_parquet(tt, "data/usgs-lc-basins.parquet")


# LC Data ! -----------------------------------------------------------

library(terra)
library(dplyr)
library(sf)

basins = sfarrow::st_read_parquet('data/usgs-lc-basins.parquet') 

conus = AOI::aoi_get(state = "conus") %>% 
  st_transform(st_crs(basins))

basins = sf::st_filter(basins, conus)

test_basins = basins %>% mutate(begin_date = as.Date(begin_date),
                  end_date   = as.Date(end_date)) %>% 
  filter(end_date > "1980-01-01") %>% 
  mutate(forcing_record_begin = pmax(begin_date, as.Date('1980-01-01')),
         forcing_record_end = pmin(end_date, as.Date("2019-12-31")),
         lc_record_begin = pmax(begin_date, as.Date('2001-01-01')),
         lc_record_end = pmin(end_date, as.Date("2019-12-31"))) %>% 
  mutate(forcing_days = as.numeric(difftime(forcing_record_end, forcing_record_begin)),
         lc_days = as.numeric(difftime(lc_record_end, lc_record_begin)),
         forcing_years = round(forcing_days/365.25, 2),
         lc_years = round(lc_days/365.25, 2),
         ) %>% 
  filter(lc_years >= 10)

paths = c('/Users/mjohnson/Downloads/nlcd_2001_land_cover_l48_20210604/nlcd_2001_land_cover_l48_20210604.img',
          '/Users/mjohnson/Downloads/nlcd_2019_land_cover_l48_20210604/nlcd_2019_land_cover_l48_20210604.img')

rast  = rast(paths) %>% 
  setNames(c("lc2001", 'lc2019'))

library(foreach)

mat = data.frame(from = seq(1,nrow(test_basins), by = 500), 
                 to = c(seq(500,nrow(test_basins), by = 500), nrow(test_basins))) 


for(i in 1:nrow(mat)){
  
file = paste0("data/output_",mat$from[i], "-", mat$to[i] ,".csv")

if(!file.exists(file)){
  nCores <- parallel::detectCores() - 1
  doParallel::registerDoParallel(nCores)
  
  collect = foreach(i = mat$from[i]:mat$to[i]) %dopar% {
      collect_lc_stats(test_basins$GAGE_ID[i], rast, test_basins)
    }

  fin = bind_rows(collect) %>% 
    left_join(select(st_drop_geometry(test_basins), GAGE_ID, AREA, 
                     begin_date, end_date, 
                     forcing_years, lc_years)) %>% 
    mutate(description = case_when(
      landcover == '1' ~ "water",
      landcover == '2' ~ "urban",
      landcover == '3' ~ "barren",
      landcover == '4' ~ "forest",
      landcover == '5' ~ "shrub",
      landcover == '6' ~ "herbaceous",
      landcover == '7' ~ "grassland",
      landcover == '8' ~ "agriculture",
      landcover == '9' ~ "wetland"
    )) %>% 
    select(GAGE_ID, everything())
  
  write.csv(fin, file = file)
}

message(i)
}


file = list.files("./data/", pattern = "output_", full.names = TRUE)

results =  bind_rows(lapply(file, read.csv)) %>% 
  mutate(GAGE_ID = sprintf("%08s", GAGE_ID), X = NULL) %>% 
  right_join(ref, by = "GAGE_ID") %>% 
  st_as_sf() %>% 
  mutate(X = st_coordinates(.)[,1], Y = st_coordinates(.)[,2]) %>% 
  st_drop_geometry()

write.csv(results, "output/results.csv", row.names = FALSE)

read.csv("output/results.csv")

                                   