library(dplyr)
library(sf)
library(units)
library(terra)

collect_lc_stats = function(ID, rast, basins){
  n   = names(rast) 
  bas = vect(filter(basins, GAGE_ID == ID ))
  
  mask(crop(rast, bas), bas) %>% 
    terra::freq() %>% 
    data.frame() %>% 
    mutate(layer = n[layer]) %>% 
    select(layer, landcover = value, count) %>% 
    tidyr::pivot_wider(id_cols = c(landcover),
                       names_from = layer, values_from = count) %>% 
    mutate_all( ~if_else(is.na(.), 0, .)) %>% 
    #mutate(landcover = substr(landcover,1,1)) %>% 
    group_by(landcover) %>% 
    summarise(lc2001 = sum(get(n[1])), lc2019 = sum(get(!!n[2]))) %>% 
    mutate(GAGE_ID = ID,
           delta = get(n[2]) - get(n[1]), 
           perD  = round((100 * delta / sum(get(n[1]))), 3))
}


# LC Data ! -----------------------------------------------------------

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

paths = c('/Volumes/Transcend/ngen/MODIS/MCD12Q1.006/mosaics/landcover_2001-01-01.tif',
          '/Volumes/Transcend/ngen/MODIS/MCD12Q1.006/mosaics/landcover_2019-01-01.tif')

rast  = rast(paths) %>% 
  setNames(c("lc2001", 'lc2019'))

library(foreach)

mat = data.frame(from = seq(1,nrow(test_basins), by = 500), 
                 to = c(seq(500,nrow(test_basins), by = 500), nrow(test_basins))) 


for(i in 1:nrow(mat)){
  
  file = paste0("data/output_modis_",mat$from[i], "-", mat$to[i] ,".csv")
  
  if(!file.exists(file)){
    nCores <- parallel::detectCores() - 1
    doParallel::registerDoParallel(nCores)
    
    collect = foreach(i = mat$from[i]:mat$to[i]) %dopar% {
      collect_lc_stats(ID = test_basins$GAGE_ID[i], rast, test_basins)
    }
    
    fin = bind_rows(collect) %>% 
      left_join(select(st_drop_geometry(test_basins), GAGE_ID, AREA, 
                       begin_date, end_date, 
                       forcing_years, lc_years)) %>% 
      mutate(description = case_when(
        landcover == '1' ~ "broadleaf_evergreen",
        landcover == '2' ~ "broadlead_deciduous",
        landcover == '3' ~ "needleleaf_evergreen",
        landcover == '4' ~ "needleaf_deciduous",
        landcover == '5' ~ "mixed_forest",
        landcover == '6' ~ "open_tree",
        landcover == '7' ~ "shrub",
        landcover == '8' ~ "herbaceous",
        landcover == '9' ~ "sparse_veg",
        landcover == '10' ~ "cropland",
        landcover == '11' ~ "cropland_veg_mosaic",
        landcover == '12' ~ "mangrove",
        landcover == '13' ~ "wetland",
        landcover == '14' ~ "bare",
        landcover == '15' ~ "urban",
        landcover == '16' ~ "snow_ice",
        landcover == '17' ~ "water"
      )) %>% 
      select(GAGE_ID, everything())
    
    data.table::fwrite(fin, file = file, row.names = FALSE)
  }
  
  message(i)
}


file = list.files("./data/", pattern = "output_modis", full.names = TRUE)
ref = st_centroid(test_basins)

results =  bind_rows(lapply(file, read.csv)) %>% 
  mutate(GAGE_ID = sprintf("%08s", GAGE_ID), X = NULL) %>% 
  right_join(select(ref, "GAGE_ID"), by = "GAGE_ID") %>% 
  st_as_sf() %>% 
  mutate(X = st_coordinates(.)[,1], Y = st_coordinates(.)[,2]) %>% 
  st_drop_geometry()

write.csv(results, "output/results_modis.csv", row.names = FALSE)
