#!/usr/bin/env Rscript
# Author: Antonio Ferraz
# Modified by: Gary Doran

library(terra)
library(sf)
library(MultiscaleDTM)
library(optparse)


# Use 9x9 window
w <- 9


option_list = list(
    make_option(
        c("-i", "--inputfile"), type="character", default=NULL,
              help="dataset file name", metavar="file"
    ),
    make_option(
        c("-o", "--outputdir"), type="character", default=NULL,
        help="output directory name", metavar="file"
    )
);
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);


print("Reading...")
r <- rast(opt$inputfile)
print("...done.")


print("Writing NetCDF...")
names(r) <- "elevation"
outputfile <- file.path(opt$outputdir, 'elevation.nc')
writeCDF(
    r, outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")


print("Computing slope and aspect...")
metrics <- c("slope", "eastness", "northness")
slp_asp <- SlpAsp(
    r=r, w=c(w, w),
    unit="degrees",
    method="queen",
    metrics=metrics,
    na.rm=TRUE,
    include_scale=TRUE
)
units(slp_asp) <- c("degrees", "n/a", "n/a")
print("...done.")
print("Writing...")
outputfile <- file.path(opt$outputdir, 'slpasp.nc')
writeCDF(
    slp_asp, outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")


# Vector ruggedness measure (Sappington et al. 2007)
print("Computing Vector Ruggedness Measure...")
vrm <- VRM(r=r, w=c(w, w), na.rm=TRUE)
print("...done")
print("Writing...")
outputfile <- file.path(opt$outputdir, 'vrm.nc')
writeCDF(
    vrm, outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")


# Topographic Position Index (Weiss, 2001)
print("Computing Topographic Position Index...")
tpi <- TPI(r=r, w=c(w, w), shape="rectangle", na.rm=TRUE)
print("...done.")
print("Writing...")
outputfile <- file.path(opt$outputdir, 'tpi.nc')
writeCDF(
    tpi, outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")


# Calculates the Roughness Index-Elevation which quantifies the standard
# deviation of residual topography (Cavalli et al., 2008).
print("Computing Roughness Index-Elevation...")
rie <- RIE(r=r, w=c(w, w), na.rm=TRUE)
print("...done.")
print("Writing...")
outputfile <- file.path(opt$outputdir, 'rie.nc')
writeCDF(
    rie, outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")


# Calculates the Surface Area to Planar Area (Jenness, 2004). Rougher surfaces
# will have a greater surface area to planar area ratio
print("Computing Surface Area to Planar Area...")
sapa <- SAPA(r=r, w=c(w, w), slope_correction=TRUE, na.rm=TRUE)
print("...done.")
print("Writing...")
outputfile <- file.path(opt$outputdir, 'sapa.nc')
writeCDF(
    sapa, outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")


# Difference from Mean Value (Lecours et al., 2017; Wilson, and Gallant, 2000) 
print("Computing Difference from Mean Value...")
dmv <- DMV(r=r, w=w, shape= "circle", na.rm = TRUE, stand="range")
print("...done.")
print("Writing...")
outputfile <- file.path(opt$outputdir, 'dmv.nc')
writeCDF(
    dmv, outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")


# This roughness metric modifies the standard deviation of elevation/bathymetry
# to account for slope (Ilich et al., 2023)
print("Computing Adjusted Standard Deviation...")
adj_SD <- AdjSD(r=r, w=c(w, w), na.rm=TRUE)
print("...done.")
print("Writing...")
outputfile <- file.path(opt$outputdir, 'adjsd.nc')
writeCDF(
    adj_SD, outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")
