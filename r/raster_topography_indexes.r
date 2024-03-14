#!/usr/bin/env Rscript
# Author: Antonio Ferraz
# Modified by: Gary Doran

library(terra)
library(sf)
library(MultiscaleDTM)
library(optparse)


option_list = list(
    make_option(
        c("-i", "--inputfile"), type="character", default=NULL, 
              help="dataset file name", metavar="file"
    ),
    make_option(
        c("-o", "--outputfile"), type="character", default=NULL, 
        help="output file name", metavar="file"
    )
);
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

print("Reading...")
r<-rast(opt$inputfile)

# TODO: Remove
#r<- r[0:5000, 0:5000, drop=FALSE]

print("...done.")

w <- 3

print(w)
metrics <- c("slope", "aspect", "eastness", "northness")
slp_asp <- SlpAsp(
    r=r, w=c(w, w),
    unit="degrees",
    method="queen",
    metrics=metrics,
    na.rm=TRUE,
    include_scale=TRUE
)
units(slp_asp) <- c("degrees", "degrees", "n/a", "n/a")

print(slp_asp)

print("Writing...")
writeCDF(
    slp_asp, opt$outputfile, overwrite=TRUE, split=TRUE
)
print("...done.")
stop()


resolution_vec<-c(3,3)## number of pixels
###   https://github.com/ailich/MultiscaleDTM?tab=readme-ov-file#roughness-1
#Slope, Aspect and Curvature: SlpAsp calculates multi-scale slope and aspect according to Misiuk et al (2021) which is a modification of the traditional 3 x 3 slope and aspect algorithms (Fleming and Hoffer, 1979; Horn et al., 1981; Ritter, 1987)
slp_asp<- SlpAsp(
    r=r, w=c(5,5),
    unit="degrees",
    method="queen",
    metrics=c("slope", "aspect", "eastness", "northness"),
    na.rm=TRUE,
    include_scale=TRUE
)

## fit calculates slope, aspect, curvature, and morphometric features by fitting a quadratic surface to the focal window using ordinary least squares
## qmetrics<- Qfit(r, w = c(5,5), unit = "degrees", metrics = c("elev", "qslope", "qaspect", "qeastness", "qnorthness", "profc", "planc", "twistc", "meanc", "maxc", "minc", "features"), na.rm = TRUE)


#Roughness
vrm<- VRM(r, w=c(5,5), na.rm = TRUE)  #vector ruggedness measure (Sappington et al. 2007)
sapa<- SAPA(r, w=c(5,5), slope_correction = TRUE, na.rm=TRUE) #Calculates the Surface Area to Planar Area (Jenness, 2004). Rougher surfaces will have a greater surface area to planar area ratio
adj_SD<- AdjSD(r, w=c(5,5), na.rm = TRUE) #This roughness metric modifies the standard deviation of elevation/bathymetry to account for slope (Ilich et al., 2023)
rie<- RIE(r, w=c(5,5), na.rm = TRUE) #Calculates the Roughness Index-Elevation which quantifies the standard deviation of residual topography (Cavalli et al., 2008).
rp<- RelPos(r, w=matrix(data = c(1,NA,1), nrow = 3, ncol=3), shape = "custom", fun = "median", na.rm = TRUE) #RelPos - A flexible and general purpose function to calculate relative position using a rectangular, circular, annulus,
tpi<- TPI(r, w=c(5,5), shape= "rectangle", na.rm = TRUE) #Topographic Position Index (Weiss, 2001)
dmv<- DMV(r, w=2, shape= "circle", na.rm = TRUE, stand="range") #Difference from Mean Value (Lecours et al., 2017; Wilson, and Gallant, 2000) 
bpi<- BPI(r, w = c(4,6), unit = "cell", stand= "sd", na.rm = TRUE) #Bathymetric Position Index (Lundblad et al., 2006) i
bpi2<- BPI(r, w = annulus_window(radius = c(4,6), unit = "cell"), stand= "sd", na.rm = TRUE) # equivalent to BPI code from earlier


print("slope")
x<-terrain(r, "slope",neighbors=8)
writeRaster(x,opt$outputfile)
print("aspect")
x<-terrain(r, "aspect",neighbors=8)
writeRaster(x,opt$outputfile)
print("flowdir")
x<-terrain(r, "flowdir",neighbors=8)
writeRaster(x,opt$outputfile)
            
print("TRIriley")            
x<-terrain(r, "TRIriley",neighbors=8)
writeRaster(x,opt$outputfile)
print("TRI")
x<-terrain(r, "TRI",neighbors=8)
writeRaster(x,opt$outputfile)
print("TPI")
x<-terrain(r, "TPI",neighbors=8)
writeRaster(x,opt$outputfile)
print("roughness")
x<-terrain(r, "roughness",neighbors=8)
writeRaster(x,opt$outputfile)
   
