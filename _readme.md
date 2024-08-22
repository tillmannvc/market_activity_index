Using satellite imagery to monitor remote rural economies at high frequency

Replication materials for von Carnap, Asiyabi, Dingus, Tompsett (2024).

This repository holds the code used to produce the figures and tables in the main text and supplemental materials of this paper. We access the satellite imagery underlying the paper through an academic research user agreement preventing us from directly making the imagery available. 

Organization of repository:

- data/data/marketActivity: activity panels underlying Figs 3&4 in the paper
- data/marketMaps:	market maps underlying Figures 3,4, S1, S3, S6
- data/other:		shapefiles for administrative regions, market validation maps, weather shocks
- scripts/dataderivation: scripts for identifying markets from satellite imagery and extracting activity measures in Google Earth Engine, as well as postprocessing with Python. 
- scripts/figuresAndTables: scripts in Stata, Python and Google Earth Engine to produce the figures and tables in the paper

The code for data derivation and some of the figures interacts with a backend managing the overall workflow. 

If you discover meaningful errors, have questions or suggestions, contact Tillmann von Carnap at tcarnap.work@gmail.com. Please also reach out if you are interested in code producing more upstream inputs. 


