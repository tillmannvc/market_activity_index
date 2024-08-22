graph set window fontface "Arial"
set scheme s1color

spshape2dta "data\other\redrawnZonesDissolved_20211129.shp", replace
use redrawnZonesDissolved_20211129_shp, clear
	gen _EMBEDDED=0
	replace _EMBEDDED=1 if _ID==24 
save redrawnZonesDissolved_20211129_shp, replace

spshape2dta "data\other\Eth_Adm1.shp", replace 
use Eth_Adm1_shp, clear
gen use= "Regions"
replace use ="s1" if inlist(_ID, 3)
replace use ="s2" if inlist(_ID, 8)
replace use ="s3" if inlist(_ID, 11)
save Eth_Adm1_shp.dta, replace

forv b=0/11{
	import delimited "data\\marketActivity\\df_ETH_20240702_batch`b'.csv", clear
	
	gen datePre = date(date, "YMD")
	drop date
	rename datePre date
	format date %td
	
	gen best_p_ring = .
	gen best_s_ring = .
	gen best_p_full = .
	gen best_s_full = .
	
	levelsof mktid, local(mkts) clean
	foreach mkt of local mkts{
		levelsof weekday if mktday==1 & mktid == "`mkt'", local(days) clean sep(,)
		drop if mktid=="`mkt'" & !inlist(weekdaythisareaisactive, `days') // dropping shapes for days on which we have no market day
		levelsof weekday if mktday==1 & mktid=="`mkt'", local(mktdays)
		foreach mktday of local mktdays{
			levelsof maxvar_s_`mktday'_maxpmax_1 if mktid=="`mkt'", local(keep) clean
			local keep = lower("`keep'")
			replace best_s_ring = `keep' if mktid=="`mkt'"  & weekdaythisareaisactive==`mktday'
			local full=subinstr("`keep'",substr("`keep'",-strpos(reverse("`keep'"),"_"),.),"_100",.)
			local full = lower("`full'")
			replace best_s_full = `full' if mktid=="`mkt'" & weekdaythisareaisactive==`mktday'		    
		}
	}
	drop sumsum* 
	save "temp/activity_appended_ETH_batch`b'.dta", replace
}

use "temp/activity_appended_ETH_batch0.dta", clear
forv b=1/11{
    append using "temp/activity_appended_ETH_batch`b'.dta"
}
	local d1=td(01jan2018)
	local d2=td(31dec2019)
	global refRange `d1', `d2'
	hist date if instrument == "PS2", name(ps2, replace) nodraw xline(`d1') xline(`d2')
	hist date if instrument != "PS2", name(notps2, replace) nodraw xline(`d1')  xline(`d2')
	graph combine ps2 notps2, row(2) xcommon name(overlap, replace)

	keep if inlist(mktday,0,1)
	keep if inrange(diff_to_median_time,-0.1,.5)
	
	keep if clear_percent>90
	egen sun_elevation_median = median(sun_elevation), by(mktid)
	gen sun_elevation_diff_to_median = abs(sun_elevation-sun_elevation_median)
	drop if sun_elevation_diff_to_median>=14 & sun_elevation_diff_to_median!=.

preserve // drop mkts where the signal doesn't actually vary between market days and non-market days
	collapse (mean) best_s_full, by(mktday mktid weekdaythisareaisactive)
	reshape wide best_s_full, i(mktid weekdaythisareaisactive) j(mktday)
	gen diff = (best_s_full1- best_s_full0)/best_s_full1
	hist diff, xline(0.25) bin(100)
	keep if diff>0.25
	keep mktid weekdaythisareaisactive
	tempfile drop
	save `drop'
restore

capture drop _merge
merge m:1 mktid weekdaythisareaisactive using `drop'
drop if _merge !=3

gen best_s_full_1 = best_s_full if mktday==1
gen best_s_full_0 = best_s_full if mktday==0

collapse (min) best_s_full_0 (max) best_s_full_1 (mean)  mkt_lat mkt_lon  , by(mktid weekday date  mktday weekdaythisareaisactive instrument)

gen best_s_full_= .
	replace best_s_full_ = best_s_full_0 if mktday==0
	replace best_s_full_ = best_s_full_1 if mktday==1

gen month=month(date)
gen year=year(date)

tab mktid
global N_mkts=r(r)

preserve // normalize to average january value
	keep if inrange(date,  $refRange) & mktday==0 & instrument=="PS2"
	collapse (mean) best_s_full_, by(mktid weekdaythisareaisactive instrument )
	rename best_s_full_ mktActRef0 
	tempfile ref
	save `ref'
restore	

merge m:1 mktid weekdaythisareaisactive instrument using `ref', nogen
gen mktAct_nonmktday_avg0 = best_s_full_ - mktActRef0

preserve
	keep if inrange(date,  $refRange) & mktday==1 & instrument=="PS2"
	collapse (mean) mktAct_nonmktday_avg0, by(mktid weekdaythisareaisactive instrument )
	rename mktAct_nonmktday_avg0 mktActRef1
	tempfile ref
	save `ref'
restore	
merge m:1 mktid weekdaythisareaisactive instrument  using `ref', nogen
	replace mktActRef1 = . if mktActRef1 <= 0 // places where average market day reading is zero or negative


	gen best_norm = 100 * (mktAct_nonmktday_avg0 / mktActRef1)
	replace best_norm = 0 if inrange(best_norm,-100,0) & mktday==1 // market day obs where the reading is below the average of non-mkt days; can't be less active than what I define as not active
	replace best_norm = . if best_norm<-100 & mktday==1 // market day obs where the reading is below the average of non-mkt days; can't be less active than what I define as not active
	
	
drop if best_norm>300 & best_norm!=.

gen origLat=mkt_lat
gen origLon=mkt_lon
replace mkt_lat = mkt_lon if mkt_lat>20
replace mkt_lon = origLat if origLon<20

 geoinpoly mkt_lat mkt_lon using redrawnZonesDissolved_20211129_shp
 
merge m:1 _ID using redrawnZonesDissolved_20211129, keep(1 3) nogen
rename _ID _ID_adm2

 geoinpoly mkt_lat mkt_lon using Eth_Adm1_shp
 
merge m:1 _ID using Eth_Adm1, keep(1 3) nogen

capture drop adm2_code
encode redrawnZon, gen(adm2_code)
bysort ADM1_NAME: su mkt_lat mkt_lon,d
 	
	
save "temp/activity_appended_ETH.dta", replace
use  "temp/activity_appended_ETH.dta", clear

local d1 = td(01nov2020)
local d2 = td(01may2021)
global refRange2 `d1',`d2'


keep if date>td(01jan2018)
gen instrument_old = (instrument=="PS2")
replace best_norm = . if instrument_old==0 // base normalized index on first gen images
replace month=month(date)
replace year=year(date)

egen ref_pre = mean(best_norm) if mktday==1 & instrument_old==1 & inrange(date, $refRange2), by(ADM1_NAME ) // Reference value is first-gen image median within overlapping time range, by region

egen ref = max(ref_pre) if mktday==1, by(ADM1_NAME ) // paste reference value to all values, even outside overlapping range

egen avg_pre = mean(best_s_full_) if mktday==1 & inrange(date, $refRange2) & instrument_old==0, by(ADM1_NAME) // for indexing, take average of all values to be indexed, from second gen

egen avg = max(avg_pre) if mktday==1 & instrument_old==0, by(ADM1_NAME) // paste reference value

replace best_norm = ref * best_s_full_/avg if instrument_old==0 // make sure that best_norm among second gen is reference value on average during reference period and free to vary otherwise

keep if mktday==1

drop if best_norm>300 & best_norm!=.

// import delimited "~\Dropbox\MarketActivityIndex\mai_shared\datasets\version_20240117\allActivity_Ethiopia.csv", clear //  rowrange(:400000)


preserve
	keep if mkt_lat !=. & mkt_lon!=. & inlist(ADM1_NAME, "Tigray", "Oromia", "Amhara")
	duplicates drop mktid, force
	keep mkt_lat mkt_lon mktid ADM1_NAME
	encode ADM1_NAME, gen(admcode)
	save temp/points.dta, replace
restore

global range "inrange(date, td(01jan2018), td(01jan2024))"

gen counter= 1 if best_norm!=.
keep if $range
bysort mktid (date) : gen cum_mktAct = sum(best_norm) if date>td(01jan2020)
bysort mktid (date) : gen cum_count = sum(counter) if date>td(01jan2020)
gen cum_mktAct_mean=cum_mktAct/cum_count
bysort mktid year (date): gen countInYear = _n

save "temp/preCollapse.dta", replace

global colO "42 157 143"
global colA "233 196 106"
global colT "231 111 81"

use "temp/preCollapse.dta", clear
	egen maxInYear=max(cum_mktAct_mean), by(mktid year)
	keep if year==2022 & inlist(ADM1_NAME, "Tigray", "Oromia", "Amhara")
	collapse (mean) maxInYear, by(_ID_adm2 ADM1_NAME)
local steps=10
//colorpalette FireBrick "253 253 150"  "  2  75  48", ipolate(`steps') power(.5) // nograph
colorpalette navy emidblue bluishgray, ipolate(`steps') power(1)  nograph
local colors `r(p)'
su maxInYear
local max=ceil(r(max))
local min=floor(r(min))

spmap maxInYear using redrawnZonesDissolved_20211129_shp.dta, ///
	legend(title("Mean market" "activity index," "2020-22", size(vlarge) justification(left)) size(vlarge) pos(1) label(2 "`min'") label(3 "") label( 4 "") label( 5 "") label( 6 "100") label( 7 "") label( 8 "") label( 9 "") label( 10 "")  label( 11 "`max'            ") symysize(*2.2)  ) /// 
	id(_ID) fcolor("`colors'") clnum(`steps') clmethod(eqint)  ndfcolor(red%10) mos(none) osize(0 0 0 0 0 0 0 0 0 0) ndsize(vvthin)  ///
	point(data(temp/points.dta) by(admcode) fcolor("$colA" "$colO" "$colT") xcoord(mkt_lon) ycoord(mkt_lat) size(medsmall medsmall medsmall)  legenda(off) leglabel("Markets") legcount) /// 
	polygon(data(temp/Eth_Adm1_shp) by(use) ocolor(black black black black) osize(thin medthick medthick medthick) opattern(solid solid solid solid) legenda(off) leglabel(1 "Regions")  legs(1)) /// 
text(13.9 32.7 "Tigray",   color("$colT") size(vlarge) placement(e)) /// 
text(12.4 32.7 "Amhara",  color("$colA") size(vlarge) placement(e)) ///  
text(10 32.7 "Oromia", bcolor(white) box fcolor(white) lw(0) color("$colO") size(vlarge) placement(e)) ///
 name(map1, replace) ///
    title("B", pos(10) size(huge) color(black))

 preserve
	import excel "data\other\Ethiopia_1997-2023_2024-01-12.xlsx", sheet("Sheet1") firstrow clear

	drop if EVENT_TYPE == "Strategic developments"
	keep if inlist(ADMIN1, "Tigray", "Oromia", "Amhara")
	keep if inrange(EVENT_DATE, td(01jan2018), td(01oct2023))
	gen month=month(EVENT_DATE)
	gen year = year(EVENT_DATE)
	gen countEvent = 1
	collapse (sum) countEvent, by(month year ADMIN1)
	reshape wide countEvent, i(month year) j(ADMIN1) string
	gen countTigrayANDAmhara = countEventTigray+ countEventAmhara
	gen countTigrayANDAmharaANDOromia = countTigrayANDAmhara+ countEventOromia
	gen day=15
	gen date=mdy(month, day, year)
	save "temp/conflict.dta", replace
restore
 
 
use "temp/preCollapse.dta", clear
append using temp/conflict.dta

local date1 =td(01jan2018)
local date2 =td(01jan2019)
local date3 =td(01jan2020)
local date4 =td(01jan2021)
local date5 =td(01jan2022)
local date6 =td(01jan2023)

local dd=`date1'-390

tw (lpolyci best_norm date if ADM1_NAME=="Tigray" & $range, clc("$colT") fc("$colT%20") lw(*2) alw(0) bw(40)) ///
   (lpolyci best_norm date if ADM1_NAME=="Amhara" & $range, clc("$colA") fc("$colA%20") lw(*2) alw(0) bw(40)) ///
   (lpolyci best_norm date if ADM1_NAME=="Oromia" & $range, clc("$colO") fc("$colO%20") lw(*2) alw(0) bw(40)) ///
   ,  graphregion(color(white) margin(+0 +0 0 +0)) ytitle("Market activity index" "(average 2018-19 = 100)" " ", size(huge)) xtitle("") name(lines, replace)  xlabel(none)  xtick(`date1' `date2' `date3' `date4' `date5' `date6') fysize(65) ylabel(40(20)120, labsize(huge)) nodraw ///    text(122 `dd'  "{bf:A}", size(huge)) /// 
   legend(off) /// legend(ring(0)  symxsize(*0.6) size(huge) pos(7) order(2 "Tigray" 4 "Amhara" 6 "Oromia" ) row(3) nobox region(lw(0) color(none))) 
   title("A", pos(10) size(huge) color(black))

tw (bar countEventTigray date, barw(32) col("$colT") lw(0)) ///
   (rbar countTigrayANDAmhara countEventTigray date , barw(32) col("$colA") lw(0)) ///
   (rbar countTigrayANDAmharaANDOromia countTigrayANDAmhara date, barw(32) col("$colO") lw(0)) ///
   , graphregion(color(white)) xlabel(`date1' "Jan 2018" `date2' "Jan '19" `date3' "Jan '20" `date4' "Jan '21" `date5' "Jan '22" `date6' "Jan '23", angle(45) labsize(huge)) xtick(`date1' `date2' `date3' `date4' `date5' `date6') name(bars, replace) ytitle("# monthly" "conflict events" " ", size(huge)) legend(off) xtitle("") fysize(35) ylabel(0(200)200, labsize(huge))  nodraw
   
  graph combine lines bars, rows(2)  graphregion(color(white) margin(0 0 0 0 )) name(left, replace) fxsize(75)  nodraw

   
graph combine left map1, row(1) graphregion(color(white) margin(0 0 0 0)) name(newonly, replace)
graph display, ysize(10) xsize(20)
