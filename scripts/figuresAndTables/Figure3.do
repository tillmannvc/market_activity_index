set scheme s1color
graph set window fontface "Arial"


/// ADJUST ///
//cd "market_activity_index"
run scripts\figuresAndTables\_fun_normalize.do
forv b=0/6{
	import delimited "data\\marketActivity\\df_KEN_20240702_batch`b'.csv", clear
	
	gen datePre = date(date, "YMD")
	drop date
	rename datePre date
	format date %td
	
	gen best_p_ring = .
	gen best_s_ring = .
	gen best_p_full = .
	gen best_s_full = .
	replace weekday = dow(date)
	//gen allmktdays=""
	
	levelsof mktid, local(mkts) clean
	foreach mkt of local mkts{
		levelsof weekday if mktday==1 & mktid == "`mkt'", local(days) clean sep(,)
		drop if mktid=="`mkt'" & !inlist(weekdaythisareaisactive, `days') // dropping shapes for days on which we have no market day
		levelsof weekday if mktday==1 & mktid=="`mkt'", local(mktdays)
		//replace allmktdays="`mktdays'" if mktid=="`mkt'"
		foreach mktday of local mktdays{
			di `mktday'
			levelsof maxvar_s_`mktday'_maxpmax_1 if mktid=="`mkt'", local(keep) clean
			local keep = lower("`keep'")
			replace best_s_ring = `keep' if mktid=="`mkt'"  & weekdaythisareaisactive==`mktday'
			local full=subinstr("`keep'",substr("`keep'",-strpos(reverse("`keep'"),"_"),.),"_100",.)
			local full = lower("`full'")
			replace best_s_full = `full' if mktid=="`mkt'" & weekdaythisareaisactive==`mktday'		    
		}
	}

	drop sumsum* 
	save "temp/activity_appended_batch`b'.dta", replace
}

use "temp/activity_appended_batch0.dta", clear
forv b=1/6{
    append using "temp/activity_appended_batch`b'.dta"
}
gen origLat=mkt_lat
gen origLon=mkt_lon
replace mkt_lat = mkt_lon if mkt_lat>30
replace mkt_lon = origLat if origLon<30

keep if mkt_lon<36
drop if mktday==99 

gen instrument_gen = ""
	replace instrument_gen = "old" if inlist(instrument, "PS2")
	replace instrument_gen = "new" if inlist(instrument, "PS2.SD", "PSB.SD")

save "temp/activity_appended.dta", replace

use "temp/activity_appended.dta", clear

geonear mktid mkt_lat mkt_lon using "data/other/weather.dta", neighbors(cell_ID lat lon) nearcount(1)

rename nid cell_ID
merge m:1 date cell_ID using "data/other/weather.dta"
keep if km_to_nid <10
replace year=year(date)
replace month=month(date)
egen month_x_year = group(month year)

save "temp\activityAndWeather.dta", replace


/////////////
// FIGURES //
/////////////

global color1 "42 157 143"
global color2 "233 196 106"
global color3 "231 111 81"

// Panel A - activity example
local target =td(23oct2022)
local range=10
local drange=`range'*2+1

use "temp\activityAndWeather.dta", clear
	keep if mktid == "lon0_1814lat34_2949" & inlist(mktday,0,1)
	
	keep if clear_percent>90
	egen sun_elevation_median = median(sun_elevation), by(mktid)
	gen sun_elevation_diff_to_median = abs(sun_elevation-sun_elevation_median)
	drop if sun_elevation_diff_to_median>=14 & sun_elevation_diff_to_median!=.

gen best_s_full_1 = best_s_full if mktday==1
gen best_s_full_0 = best_s_full if mktday==0

collapse (min) best_s_full_0 (max) best_s_full_1 , by(date mktday weekdaythisareaisactive instrument_gen mktid)

gen best_s_full= .
	replace best_s_full = best_s_full_0 if mktday==0
	replace best_s_full = best_s_full_1 if mktday==1

egen tmp_min_pre = min(date), by(`groupvars')
egen tmp_min =mean(tmp_min_pre),  by(`groupvars')
egen tmp_max_pre = max(date), by(`groupvars')
egen tmp_max =mean(tmp_max_pre),  by(`groupvars')
gen minInterval = date + max(tmp_min-date, -182)  	if tmp_max-date >  182 & tmp_max!=.
gen maxInterval = minInterval +365 				    if tmp_max-date >  182 & tmp_max!=.
replace maxInterval = date + min(tmp_max-date, 182) if tmp_max-date <= 182 
replace minInterval = maxInterval -365 				if tmp_max-date <= 182 
drop tmp_*	
	
	_fun_normalize 90
	keep if instrument!="PS2"
keep if inrange(date, `target'-182, `target'+182) 

tw (bar mkt_avg100 date if inrange(date, `target'-`range', `target'+`range'))

gen diff1 = .
gen indexed = .
levelsof weekdaythisareaisactive, local(days) clean
foreach day of local days{
	su best_s_full if mktday==0 & weekdaythisareaisactive == `day', d
	replace diff1 = best_s_full-r(mean) if weekdaythisareaisactive == `day'
	su diff1 if mktday==1 &  weekdaythisareaisactive == `day' ,d
	replace indexed = 100 * diff1/r(mean)  if weekdaythisareaisactive == `day'
}

su date if inrange(date, `target'-`range', `target'+`range')
local min=r(min) //-1
local max=r(max) //+1
format date  %tdDay_Mon_DD
generate date_text2 = string(date, "%tdDay_DD")
	replace date_text2 = subinstr(date_text2, "Wed", "We",.)
	replace date_text2 = subinstr(date_text2, "Thu", "Th",.)
	replace date_text2 = subinstr(date_text2, "Fri", "Fr",.)
	replace date_text2 = subinstr(date_text2, "Sat", "Sa",.)
	replace date_text2 = subinstr(date_text2, "Sun", "Su",.)
	replace date_text2 = subinstr(date_text2, "Mon", "Mo",.)
	replace date_text2 = subinstr(date_text2, "Tue", "Tu",.)

forv d=`min'/`max'{
    di "`d'", "`min'", "`max'"
    levelsof date_text2 if date==`d' & mktday==1, local(dd) clean
	if "`dd'"==""{
		//label def dates `d' " ", add
	}

	else{
		label def dates `d' "`dd'", add
		local labels `labels' `d'
	}
	levelsof date_text2 if date==`d' & mktday==0, local(dd) clean
	if "`dd'"=="" & !inlist(`d', td(13oct2022), td(16oct2022), td(23oct2022), td(30oct2022)){
		label def mdates `d' " ", add
	}

	else if inlist(`d', td(13oct2022), td(16oct2022), td(23oct2022), td(30oct2022)){
	    //local dd = substr("`dd'",1,2)
		label def dates `d' "`dd'", add
		local mlabels `mlabels' `d'
	}
}
di "`labels', `mlabels'"
label val date dates
gen mdate=date
label val mdate mdates

su date if mktday==0 & inrange(date, `target'-`range', `target'+`range') & weekdaythisareaisactive==2
local left=r(min)
tw (bar indexed date if mktday==0 & inrange(date, `target'-`range', `target'+`range') & weekdaythisareaisactive==2, lw(0) color("$color1*0.5") barw(0.7)) ///
   (bar indexed date if mktday==1 & inrange(date, `target'-`range', `target'+`range'), lw(0) color("$color1") barw(0.7)) ///
   , graphregion(color(white) margin(zero)) legend(off) ///
   xtitle("Valid imagery acquisitions, Oct - Nov '22") xlabel(`labels',valuelabel labcolor("$color1") angle(90)) xmlabel(`mlabels',valuelabel angle(90) tlength(*2) labsize(medsmall)) xscale(range(`min'(1)`max')) ///
   yscale(range(-10(25)125)) ylabel(0 50 100 ) ytitle("Market activity" "(0) 100: (non-) market day mean, {&plusmn}182 days   ") title("A", pos(10) color(black)) /// title("(a) Activity readings over three weeks in 2022 for a market in Kenya", size(medsmall) color(black)) ///
    name(panela, replace) ///
	text(65 `left' "Non-market" "days", color("$color1*0.5") placement(e) ) ///
	text(110 `left' "Market days", color("$color1") placement(e) )

//////////////
// Panel B  //
//////////////
// The firm revenue data in this panel belongs to the research team of this paper and is therefore not made available here. 


global target_var best_s_full
use "temp/activityAndWeather.dta", clear

keep if inlist(mktday, 0,1) & mkt_lon<36 // only consider markets close to siaya

	keep if inlist(mktid, "lon0_0417lat34_3758","lon0_0863lat34_2345","lon0_1105lat34_1039","lon0_1547lat34_3812","lon0_1739lat34_2266","lon0_1814lat34_2949","lon0_2067lat34_4904") | inlist(mktid,"lon0_2073lat34_4915","lon0_2623lat34_2252","lon0_2793lat34_1182","lon0_2799lat34_1194","lon0_3054lat34_201","lon0_3259lat34_3057","lon0_3407lat34_339","lon34_2871lat0_0562")
preserve
	duplicates drop mktid, force
	keep mktid mkt_lat mkt_lon
	export delimited using "siaya_market_coordinates.csv", replace
restore
keep if inrange(diff_to_median_time,-0.1,.5)
keep if clear_percent>90
egen sun_elevation_median = median(sun_elevation), by(mktid)
gen sun_elevation_diff_to_median = abs(sun_elevation-sun_elevation_median)
drop if sun_elevation_diff_to_median>=14 & sun_elevation_diff_to_median!=.

gen best_s_full_1 = $target_var if mktday==1
gen best_s_full_0 = $target_var if mktday==0

collapse (min) best_s_full_0 (max) best_s_full_1 (mean) mktday mkt_lat mkt_lon, by(mktid weekdaythisareaisactive date instrument_gen )

gen $target_var= .
	replace $target_var = best_s_full_0 if mktday==0
	replace $target_var = best_s_full_1 if mktday==1

egen tmp_min_pre = min(date), by(`groupvars')
egen tmp_min =mean(tmp_min_pre),  by(`groupvars')
egen tmp_max_pre = max(date), by(`groupvars')
egen tmp_max =mean(tmp_max_pre),  by(`groupvars')
gen minInterval = date + max(tmp_min-date, -182)  	if tmp_max-date >  182 & tmp_max!=.
gen maxInterval = minInterval +365 				    if tmp_max-date >  182 & tmp_max!=.
replace maxInterval = date + min(tmp_max-date, 182) if tmp_max-date <= 182 
replace minInterval = maxInterval -365 				if tmp_max-date <= 182 
drop tmp_*

_fun_normalize 90
gen best_norm = mkt_avg100	

tw (lpolyci best_norm date, bw(15))
keep best_norm date mktid date weekdaythisarea instrument_gen mktday
su best_norm if mktday==1,d
replace best_norm = . if (best_norm <=r(p1) | best_norm>=r(p99)) & mktday==1
lpoly best_norm date if mktday==1, bw(15) generate(lp_x lp_y) se(lp_se) n(3000)

gen low = lp_y-1.96*lp_se
gen up = lp_y+1.96*lp_se
format lp_x %td
local cond inrange(lp_x, td(01jan2022), td(31may2022))
tw (rarea up low lp_x if `cond') (line lp_y lp_x if `cond')

local m1 = td(15jan2022)
local m2 = td(14feb2022)
local m3 = td(15mar2022)
local m4 = td(15apr2022)

local cond inrange(lp_x, td(01jan2022), td(30apr2022))
tw (rarea up low lp_x if `cond',fcolor("$color1*0.2") alw(0)) (line lp_y lp_x if `cond',lcolor("$color1")) ///  
	, graphregion(color(white) margin(zero)) legend(off) ///
	xlabel(`m1' "Jan 2022" `m2' "Feb" `m3' "Mar" `m4' "Apr 2022" ) xtitle("") name(rev_corr, replace) title("B  ", pos(10) color(black)) ylabel(80(20)120) yscale(range(80(20)120)) ytitle("Market-day activity and firm revenues & profits")
	

///////////////////////////
// PANEL C - SEASONALITY //
///////////////////////////
global target_var best_s_full
use "temp/activityAndWeather.dta", clear

keep if inlist(mktday, 0,1) & mkt_lon<36
keep if inrange(diff_to_median_time,-0.1,.5)
keep if clear_percent>90
egen sun_elevation_median = median(sun_elevation), by(mktid)
gen sun_elevation_diff_to_median = abs(sun_elevation-sun_elevation_median)
drop if sun_elevation_diff_to_median>=14 & sun_elevation_diff_to_median!=.

keep if $target_var !=. 

preserve // drop mkts where the signal doesn't actually vary between market days and non-market days
	keep if inlist(mktday, 0,1)
	collapse (mean) best_s_full, by(mktday mktid weekdaythisareaisactive)
	reshape wide best_s_full, i(mktid weekdaythisareaisactive) j(mktday)
	gen diff = (best_s_full1- best_s_full0)/best_s_full1
	hist diff, xline(0.25) bin(100) name(gnew2, replace)
	keep if diff>0.25
	keep mktid weekdaythisareaisactive
	tempfile drop
	save `drop'
restore

capture drop _merge
merge m:1 mktid weekdaythisareaisactive using `drop'
drop if _merge !=3

drop if inlist(mktid, "lon-0_0598lat34_4262","lon-0_0629lat34_4376","lon0_0828lat34_0457") // markets where activity pattern is not clear
//
capture drop _merge

gen best_s_full_1 = $target_var if mktday==1
gen best_s_full_0 = $target_var if mktday==0

collapse (min) best_s_full_0 (max) best_s_full_1 (mean) precip_by_month mktday mkt_lat mkt_lon, ///
 by(mktid weekdaythisareaisactive date instrument_gen)

gen $target_var= .
	replace $target_var = best_s_full_0 if mktday==0
	replace $target_var = best_s_full_1 if mktday==1

gen month=month(date)
gen year=year(date)

tab mktid
global N_mkts=r(r)



egen tmp_min_pre = min(date), by(`groupvars')
egen tmp_min =mean(tmp_min_pre),  by(`groupvars')
egen tmp_max_pre = max(date), by(`groupvars')
egen tmp_max =mean(tmp_max_pre),  by(`groupvars')
gen minInterval = date + max(tmp_min-date, -182)  	if tmp_max-date >  182 & tmp_max!=.
gen maxInterval = minInterval +365 				    if tmp_max-date >  182 & tmp_max!=.
replace maxInterval = date + min(tmp_max-date, 182) if tmp_max-date <= 182 
replace minInterval = maxInterval -365 				if tmp_max-date <= 182 
drop tmp_*

_fun_normalize 90
gen best_norm = mkt_avg100
save preregression.dta, replace
use preregression.dta, clear
drop if inrange(date, td(01mar2020)-182, td(01jun2020)+182)


su best_norm if mktday==0, d
replace best_norm = . if mktday==0 & (best_norm<r(p1) | best_norm>r(p99))
su best_norm if mktday==1, d
replace best_norm = . if mktday==1 & (best_norm<r(p1) | best_norm>r(p99))
	

matrix coll = J(12,7,.)
matrix coll[1,1] = 1

egen mktid_x_day = group(mktid weekdaythisareaisactive)

reg best_norm i.month if mktday == 1, vce(cluster mktid_x_day)

bysort mktid month year: gen one_per_mkt_and_mth = _n
su precip_by_month if one_per_mkt_and_mth==1 & month==1, d
matrix coll[1,6]=r(p50)
matrix coll[1,2]=_b[_cons]
forv m=1/12{
    //su best_norm if mktday == 1 & month == `m'
	matrix coll[`m',1]=`m'
	matrix coll[`m',2]=  _b[`m'.month]+_b[_cons]
	matrix coll[`m',3]=  _se[`m'.month]	
	su precip_by_month if one_per_mkt_and_mth == 1 & month == `m',d
	matrix coll[`m',6]=r(p50)
}


reg best_norm i.month if mktday == 0,  vce(cluster mktid_x_day)

matrix coll[1,4]=_b[_cons]
forv m=1/12{
    //su best_norm if mktday == 0 & month == `m'
	matrix coll[`m',1]=`m'
	matrix coll[`m',4]=  _b[`m'.month]+_b[_cons]
	matrix coll[`m',5]=  _se[`m'.month]	
    
}


clear
svmat coll
gen lower=coll2 - 1.96*coll3
gen upper=coll2 + 1.96*coll3
gen lower0=coll4 - 1.96*coll5
gen upper0=coll4 + 1.96*coll5
rename coll1  month
gen rain = coll6
gen month_alt = month-0.5	
	replace month_alt = 12.5 if month_alt==11.5
	
gen line_plant_s = 9.5 in 1 // from Oct-Nov https://ipad.fas.usda.gov/rssiws/al/crop_calendar/eafrica.aspx
	replace line_plant_s = 11.5 in 2
gen line_plant_l = 3 in 1 // from mid-March to end May
	replace line_plant_l = 5.5 in 2

gen line_grow_s1 = 11.5 in 1 
	replace line_grow_s1 = 12.5 in 2
gen line_grow_s2 = 0.5 in 1 
	replace line_grow_s2 = 2.5 in 2
gen line_grow_l = 5.5 in 1 
	replace line_grow_l = 9.5 in 2
	
gen line_harv_s = 2.5 in 1 
	replace line_harv_s = 4 in 2
gen line_harv_l = 9.5 in 1 
	replace line_harv_l = 12 in 2

local gap =30
local up =200
local low = `up'-2*`gap'
gen lev_l_0 = 200
	gen lev_l_1 =lev_l_0 -`gap'
	gen lev_l_2 =lev_l_1 -`gap'

gen lev_s_0 = `up'-4*`gap'
	gen lev_s_1 =lev_s_0 - `gap'
	gen lev_s_2 =lev_s_1 - `gap'
	
replace coll4 = coll4+100	
	
gen month_up=month - 0.5

local lowerRain = 50
local upperRain = 80
local half = (`lowerRain'+`upperRain')/2
su rain 
local upperRain_old = int(r(max))
local lowerRain_old = 0

local scale = (`upperRain' - `lowerRain') / (r(max) - `lowerRain_old')
local shift = `lowerRain' - `lowerRain_old' * `scale'
di "`scale', `shift'"
generate rain_shift = rain * `scale' + `shift'

local m0 = `lowerRain' 
local m100 = 100 * `scale' + `shift'
local m200 = 200 * `scale' + `shift'
local start200=`m200'+4
gen rain_lower=`shift'
	

global fs small
tw  (rspike upper lower month, sort color("$color1*0.5") lw(*2)) /// 
	(scatter coll2 month, msize(1.5) color("$color1")) /// (scatter coll7 month, msize(2) color(black) ms(sh)) ///
	(line coll2 month, lc("$color1") sort) ///
	(rspike upper lower month, sort color("$color1*0")  msize(0) lw(0) yaxis(2)) /// 
	(scatter coll4 month, msize(1.5) color("$color1*0.5") yaxis(2)) ///	
	(rbar rain_shift rain_lower month, bc(gs12)  barw(0.8) lw(none) yaxis(2)) ///
	,  graphregion(color(white) margin(-4 +3 +0 +0)) ytitle("Market-day activity" "(0) 100: (non-) market day mean, {&plusmn}182 days", size($fs)) xscale(range(0.5(1)12.5)) ///
	ytitle("", axis(2) orientation(rvertical))  ///
	legend(order(2 "Market days (lhs)" 5 "Non-market days (rhs)") size(small ) row(2) ring(0) pos(11) region(lw(0) lc(gs12) fcolor(white%0)))  yscale(range(47(10)120)) ylabel(70(10)120, labsize($fs)) /// title("(b) Seasonal variation in market activity and precipitation", size(medsmall)) - " " 1 "Mean firm revenues" - "in region, 2022"
	name(seasonal_new1, replace) xlabel(1 "January" 4 "April" 7 "July" 10 "October",labsize($fs)) title("C", pos(10) ring(1) color(black) size(medium)) xscale(range(1(1)12))  /// 
	yscale(range(47(10)120) axis(2) lw(0)) ylabel(`m0' "0" `m100' "100"  `m200' "200" 95 "-5" 100 "0" 105 "5" , axis(2) labsize($fs) angle(270)) ///
	xtitle("") ///
	text(49 2.6 "{bf:______}", color("$color3") placement(ne) ) ///
	text(49 3.3 "Harvest season", color("$color3") placement(s) size($fs)) ///
	text(49 9.6 "{bf:__________}", color("$color3") placement(ne)  ) ///
	text(49 10.8 "Harvest s.", color("$color3") placement(s)   size($fs)) ///
	text(`half' 13.55 "Mean preci-" "pitation (mm)", placement(e) yaxis(2) orientation(rvertical) size($fs)) ///
	text(100 13.55 "Non-market" "day activity", placement(e) yaxis(2) orientation(rvertical) size($fs)) ///
	text(100 12.725 "______", orientation(vertical) placement(w)) ///
	text(`start200' 12.725 "____________", orientation(vertical) placement(sw))
	



	
/////////////////////////////////////	
// PANEL D- ACROSS YEAR VARIATION //
/////////////////////////////////////	
	
global lags 6
global lagsmin = $lags - 1
use "temp/weather.dta", replace
	gen year=year(date)
	gen month=month(date)
	collapse (sum) precipitation (mean) lat lon, by(month cell_ID year)
	egen meanByMonth = mean(precipitation), by(month cell_ID)
	egen sdByMonth = sd(precipitation), by(month cell_ID)
	gen rainShock = (precipitation-meanByMonth)/sdByMonth
	hist rainShock if inrange(year, 2016,2023) // not normal; rainfall is trending
	bysort cell_ID (year month): gen monthCounter=_n
	xtset cell_ID monthCounter
	forv m=0/$lags{
	    gen rainShock_L`m' = L`m'.rainShock
	    gen rainShock_F`m' = F`m'.rainShock
		gen rain_L`m'      = L`m'.precipitation
	}
	keep if inrange(year, 2016,2023)
save "temp/weatherShocks.dta", replace
	
global start_date = td(01jul2017)
global target_var  best_s_full

use "temp/activityAndWeather.dta", clear

keep if mkt_lon<36

keep if inrange(diff_to_median_time,-0.1,.5)
keep if clear_percent>90
egen sun_elevation_median = median(sun_elevation), by(mktid)
gen sun_elevation_diff_to_median = abs(sun_elevation-sun_elevation_median)
drop if sun_elevation_diff_to_median>=14 & sun_elevation_diff_to_median!=.

keep if inrange(date, $start_date, td(01jul2024))
drop if mktday==99

gen best_s_full_1 = $target_var if mktday==1
gen best_s_full_0 = $target_var if mktday==0

collapse (min) best_s_full_0 (max) best_s_full_1 (mean) mktday mkt_lat mkt_lon, by(mktid weekdaythisareaisactive date instrument_gen)

gen $target_var= .
	replace $target_var = best_s_full_0 if mktday==0
	replace $target_var = best_s_full_1 if mktday==1

gen year=year(date)
		
keep if  $target_var !=. // & !inrange(date, td(01mar2020), td(01jun2020))

capture drop month
capture drop year
gen year = year(date)
gen month = month(date)
capture drop month_x_year


save test.dta, replace

use test.dta, clear

egen tmp_min_pre = min(date), by(`groupvars')
egen tmp_min =mean(tmp_min_pre),  by(`groupvars')
	replace tmp_min = tmp_min+10
egen tmp_max_pre = max(date), by(`groupvars')
egen tmp_max =mean(tmp_max_pre),  by(`groupvars')
	replace tmp_max = tmp_max-10

gen minInterval = date + max(tmp_min-date, -365)  	 if tmp_max-date >  365 & tmp_max!=.
gen maxInterval = minInterval +365 				     if tmp_max-date >  365 & tmp_max!=.
replace maxInterval = date  if tmp_max-date <= 365 
replace minInterval = date 	+ min(tmp_max-date, -365) 				if tmp_max-date <= 365 
drop tmp_*

tw (rspike maxInterval minInterval date if instrument_gen=="new" & mktid=="lon35_8363lat-0_5977") (scatter  date  date if instrument_gen=="new" & mktid=="lon35_8363lat-0_5977") (rspike maxInterval minInterval date if instrument_gen=="old" & mktid=="lon35_8363lat-0_5977") (scatter  date  date if instrument_gen=="old" & mktid=="lon35_8363lat-0_5977") (scatter  maxInterval  date if  mktid=="lon35_8363lat-0_5977", msize(tiny)) (scatter  minInterval  date if  mktid=="lon35_8363lat-0_5977", msize(tiny))

_fun_normalize 90

preserve // drop mkts where the signal doesn't actually vary between market days and non-market days
	keep if inlist(mktday, 0,1)
	collapse (mean) mkt_avg100, by(mktday mktid weekdaythisareaisactive instrument_gen)
	reshape wide mkt_avg100, i(mktid weekdaythisareaisactive instrument_gen) j(mktday)
	gen diff = (mkt_avg1001- mkt_avg1000)/mkt_avg1001
	hist diff if inrange(diff, 0.8,2), xline(0.25) bin(100)
	keep if inrange(diff, 0.9,1.1)
	keep mktid weekdaythisareaisactive instrument_gen
	tempfile drop
	save `drop'
restore
capture drop _merge
merge m:1 mktid weekdaythisareaisactive instrument_gen using `drop'
drop if _merge != 3
drop  _merge

gen normalized = mkt_avg100	
keep if date>$start_date
drop if inrange(date, td(01mar2020),td(01jul2020))
keep if mktday==1

su normalized, d
replace normalized = . if (normalized<=r(p1) | normalized>=r(p99)) 


preserve // identify the busiest months  //TEST
	keep if mktday==1
	collapse (mean) normalized, by(mktid month)
	bysort mktid (normalized): gen rank_per_month=_n
	keep mktid month rank_per_month
	tempfile ranks
	save `ranks'
restore

merge m:1 mktid month using `ranks', nogen

egen month_x_year=group(month year)

geonear mktid mkt_lat mkt_lon using temp/weatherShocks.dta, neighbors(cell_ID lat lon) nearcount(1)
rename nid cell_ID
merge m:1 month year cell_ID using temp/weatherShocks.dta, nogen keep(3)
keep if km_to_nid <10

drop monthCounter
gen monthCounter = .
levelsof year, local(years) clean
foreach year of local years{
    forv m=1/12{
	    local cc=`cc'+1
		replace monthCounter = `cc' if month==`m'& year==`year'
	}
}

gen monthCounter_sq =monthCounter*monthCounter

encode mktid, gen(mktcode)

egen cell_x_month=group(month year cell_ID )

gen harvMonth = .
	replace harvMonth = 1 if inlist(month, 4) // & inrange(rank_per_month,9,12) 
	replace harvMonth = 0 if inlist(month, 1) 


matrix coll=J($lags+1,5,.)
local rr=2

forv l=0/6{
	local vars `vars' rainShock_L`l' c.rainShock_L`l'#c.rainShock_L`l'  
}
	reg mkt_avg100 `vars'    if harvMonth==1, vce(cluster cell_x_month) 
	
forv l=0/6{
    local mm=`l'+1
	margins, dydx(rainShock_L`l') atmeans at(rainShock_L`l'=0) 
	marginsplot, name(g`l', replace) nodraw yline(0) level(90)
	local comb `comb' g`l' 
	matrix coll[`mm',1] = `l' 
	matrix coll[`mm',2] = _b[rainShock_L`l']
	matrix coll[`mm',3] = _se[rainShock_L`l']

}
graph combine `comb', col(7) ycommon name(all, replace)
matrix list coll

capture drop coll*
capture drop lag*
capture drop lower* upper* abs* zeroIncl*
matrix list coll
svmat coll 
rename coll1 lag
gen lowerHarv = coll2-1.64*coll3
gen upperHarv = coll2+1.64*coll3
gen abslowerHarv=abs(lowerHarv)
gen absupperHarv=abs(upperHarv)
gen zeroInclHarv= ((abslowerHarv+absupperHarv)>(upperHarv-lowerHarv))
replace lag = lag-0.125

qui tab mktid if harvMonth!=. & mkt_lon<36
global N_mkts=r(r)
tw  (rspike upperHarv lowerHarv lag if lag<=$lags  & zeroInclHarv==1, color("$color1*0.5") lw(*2)) ///
	(rspike upperHarv lowerHarv lag if lag<=$lags  & zeroInclHarv==0, color("$color1*0.5") lw(*2)) /// 
	(scatter coll2 lag if lag<=$lags & zeroInclHarv==1, mc("$color1")) ///
	(line coll2 lag if lag<=$lags, lc("$color1")) /// 
	 (scatter coll2 lag if lag<=$lags & zeroInclHarv==0, mc("$color1")) ///	
, yline(0, lpattern(dash) lcolor(gs8)) graphregion(color(white) margin(zero)) ytitle("Marginal effect of rainfall deviation on April" "market activity at shock size = 0 (p.p)") xtitle("Months before April") xlabel(0(1)$lags) legend(off)  ///
 name(across1, replace) title("D", pos(10) color(black)) 

 
 /// Panel E ///

	local tgtprd 2
tw  (lpolyci normalized rainShock_L`tgtprd' if harvMonth==1 , bw(0.75) alw(0)  lcolor("$color1") acolor("$color1*0.2")) ///
	(hist rainShock_L`tgtprd' if harvMonth==1  & mkt_lon<36, yaxis(2) lw(0) color(gs8%50)) ///
, graphregion(color(white) margin(0 +1.5 0 0)) name(hetero, replace) xtitle("Two-months lagged rainfall deviations" "from long-term mean (SD)") ytitle("Market activity during April" "100: market-day avg. on previous 365 days") legend(off) yscale(range(0(.25)3) axis(2) lstyle(none)) ytitle("", axis(2)) ylabel(, noticks nolabels axis(2) nogrid) title("E", pos(10) color(black))  ///
text( 104.3 3.84 "________", orientation(vertical) placement(n) size(medlarge)) ///
text(107.5 4.25 "Density", orientation(rvertical)) ylabel(105(5)125) xlabel(-1(1)3)

graph set window fontface arial

graph combine panela rev_corr, name(upper, replace) graphregion(color(white) margin(zero)) nodraw
graph combine across1 hetero, name(lower, replace) graphregion(color(white) margin(zero)) nodraw
	
graph combine upper seasonal_new1 lower, row(3) graphregion(color(white)  margin(zero)) 
graph display, ysize(29.7) xsize(21)	

