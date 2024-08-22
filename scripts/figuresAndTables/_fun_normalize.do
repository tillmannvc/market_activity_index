capture program drop _fun_normalize
program define _fun_normalize
	
	local bw = `1'
	
	preserve 
		clear
		tempfile tmp
		save `tmp', emptyok
	restore
	
	local groupvars mktid weekdaythisareaisactive instrument_gen
	foreach gen in new old{
		su date if instrument_gen=="`gen'"
		local exp = r(max) - r(min) +1
		local min=r(min)
		preserve
			keep `groupvars'
			qui keep if instrument_gen=="`gen'"
			qui duplicates drop mktid weekdaythisareaisactive,force
			expand `exp'
			bysort mktid weekdaythisareaisactive: gen date = _n + `min'-1
			format date %td
			append using `tmp'
			tempfile tmp
			save `tmp'
		restore
	}
	capture drop _merge
	merge m:1 `groupvars' date using `tmp', nogen
	gen weekday=dow(date)
	
	
	gen mkt_avg100 = .

	timer clear
	levelsof mktid, local(mkts)
	local  m=0
	foreach mkt of local mkts{
		local m=`m'+1
		timer on 1
		di "`mkt'"
		qui levelsof weekdaythisareaisactive if mktid=="`mkt'", local(wkds)
		foreach wkd of local wkds{
			qui levelsof instrument_gen if mktid=="`mkt'" & weekdaythisareaisactive == `wkd', local(gens) clean
			foreach gen of local gens{
				quietly{
				preserve
					keep if instrument_gen=="`gen'" & mktid=="`mkt'" & weekdaythisareaisactive==`wkd'
					
					// Create a smoothed time series of non-market day values across the time range. 
					// Important so averages are not affected by temporal clustering of observations
					lpoly best_s_full date if mktday == 0, at(date) bw(`bw') gen(nonmkt_lp) nograph // name(g`m'`wkd'`gen'_non, replace) 
					// Calculate for each observation the mean of non-market day values within 365 days
					rangestat (mean) nonmkt_lp, i(date minInterval maxInterval)
					// Normalize each observation by their reference non-market day observation. Ensures index is 0 on average
					gen nonmkt_avg0 = best_s_full - nonmkt_lp_mean
					// There cannot be negative market activity by assumption. Less conservative alternative would be to drop negative obs
					replace nonmkt_avg0 = 0 if mktday==1 & nonmkt_avg0 <0
					
					// Create a smoothed time series of market day values across the time range. 
					lpoly nonmkt_avg0 date if mktday==1, at(date) bw(`bw') gen(mkt_lp)  nograph //  name(g`m'`wkd'`gen'_mkt, replace) 
					// Calculate for each observation the mean of market day values within 365 days
					rangestat (mean) mkt_lp, i(date minInterval maxInterval)
					// Normalize so that market-day observations are 100 on average
					replace mkt_avg100 = 100*(nonmkt_avg0/mkt_lp_mean)

					keep mkt_avg100 date `groupvars'
					tempfile tmp
					save `tmp'
				restore
				merge 1:1 date `groupvars' using `tmp', nogen update assert(1 3 4)
				}	
			}
		}
		timer off 1
		timer list
	}	
	keep if mkt_avg100!=.
end
