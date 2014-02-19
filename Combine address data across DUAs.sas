/******************************************************************************

PURPOSE: to create a long address dataset containing all unique addresses for a
case that ever appeared in a DUA; this will facilitate management of the
georaphic data, as well as allow for more accurate classification of past PLWHA

OUTPUTS: 
	-a long, address-based dataset containing (1) stateno, (2) all address variables
	in the DUA (street address 1 & 2, zip, city, county), (3) their counterparts
	as standardized by ZP4, and (4) and geographic data from GIS (match address, 
	x, y, ct0010, ct10, place)

NOTES: 
	-NA

******************************************************************************/

/* INPUTS: */ 

	/* raw DUA datasets from the state (both the BERKELEY and the ALAMEDA CO
	person view datasets provided by the state)*/

/*****************************************************************************/

/* MACROS CALLED: */

	/*-------------------------------------------------------------------------
	%AVAIL_ADDR_VAR: returns a macro var (&AVAIL_ADDR_VAR.) containing a space-delimited 
	list of address variables in the dataset
	-------------------------------------------------------------------------*/
		%macro avail_addr_var(libname, memname);
			%global avail_addr_var;
			%let avail_addr_var=;

			proc sql noprint;
				select distinct name
					into :avail_addr_var separated by ' '
				from dictionary.columns 
				where libname="%upcase(&libname.)" 
						and memname="%upcase(&memname.)" 
						and scan(name,1,'_') in ('rsh' 'rsa' 'rsd' 'cur' 'rad' 'rsx' 'rsxc')
						and scan(name,-2,'_')||'_'||scan(name,-1,'_') in ('street_address1' 'street_address2' 'zip_cd' 'city_name' 'county_name' 'state_cd')
				order by name; 

			%put &avail_addr_var.;
		%mend;

	/*-------------------------------------------------------------------------
	%AVAIL_GEO_VAR: returns a macro var (&AVAIL_GEO_VAR.) containing a 
		space-delimited list of geographic variables in the dataset
	-------------------------------------------------------------------------*/
	%macro avail_geo_var(libname, memname);
			%global avail_geo_var;
			%let avail_geo_var=;

			proc sql noprint;
				select distinct name
					into :avail_geo_var separated by ' '
				from dictionary.columns 
				where libname="%upcase(&libname.)" 
						and memname="%upcase(&memname.)" 
						and scan(name,1,'_') in ('rsh' 'rsa' 'rsd' 'cur' 'rad' 'rsx' 'rsxc')
						and scan(name,-1,'_') in ('matchaddr' 'x' 'y' 'ct0010' 'ct10' 'place')
				order by name; 

			%put &avail_geo_var.;
		%mend;

	/*-------------------------------------------------------------------------
	%STANDARDIZE_VAR_LENGTHS: returns  a dataset (&DATASET._LEN) containing all 
		variables identified by the VARS parameter, all having a standardized 
		length (the maximum seen for that variable looking across all available 
		DUAs)
	-------------------------------------------------------------------------*/

		%macro standardize_var_lengths(	dataset,
										vars /*a space-delimited list of variable names*/,
										also_keep /*a space-delimited list of variables that do NOT need to have their lengths standardized but should be retained in the output dataset*/)

			proc sql noprint;
				select 	name, max_length
					into 	:names separated by ' ',
							:lengths separated by ' '
				from DUA_root.all_duas_attr
				where name in ("%sysfunc(tranwrd(&vars.,%quote( ),%quote(" ")))");

			data &dataset._len;
				set &dataset. 
					(rename=(%do j=1 %to %sysfunc(countw(&vars.));
								%scan(&names.,&j.)=%scan(&names.,&j.)1
							%end;
							)
					);
				%do j=1 %to %sysfunc(countw(&names.));
					length %scan(&names.,&j.) $ %scan(&lengths.,&j.);
					%scan(&names.,&j.)=%scan(&names.,&j.)1;
				%end;

				keep &vars. &also_keep.;
			run;
		%mend;

	/*-------------------------------------------------------------------------
	%IMPUTE_ADDR_ELEMENTS & %COMITT_IMPUTATION: Impute address elements as 
		needed and as possible
	NOTE: construct a single, cleaned street_address variable before running this program
	-------------------------------------------------------------------------*/

/*	%let dataset=all_addr_long;*/
/*	%let impute_var=state;*/
/*	%let match_vars=zip;*/

	%macro impute_addr_elements(dataset 	/*the dataset to clean*/,
								impute_var 	/*variable whose values to impute*/,
								match_vars 	/*variable(s) on which another record must match (besides STATENO) in order for that record's value of IMPUTE_VAR to populate IMPUTE_VAR for the index record*/);

		/* create a dataset with distinct combinations of 1) stateno, (2) the variable to impute, and (3) the matching variable (where all non-missing) */
		proc sql;
			create table &impute_var._&match_vars. as
				select distinct stateno, 
								&match_vars., 
								&impute_var. as &impute_var._cmplt
				from &dataset. as A
				where not missing(&match_vars.) 
					and not missing(&impute_var.)
					and not exists (select stateno, &match_var., count(*) as count
									from &dataset. as B
									where A.stateno=B.stateno
										and A.&match_var.=B.&match_var.
									group by stateno, &match_var.
									having count(*) > 1) /* there is only one value of IMPUTE_VAR for the given value of MATCH_VAR for STATENO */
				order by stateno, &match_vars.;

		/* merge by stateno AND the matching variable and impute values of IMPUTE_VAR where missing */
		proc sort data=&dataset.; by stateno &match_vars.; run;
		data &dataset._&impute_var._imp(drop=&impute_var._cmplt)
				&dataset._CHECK_&impute_var._imp;
			merge 	&dataset.(in=A)
					&impute_var._&match_vars.(in=B);
			by stateno &match_vars.;

			if missing(&impute_var.) then do;
				&impute_var.=&impute_var._cmplt;
				&impute_var._imp_flag=1;
			end;
		run;

		proc sql;
			create table check_&impute_var._imp as
				select 	stateno, 
						&match_vars., 
						&impute_var., 
						&impute_var._cmplt, 
						&impute_var._imp_flag
				from &dataset._CHECK_&impute_var._imp
				order by stateno, &match_vars., &impute_var., &impute_var._cmplt, &impute_var._imp_flag;

	%mend; 

	%macro committ_imputation(	dataset,
								&impute_var.);
		data &dataset.;
			set &dataset._&impute_var._imp;
		run;
	%mend;

	/* impute zip_cd */
	*%macro impute_addr_elements(dataset=all_addr_long,
								impute_var=zip_cd,
								match_vars=street_address);

	/* impute city_name */
	*%macro impute_addr_elements(dataset=all_addr_long,
								impute_var=city_name,
								match_vars=street_address);

	/* impute county_name */
	*%macro impute_addr_elements(dataset=all_addr_long,
								impute_var=county_name,
								match_vars=street_address);

	/* impute state_cd */
	*%macro impute_addr_elements(dataset=all_addr_long,
								impute_var=state_cd,
								match_vars=street_address);

		*%macro impute_addr_elements(dataset=all_addr_long,
									impute_var=state_cd,
									match_vars=zip_cd);

/*****************************************************************************/

/*=============================================================================
Create wide address datasets for each DUA
=============================================================================*/

%macro make_addr_wide(DUA_year, DUA_qtr);

	%let DUA=Q&DUA_qtr._&DUA_year.;

	%if &DUA_year.=2011 or (&DUA_year.=2012 and &DUA_qtr.=1) %then %do; /*b/c we don't have SAS datasets for these DUAs, only Excel files which need to be imported*/
		%do i=1 %to 2;
			%if &i.=1 %then %let berkeley=BERKELEY_;
			%else %if &i.=2 %then %let BERKELEY=;
				proc import 
					datafile="&DUA_path.\&DUA_year.\Q&DUA_qtr.\&DUA._&berkeley.person.csv" 
					out=&DUA._&berkeley.person_import 
					DBMS=csv 
					replace; 
				run;

				%avail_addr_var(libname=WORK, 
								memname=&DUA._&berkeley.person_import); run;

				data &DUA._&berkeley.person;
					set &DUA._&berkeley.person_import;
					if _N_=1 then delete; /*Delete the additional header row (used to force all variables to be read with type char)*/
					keep stateno status_flag &avail_addr_var.;
				run;
			
				/*Re-declare the variable with the maximum length seen across DUAs*/
				%standardize_var_lengths(	dataset=&DUA._&berkeley.person, 
											vars=stateno status_flag &avail_addr_var. /*a space-delimited list of character variable names*/,
											also_keep=
										); run;

		%end;
		/*concatenate*/
		data &DUA._AlCo_person;
			set &DUA._person_len
				&DUA._berkeley_person_len;
		run;
			
		/*de-duplicate*/
		proc sort data=&DUA._AlCo_person nodupkey out=&DUA._AlCo_person_dedup; by stateno; run;

		/*Write to disk*/
		data &DUA..&DUA._addr_wide;
			set &DUA._AlCo_person_dedup;
		run;
	%end;

	%else %if &DUA_year.=2010 %then %do;
		%avail_addr_var(libname=&DUA.,
						memname=&DUA._AC_and_Berk); run;

		data &DUA._addr;
			set &DUA..&DUA._AC_and_Berk;
			keep stateno status_flag &avail_addr_var.;
		run;

		%standardize_var_lengths(dataset=&DUA._addr, 
								vars=stateno status_flag &avail_addr_var.,
								also_keep=); run;

		/*Write to disk*/
		data &DUA..&DUA._addr_wide;
			set &DUA._addr_len;
		run;
	%end;

	%else %do;
		%avail_addr_var(libname=&DUA.,
						memname=AlCo_&DUA.); run;

		data &DUA._addr;
			set &DUA..AlCo_&DUA.;
			keep stateno status_flag &avail_addr_var.;
		run;

		%standardize_var_lengths(dataset=&DUA._addr, 
								vars=stateno status_flag &avail_addr_var.,
								also_keep=&avail_geo_var.); run;

		/*Write to disk*/
		data &DUA..&DUA._addr_wide;
			set &DUA._addr_len;
		run;

	%end;

	/*Check table RxCs*/
	proc sql;
		select libname, memname, nobs, nvar, crdate
		from dictionary.tables
		order by crdate desc;
%mend;

%make_addr_wide(DUA_year=2010, DUA_qtr=4); run;
%make_addr_wide(DUA_year=2011, DUA_qtr=1); run;
%make_addr_wide(DUA_year=2011, DUA_qtr=2); run;
%make_addr_wide(DUA_year=2011, DUA_qtr=3); run;
%make_addr_wide(DUA_year=2011, DUA_qtr=4); run;
%make_addr_wide(DUA_year=2012, DUA_qtr=1); run;
%make_addr_wide(DUA_year=2012, DUA_qtr=2); run;
%make_addr_wide(DUA_year=2012, DUA_qtr=3); run;
%make_addr_wide(DUA_year=2012, DUA_qtr=4); run;
%make_addr_wide(DUA_year=2013, DUA_qtr=1); run;
%make_addr_wide(DUA_year=2013, DUA_qtr=2); run;
%make_addr_wide(DUA_year=2013, DUA_qtr=3); run;
%make_addr_wide(DUA_year=2013, DUA_qtr=4); run;

/*=============================================================================
Reshape address datasets from long to wide
=============================================================================*/
/*	%let DUA_qtr=1;*/
/*	%let DUA_year=2013;*/
/*	%let i=1;*/

%macro reshape_addr(DUA_qtr /*quarter of the DUA (1, 2, 3 or 4)*/, 
					DUA_year /*year of the DUA (i.e., 2013)*/);

	%let DUA=Q&DUA_qtr._&DUA_year.; %put &DUA.;

	/*Generate a list of available sets of address data...*/
	proc sql;
		select distinct scan(name,1,'_')
			into :avail_addr_types separated by ' '
		from dictionary.columns 
		where libname="&DUA." 
				and memname="&DUA._ADDR_WIDE" 
				and scan(name,-2,'_')||'_'||scan(name,-1,'_') in ('street_address1' 'street_address2' 'zip_cd' 'city_name' 'county_name' 'state_cd'); %put &avail_addr_types.;

	/*For each available set of address data (cur, rsa, rsh, etc.)…*/
	%do i=1 %to %sysfunc(countw(&avail_addr_types.));
		%let addr_type=%scan(&avail_addr_types.,&i.);

		/*Identify any geographic variables that may already exist in the DUA for this address type*/
		%let geo_var=;
		proc sql noprint;
			select distinct name
				into :geo_var separated by ' '
			from dictionary.columns 
			where libname="&DUA." 
					and memname="&DUA._ADDR_WIDE" 
					and scan(name,1,'_')="&addr_type."
					and scan(name,-1,'_') in ('matchaddr' 'x' 'y' 'ct0010' 'ct10' 'place')
			order by name; %put &geo_var.;	

		data &addr_type._addr_data;
			set &DUA..&DUA._addr_wide;

		/*Add variables identifying the dataset from which the data came*/
			length 	DUA_yr 4
					DUA_qtr 3;
			label 	DUA_yr='Year of the first DUA in which the address appeared for the case'
					DUA_qtr='Quarter of the first DUA in which the address appeared for the case';
			DUA_yr=&DUA_year.;
			DUA_qtr=&DUA_qtr.;

		/*Create a variable reflecting the address type (cur, rsh, rsa)*/
			length addr_type $3;
			label addr_type='Either cur, rsd, rsh, rsa, rsx, rsxc, or rad (see data dictionary)';
			addr_type="&addr_type.";

		/*Declare geographic variables with generic names 
			(i.e., street_address1 instead of cur_street_address1)*/
			length 	street_address1 street_address2	$50
					zip_cd							$10
					city_name						$128
					county_name						$64
					state_cd						$50
					matchaddr 						$50
					x y								8
					ct0010 ct10						$11
					place 							$20;

			label 	x='x-coordinate of geocoded address (in ft)'
					y='y-coordinate of geocoded address (in ft)'
					ct0010='2000 census tract (as ascertained by geocoding)'
					ct10='2010 census tract (as ascertained by geocoding)'
					place='Census-designated place (as ascertained by geocoding)';

			street_address1=&addr_type._street_address1;
			street_address2=&addr_type._street_address2;
			zip_cd=&addr_type._zip_cd;
			city_name=&addr_type._city_name;
			county_name=&addr_type._county_name;
			state_cd=&addr_type._state_cd;

			%if %length(&geo_var.) NE 0 %then %do;
				matchaddr=&addr_type._matchaddr;
				x=&addr_type._x;
				y=&addr_type._y;
				ct0010=&addr_type._ct0010;
				ct10=&addr_type._ct10;
				place=put(upcase(&addr_type._place),$NewCityFs.);
				format place $NewCityFl.;
			%end;

			keep 	stateno status_flag DUA_yr DUA_qtr addr_type
					%if %length(&geo_var.) NE 0 %then %do;
						matchaddr x y ct0010 ct10 place
					%end;
					street_address1 street_address2 zip_cd city_name county_name state_cd;
		run;
	%end;

	/*Concatenate the cur, rsa, and rsh datasets*/
	data &DUA..&DUA._addr_long;
		set %sysfunc(tranwrd(&avail_addr_types.,%quote( ),_addr_data%quote( )))_addr_data;
	run;
%mend reshape_addr; run;

%reshape_addr(DUA_year=2010, DUA_qtr=4); run;
%reshape_addr(DUA_year=2011, DUA_qtr=1); run;
%reshape_addr(DUA_year=2011, DUA_qtr=2); run;
%reshape_addr(DUA_year=2011, DUA_qtr=3); run;
%reshape_addr(DUA_year=2011, DUA_qtr=4); run;
%reshape_addr(DUA_year=2012, DUA_qtr=1); run;
%reshape_addr(DUA_year=2012, DUA_qtr=2); run;
%reshape_addr(DUA_year=2012, DUA_qtr=3); run;
%reshape_addr(DUA_year=2012, DUA_qtr=4); run;
%reshape_addr(DUA_year=2013, DUA_qtr=1); run;
%reshape_addr(DUA_year=2013, DUA_qtr=2); run;
%reshape_addr(DUA_year=2013, DUA_qtr=3); run;
%reshape_addr(DUA_year=2013, DUA_qtr=4); run;


/*=============================================================================
Concatenate long address datasets
=============================================================================*/
/*Concatenate all wide address datasets and delete rows:
	(1) missing stateno, 
	(2) lacking any address data, 
	(3) or for cases with status_flag in ('D' 'E' 'R' 'P')*/

data 	all_addr_long1 
		null_addr 
		stateno_status_flag;
	set 
		%macro temp;
		%do year=2010 %to 2013;
			%do qtr=1 %to 4;
				%if &year.=2010 %then %let qtr=4;
				Q&qtr._&year..Q&qtr._&year._addr_long
			%end;
		%end;
		;
		%mend; %temp;
	if missing(street_address1) and missing(street_address2) and missing(zip_cd) and missing(city_name) and missing(county_name) and missing(state_cd) then 
		output null_addr;
		else if missing(stateno) or status_flag not in ('A' '(A)Active' 'W' '(W)Warning') then output stateno_status_flag;
		else output all_addr_long1;
run;

/*Create ordered numeric variable based on address type for use in de-duplication*/
proc format;
	value $addr_typeF
		'rsh'='1'
		'rsa'='2'
		'rsd'='3'
		'rsxc'='4'
		'rsx'='5'
		'rad'='6'
		'cur'='7';

data DUA_root.all_addr_long(drop=status_flag state_cd1);
	set all_addr_long1(rename=(state_cd=state_cd1));

	/*Create a numeric version of addr_type so that addresses can be sorted as desired*/
	length addr_type_num $ 3;
	addr_type_num=put(addr_type, $addr_typeF.);

	/*Clean the state_cd variable (formatted value was stored in the dataset prior to Q2_2012)*/
	length state_cd $ 2;
	if length(state_cd1)>2 then do;
		state_cd=substr(state_cd1,2,2);
	end;
	else state_cd=state_cd1;
run;


/*=============================================================================
Send address data elements to ZP4 for cleaning
=============================================================================*/
data to_zp4;
	set DUA_root.all_addr_long;

	/* rename columns as necessary to be consistent with .DBF column name constraints (10 char max) */
	rename 	street_address1=street1 
			street_address2=street2
			county_name=county;

	/* declare new variables in which to store zp4 output (cleaned and standardized address elements) */
	length 	str1_cln $ 50
			str2_cln $ 50
			zip_cln $ 10
			city_cln $ 128
			county_cln $ 64
			state_cln $ 2;

	keep 	stateno DUA_yr DUA_qtr addr_type
			street_address1 	street_address2 	zip_cd 		city_name 	county_name 	state_cd
			str1_cln 			str2_cln 			zip_cln 	city_cln 	county_cln 		state_cln;
run;

/* add a record containing values the full length of the column 
in order to force the .DBF file to have columns of the appropriate width 
(column widths are determined by the maximum length seen in the data) */
data n1;
	set DUA_root.to_zp4;

	if _N_=1 then do;
		stateno='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
		DUA_yr=4444;
		DUA_qtr=333; 
		addr_type='aaa';
		street1='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; 
		street2='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; 
		zip_cd='aaaaaaaaaa'; 
		city_name='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; 
		county='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; 
		state_cd='aa';
		str1_cln='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
		str2_cln='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
		zip_cln='aaaaaaaaaa';
		zp4_cln='aaaa';
		city_cln='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
		county_cln='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
		state_cln='aa';
	end;
	else delete;
run;

data DUA_root.to_zp4_%sysfunc(tranwrd(&today_date.,-,_));
	set n1 
		to_zp4;
run;

/* #################################################################################################################################### */
/* Run ZP4batch to clean address data */
/* #################################################################################################################################### */

data from_zp4;
	set DUA_root.to_zp4_2014_02_18(drop=county_nam zp4_cln);
	if _N_ NE 1; /* delete extra row used to force desired column widths */

	/* restore original column names */
	rename 	street1=street_address1 
			street2=street_address2
			county=county_name

			str1_cln=street_address1_cln
			str2_cln=street_address2_cln
			zip_cln=zip_cd_cln
			city_cln=city_name_cln
			county_cln=county_name_cln
			state_cln=state_cd_cln;

	/* populate county_name_cln variable where ZP4 failed to find a matching street address */
	if not missing(county_name) and missing(county_name_cln) then county_name_cln=county_name;

	length addr_type_num $ 3;
	addr_type_num=put(addr_type, $addr_typeF.);
run;

/*=============================================================================
Sort, impute values of select missing variables, and de-duplicate
=============================================================================*/
/*Sort so that when de-duplicated on stateno and address, the earliest record for that address will sort highest 
(and, within quarter, the record with the earliest and most precise associated point in time; i.e. rsh, rsa)*/
proc sort 
	data=from_zp4; 
	by 	stateno 
		street_address1_cln 
		street_address2_cln
		zip_cd_cln 
		city_name_cln 
		county_name_cln 
		state_cd_cln
		DUA_yr 
		DUA_qtr 
		addr_type_num; 
run;

/*Remove duplicate address records*/
proc sort 
	data=from_zp4
	out=dedup_cln_addr(drop=addr_type_num)
	nodupkey; 
	by 	stateno 
		street_address1_cln 
		street_address2_cln
		zip_cd_cln 
		city_name_cln 
		county_name_cln 
		state_cd_cln; 
run; /* 517,848 obs reduced to 37,997 */

/*=============================================================================
Merge in original geographic data
=============================================================================*/

data geo_Q2_2013;
	set Q2_2013.Q2_2013_addr_long;
	where not missing(x);
	keep street_address1
			zip_cd
			matchaddr
			x
			y
			ct0010
			ct10
			place;
run;

proc sort 
	data=geo_Q2_2013
	nodupkey; 
	by 	street_address1
		zip_cd;
run;

proc sort 
	data=dedup_cln_addr;
	by 	street_address1
		zip_cd; 
run;

data geo_merged;
	merge 	dedup_cln_addr(in=A)
			geo_Q2_2013(in=B);
	by 	street_address1
		zip_cd;
	if A; /* exclude records in geo_Q2_213 only */
run;

/*=============================================================================
Associate dates with each address
=============================================================================*/

/*Merge in dates (DUA dates and dx dates)*/
data dates;
	set Q4_2013.alco_q4_2013;
	where not missing(stateno) 
		and status_flag in ('A' 'W');

	keep 	stateno 
			hiv_dx_dt_num_MDImp 		hiv_dx_dt_num_DImp			hiv_dx_dt_num			hiv_dx_dt
			aids_dx_dt_num_MDImp		aids_dx_dt_num_DImp			aids_dx_dt_num			aids_dx_dt
			hiv_aids_dx_dt_num_MDImp	hiv_aids_dx_dt_num_DImp		hiv_aids_dx_dt_num		hiv_aids_dx_dt
			dod_num_MDImp				dod_num_DImp				dod_num					dod;
run;

proc sort data=geo_merged; by stateno; run;
proc sort data=dates; by stateno; run;
data dt_merged
	BnotA;
	merge 	geo_merged(in=A)
			dates(in=B);
	by stateno;

	if A; /* exclude records for cases without any address data in any DUA (only 11 of these as of 2/18/2014) */
	
	length addr_start_dt $8;
	length addr_start_dt_num 4;
	length addr_start_dt_num_DImp 4;
	length addr_start_dt_num_MDImp 4;
	label addr_start_dt='Earliest date address was documented (date of corresponding event--e.g., dx, death--or release date of the first DUA in which the address appears for the case)';

	if addr_type in ('cur' 'rsx' 'rsxc') then do;
		if DUA_qtr=1 then addr_start_dt_m=4;
		else if DUA_qtr=2 then addr_start_dt_m=7;
		else if DUA_qtr=3 then addr_start_dt_m=10;

		if DUA_qtr in (1 2 3) then do;
			addr_start_dt_num=mdy(addr_start_dt_m,1,DUA_yr);
			addr_start_dt_num_DImp=mdy(addr_start_dt_m,1,DUA_yr);
			addr_start_dt_num_MDImp=mdy(addr_start_dt_m,1,DUA_yr);
		end;
		else do;
			addr_start_dt_num=mdy(1,1,DUA_yr+1);
			addr_start_dt_num_DImp=mdy(1,1,DUA_yr+1);
			addr_start_dt_num_MDImp=mdy(1,1,DUA_yr+1);
		end;
	end;
	else if addr_type='rsa' then do;
		addr_start_dt=aids_dx_dt;
		addr_start_dt_num=aids_dx_dt_num;
		addr_start_dt_num_DImp=aids_dx_dt_num_DImp;
		addr_start_dt_num_MDImp=aids_dx_dt_num_MDImp;
	end;
	else if addr_type='rsh' then do;
		addr_start_dt=hiv_dx_dt;
		addr_start_dt_num=hiv_dx_dt_num;
		addr_start_dt_num_DImp=hiv_dx_dt_num_DImp;
		addr_start_dt_num_MDImp=hiv_dx_dt_num_MDImp;
	end;
	else if addr_type='rsd' then do;
		addr_start_dt=hiv_aids_dx_dt;
		addr_start_dt_num=hiv_aids_dx_dt_num;
		addr_start_dt_num_DImp=hiv_aids_dx_dt_num_DImp;
		addr_start_dt_num_MDImp=hiv_aids_dx_dt_num_MDImp;
	end;
	else if addr_type='rad' then do;
		addr_start_dt=dod;
		addr_start_dt_num=dod_num;
		addr_start_dt_num_DImp=dod_num_DImp;
		addr_start_dt_num_MDImp=dod_num_MDImp;
	end;

	format addr_start_dt_num addr_start_dt_num_DImp addr_start_dt_num_MDImp date9.;

	drop 	addr_start_dt_m
			hiv_dx_dt_num_MDImp 		hiv_dx_dt_num_DImp			hiv_dx_dt_num			hiv_dx_dt
			aids_dx_dt_num_MDImp		aids_dx_dt_num_DImp			aids_dx_dt_num			aids_dx_dt
			hiv_aids_dx_dt_num_MDImp	hiv_aids_dx_dt_num_DImp		hiv_aids_dx_dt_num		hiv_aids_dx_dt
			dod_num_MDImp				dod_num_DImp				dod_num					dod;
run;

proc freq data=dt_merged;
	table addr_start_dt_num_MDImp / missing;
	format addr_start_dt_num_MDImp missing_num.;
run;

data dt_merged1;
	set dt_merged;
	DUA=catx('_',DUA_yr,DUA_qtr);
run;

proc freq data=dt_merged1;
	table DUA*addr_start_dt_num_MDImp / missing nofreq nopercent norow;
	format addr_start_dt_num_MDImp missing_num.;
run;

proc sql;
	create table impersistent_stateno as
		select stateno
		from DUA_root.stateno_merged
		where missing(stateno_Q4_2013)
		order by stateno;

	create table stateno_missing_addr_dt as
		select distinct stateno
		from dt_merged
		where missing(addr_start_dt_num_MDImp)
		order by stateno;

	create table stateno_check as
		select A.*, case
					when exists (	select stateno 
									from impersistent_stateno as B 
									where B.stateno=A.stateno) then 'impersistent'
					else ''
					end as impersistent
		from stateno_missing_addr_dt as A;

proc freq data=stateno_check;
	table impersistent / missing;
run;

data stateno_merged;
	set DUA_ROOT.stateno_merged;

	array statenos 	{13}
					stateno_Q4_2010
					stateno_Q1_2011
					stateno_Q2_2011
					stateno_Q3_2011
					stateno_Q4_2011
					stateno_Q1_2012
					stateno_Q2_2012
					stateno_Q3_2012
					stateno_Q4_2012
					stateno_Q1_2013
					stateno_Q2_2013
					stateno_Q3_2013
					stateno_Q4_2013;

	do i=1 to 13;
		if not missing(statenos{i}) then do;
			first_non_missing=i;
			goto done;
		end;
	end;
	done:
run;

/*Add start and end date each address record*/
/*proc sort data=dt_merged; by stateno descending addr_start_dt_num_MDImp; run;*/
/*data addr_dates;*/
/*	set dt_merged;*/
/**/
/*	length 	addr_end_dt_num */
/*			addr_end_dt_num_DImp */
/*			addr_end_dt_num_MDImp */
/*	4;*/
/**/
/*	by stateno;*/
/*	retain 	addr_start_dt_num */
/*			addr_start_dt_num_DImp */
/*			addr_start_dt_num_MDImp;*/
/*	if not missing(lag(addr_start_dt_num)) then addr_end_dt_num=lag(addr_start_dt_num)-1;*/
/*		else addr_end_dt_num=.;*/
/*	if not missing(lag(addr_start_dt_num_DImp)) then addr_end_dt_num_DImp=lag(addr_start_dt_num_DImp)-1;*/
/*		else addr_end_dt_num_DImp=.;*/
/*	if not missing(lag(addr_start_dt_num_MDImp)) then addr_end_dt_num_MDImp=lag(addr_start_dt_num_MDImp)-1;*/
/*		else addr_end_dt_num_MDImp=.;*/
/*	if first.stateno then do;*/
/*		addr_end_dt_num=.;*/
/*		addr_end_dt_num_DImp=.;*/
/*		addr_end_dt_num_MDImp=.;*/
/*	end;*/
/**/
/*	format 	addr_end_dt_num */
/*			addr_end_dt_num_DImp */
/*			addr_end_dt_num_MDImp */
/*	date9.;*/
/*run;*/

/*=============================================================================
Merge in earliest date associated with each cur address from the CDPH-provided
address dataset
=============================================================================*/

/*######################################################################################################################################################

######################################################################################################################################################*/

	/*Code to identify PLWHA at different points in time*/
	/*Test code and new address dataset against address data in original DUAs*/

/*######################################################################################################################################################

######################################################################################################################################################*/

/*=============================================================================
Export addresses needing geocoding
=============================================================================*/
data ...;
	set ...;

	/*Create a variable indicating why it wasn’t geocoded (missing or unknown street address, homeless, P.O. box, not possibly in AlCo)*/
		length addr_flag $3;
		label addr_flag='If the address could not be geocoded, the reason why';
		if cmiss(street_address1,street_address2,zip_cd,city_name,county_name,state_cd)=6
			then addr_flag='01' /*'No address data'*/;
		else if zip_cd not in (&AlCo_zips_all.)
			and prxmatch("/&AlCo_cities_vbar./",upcase(city_name))=0
			and index(upcase(county_name),'ALAMEDA')=0
			then addr_flag='02' /*'Out of county'*/;
		else if missing(street_address1) or street_address1 in ('UNKNOWN' 'UNKOWN' 'UNK' 'NO STREET ADDRESS GIVEN') 
			then addr_flag='03' /*'Missing or unknown street address'*/;
		else if not (missing(street_address1) or street_address1='UNKNOWN') and missing(zip_cd) and missing(city_name) and missing(county_name)
			then addr_flag='04' /*'Indeterminate street address (missing zip, city, and county)'*/;
		else if index(street_address1,'HOMELESS') or index(street_address1,'HOMLESS')
			then addr_flag='05' /*'Homeless'*/;
		else if prxmatch("/P? ?[.]? ?O? ?[.]? ?BOX|POB /",street_address1) NE 0 
			then addr_flag='06' /*'PO BOX'*/;
		else addr_flag='07' /*'Potentially geocodable address potentially in Alameda County'*/;
		format addr_flag $addr_flagFl.;
run;

/*Select addresses that were not geocoded but are potentially in Alameda County*/
proc sql;
	create table not_geocoded as
		select * 
		from Q&DUA_qtr._&DUA_yr._addr_long
		where (missing(x) or missing(y)) and addr_flag='07' /*'Potentially geocodable address potentially in Alameda County'*/;

/*Export addresses needing geocoding (first cleanse address data using ZP4)*/
proc export 
	data=geocodable_addr_not_geocoded 
	dbms=dbf
	file="&output_path.\&geo_DUA. geocodable_addr_not_geocoded.csv" 
	replace; 
run;

/*Merge the geocoded dataset back into the original on stateno and addr_type, 
specifying the geocoded dataset last in the merge statement so that it’s values will replace any similarly-named variable in the original address dataset 
(these will match was already there if not originally missing)*/

/********************************************************************************************************************************************************/
/*Check code*/ data _null_;
/********************************************************************************************************************************************************/

Proc sql;
	create table check_place as
		select distinct cur_place as orig_cur_place, 
						put(upcase(cur_place),$NewCityFs.) as new_cur_place,
						put(upcase(cur_place),$NewCityFs.) format=$NewCityFl. as new_cur_place_formatted
		from &geo_DUA.;

proc sql;
	create table check_bad_addr as
		select * 
		from  geocoded
		where missing(x) or missing(y)
		order by addr_flag;

%macro check_addr_completeness;
	%do i=1 %to 3;
		%if &i.=1 %then %let addr_type=cur;
		%else %if &i.=2 %then %let addr_type=rsa;
		%else %if &i.=3 %then %let addr_type=rsh;
		proc sql;
			create table check_&addr_type._completeness as
				select &addr_type._street_address1, &addr_type._street_address2
				from &geo_DUA.
				where not missing(stateno)
				order by &addr_type._street_address1, &addr_type._street_address2;
		run;
	%end;
%mend; %check_addr_completeness; run;

data temp;
	set Q4_2013.AlCO_Q4_2013;

	hiv_aids_dx_yr=year(hiv_aids_dx_dt_num_MDImp);
run;

	proc freq data=temp;
		where status_flag in ('A' 'W') and year(hiv_aids_dx_dt_num_MDImp) >= 2006;
		table 	hiv_aids_dx_yr*rsd_street_address1
			/ missing nopercent nocol nofreq;
		format rsd_street_address1 $missing_str.;
	run;

%macro test;
%do i=1 %to 4;
	%if &i.=1 %then %let dt_type=;
	%else %if &i.=2 %then %let dt_type=_num;
	%else %if &i.=3 %then %let dt_type=_num_DImp;
	%else %if &i.=4 %then %let dt_type=_num_MDImp;
	proc sql;
		create table check_&dt_type. as
			select distinct addr_type, DUA_yr, DUA_qtr,
				hiv_dx_dt&dt_type., 
				aids_dx_dt&dt_type., 
				hiv_aids_dx_dt&dt_type., 
				dod&dt_type.,
				addr_dt&dt_type.
			from dt_merged
			order by addr_type,DUA_yr, DUA_qtr;
%end;
%mend; %test; run;

proc sql;
	select distinct addr_start_dt_num_MDImp, DUA_yr, DUA_qtr
	from dt_merged
	where addr_type='cur'
	order by DUA_yr, DUA_qtr;