/*######################################################################################################################################################*/
/*Compare structure of different DUAs (WRT lab var)*/ data _null_;
/*######################################################################################################################################################*/

	%macro compare_DUAs;
		Proc sql;
			select name into :lab_var_apos separated by '" "'
			from dictionary.columns
			where 	libname='Q2_2013' 
					and memname='ALCO_Q2_2013' 
					and (index(name,'vl') NE 0 or index(name,'cd4') NE 0) 
					and index(name,'num')=0
			order by name;
		quit;

		%do i=1 %to 11;
			%if &i.=1 %then %let dat=Q1_2011;
			%if &i.=2 %then %let dat=Q2_2011;
			%if &i.=3 %then %let dat=Q3_2011;
			%if &i.=4 %then %let dat=Q4_2011;

			%if &i.=5 %then %let dat=Q1_2012;
			%if &i.=6 %then %let dat=Q2_2012;
			%if &i.=7 %then %let dat=Q3_2012;
			%if &i.=8 %then %let dat=Q4_2012;

			%if &i.=9 %then %let dat=Q1_2013;
			%if &i.=10 %then %let dat=Q2_2013;
			%if &i.=11 %then %let dat=Q3_2013;
				Proc sql;
					create table &dat._lab_var as
						select name, type as type_&dat., length as len_&dat.
						from dictionary.columns
						where libname="&dat." and memname="ALCO_&dat." 
							and (name='stateno' or name in ("&lab_var_apos."))
						order by name;
				%end;

	data merged;
		merge 	Q1_2011_lab_var 
				Q2_2011_lab_var
				Q3_2011_lab_var
				Q4_2011_lab_var

				Q1_2012_lab_var
				Q2_2012_lab_var
				Q3_2012_lab_var 
				Q4_2012_lab_var 

				Q1_2013_lab_var 
				Q2_2013_lab_var 
				Q3_2013_lab_var;
		by name;

		if type_Q1_2011=type_Q2_2011=type_Q3_2011=type_Q4_2011=type_Q2_2012=type_Q3_2012=type_Q4_2012=type_Q1_2013=type_Q2_2013=type_Q3_2013 then type_const=1; 
		else type_const=0;

		if len_Q1_2011=len_Q2_2011=len_Q3_2011=len_Q4_2011=len_Q2_2012=len_Q3_2012=len_Q4_2012=len_Q1_2013=len_Q2_2013=len_Q3_2013 then len_const=1; 
		else len_const=0;
	run;
	%mend;
	%compare_DUAs; run;

/*######################################################################################################################################################*/
/*Identify lab variables*/ data _null_;
/*######################################################################################################################################################*/

	Proc sql;
		select name into :lab_var separated by ' '
		from dictionary.columns
		where 	libname='Q2_2013' 
				and memname='ALCO_Q2_2013' 
				and (index(name,'vl') NE 0 or index(name,'cd4') NE 0) 
				and index(name,'num')=0
		order by name;

/*######################################################################################################################################################*/
/*Pull lab variables from each available DUA*/ data _null_;
/*######################################################################################################################################################*/

	%macro pull_lab_dat;
		%do i=2 %to 12;
			%if &i.=1 %then %let dat=Q4_2010;

			%if &i.=2 %then %let dat=Q1_2011;
			%if &i.=3 %then %let dat=Q2_2011;
			%if &i.=4 %then %let dat=Q3_2011;
			%if &i.=5 %then %let dat=Q4_2011;

			%if &i.=6 %then %let dat=Q1_2012;
			%if &i.=7 %then %let dat=Q2_2012;
			%if &i.=8 %then %let dat=Q3_2012;
			%if &i.=9 %then %let dat=Q4_2012;

			%if &i.=10 %then %let dat=Q1_2013;
			%if &i.=11 %then %let dat=Q2_2013;
			%if &i.=12 %then %let dat=Q3_2013;
			data &dat._labs; 
				set &dat..AlCo_&dat.;
				keep stateno &lab_var.;
			run;
		%end;
	%mend;
	%pull_lab_dat; run;

/*######################################################################################################################################################*/
/*Concatenate datasets*/ data _null_;
/*######################################################################################################################################################*/

	data all_labs_wide;
		set Q1_2011_labs
			Q2_2011_labs
			Q3_2011_labs 
			Q4_2011_labs

			Q1_2012_labs
			Q2_2012_labs
			Q3_2012_labs 
			Q4_2012_labs 

			Q1_2013_labs 
			Q2_2013_labs 
			Q3_2013_labs;
	run;

/*######################################################################################################################################################*/
/*Reshape data*/ data _null_;
/*######################################################################################################################################################*/

	%macro reshape_lab;
		%do i=1 %to 9; /*for each set of lab variables (includes an associated date, value, and sometimes type)...*/
			%if &i.=1 %then %let test=cd4_first_14;
			%else %if &i.=2 %then %let test=cd4_first_200;
			%else %if &i.=3 %then %let test=cd4_low_cnt;
			%else %if &i.=4 %then %let test=cd4_low_pct;
			%else %if &i.=5 %then %let test=cd4_recent_cnt;
			%else %if &i.=6 %then %let test=cd4_recent_pct;
			%else %if &i.=7 %then %let test=vl_first_det;
			%else %if &i.=8 %then %let test=vl_recent;
			%else %if &i.=9 %then %let test=cd4_first_hiv;
/*			%else %if &i.=10 %then %let test=vl_last_non_det;*/

			data &test._1; /*...extract just those variables (along with stateno) from the total lab dataset...*/
				set all_labs_wide;
/*				%if &i.=10 %then %do;*/
/*					keep stateno vl_last_non_det_dt;*/
/*				%end;*/
/*				%else */
				%if &i.=9 %then %do;
					keep stateno cd4_first_hiv_dt cd4_first_hiv_value cd4_first_hiv_type;
				%end;
				%else %do;
					keep stateno &test._dt &test._value;
				%end;
			run;

			data &test._2; /*...create the generic lab_test, lab_dt, and lab_value variables and populate with the value of the appropriate DUA variable...*/
				set &test._1;

				length lab_date $ 8;
				lab_date=&test._dt;

				length lab_value $ 8;
/*				if "&test."='vl_last_non_det' then lab_value='0.5';*/
/*				else */
				lab_value=&test._value;

				length lab_test $ 8;
/*				if &i.=10 then lab_test='VL';*/
/*				else */
				if &i.=9 then do;
					if cd4_first_hiv_type='CNT' then lab_test='CD4 cnt';
					else if cd4_first_hiv_type='PCT' then lab_test='CD4 pct';
					else lab_test='';
				end;
				else if (index("&test.",'cd4') NE 0 and index("&test.",'pct') NE 0)
					or (index("&test.",'cd4') NE 0 and index("&test.",'14') NE 0)
					then lab_test='CD4 pct';
				else if (index("&test.",'cd4') NE 0 and index("&test.",'cnt') NE 0)
					or (index("&test.",'cd4') NE 0 and index("&test.",'200') NE 0)
					then lab_test='CD4 cnt';
				else if index("&test.",'vl') then lab_test='VL';

			run;

			Proc sql; /*...and keep only these new generically-named variables*/
				create table &test. as
					select stateno, lab_date, lab_test, lab_value
					from &test._2;
		%end;
	%mend;
	%reshape_lab; run;

	data all_labs;
		set 
			cd4_first_14
			cd4_first_200
			cd4_low_cnt
			cd4_low_pct
			cd4_recent_cnt
			cd4_recent_pct
			vl_first_det
			vl_recent
/*			vl_last_non_det*/
			cd4_first_hiv;

			where 	not missing(stateno) 
					and not missing(lab_date);
	run;

/*######################################################################################################################################################*/
/*De-duplicate on stateno and test date and type*/ data _null_;
/*######################################################################################################################################################*/

	proc sort data=all_labs out=labs_dedup nodupkey; 
		by stateno lab_date lab_test; 
	run;

/*##########################################################################################################################################################*/
/*Create numeric counterparts to dates and test result values stored in character variables*/
/*##########################################################################################################################################################*/

	data labs_recode(rename=(lab_value_num=lab_value));
		set labs_dedup;

		length lab_value_num 4.;
		label lab_value='lab_value';
		lab_value_num=input(lab_value,8.);

		%MDImp(lab_date);
		%DImp(lab_date);

/*		drop lab_value;*/
	run;

/*######################################################################################################################################################*/
/*Write dataset to disk*/ data _null_;
/*######################################################################################################################################################*/

/*	options replace;*/
	data lablib.all_DUA_labs(label='Q1_2011_to_Q3_2013'); set labs_recode; run;
	options noreplace;

/*######################################################################################################################################################*/
/*######################################################################################################################################################*/
/*######################################################################################################################################################*/

/*######################################################################################################################################################*/
/*Preliminary retention analyses*/ data _null_;
/*######################################################################################################################################################*/

	Proc sql;
		create table unique_dates as
			select stateno, lab_date_num_DImp, count(*) as numbr_tests
			from lablib.all_DUA_labs
			group by stateno, lab_date_num_DImp
			order by stateno, lab_date_num_DImp;

	/*------------------------------------------------------------------------------------------------------------------------------------------------------*/
	/*Number of lab visits per stateno*/
	/*------------------------------------------------------------------------------------------------------------------------------------------------------*/

		Proc sql;
			create table visit_count_per_stateno as
				select stateno, count(*) as visit_count
				from unique_dates
				group by stateno
				order by visit_count;

		proc univariate data=visit_count_per_stateno;
			var visit_count;
			hist visit_count;
		run;

	/*------------------------------------------------------------------------------------------------------------------------------------------------------*/
	/*Time between visits*/
	/*------------------------------------------------------------------------------------------------------------------------------------------------------*/

		data lab_lag;
			set unique_dates;
			by stateno;

			length last_lab_date_num_DImp 4.;
			label last_lab_date_num_DImp='Date of the last reported lab test';
			format last_lab_date_num_DImp date9.;

			length lab_lag 5.;
			label lab_lag='Days since the last reported lab test';

			last_lab_date_num_DImp=lag1(lab_date_num_DImp);

			if first.stateno then do;
				last_lab_date_num_DImp=.;
				lab_lag=.;
			end;

			lab_lag=lab_date_num_DImp-last_lab_date_num_DImp;

			length visit_seq 3;
			label visit_seq="Sequence number of the visit at which the lab test was ordered (relative the the first since 30MAR2012";
			retain visit_seq 0;
			if first.stateno then visit_seq = 0;
			visit_seq + 1;

		run;

/*	options replace;*/
	data lablib.all_DUA_lab_visits(label='Q2_2012_to_Q3_2013'); set lab_lag; run;
	options noreplace;