/*######################################################################################################################################################*/
/*Look at the "life" of individual stateno*/
/*######################################################################################################################################################*/

	%macro stateno_merge(datasets);
		data stateno_merged; 
			set %scan(&datasets.,1).ALCO_%scan(&datasets.,1); 
			stateno_%scan(&datasets.,1)=stateno;
			keep stateno stateno_%scan(&datasets.,1);
		run;
		proc sort data=merged; by stateno; run;

		%do i=2 %to %sysfunc(countw("&datasets."));
			%let dataset=%scan(&datasets.,&i.);

			data &dataset.; 
				set &dataset..ALCO_&dataset.; 
				stateno_&dataset.=stateno;
				keep stateno stateno_&dataset.;
			run;

			proc sort data=&dataset.; by stateno; run;

			data merged;
				merge stateno_merged &dataset.(in=in);
				by stateno;
				if in then in&i.=1; else in&i.=0;
			run;
		%end;
	%mend;
	%stateno_merge(datasets=Q1_2011 Q2_2011 Q3_2011 Q4_2011 Q1_2012 Q2_2012 Q3_2012 Q4_2012 Q1_2013 Q2_2013 Q3_2013 Q4_2013); run;

	data stateno_merged_count;
		set stateno_merged;

		array statenos $	stateno_Q1_2011 stateno_Q2_2011 stateno_Q3_2011 stateno_Q4_2011 
							stateno_Q1_2012 stateno_Q2_2012 stateno_Q3_2012 stateno_Q4_2012 
							stateno_Q1_2013 stateno_Q2_2013 stateno_Q3_2013 stateno_Q4_2013;
		array inDUA $		in_1			in_2			in_3 			in_4 
							in_5			in_6			in_7 			in_8
							in_9			in_10			in_11 			in_12;

		do over statenos;
			if not missing(statenos) then inDUA=1;
				else inDUA=0;
		end;

		DUA_count=sum(of in_1-in_12);
		drop in_1-in_12;
		label DUA_count='number of DUAs in which the stateno appears';

		if not missing(stateno_Q4_2013) then persistent=1;
		else persistent=0;
		label persistent='stateno in consistent use since inception';
	run;

	proc freq data=dua_root.stateno_merged;
		table 	persistent 
			/ missing;
	run; /*97.04% of stateno persist from their inception; 406 stopped being used at some point 
	(i.e., merged with another)*/

/*######################################################################################################################################################*/
/*Save stateno dataset to DUA root directory for reference in longitudinal analyses*/
/*######################################################################################################################################################*/

	Proc sql;
		create table dua_root.stateno_merged as
			select stateno, persistent, DUA_count, *
			from stateno_merged_count
			where not missing(Stateno)
			order by stateno;

/*######################################################################################################################################################*/
/*Look for re-used stateno*/
/*######################################################################################################################################################*/

	Proc sql;
		create table stateno_gap as
			select *
			from dua_root.stateno_merged
			where DUA_count not in (1 12)
			order by DUA_count;
	/*quickly scanned 1,000 of all 3,000 candidate records and only 1 stateno--4271--appears to have been re-used*/