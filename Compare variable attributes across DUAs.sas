/*%let year=2010;*/
/*%let qtr=4;*/

%macro compare_DUA_contents;
	%do year=2010 %to 2013;
		%do qtr=1 %to 4;
			%if &year.=2010 %then %let qtr=4; /*b/c first DUA in 2010 was the Q4*/

			%if &year.=2010 or &year.=2011 or &year.=2012 and &qtr.=1 %then %do; /*b/c we don't have SAS datasets for these DUAs, only Excel files which need to be imported*/
				proc import 
					datafile="&DUA_path.\&year.\Q&qtr.\Q&qtr._&year._berkeley_person.csv" 
					out=Q&qtr._&year._berkeley_person_import 
					DBMS=csv 
					replace; 
				run;

				data Q&qtr._&year._berkeley_person;
					set Q&qtr._&year._berkeley_person_import;
					if _N_=1 then delete; 
				run;

				proc sql;
					create table Q&qtr._&year._attr as
						select 	lowcase(name) as name, 
								type as type_Q&qtr._&year., 
								length as length_Q&qtr._&year.
						from dictionary.columns
						where libname="WORK"
							and memname="Q&qtr._&year._BERKELEY_PERSON"
						order by name;

			%end;

			%else %do;
				proc sql;
					create table Q&qtr._&year._attr as
						select lowcase(name) as name, 
								type as type_Q&qtr._&year., 
								length as length_Q&qtr._&year.
						from dictionary.columns
						where libname="Q&qtr._&year."
							and memname="Q&qtr._&year._BERKELEY_PERSON"
						order by name;
			%end;
		%end;
	%end;

	data all_DUAs_attr;
		merge %do year=2010 %to 2013;
				%do qtr=1 %to 4;
				%if &year.=2010 %then %let qtr=4;
				Q&qtr._&year._attr
				%end;
			%end;;
		by name;
	run;

	data DUA_root.all_DUAs_attr;
		set all_DUAs_attr;
		max_length=max(of length:);
	set;

%mend; %compare_DUA_contents; run;