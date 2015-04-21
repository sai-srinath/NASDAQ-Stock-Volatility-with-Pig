 /*volcalc.pig*/

A = load 'hdfs:///pigdata/*.csv' USING PigStorage(',','-tagFile')  AS (Filename:chararray,Date:chararray,Open,High,Low,Close,Volume,AdjClose:double);

B = Filter A by Date!='Date';
C = FOREACH B GENERATE Filename,Date,AdjClose;
D = FOREACH C GENERATE $0,FLATTEN(ToDate($1)),$2;   
E =  FOREACH D GENERATE $0,FLATTEN(GetYear($1)),FLATTEN(GetMonth($1)),FLATTEN(GetDay($1)),$2;
F = GROUP E by ($0,$1,$2);
H = FOREACH F{  
H1 = ORDER E by $3;   
H2 = LIMIT H1 1;   
GENERATE FLATTEN(group),FLATTEN(H2.$3),FLATTEN(H2.$4);
 }; 
 I = FOREACH F{  
I1 = ORDER E by $3 DESC;   
I2 = LIMIT I1 1;   
GENERATE FLATTEN(group),FLATTEN(I2.$3),FLATTEN(I2.$4);
 };     
K = JOIN H by($0,$1,$2),I by ($0,$1,$2);
DIFF = FOREACH K GENERATE $0,(($9-$4)/$4);
grouped = group DIFF by $0;  
avg = foreach grouped generate FLATTEN(DIFF.$0),FLATTEN(DIFF.$1),FLATTEN(AVG(DIFF.$1));
avg = DISTINCT avg;
DIFF1 = foreach avg GENERATE $0,($1-$2)*($1-$2);
grouped = group DIFF1 by $0;   
volatility = foreach grouped generate FLATTEN(SQRT(SUM(DIFF1.$1)/(COUNT(DIFF1.$1)))),FLATTEN(DIFF1.$0);
final = DISTINCT volatility;
final_top_10 = ORDER final by $0 DESC; 
final_top_10 = LIMIT final_top_10 10;


final_bot_10 = ORDER final by $0; 
final_bot_10 = FILTER final_bot_10 by$0!=0;
final_bot_10 = LIMIT final_bot_10 10;
grand = UNION final_top_10, final_bot_10;
store grand into 'hdfs:///pigdata/hw3_out' using PigStorage(',');  
  

