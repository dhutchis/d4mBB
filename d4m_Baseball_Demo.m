%% Demonstrates D4M's capabilities on a small Baseball statistic data set by
% (1) Parsing data into a form ready for ingestion
% (2) Ingesting data into memory or an Accumulo Table
% (3) Querying data to answer several questions of interest

% User Parameters:
doDB = 0;   % Use an Accumulo Database instead of in-memory Associative Arrays
DB = DBserver('localhost:2181','Accumulo','accumulotraining', ...
    'root','password'); % configure this for your cluster
% assume the table names below

%% Part 1: Loading data set into memory / Accumulo
% First read the data in.  In the case of a larger data set that cannot fit
%  in memory, we would split the csv file into parts, load each part
%  separately, and ingest into Accumulo (sequentially or parallel)
% Note these files are modified from the original database:
%  -some irrelevant columns removed
%  -all internal commas removed
%  -weights and salaries zeropadded to constant width
Amall = ReadCSV('Master_mod.csv');
Asall = ReadCSV('Salaries_mod.csv');
% Only include columns of interest
Am = Amall(:,'playerID,birthYear,birthCountry,birthState,nameFirst,nameLast,weight,height,bats,');
As = Asall;
clear Amall Asall % save some memory

% Explode the salary schema
% Original RCV (Row Column Value) Salary Schema:
% 			col1	col2	col3
% 		ID	-val-	-val-	-val-
% Example:
% 			yearID	teamID	playerID	salary
% 		22	1985	BAL		sheetla01	000060000
% 		23	1985	BOS		stanlbo01	001075000
% New Desired Exploded Schema:
%			col1|val1	col1|val2	col2|val1 ...
%		ID	1			1			1		  ...
% Example:
% 			yearID|1985	teamID|BAL	teamID|BOS	playerID|sheetla01	playerID|stanlbo01	salary|000060000	salary|001075000
% 		22	1			1						1										1
% 		23	1						1								1										1
As_exp = val2col(As,'|');

% Now we'll do the same thing with the Master table, but let's replace the
% rowID with the playerName and restrict our analysis to just players (no managers)
As_pid = Am(:,'playerID,');                       % select row -> playerID
As_pid = val2col(As_pid, '|');                    % explode
As_not_pid = Am(:,'!,:,playerIC,playerIE,:,~,');  % select row -> every column except playerID
As_not_pid = val2col(As_not_pid, '|');            % explode
% now to create a matrix from playerID -> otherColumns
% do a sparse matrix multiplication (SQL programmers: think JOIN)
%      playerID -> rowID * rowID -> otherColumns
% take note of the transpose .' on As_pid
Am_exp = As_pid.' * As_not_pid;

% Form degree tables - tables with the count of how many times an exploded
% column appears in the main tables
Am_deg = putRow(sum(Am_exp,1), 'degree,').';
As_deg = putRow(sum(As_exp,1), 'degree,').';

%% Part 2: Ingest data into Accumulo
if doDB
    % insert to Accumulo
    Tm = DB('baseballMaster', 'baseballMasterT');
    Tmd = DB('baseballMasterDeg');
    Ts = DB('baseballSalaries', 'baseballSalariesT');
    Tsd = DB('baseballSalariesDeg');
    % For a fresh ingest, delete data in tables and recreate them
    Tm = deleteForce(Tm); Tmd = deleteForce(Tmd);
    Ts = deleteForce(Ts); Tsd = deleteForce(Tsd);
    Tm = DB('baseballMaster', 'baseballMasterT');
    Tmd = DB('baseballMasterDeg');
    Ts = DB('baseballSalaries', 'baseballSalariesT');
    Tsd = DB('baseballSalariesDeg');
    % Add combiner iterator on degree tables
    %   No use for this simple example, but powerful when ingesting
    %   in parallel or from multiple input files.
    % The sum combiner will do a server-side sum of every value added to a
    % unique Row-Column.
    Tmd = addColCombiner(Tmd, 'degree,', 'sum');
    Tsd = addColCombiner(Tsd, 'degree,', 'sum');
    
    % Ingest!
    % num2str is necessary because Accumulo stores everything as Strings
    Tm = put(Tm, num2str(Am_exp));
    Ts = put(Ts, num2str(As_exp));
    Tmd = put(Tmd, num2str(Am_deg));
    Tsd = put(Tsd, num2str(As_deg));
else
    % Just use the in-memory tables instead of Accumulo
    Tm = Am_exp; Tmd = Am_deg;
    Ts = As_exp; Tsd = As_deg;
end

%% Part 3: Querying the tables and performing interesting analytics

%% First, a collection of simple queries:
% Find all stored information about a specific player: zobribe01 (Ben Zobrist)
% the master table is easy since the rowID is the playerID
Tm('playerID|zobribe01,',:)
% Quering the salary table is a bit tougher since playerIDs are columns (exploded)
Az1 = Ts(:, 'playerID|zobribe01,'); % rowid -> playerID|zobribe01
Az2 = Ts(Row(Az1),:);   % same rowid -> all columns
Az1.' * Az2             % link playerID|zobribe01 -> all columns

% Return a list of all player IDs
[player_list, ~, ~] = Tsd(StartsWith('playerID|,'),:);
player_list = strrep(player_list, 'playerID|', '');
player_list


%% Return a table linking playerIDs to full names
% Construct triples (playerID, ~, firstname)
Afirst_c = Tm(:,StartsWith('nameFirst|,'));
Afirst_v = col2type(Afirst_c,'|');
% Construct triples (playerID, ~, lastname)
Alast_c = Tm(:,StartsWith('nameLast|,'));
Alast_v = col2type(Alast_c,'|');
% Combine with concatenation: (playerID, ~, firstname,lastname)
[r, c, v] = find(Afirst_v + Alast_v);
A = Assoc(r,'name,',v,@AssocCatStrFunc);


%% Find how many players weigh < 200 lb. and bat with left hand or both hands
% We will use the degree table to answer in an optimized fashion.
% First find out whether there are (A) fewer players that weigh < 200 lbs.
%   or (B) fewer players that bat with left hand or both hands
% Note that we need to use str2num to convert an Accumulo string to a number for summing
A = sum(str2num(Tmd('weight|000,:,weight|199,',:)),1); % 13182 weight < 200lb.
B = str2num(Tmd('bats|L,bats|B,',:));    % 4629 bat with left, 1106 both hands

% A < B, so we will first query for all the rows of players that weigh < 200 lb.
% Then, within those rows, we will find the players that bat L or B
A_light = Tm(:,'weight|000,:,weight|199,');
A_light_all = Tm(Row(A_light),:);
A_light_LB = A_light_all(:,'bats|L,bats|B,');
NumStr(Row(A_light_LB))                               % (Answer) 4463 players
% The query is optimized because going the other way around, querying for
%   LB batters and then refining to those weighing <200lb., would query from
%   the DB 7447 more rows than necessary.



%% Find the top 5 players with highest total salary -- with Iteration
% NOTE: This works with both doDB=1 (using Accumulo) and doDB=0 (using local Assoc)
% Let's not assume we can hold all the data in memory at once.
% Assume we can hold in memory the the salary sum for all players
%  + the entire salary history for up to 1000 players.
%  (Pretend if we load any more data we will run out of local memory.)
% Iterate over 1000 players at a time, gathering all the yearly salaries
% for each of the 1000 players and retain the cummulative sum of every player
itCount = 1000;
Titer = Iterator(Ts, 'elements', itCount);
Apsalsum = Assoc('','','');
plrow = '';

% First link each player to all the salary columns he's earned across his career
% The iterator will return 1000 rows at a time, since each row only
%  has a single playerID column: rowID -> playerID
Apl = Titer(:,StartsWith('playerID|,')); % row -> playerID

% While we have rows to iterate over
while nnz(Apl)
    % Get all info for for same rows from DB. We will use the salary part
    Asal = Ts(Row(Apl),:);
    % Link playerIDs -> salary
    Apsal = Apl.' * Asal(:,StartsWith('salary|,'));
    
    % Apsal contains some repeated salaries for players.  For example,
    %  if a player earned the same salaryA for two years in his career, he would
    %  have a value of 2 in the value of Apsal(player,salaryA)
    % We need to keep this frequency and multiply it with the corresponding salary
    [plid, sal, freq] = find(Apsal);
    % Let's get the salary values themselves from the column into the value
    [~, salval] = SplitStr(sal,'|');
    salnum = str2num(Str2mat(salval));
    
    % Now we can element-wise multiply the salary and frequency
    saltot = salnum .* freq;
    % Now reform the Assoc, using the sum collision function to combine different
    % salaries of different years (each already multiplied by their freq)
    Apsalsum_part = Assoc(plid, 'salary,', saltot, @sum); % answer part

    % We computed the portion of the answer corresponding to the players we
    % iterated over.  Bring this into the final result.
    Apsalsum = Apsalsum + Apsalsum_part;
    
    % Get next part of table from DB
    Apl = Titer();
end
%Apsalsum has the answer; links all players to their lifetime salaries
Atop = TopRowPerCol(Apsalsum, 5); % top 5 highest
Atop

% Here's some other nice correlations we can do between other variables and
% the career salary of the top 5 players with highest career salary
Am_exp(:,StartsWith('weight|,')).' * Atop % link weight to top earning players
Am_exp(:,StartsWith('height|,')).' * Atop % link height
Am_exp(:,StartsWith('bats|,')).' * Atop   % link bats
% This keeps the individual salaries in the values for examination
% FIXME: Make Matlab not use exponential notation here
CatValMul(num2str(Am_exp(:,StartsWith('bats|,')).'), num2str(Atop))

%% For archival, let's save our career player salary table to disk
Assoc2CSV(Apsalsum,char(10),',','Results_playerToSalarySum.csv');


%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M Baseball Demo Script
% Thanks to SeanLahman.com for the Baseball data set!
%   Data set provided under CC ShareAlike License:
%   http://creativecommons.org/licenses/by-sa/3.0/
% Created by Dylan Hutchison, June 2013
%