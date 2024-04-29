
use game_analysis;

-- Problem Statement - Game Analysis dataset
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.

alter table pd modify L1_Status varchar(30);
alter table pd modify L2_Status varchar(30);
alter table pd modify P_ID int primary key;
alter table pd drop myunknowncolumn;

alter table ld drop myunknowncolumn;
alter table ld change timestamp start_datetime datetime;
alter table ld modify Dev_Id varchar(10);
alter table ld modify Difficulty varchar(15);
alter table ld add primary key(P_ID,Dev_id,start_datetime);

-- pd (P_ID,PName,L1_status,L2_Status,L1_code,L2_Code)
-- ld (P_ID,Dev_ID,start_time,stages_crossed,level,difficulty,kill_count,
-- headshots_count,score,lives_earned)


-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players 
-- at level 0
select p.P_ID,l.Dev_ID,p.PName,l.Difficulty from game.player_details p join game.level_details l
on p.P_ID=l.P_ID where l.Level=0;
-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast
--    3 stages are crossed
select p.L1_Code,avg(l.Kill_Count) Kills from game.player_details p join game.level_details l
on p.P_ID=l.P_ID where l.Lives_Earned = 2 and l.Stages_crossed >= 3
group by p.L1_Code;
-- Q3) Find the total number of stages crossed at each diffuculty level
-- where for Level2 with players use zm_series devices. Arrange the result
-- in decsreasing order of total number of stages crossed.
select sum(l.Stages_crossed) Stages,l.Difficulty from game.player_details p join game.level_details l
on p.P_ID=l.P_ID where l.Level=2 and l.Dev_ID like "zm_%"
group by l.Level,l.Difficulty order by Stages desc;
-- Q4) Extract P_ID and the total number of unique dates for those players 
-- who have played games on multiple days.
select l.P_ID,count(date(l.start_datetime)) Total_Days from game.level_details l
group by l.P_ID having count(date(l.start_datetime)) > 1 order by Total_Days;
-- Q5) Find P_ID and level wise sum of kill_counts where kill_count
-- is greater than avg kill count for the Medium difficulty.
select l.P_ID,sum(l.Kill_Count) kills,l.Level from game.level_details l
where l.Difficulty="Medium"
group by l.P_ID,l.Level
having kills>avg(l.Kill_Count);
-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.
select l.Level,sum(l.Lives_Earned) Lives_Earned from game.player_details p join game.level_details l
on p.P_ID=l.P_ID
where l.Level != 0
group by l.Level,p.L1_Status,p.L2_Status 
order by l.Level;
-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well. 
WITH   cte
          AS ( SELECT    *,ROW_NUMBER() OVER ( PARTITION BY Dev_ID
          ORDER BY Score DESC) AS ROW_NUM
               FROM  game.level_details
             )
    SELECT  Dev_ID,Score,Difficulty
    FROM    cte
    where ROW_NUM < 4
    order by Dev_ID,ROW_NUM desc;
-- Q8) Find first_login datetime for each device id
select l1.Dev_ID,min(l1.start_datetime) first_login from game.level_details l1
group by l1.Dev_ID order by first_login;
-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.
WITH
    T AS (
        SELECT *,
            RANK() OVER (
                PARTITION BY Difficulty
                ORDER BY Score desc
            ) AS rk
        FROM game.level_details
    )
SELECT Dev_ID,Difficulty,Score,rk Ranks
FROM T
where rk<6
order by Difficulty,Ranks desc;
-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(p_id). Output should contain player id, device id and 
-- first login datetime.
WITH
    T AS (
        SELECT
            *,
            RANK() OVER (
                PARTITION BY P_ID
                ORDER BY start_datetime
            ) AS rk
        FROM game.level_details
    )
SELECT P_ID,Dev_ID,start_datetime
FROM T
WHERE rk = 1;
-- Q11) For each player and date, how many kill_count played so far by the player. 
-- That is, the total number of games played 
-- by the player until that date.
-- a) window function
-- b) without window function
select P_ID,date(start_datetime) Date,sum(Kill_count) Kills from game.level_details
group by P_ID,Date;
-- Q12) Find the cumulative sum of stages crossed over a start_datetime 
-- Q13) Find the cumulative sum of an stages crossed over a start_datetime 
-- for each player id but exclude the most recent start_datetime
WITH
    T AS (
        SELECT
            *,
            RANK() OVER (
                PARTITION BY P_ID
                ORDER BY date(start_datetime) desc
            ) AS rk
        FROM game.level_details
    )
SELECT P_ID,date(start_datetime),sum(Stages_Crossed) Stages
FROM T
WHERE rk != 1
group by P_ID,date(start_datetime);
-- Q14) Extract top 3 highest sum of score for each device id and the corresponding player_id
WITH   cte
          AS ( SELECT    *,ROW_NUMBER() OVER ( PARTITION BY Dev_ID
          ORDER BY Score DESC) AS Rank_Num
               FROM  game.level_details
             )
    SELECT  P_ID,Dev_ID,sum(Score),Rank_Num
    FROM    cte
    where Rank_Num<4
    group by Dev_ID,P_ID
    order by Dev_ID,P_ID,Rank_Num;
-- Q15) Find players who scored more than 50% of the avg score scored by sum of 
-- scores for each player_id
WITH   cte
          AS ( SELECT    *, avg(Score) over (partition by
          P_ID) Average
               FROM  game.level_details
             )
    SELECT  P_ID,Score
    FROM    cte
    where Score > (Average)/0.5
    group by P_ID,Score
    order by P_ID;
-- Q16) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well.
WITH   cte
          AS ( SELECT    *,ROW_NUMBER() OVER ( PARTITION BY Dev_ID
          ORDER BY Headshots_Count DESC) AS count_value
               FROM  game.level_details
             )
    SELECT  Dev_ID,count_value,Difficulty
    FROM    cte
    group by Dev_ID,Difficulty,count_value
    order by Dev_ID,count_value desc;
-- Q17) Create a function to return sum of Score for a given player_id.
DELIMITER //
CREATE FUNCTION TotalSum(PID int) RETURNS INT
    BEGIN
        DECLARE result INT;
            SET result = (SELECT sum(Score) FROM game.level_details where P_ID=PID);
        RETURN result;
    END //

DELIMITER ;
-- Calling the function 
SELECT TotalSum(211);