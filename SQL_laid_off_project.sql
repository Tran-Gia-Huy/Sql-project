 -- data source: https://github.com/AlexTheAnalyst/MySQL-YouTube-Series/blob/main/layoffs.csv
 -- Data cleaning
 select * from layoffs;
 
     -- Create a new table for cleaning
 Create table layoffs_cleaning like layoffs;
 
     -- Insert data into the new database
 insert layoffs_cleaning select * from layoffs;
 
    -- Remove duplicate row
 with duplicate as(
 select *,row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as dup
 from layoffs_cleaning
 )
select * from duplicate;

CREATE TABLE `layoffs_cleaning_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    -- insert the duplicate columns to the data
insert into layoffs_cleaning_2 
 select *,row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as dup
 from layoffs_cleaning;
    -- delete duplicate rows
delete from layoffs_cleaning_2 where row_num>1;



-- Standardizing data
select * from layoffs_cleaning_2;
select distinct(company) from layoffs_cleaning_2;
    -- Remove extra space, Rename columns error or similar
update layoffs_cleaning_2
set company = trim(company);

select distinct(location) from layoffs_cleaning_2 order by 1;
select distinct(stage) from layoffs_cleaning_2 order by 1;
select distinct(industry) from layoffs_cleaning_2 order by 1;

select distinct(industry) from layoffs_cleaning_2 where industry like '%Crypto%';

update layoffs_cleaning_2 
set industry = 'Crypto'
where industry like '%Crypto%';

select distinct(country) from layoffs_cleaning_2 order by 1;
update layoffs_cleaning_2
set country = 'United States'
where country like 'United States%';

select * from layoffs_cleaning_2;
    -- Change date format
update layoffs_cleaning_2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_cleaning_2
modify column `date` date;

    -- Update/delete null and blank value
select * from layoffs_cleaning_2
where industry is null
or industry = '';

select * from layoffs_cleaning_2
where company ='Airbnb';

select * from
layoffs_cleaning_2 t1
join layoffs_cleaning_2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry ='')
and t2.industry is not null
and t2.industry <> '';

update layoffs_cleaning_2 t1
join layoffs_cleaning_2 t2
 on t1.company = t2.company
 set t1.industry = t2.industry
 where (t1.industry is null or t1.industry ='')
and t2.industry is not null
and t2.industry <> '';

select * from layoffs_cleaning_2
where industry is null or industry ='';

alter table layoffs_cleaning_2
drop column row_num;

delete from layoffs_cleaning_2
where total_laid_off is null
and percentage_laid_off is null;

-- Data Exploration
select min(`date`), max(`date`) 
from layoffs_cleaning_2;

select country, sum(total_laid_off)
from layoffs_cleaning_2
group by country
order by 2 desc;

select year(`date`) as `year`,month(`date`) as `month`, sum(total_laid_off) as total_laid_off
from layoffs_cleaning_2
group by 1,2
order by 1,2,3 desc;

select year(`date`),country, sum(total_laid_off) as total_laid_off
from layoffs_cleaning_2
group by 1,country
order by 1,3 desc;

select stage, sum(total_laid_off) as total_laid_off
from layoffs_cleaning_2
group by stage
order by 2 desc;

with laid_off_date as(
select year(`date`) as `year`,month(`date`) as `month`, sum(total_laid_off) as total_laid_off
from layoffs_cleaning_2
group by 1,2
order by 1,2,3 desc
)

select *,sum(total_laid_off) over (partition by year order by year, month) as Yearly_running_total_laid_off
from laid_off_date
group by year,month;

select year(`date`),company, sum(total_laid_off)
from layoffs_cleaning_2
group by 1,company
order by 2 asc, 3 desc;

