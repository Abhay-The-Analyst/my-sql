select *
from layoffs ;

create table layoffs_staging
like layoffs;

insert  layoffs_staging
select *
from layoffs ;

select *, row_number() over(partition by company , location,industry ,total_laid_off , percentage_laid_off , `date` , 
stage , country ,funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte as
(select *, row_number() over(partition by company , location,industry ,total_laid_off , percentage_laid_off , `date` , 
stage , country ,funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num >1;

select * 
from layoffs_staging
where company = 'Cazoo';

CREATE TABLE `layoffs_staging1` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select  * 
from layoffs_staging1;

Insert into  layoffs_staging1
select * , row_number() over(partition by company , location,industry ,total_laid_off , percentage_laid_off , `date` , 
stage , country ,funds_raised_millions) as row_num
from layoffs_staging;


select *
from layoffs_staging1
where row_num>1;


delete 
from layoffs_staging1
where row_num >1;

--  Standerdizing Data
select *
from layoffs_staging1;

select company , trim(company)
from layoffs_staging1;

update layoffs_staging2
set company = trim(company);

Select distinct(industry)
from layoffs_staging1
order by 1;

update layoffs_staging1
set industry = "Crypto"
where industry like "Crypto%";

select *
from layoffs_staging1;

SELECT 
    *
FROM
    layoffs_staging1
WHERE
    industry IS NULL OR industry = ''
ORDER BY industry;

update layoffs_staging1
set industry = null
where industry = "";

select distinct(country)
from layoffs_staging1
order by 1;

Update layoffs_staging1
set country = 'United States'
where country like "United States%";

-- null values

select *
from layoffs_staging1;


SELECT 
    *
FROM
    layoffs_staging1
WHERE
    industry IS NULL OR industry = ''
ORDER BY industry;

select *
from layoffs_staging1
where company = 'Airbnb';


select t1.industry , t2.industry
from layoffs_staging1 t1
join layoffs_staging1 t2
	on t1.company =  t2.company
where (t1.industry is null) and t2.industry is not null;


update layoffs_staging1 t1
join layoffs_staging1 t2
	on t1.company =  t2.company
set t1.industry =  t2.industry
where (t1.industry is null) and t2.industry is not null;

select *
from layoffs_staging1
where total_laid_off is null  and percentage_laid_off is null;

delete
from  layoffs_staging1
where total_laid_off is null  and percentage_laid_off is null;

alter table layoffs_staging1
drop column row_num;

select *
from layoffs_staging1


