select distinct case when trim(u_saff_rentacc) = '1938001204' then '1938001203'
                     when trim(u_saff_rentacc) = '1935256106' then '1935256105'
                     else trim(u_saff_rentacc)
                end as u_saff_rentacc, trim(tag_ref) tag_ref
from [uhtlive].[dbo].[tenagree]
;
