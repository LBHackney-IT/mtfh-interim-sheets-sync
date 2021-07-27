select distinct trim(property.prop_ref) as prop_ref,
                coalesce(trim(property.u_llpg_ref), '') property_llpg_ref,
                coalesce(trim(address1) + ', ' + trim(post_code), '') property_full_address,
                case coalesce(property_type_lookup.lu_desc, property.cat_type)
                    when 'Block' then 'Block'
                    when 'Concierge' then 'Concierge'
                    when 'Dwelling' then 'Dwelling'
                    when 'Lettable Non-Dwelling' then 'LettableNonDwelling'
                    when 'Medium Rise Block (3-5 storeys)' then 'MediumRiseBlock'
                    when 'NA' then 'NA'
                    when 'Traveller Site' then 'TravellerSite'
                    else coalesce(property_type_lookup.lu_desc, property.cat_type)
                end as asset_type
from [uhtlive].[dbo].[property]
left outer join(
    select distinct lu_ref, lu_desc
    from [uhtlive].[dbo].[lookup]
    where lu_type in ('UCT', 'PST')
) property_type_lookup on property_type_lookup.lu_ref = property.subtyp_code
;
