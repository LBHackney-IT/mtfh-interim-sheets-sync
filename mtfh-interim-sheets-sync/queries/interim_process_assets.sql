with props as (
    select distinct coalesce(tenagree.prop_ref, trim(prop.prop_ref)) as prop_ref,
                    prop.major_ref,
                    subtyp_code,
                    post_preamble
    from [uhtlive].[dbo].[property] prop
    left outer join [uhtlive].[dbo].[tenagree] as tenagree on prop.prop_ref = tenagree.prop_ref
)
select distinct props.prop_ref as prop_ref,
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
join props on property.prop_ref = props.prop_ref
left outer join(
    select distinct lu_ref, lu_desc
    from [uhtlive].[dbo].[lookup]
    where lu_type in ('UCT', 'PST')
) property_type_lookup on property_type_lookup.lu_ref = property.subtyp_code
;
