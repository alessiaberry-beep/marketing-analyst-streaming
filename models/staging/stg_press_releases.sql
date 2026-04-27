with source as (
    select * from {{ source('raw_scrape', 'raw_press_releases') }}
),

renamed as (
    select
        press_release_id,
        title,
        published_date::date          as published_date,
        url,
        summary,
        full_text,
        len(full_text)                as full_text_length,
        scraped_at,
        _loaded_at
    from source
    where press_release_id is not null
      and title is not null
)

select * from renamed
