--generate aggregate table from posts table in redshift
create table fbid_aggregate as
 select fbid_user, count(*) as num_rows,count(distinct fbid_post) as num_posts, min(ts) as earliest_post,
 max(ts) as latest_post,
 count(distinct post_from) as num_posters,
 count(distinct case when fbid_user=post_from then fbid_post else null end) as num_self_posts,
 count(distinct case when type = 'photo' then fbid else 0 end) as num_post_photo,
  sum(case when type = 'link' then 1 else 0 end) as num_post_link,
 sum(case when type = 'status' then 1 else 0 end) as num_post_status,
 sum(case when type = 'checkin' then 1 else 0 end) as num_post_checkin,
 sum(case when type = 'video' then 1 else 0 end) as num_post_video
 from posts
 group by 1;
 
 
