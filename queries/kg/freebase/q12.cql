MATCH (v0)-[:`file:/mnt/sdb/turbograph-s62/competitor-dataset/kg/freebase/freebase-easy-latest/source/Gender`]->(v1), (v1)-[:`file:/mnt/sdb/turbograph-s62/competitor-dataset/kg/freebase/freebase-easy-latest/source/Institution`]->(v2), (v1)-[:`file:/mnt/sdb/turbograph-s62/competitor-dataset/kg/freebase/freebase-easy-latest/source/Disease_with_this_Risk_Factor`]->(v3), (v0)-[:`file:/mnt/sdb/turbograph-s62/competitor-dataset/kg/freebase/freebase-easy-latest/source/TV_episode_performance`]->(v4), (v4)-[:`file:/mnt/sdb/turbograph-s62/competitor-dataset/kg/freebase/freebase-easy-latest/source/kg/object_profile/prominent_type`]->(v5) WHERE v4.`file:/mnt/sdb/turbograph-s62/competitor-dataset/kg/freebase/freebase-easy-latest/source/Tagline` IS NOT NULL AND v5.`file:/mnt/sdb/turbograph-s62/competitor-dataset/kg/freebase/freebase-easy-latest/source/prominence-score` IS NOT NULL RETURN v0.uri, v1.uri, v2.uri, v3.uri, v4.uri, v5.uri